#include "lib739kv.h"
#include <iostream>
#include <thread>
#include <random>
#include <vector>
#include <string>
#include <utility>
#include <fstream>
#include <chrono>
#include <algorithm>
#include <cctype>
#include <locale>

#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using raft_group::GetRequest;
using raft_group::GetResponse;
using raft_group::InitRequest;
using raft_group::InitResponse;
using raft_group::PutRequest;
using raft_group::PutResponse;
using raft_group::ShutdownRequest;
using raft_group::ShutdownResponse;
using raft_group::KeyValueStore;

// Global variables to hold the gRPC objects
std::map<int, std::shared_ptr<grpc::Channel>> channels_;
std::map<int, std::unique_ptr<KeyValueStore::Stub>> stubs_;

std::map<int, std::string> leader_addresses_;
std::map<int, std::vector<std::string>> partition_instances_;

int NUM_PARTITIONS = 20;
const int NODES_PER_PARTITION = 5;

// Global variables to store server names
std::vector<std::string> service_instances_;

int HashKey(const std::string &key){
    std::hash<std::string> hash_fn;
    return hash_fn(key) % NUM_PARTITIONS;
}

// Channel arguments for automatic reconnection and keepalive
grpc::ChannelArguments GetChannelArguments()
{
    grpc::ChannelArguments args;
    // Set reconnection policies
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 1000); // Initial backoff 1 second
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 20000);    // Max backoff 20 seconds
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);           // Keepalive ping every 10 seconds
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);         // Timeout after 5 seconds if no response
    return args;
}

// Utility function to trim strings
static inline std::string trim(const std::string &s) {
    auto start = s.begin();
    while (start != s.end() && std::isspace(*start)) {
        start++;
    }

    auto end = s.end();
    do {
        end--;
    } while (std::distance(start, end) > 0 && std::isspace(*end));

    return std::string(start, end + 1);
}

// Function to read the configuration file with host addresses
std::vector<std::string> read_config_file(const std::string &config_file)
{
    std::ifstream file(config_file);
    std::vector<std::string> servers;

    if(!file.is_open()){
        std::cerr << "Error: Unable to open config file: " << config_file << std::endl;
        return servers;
    }
    std::string line;
    while(std::getline(file, line))
    {
        line = trim(line);
        if(!line.empty())
        {
            servers.push_back(line);
        }
    }

    if (servers.empty()) {
        std::cerr << "Error: No hosts found in the config file." << std::endl;
    }
    file.close();
    return servers;
}

int kv739_init(const std::string &file_name) {
    // Ensure streams are not already initialized
    for (int i = 0; i < NUM_PARTITIONS; i++) {
        if (stubs_[i]) {
            std::cerr << "Error: Client is already initialized for partition " << i << std::endl;
            return -1;
        }
    }

    // Read the list of service instances from the file and map them to partitions
    service_instances_ = read_config_file(file_name);
    NUM_PARTITIONS = service_instances_.size() / NODES_PER_PARTITION;

    if (service_instances_.empty() || NUM_PARTITIONS == 0) {
        std::cerr << "Error: Failed to read service instances from file." << std::endl;
        return -1;
    }

    // Initialize partition_instances_ based on NUM_PARTITIONS and NODES_PER_PARTITION
    for (int i = 0; i < NUM_PARTITIONS; i++) {
        for (int j = 0; j < NODES_PER_PARTITION; j++) {
            int instance_idx = i * NODES_PER_PARTITION + j;
            if (instance_idx < service_instances_.size()) {
                partition_instances_[i].push_back(service_instances_[instance_idx]);
            }
        }
    }

    // Initialize stubs for each service instance
    for (int i = 0; i < service_instances_.size(); i++) {
        std::string server_address = service_instances_[i];
        auto channel = grpc::CreateCustomChannel(server_address, grpc::InsecureChannelCredentials(), GetChannelArguments());
        stubs_[i] = KeyValueStore::NewStub(channel);
    }

    // Initialize connection by selecting the first node of each partition
    for (int i = 0; i < NUM_PARTITIONS; i++) {
        leader_addresses_[i] = partition_instances_[i][0];  // Assume first node in partition group is the leader
        std::string leader_address = leader_addresses_[i];

        // Find the leader id based on the leader address
        int leader_id = -1;
        for (int j = 0; j < service_instances_.size(); j++) {
            if (service_instances_[j] == leader_address) {
                leader_id = j;
                break;
            }
        }

        if (leader_id == -1) {
            std::cerr << "Error: Could not find the leader in service instances." << std::endl;
            return -1;
        }

        // Get the stub for the leader
        auto leader_stub = stubs_[leader_id].get();  // Use .get() to get the raw pointer

        // Prepare the Init request
        ClientContext context;
        InitRequest init_request;
        init_request.set_server_name(leader_address);
        InitResponse init_response;

        // Send the Init request to the leader
        Status status = leader_stub->Init(&context, init_request, &init_response);

        // Check if the gRPC call was successful
        if (!status.ok()) {
            std::cerr << "gRPC Init failed: " << status.error_message() << std::endl;
            return -1;
        }

        // If the request returns a leader redirection
        if (init_response.leader_server() != leader_address) {
            std::cout << "Leader redirection detected during Init. Updating leader info for partition " << i << "." << std::endl;

            // Update the leader address for the partition
            leader_addresses_[i] = init_response.leader_server();

            // Retry the request with the new leader
            leader_address = init_response.leader_server();

            // Find the new leader id based on the updated leader address
            for (int j = 0; j < service_instances_.size(); j++) {
                if (service_instances_[j] == leader_address) {
                    leader_id = j;
                    break;
                }
            }

            if (leader_id == -1) {
                std::cerr << "Error: Could not find the new leader in service instances." << std::endl;
                return -1;
            }

            leader_stub = stubs_[leader_id].get();  // Use .get() to get the raw pointer

            // Retry Init request with the new leader
            ClientContext new_context;
            Status retry_status = leader_stub->Init(&new_context, init_request, &init_response);

            if (!retry_status.ok()) {
                std::cerr << "gRPC Init retry failed: " << retry_status.error_message() << std::endl;
                return -1;
            }
        }

        if (!init_response.success()) {
            std::cerr << "Init operation failed for partition " << i << "." << std::endl;
            return -1;
        }

        std::cout << "Init operation successful for partition " << i << "." << std::endl;
    }

    return 0;
}

int kv739_shutdown()
{
    for(int i = 0; i < service_instances_.size(); i++){
        ClientContext context;
        ShutdownRequest request;
        ShutdownResponse response;
        Status status = stubs_[i]->Shutdown(&context, request, &response);
        if(!status.ok()){
            std::cerr << "Error: Shutdown failed for server: " << service_instances_[i] << std::endl;
            return -1;
        }
    }
    return 0;
}

int kv739_get(const std::string &key, std::string &value) {
    // Hash the key to determine the partition
    int partition = HashKey(key);

    // print partition and partition leader address
    std::cout << "Partition: " << partition << " with leader address " << leader_addresses_[partition] << std::endl;

    // Get the leader address for the partition
    std::string leader_address = leader_addresses_[partition];

    // Get the leader id based on the leader address
    int leader_id = -1;
    for (int i = 0; i < service_instances_.size(); i++) {
        if (service_instances_[i] == leader_address) {
            leader_id = i;
            break;
        }
    }

    if (leader_id == -1) {
        std::cerr << "Error: Could not find the leader in service instances." << std::endl;
        return -1;
    }

    // Get the stub for the leader
    auto* leader_stub = stubs_[leader_id].get();  // Use .get() to get the raw pointer

    // Send the Get request to the leader
    ClientContext context;
    GetRequest get_request;
    get_request.set_key(key);
    GetResponse get_response;
    Status status = leader_stub->Get(&context, get_request, &get_response);

    // Check if the gRPC call was successful
    if (!status.ok()) {
        std::cerr << "gRPC Get failed: " << status.error_message() << std::endl;
        return -1;
    }

    // If the request returns a leader redirection
    if (get_response.leader_server() != leader_address) {
        std::cout << "Leader redirection detected. Updating leader info." << std::endl;

        // Update the leader address for the partition
        leader_addresses_[partition] = get_response.leader_server();

        // Retry the request with the new leader
        leader_address = get_response.leader_server();

        // Find the new leader id based on the updated leader address
        for (int i = 0; i < service_instances_.size(); i++) {
            if (service_instances_[i] == leader_address) {
                leader_id = i;
                break;
            }
        }

        if (leader_id == -1) {
            std::cerr << "Error: Could not find the new leader in service instances." << std::endl;
            return -1;
        }

        leader_stub = stubs_[leader_id].get();  // Use .get() to get the raw pointer

        // Retry Get request with the new leader
        ClientContext new_context;
        Status retry_status = leader_stub->Get(&new_context, get_request, &get_response);

        if (!retry_status.ok()) {
            std::cerr << "gRPC Get retry failed: " << retry_status.error_message() << std::endl;
            return -1;
        }
    }

    // If key is found, return the value
    if (get_response.key_found()) {
        value = get_response.value();
        std::cout << "Get operation successful. Key: '" << key << "', Value: '" << value << "'." << std::endl;
        return 0;
    } else {
        std::cout << "Key '" << key << "' not found." << std::endl;
        return 1;  // Key not found
    }

    std::cerr << "Error: Get operation failed for key: '" << key << "'." << std::endl;
    return -1;
}

int kv739_put(const std::string &key, const std::string &value, std::string &old_value) {
    // Hash the key to determine the partition
    int partition = HashKey(key);
    std::cout << "Partition: " << partition << " with leader address " << leader_addresses_[partition] << std::endl;

    // Get the leader address for the partition
    std::string leader_address = leader_addresses_[partition];

    // Get the leader id based on the leader address
    int leader_id = -1;
    for (int i = 0; i < service_instances_.size(); i++) {
        if (service_instances_[i] == leader_address) {
            leader_id = i;
            break;
        }
    }

    if (leader_id == -1) {
        std::cerr << "Error: Could not find the leader in service instances." << std::endl;
        return -1;
    }

    // Get the stub for the leader
    auto* leader_stub = stubs_[leader_id].get();  // Use .get() to get the raw pointer

    // Prepare the Put request
    ClientContext context;
    PutRequest put_request;
    put_request.set_key(key);
    put_request.set_value(value);
    PutResponse put_response;

    // Send the Put request to the leader
    Status status = leader_stub->Put(&context, put_request, &put_response);

    // Check if the gRPC call was successful
    if (!status.ok()) {
        std::cerr << "gRPC Put failed: " << status.error_message() << std::endl;
        return -1;
    }

    // If the request returns a leader redirection
    if (put_response.leader_server() != leader_address) {
        std::cout << "Leader redirection detected. Updating leader info." << std::endl;

        // Update the leader address for the partition
        leader_addresses_[partition] = put_response.leader_server();

        // Retry the request with the new leader
        leader_address = put_response.leader_server();

        // Find the new leader id based on the updated leader address
        for (int i = 0; i < service_instances_.size(); i++) {
            if (service_instances_[i] == leader_address) {
                leader_id = i;
                break;
            }
        }

        if (leader_id == -1) {
            std::cerr << "Error: Could not find the new leader in service instances." << std::endl;
            return -1;
        }

        leader_stub = stubs_[leader_id].get();  // Use .get() to get the raw pointer

        // Retry Put request with the new leader
        ClientContext new_context;
        Status retry_status = leader_stub->Put(&new_context, put_request, &put_response);

        if (!retry_status.ok()) {
            std::cerr << "gRPC Put retry failed: " << retry_status.error_message() << std::endl;
            return -1;
        }
    }

    // If the key was found, return the old value
    if (put_response.key_found()) {
        old_value = put_response.old_value();
        std::cout << "Put operation successful. Old value for key: '" << key << "' was: '" << old_value << "'." << std::endl;
        return 0;
    } else {
        std::cout << "Put operation successful. No old value existed for key: '" << key << "'." << std::endl;
        return 1; // No old value
    }

    std::cerr << "Error: Put operation failed for key: '" << key << "'." << std::endl;
    return -1;
}