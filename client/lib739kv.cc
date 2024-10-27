#include "lib739kv.h"
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <thread>
#include <algorithm>

#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using keyvaluestore::GetRequest;
using keyvaluestore::GetResponse;
using keyvaluestore::InitRequest;
using keyvaluestore::InitResponse;
using keyvaluestore::KeyValueStore;
using keyvaluestore::PutRequest;
using keyvaluestore::PutResponse;
using keyvaluestore::ShutdownRequest;
using keyvaluestore::ShutdownResponse;
using keyvaluestore::DieRequest;
using keyvaluestore::DieResponse;

// Global variables to hold the gRPC objects
std::map<int, std::shared_ptr<grpc::Channel>> channels_; // Channel for each Raft node
std::map<int, std::unique_ptr<KeyValueStore::Stub>> stubs_; // Stub for each Raft node

std::map<int, std::string> leader_addresses_;  // Maps partition IDs to current leader addresses
std::map<int, std::vector<std::string>> partition_instances_;  // Maps partition IDs to list of nodes

int nodes_per_partition = 5;
int num_partitions;

ConsistentHashing* ch;

std::vector<std::string> service_instances_;  // List of service instances (host:port)
const int max_retries = 3;

// Hash function to map keys to Raft partitions
int HashKey(const std::string &key) {
    std::hash<std::string> hasher;
    return hasher(key) % num_partitions;
}

bool ReadServiceInstancesFromFile(const std::string &file_name) {
    std::ifstream file(file_name);
    if (!file.is_open()) {
        std::cerr << "Error: Unable to open service instance file: " << file_name << std::endl;
        return false;
    }

    std::string instance;
    int current_partition = 0;
    int instance_count = 0;
    while (std::getline(file, instance)) {
        if (!instance.empty()) {
            service_instances_.push_back(instance);
            partition_instances_[current_partition].push_back(instance);
            instance_count++;

            // Move to the next partition after nodes_per_partition nodes
            if (instance_count % nodes_per_partition == 0) {
                current_partition++;
            }
        }
    }

    file.close();
    if (service_instances_.size() < num_partitions * nodes_per_partition) {
        std::cerr << "Error: Service instance file contains fewer instances than expected." << std::endl;
        return false;
    }
    return !service_instances_.empty();
}

// Helper function to handle retries within a partition
template <typename RequestType, typename ResponseType, typename FuncType>
Status RetryRequest(int partition_id, const RequestType& request, ResponseType* response, FuncType rpc_func) {
    int retries = 0;
    std::vector<std::string> tried_nodes;
    std::string current_leader = leader_addresses_[partition_id];  // Start with the known leader

    while (retries < max_retries) {
        // Update tried nodes and track the current leader
        tried_nodes.push_back(current_leader);

        ClientContext context;
        Status status = (stubs_[partition_id].get()->*rpc_func)(&context, request, response);

        
        if (status.ok() && !response->leader_server().empty()) {
            // Print the enitre response object for debugging
            // std::cout << "Response: Client side leader: " << response->leader_server() << std::endl;
            // std::cout << "Response: Success: " << response->success() << std::endl;
            // Check if we were redirected to a new leader
            if (response->leader_server() != current_leader) {
                // Update the leader to the new one
                current_leader = response->leader_server();
                leader_addresses_[partition_id] = current_leader;

                // Create new gRPC channel and stub for the new leader
                channels_[partition_id] = grpc::CreateChannel(current_leader, grpc::InsecureChannelCredentials());
                stubs_[partition_id] = KeyValueStore::NewStub(channels_[partition_id]);

                // std::cerr << "Redirected to new leader at " << current_leader << " for partition " << partition_id << std::endl;
                continue;  // Retry with the new leader
            }
            // std::cerr << "Request succeeded for partition " << partition_id << " with leader " << current_leader << std::endl;
            return status;  // Request succeeded
        } else {
            // std::cerr << "Failed to connect to " << current_leader << " for partition " << partition_id << std::endl;
        }

        // Find the next available node that has not yet been tried
        auto& nodes = partition_instances_[partition_id];
        auto it = std::find_if(nodes.begin(), nodes.end(), [&tried_nodes](const std::string& node) {
            return std::find(tried_nodes.begin(), tried_nodes.end(), node) == tried_nodes.end();
        });

        if (it != nodes.end()) {
            // Update to the next available node and retry
            current_leader = *it;
            leader_addresses_[partition_id] = current_leader;
            channels_[partition_id] = grpc::CreateChannel(current_leader, grpc::InsecureChannelCredentials());
            stubs_[partition_id] = KeyValueStore::NewStub(channels_[partition_id]);
            // std::cerr << "Retrying with new node at " << current_leader << " for partition " << partition_id << std::endl;
        } else {
            // No more nodes to try, fail the request
            std::cerr << "All nodes in partition " << partition_id << " have been tried. Request failed." << std::endl;
            break;
        }

        retries++;
    }

    return Status::CANCELLED;  // Return a failure status after exhausting retries
}

int kv739_init(const std::string &file_name) {
    if (!ReadServiceInstancesFromFile(file_name)) {
        return -1;
    }

    // initialize the num_partitions and the consistent hashing object
    num_partitions = partition_instances_.size();
    // initialize service instances to be the number of unique keys in the partition_instances_
    std::vector<std::string> keys(num_partitions);
    for (int i = 0; i < num_partitions; i++) {
        keys[i] = std::to_string(i);
    }
    ch = new ConsistentHashing(num_partitions, keys);
    int successful_init = 0;
    for (int partition_id = 0; partition_id < num_partitions; partition_id++) {
        leader_addresses_[partition_id] = partition_instances_[partition_id][0];  // Assume first node in partition group is the leader
        std::string leader_address = leader_addresses_[partition_id];

        // Create gRPC channel and stub for the leader of this partition
        channels_[partition_id] = grpc::CreateChannel(leader_address, grpc::InsecureChannelCredentials());
        stubs_[partition_id] = KeyValueStore::NewStub(channels_[partition_id]);

        // Make an InitRequest
        InitRequest init_request;
        init_request.set_server_name(leader_address);

        InitResponse init_response;
        ClientContext context;

        Status status = RetryRequest(partition_id, init_request, &init_response, &KeyValueStore::Stub::Init);
        if (status.ok()) {
            if (init_response.success()) {
                successful_init++;
            } else {
                return -1;
            }
        }
    }

    if (successful_init == num_partitions) {
        return 0;
    }

    return -1;
}

int kv739_shutdown() {
    int shutdown_success = 0;
    for (int partition_id = 0; partition_id < num_partitions; partition_id++) {
        ClientContext context;
        ShutdownRequest request;
        ShutdownResponse response;

        Status status = stubs_[partition_id]->Shutdown(&context, request, &response);
        if (status.ok() && response.success()) {
            shutdown_success++;
        } else {
            std::cerr << "Error: Failed to receive shutdown response from server " << std::endl;
        }

        // Clean up connection
        if (stubs_.find(partition_id) != stubs_.end()) {
            stubs_.erase(partition_id);
            channels_.erase(partition_id);
        }
    }
    if (shutdown_success == num_partitions) {
        return 0;
    }
    return -1;
}

int kv739_get(const std::string &key, std::string &value) {
    // Determine partition based on consistent hashing
    // it will return string so we need to convert it to int
    int partition_id = std::stoi(ch->GetPartition(key));

    if (stubs_.find(partition_id) == stubs_.end()) {
        std::cerr << "Error: Client not initialized. Call kv739_init() first." << std::endl;
        return -1;
    }

    ClientContext context;
    GetRequest get_request;
    get_request.set_key(key);
    GetResponse get_response;

    Status status = RetryRequest(partition_id, get_request, &get_response, &KeyValueStore::Stub::Get);
    if (status.ok()) {
        if (get_response.key_found()) {
            value = get_response.value();
            // std::cout << "Get operation successful. Key: '" << key << "', Value: '" << value << "'." << std::endl;
            return 0;
        } else {
            // std::cout << "Key '" << key << "' not found." << std::endl;
            return 1;  // Key not found
        } 
    }

    // std::cerr << "Error: Get operation failed for key: '" << key << "'." << std::endl;
    return -1;
}

int kv739_put(const std::string &key, const std::string &value, std::string &old_value) {
    // Determine partition based on consistent hashing
    int partition_id = std::stoi(ch->GetPartition(key));

    if (stubs_.find(partition_id) == stubs_.end()) {
        std::cerr << "Error: Client not initialized. Call kv739_init() first." << std::endl;
        return -1;
    }

    ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(10000));
    PutRequest put_request;
    put_request.set_key(key);
    put_request.set_value(value);
    PutResponse put_response;

    Status status = RetryRequest(partition_id, put_request, &put_response, &KeyValueStore::Stub::Put);
    if (status.ok()) {
        if (put_response.key_found()) {
            old_value = put_response.old_value();
            // std::cout << "Put operation successful. Old value for key: '" << key << "' was: '" << old_value << "'." << std::endl;
            return 0;
        } else {
            // std::cout << "Put operation successful. No old value existed for key: '" << key << "'." << std::endl;
            return 1;
        }
    }

    std::cerr << "Error: Put operation failed for key: '" << key << "'." << std::endl;
    return -1;
}

// Function to trim leading and trailing whitespace in place
void trim(std::string &str) {
    // Remove leading whitespace
    str.erase(str.begin(), std::find_if(str.begin(), str.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));

    // Remove trailing whitespace
    str.erase(std::find_if(str.rbegin(), str.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), str.end());
}


int kv739_die(const std::string &server_name, int clean) {
    std::string server_addr = server_name;
    trim(server_addr);
    // Find the server based on the server name (which is the server address)
    int server_id = -1;
    for (int i = 0; i < service_instances_.size(); i++) {
        trim(service_instances_[i]);
        if (service_instances_[i] == server_addr) {
            server_id = i;
            break;
        }
    }

    if (server_id == -1) {
        std::cerr << "Error: Could not find the server in service instances." << std::endl;
        return -1;
    }

    // Create a new gRPC channel and stub for the specific server
    auto channel = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
    auto server_stub = KeyValueStore::NewStub(channel);  // Create a stub specifically for the server

    // Prepare the Die request
    ClientContext context;
    DieRequest die_request;
    die_request.set_server_name(server_addr);  // Server name is the same as the server address
    die_request.set_clean(clean == 1);  // Set clean flag: true for clean shutdown, false for abrupt exit

    DieResponse die_response;

    // Send the Die request to the specific server
    Status status = server_stub->Die(&context, die_request, &die_response);

    // Check if the gRPC call was successful
    if (!status.ok()) {
        std::cerr << "gRPC Die failed: " << status.error_message() << std::endl;
        return -1;
    }

    // Check if the server successfully initiated termination
    if (die_response.success()) {
        // std::cout << "Server '" << server_addr << "' successfully initiated termination." << std::endl;
        return 0;
    } else {
        // std::cerr << "Error: Server failed to initiate termination." << std::endl;
        return -1;
    }
}
