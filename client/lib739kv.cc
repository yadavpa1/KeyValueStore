#include "lib739kv.h"
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <thread>
#include <algorithm>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

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
using keyvaluestore::StartRequest;
using keyvaluestore::StartResponse;
using keyvaluestore::LeaveRequest;
using keyvaluestore::LeaveResponse;
using keyvaluestore::PartitionChangeRequest;
using keyvaluestore::PartitionChangeResponse;
using keyvaluestore::NewRaftGroupRequest;
using keyvaluestore::NewRaftGroupResponse;

// Global variables to hold the gRPC objects
std::map<int, std::shared_ptr<grpc::Channel>> channels_; // Channel for each Raft node
std::map<int, std::unique_ptr<KeyValueStore::Stub>> stubs_; // Stub for each Raft node

const int nodes_per_partition = 5;  // Number of nodes per partition
const int num_replicas = 3;  // Number of replicas per partition
const int min_nodes = 3;

const int max_retries = 3;

// Maintain a last_modified timestamp for the configuration file
// set it to null initially
std::string config_file = "";
std::string last_modified = "";

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
                std::cout << "Redirected to new leader at " << current_leader << " for partition " << partition_id << std::endl;
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

int establishStub(int partition_id){
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
            std::cout << "Successfully initialized connection to leader at " << leader_addresses_[partition_id] << " for partition " << partition_id << std::endl;
        } else {
            std::cerr << "Failed to initialize connection to leader at " << leader_addresses_[partition_id] << " for partition " << partition_id << std::endl;
            return -1;
        }
    }
    return 0;
}

bool ReadServiceInstancesFromFile(const std::string &file_name) {
    int fd = open(file_name.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Error: Unable to open service instance file: " << file_name << std::endl;
        return false;
    }
    // Apply a shared lock for reading
    if(flock(fd, LOCK_SH) == -1) {
        std::cerr << "Error: Unable to acquire shared lock on service instance file: " << file_name << std::endl;
        return false;
    }

    // Now safely read the file
    std::ifstream file(file_name);

    std::string instance;
    int current_partition = 0;
    int instance_count = 0;
    
    partition_instances_.clear(); // Clear the partition instances to avoid duplicates
    service_instances_.clear();  // Clear the service instances to avoid duplicates

    while (std::getline(file, instance)) {
        if (!instance.empty()) {
            // if the instance is set to "NULL", skip it
            if(instance != "NULL"){
                service_instances_.push_back(instance);
                partition_instances_[current_partition].push_back(instance);
            }
            instance_count++;

            // Move to the next partition after nodes_per_partition nodes
            if (instance_count % nodes_per_partition == 0) {
                current_partition++;
            }
        }
    }

    // Set the last modified timestamp to the last modified time of the file
    struct stat file_stat;
    if (fstat(fd, &file_stat) == 0) {
        last_modified = std::to_string(file_stat.st_mtime);
    }

    flock(fd, LOCK_UN);  // Release the shared lock
    close(fd);
    num_partitions = partition_instances_.size();

    if(partition_instances_[num_partitions - 1].size() < min_nodes){
        free_hanging_nodes.clear();
        for(const auto& node : partition_instances_[num_partitions - 1]){
            free_hanging_nodes.push_back(node);
        }
        num_partitions--;
    }

    std::vector<std::string> nodes(num_partitions);
    for(int i = 0; i < num_partitions; i++){
        nodes[i] = std::to_string(i);
    }
    ch = new ConsistentHashing(num_replicas, nodes);
    // Check if stubs and channels need to be established for any partition
    // Don't establish stubs for free hanging nodes
    // Don't establish stubs if they are already established
    
    for (int partition_id = 0; partition_id < num_partitions; ++partition_id) {
        if (stubs_.find(partition_id) == stubs_.end()) {
            if(establishStub(partition_id) == -1){
                return false;
            }
        }
    }
    return !service_instances_.empty();
}

bool UpdateServiceInstancesInFile(const std::string &file_name, const std::string old_server, const std::string new_server, int offset){
    int start_line = offset * nodes_per_partition;
    int fd = open(file_name.c_str(), O_RDWR);

    if (fd == -1) {
        std::cerr << "Error: Unable to open service instance file: " << file_name << std::endl;
        return false;
    }

    // Apply an exclusive lock for writing
    if(flock(fd, LOCK_EX) == -1) {
        std::cerr << "Error: Unable to acquire exclusive lock on service instance file: " << file_name << std::endl;
        return false;
    }

    // Now start reading the file from line number offset * nodes_per_partition+1
    // and update the line with old_server to new_server
    std::ifstream file(file_name);
    std::string line;
    std::vector<std::string> lines;
    int line_number = 0;
    while (std::getline(file, line)) {
        if (line_number >= offset * nodes_per_partition && line_number < (offset + 1) * nodes_per_partition) {
            trim(line);
            if(line == old_server){
                line = new_server;
            }
        }
        lines.push_back(line);
        line_number++;
    }
    file.close();

    std::ofstream output_file(file_name, std::ios::trunc);
    if(!output_file.is_open()){
        std::cerr << "Error: Unable to open service instance file: " << file_name << std::endl;
        flock(fd, LOCK_UN);  // Release the exclusive lock
        close(fd);
        return false;
    }
    for(const auto& line : lines){
        output_file << line << std::endl;
    }

    flock(fd, LOCK_UN);  // Release the exclusive lock
    close(fd);
    return true;
}

bool AppendInstanceToFile(int fd, const std::string &new_server) {
    // Ensure the file is open
    if(fd == -1){
        std::cerr << "Error: Invalid file descriptor for service instance file" << std::endl;
        return false;
    }
    // Use the open file descriptor to create an `std::ofstream` in append mode
    std::ofstream output_file;
    output_file.rdbuf()->pubsetbuf(nullptr, 0);  // Disable buffering
    output_file.open("/proc/self/fd/" + std::to_string(fd), std::ios::app);

    if (!output_file.is_open()) {
        std::cerr << "Error: Unable to open service instance file"<< std::endl;
        return false;
    }

    output_file << new_server << std::endl;
    output_file.close();
    return true;
}

int kv739_init(const std::string &file_name) {
    config_file = file_name;
    if (!ReadServiceInstancesFromFile(file_name)) {
        return -1;
    }

    return 0;
}

int kv739_shutdown() {
    for (int partition_id = 0; partition_id < num_partitions; partition_id++) {
        ClientContext context;
        ShutdownRequest request;
        ShutdownResponse response;

        Status status = stubs_[partition_id]->Shutdown(&context, request, &response);
        if (status.ok() && response.success()) {
            std::cout << "Shutdown successful for Raft leader of partition " << partition_id << std::endl;
        } else {
            std::cerr << "Error: Failed to receive shutdown response from partition " << partition_id << std::endl;
        }

        // Clean up connection
        if (stubs_.find(partition_id) != stubs_.end()) {
            stubs_.erase(partition_id);
            channels_.erase(partition_id);
        }
    }
    return 0;
}

int kv739_get(const std::string &key, std::string &value) {
    struct stat file_stat;
    if (stat(config_file.c_str(), &file_stat) == 0) {
        if (std::to_string(file_stat.st_mtime) != last_modified) {
            if (!ReadServiceInstancesFromFile(config_file)) {
                return -1;
            }
        }
    }
    int partition_id = std::stoi(ch->GetPartition(key));  // Determine partition

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
            return 0;
        } else {
            return 1;  // Key not found
        } 
    }

    return -1;
}

int kv739_put(const std::string &key, const std::string &value, std::string &old_value) {
    struct stat file_stat;
    if (stat(config_file.c_str(), &file_stat) == 0) {
        if (std::to_string(file_stat.st_mtime) != last_modified) {
            if (!ReadServiceInstancesFromFile(config_file)) {
                return -1;
            }
        }
    }
    int partition_id = std::stoi(ch->GetPartition(key));  // Determine partition

    if (stubs_.find(partition_id) == stubs_.end()) {
        std::cerr << "Error: Client not initialized. Call kv739_init() first." << std::endl;
        return -1;
    }

    ClientContext context;
    PutRequest put_request;
    put_request.set_key(key);
    put_request.set_value(value);
    PutResponse put_response;

    Status status = RetryRequest(partition_id, put_request, &put_response, &KeyValueStore::Stub::Put);
    if (status.ok()) {
        if (put_response.key_found()) {
            old_value = put_response.old_value();
            std::cout << "Put operation successful. Old value for key: '" << key << "' was: '" << old_value << "'." << std::endl;
            return 0;
        } else {
            std::cout << "Put operation successful. No old value existed for key: '" << key << "'." << std::endl;
            return 1;
        }
    }

    std::cerr << "Error: Put operation failed for key: '" << key << "'." << std::endl;
    return -1;
}


int kv739_die(const std::string &server_name, int clean) {
    // Read the config file to check if it has been modified
    struct stat file_stat;
    if (stat(config_file.c_str(), &file_stat) == 0) {
        if (std::to_string(file_stat.st_mtime) != last_modified) {
            if (!ReadServiceInstancesFromFile(config_file)) {
                return -1;
            }
        }
    }

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
        // Update the config file to remove the server instance
        if (!UpdateServiceInstancesInFile(config_file, server_addr, "NULL", server_id / nodes_per_partition)) {
            std::cerr << "Error: Failed to update service instances in file." << std::endl;
            return -1;
        }
        std::cout << "Server '" << server_addr << "' successfully initiated termination." << std::endl;
        return 0;
    } else {
        std::cerr << "Error: Server failed to initiate termination." << std::endl;
        return -1;
    } 
}

//This logic can be updated to find the best group among multiple possible ones.
int FindPartition() {
    for (int partition_id = 0; partition_id < num_partitions; partition_id++) {
        if (partition_instances_[partition_id].size() < nodes_per_partition && partition_instances_[partition_id].size() > 2) {
            return partition_id;
        }
    }
    return -1;
}

// Function to transfer keys to the new partition
bool TransferKeysToNewPartition(const PartitionChangeResponse& partition_change_response, int new_partition_id) {
    // Get the leader of the new partition
    std::string new_partition_leader = leader_addresses_[new_partition_id];
    auto new_leader_stub = KeyValueStore::NewStub(grpc::CreateChannel(new_partition_leader, grpc::InsecureChannelCredentials()));

    // Iterate over each key-value pair in the response
    for (const auto& key_value : partition_change_response.key_values()) {
        ClientContext context;
        PutRequest put_request;
        PutResponse put_response;

        // Set key and value in the Put request
        put_request.set_key(key_value.key());
        put_request.set_value(key_value.value());

        // Send the Put request to the new partition's leader
        Status status = new_leader_stub->Put(&context, put_request, &put_response);
        
        // Check if the Put operation was successful
        if (!status.ok() || !put_response.key_found()) {
            std::cerr << "Error transferring key: " << key_value.key() << " to new partition " << new_partition_id << std::endl;
            return false;
        }
    }

    std::cout << "All keys transferred successfully to new partition " << new_partition_id << std::endl;
    return true;
}

bool NotifyNewRaftGroup(int partition_id, const std::vector<std::string>& server_instances) {
    NewRaftGroupRequest request;
    for (const auto& instance : server_instances) {
        request.add_server_instances(instance);
    }

    NewRaftGroupResponse response;

    // Notify each server in the new partition by establishing a new channel and retrying if needed
    for (const auto& instance : server_instances) {
        request.set_server_name(instance);

        // Create a unique channel and stub for each server instance
        auto channel = grpc::CreateChannel(instance, grpc::InsecureChannelCredentials());
        auto stub = KeyValueStore::NewStub(channel);

        // Retry sending NotifyNewRaftGroup request
        Status status = RetryRequest(partition_id, request, &response, &KeyValueStore::Stub::NotifyNewRaftGroup);
        if (!status.ok() || !response.success()) {
            std::cerr << "Failed to notify server " << instance << " about new Raft group " << partition_id 
                      << " with error: " << status.error_message() << std::endl;
            return false;
        }
        std::cout << "Successfully notified " << instance << " about the new Raft group " << partition_id << std::endl;
    }
    return true;
}

int addFreeNodes(std::string instance_name){
    // Open config file and acquire an exclusive lock
    int fd = open(config_file.c_str(), O_RDWR);
    if (fd == -1) {
        std::cerr << "Error: Unable to open config file: " << config_file << std::endl;
        return -1;
    }

    if (flock(fd, LOCK_EX) == -1) {
        std::cerr << "Error: Unable to acquire exclusive lock on config file: " << config_file << std::endl;
        close(fd);
        return -1;
    }

    // Append instance to the end of config file without releasing the lock
    if (!AppendInstanceToFile(fd, instance_name)) {
        flock(fd, LOCK_UN);
        close(fd);
        return -1;
    }
    free_hanging_nodes.push_back(instance_name);

    if (free_hanging_nodes.size() == min_nodes) {
        ch->PrintHashRing();
        ch->AddPartition(std::to_string(num_partitions));
        num_partitions++;
        std::cout << "***************Added a new partition***************" << std::endl;
        ch->PrintHashRing();

        // Assign the new free-hanging nodes to the new partition
        int new_partition_id = num_partitions - 1;
        partition_instances_[new_partition_id] = free_hanging_nodes;

        // Notify each server about the new Raft group
        if (!NotifyNewRaftGroup(new_partition_id, free_hanging_nodes)) {
            std::cerr << "Failed to notify servers about new Raft group " << new_partition_id << std::endl;
            flock(fd, LOCK_UN);
            close(fd);
            return -1;
        }

        // Initiate key transfer for the new partition
        std::vector<std::thread> threads;  // Vector to store threads
        std::vector<bool> results(num_partitions - 1, true);  // Result vector to track success or failure for each partition

        // Iterate over all existing partitions (excluding the new one)
        for (int i = 0; i < num_partitions - 1; i++) {
            // Create a thread for each partition to handle key transfer concurrently
            threads.emplace_back([i, &results]() {
                // Fetch the key ranges to transfer for this partition
                std::vector<std::pair<unsigned long, unsigned long>> key_space = ch->GetKeySpaceToTransfer(std::to_string(i), std::to_string(num_partitions-1));
                
                // Prepare the PartitionChangeRequest message
                PartitionChangeRequest partition_change_request;

                // Add key ranges to the request
                for (const auto& range : key_space) {
                    keyvaluestore::KeyRange* key_range = partition_change_request.add_key_ranges();
                    key_range->set_key_start(range.first);
                    key_range->set_key_end(range.second);
                }

                // Send the PartitionChangeRequest to the partition's leader
                PartitionChangeResponse partition_change_response;
                Status status = RetryRequest(i, partition_change_request, &partition_change_response, &KeyValueStore::Stub::PartitionChange);

                // Check the status of the response
                if (status.ok() && partition_change_response.success()) {
                    // print the key values that are being transferred if not empty. Else print You're good
                    if(partition_change_response.key_values().size() > 0){
                        std::cout << "Move following key values from partition " << i << " to partition " << num_partitions-1 << std::endl;
                        for(const auto& key_value : partition_change_response.key_values()){
                            std::cout << "Key: " << key_value.key() << " Value: " << key_value.value() << std::endl;
                        }

                        // Transfer the keys to the new partition
                        if (!TransferKeysToNewPartition(partition_change_response, num_partitions - 1)) {
                            std::cerr << "Failed to transfer keys to new partition " << num_partitions - 1 << std::endl;
                            results[i] = false;  // Mark as failed
                        }
                    } else {
                        std::cout << "You're good" << std::endl;
                    }
                } else {
                    std::cerr << "Failed to send PartitionChangeRequest to leader of partition " << i << std::endl;
                    results[i] = false;  // Mark as failed
                }
            });
        }

        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }

        // Check results for any failures
        for (bool result : results) {
            if (!result) {
                std::cerr << "Key transfer failed for a partition." << std::endl;
                
                // Release the lock and close file descriptor before returning on failure
                flock(fd, LOCK_UN);
                close(fd);
                return -1;
            }
        }

        // Clear free hanging nodes after successful creation of the new partition
        free_hanging_nodes.clear();
    }

    // Release the lock and close the file
    flock(fd, LOCK_UN);
    close(fd);
    return 0;
}

int kv739_start(const std::string &instance_name, int new_instance) {
    // Read the config file to check if it has been modified
    struct stat file_stat;
    if (stat(config_file.c_str(), &file_stat) == 0) {
        if (std::to_string(file_stat.st_mtime) != last_modified) {
            if (!ReadServiceInstancesFromFile(config_file)) {
                return -1;
            }
        }
    }
    
    // Find an available partition for the new instance
    int partition_id = FindPartition();
    if (partition_id == -1) {
        // call addFreeNodes 
        return addFreeNodes(instance_name);
    }

    StartRequest start_request;
    StartResponse start_response;
    start_request.set_instance_name(instance_name);
    start_request.set_new_instance(new_instance == 1);

    // Use RetryRequest to find an available node in the partition to handle the start request
    Status status = RetryRequest(partition_id, start_request, &start_response, &KeyValueStore::Stub::Start);
    if (!status.ok() || !start_response.success()) {
        std::cerr << "Error starting instance " << instance_name << std::endl;
        return -1;
    }

    std::cout << "New instance " << instance_name << " has successfully joined partition " << partition_id << std::endl;

    // Update client-side configuration
    service_instances_.push_back(instance_name);
    partition_instances_[partition_id].push_back(instance_name);

    // Update the service instances in the file
    if(!UpdateServiceInstancesInFile(config_file, "NULL", instance_name, partition_id)){
        std::cerr << "Failed to update " << config_file << std::endl;
        return -1;
    }

    return 0;
}

int kv739_leave(const std::string &instance_name, int clean) {
    // Check if the instance exists in the service instances
    struct stat file_stat;
    if (stat(config_file.c_str(), &file_stat) == 0) {
        if (std::to_string(file_stat.st_mtime) != last_modified) {
            if (!ReadServiceInstancesFromFile(config_file)) {
                return -1;
            }
        }
    }

    auto it = std::find(service_instances_.begin(), service_instances_.end(), instance_name);
    if (it == service_instances_.end()) {
        std::cerr << "Instance " << instance_name << " not found in service instances" << std::endl;
        return -1;
    }

    int server_id = std::find(service_instances_.begin(), service_instances_.end(), instance_name) - service_instances_.begin();
    int partition_id = server_id / nodes_per_partition;

    // Check the size of the prtition_id
    if(partition_instances_[partition_id].size() <= min_nodes){
        std::cerr << "Error: Cannot leave partition " << partition_id << " with less than " << min_nodes << " nodes." << std::endl;
        return -1;
    }

    LeaveRequest leave_request;
    LeaveResponse leave_response;
    leave_request.set_instance_name(instance_name);
    leave_request.set_clean(clean == 1);

    // Use RetryRequest to find an available node in the partition to handle the leave request
    // YOU COME AT THE KING, YOU BEST NOT MISS
    Status status = RetryRequest(partition_id, leave_request, &leave_response, &KeyValueStore::Stub::Leave);
    
    std::cout << "Instance " << instance_name << " has successfully left partition " << partition_id << std::endl;

    // Update client-side configuration
    service_instances_.erase(it);
    for (auto &partition : partition_instances_) {
        partition.second.erase(std::remove(partition.second.begin(), partition.second.end(), instance_name), partition.second.end());
    }

    // Update the service instances in the file
    if(!UpdateServiceInstancesInFile(config_file, instance_name, "NULL", partition_id)){
        std::cerr << "Failed to update " << config_file << std::endl;
        return -1;
    }

    return 0;
}