#include "lib739kv.h"
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <thread>

#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using keyvaluestore::ClientRequest;
using keyvaluestore::GetRequest;
using keyvaluestore::GetResponse;
using keyvaluestore::InitRequest;
using keyvaluestore::InitResponse;
using keyvaluestore::KeyValueStore;
using keyvaluestore::PutRequest;
using keyvaluestore::PutResponse;
using keyvaluestore::ServerResponse;
using keyvaluestore::ShutdownRequest;
using keyvaluestore::ShutdownResponse;

// Global variables to hold the gRPC objects
std::map<int, std::shared_ptr<grpc::Channel>> channels_; // Channel for each Raft node
std::map<int, std::unique_ptr<KeyValueStore::Stub>> stubs_; // Stub for each Raft node
std::map<int, std::shared_ptr<grpc::ClientReaderWriter<ClientRequest, ServerResponse>>> streams_; // Persistent streams
std::map<int, std::unique_ptr<grpc::ClientContext>> contexts_; // Persistent contexts for each stream

std::map<int, std::string> leader_addresses_;  // Maps partition IDs to current leader addresses
std::map<int, std::vector<std::string>> partition_instances_;  // Maps partition IDs to list of nodes

const int num_partitions = 20;  // Number of partitions (based on server configuration)
const int nodes_per_partition = 5;  // Number of nodes per partition

std::vector<std::string> service_instances_;  // List of service instances (host:port)


// Hash function to map keys to Raft partitions
int HashKey(const std::string &key) {
    std::hash<std::string> hasher;
    return hasher(key) % num_partitions;
}


// Helper function to close the streams and clean up contexts if a connection fails
void CleanupConnection(int partition_id) {
    if (streams_[partition_id]) {
        streams_[partition_id]->WritesDone();
        streams_[partition_id]->Finish();
        streams_[partition_id].reset();
        contexts_[partition_id].reset();
    }
}


bool ConnectToLeader(int partition_id) {
    std::string leader_address = leader_addresses_[partition_id];

    // Create gRPC channel and stub for the leader of this partition
    channels_[partition_id] = grpc::CreateChannel(leader_address, grpc::InsecureChannelCredentials());
    stubs_[partition_id] = KeyValueStore::NewStub(channels_[partition_id]);

    // Initialize stream and context for persistent connection
    contexts_[partition_id] = std::make_unique<grpc::ClientContext>();
    streams_[partition_id] = stubs_[partition_id]->ManageSession(contexts_[partition_id].get());

    InitRequest init_request;
    init_request.set_server_name(leader_address);

    ClientRequest client_request;
    client_request.mutable_init_request()->CopyFrom(init_request);

    if (!streams_[partition_id]->Write(client_request)) {  // Send Init request
        CleanupConnection(partition_id);  // Clean up on failure
        return false;
    }

    ServerResponse response;
    if (streams_[partition_id]->Read(&response) && response.has_init_response() && response.init_response().success()) {
        return true;
    }

    CleanupConnection(partition_id);  // Clean up on failure
    return false;
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

            // Move to the next partition after `nodes_per_partition` nodes
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


// Retry the request with a new leader if there was a leader redirection
bool RetryWithNewLeader(int partition_id, const ClientRequest &client_request, ServerResponse &response) {

    // Try connecting to other nodes in the partition
    for (const std::string& node_address : partition_instances_[partition_id]) {
        // Close the current stream before connecting to the new leader
        CleanupConnection(partition_id);
        leader_addresses_[partition_id] = node_address;
        std::cout << "Attempting to reconnect to node: " << node_address << " for partition " << partition_id << std::endl;

        if (ConnectToLeader(partition_id)) {
            // Retry the request with the new leader
            if (!streams_[partition_id]->Write(client_request)) {
                continue;
            }
            if (streams_[partition_id]->Read(&response)) {
                return true;
            }
        }
    }
    return false; // All nodes failed
}


// Handles leader redirection and retries the request
bool HandleLeaderRedirection(int partition_id, const ClientRequest &client_request, ServerResponse &response) {
    // Attempt to send the request using the persistent stream
    if (!streams_[partition_id]->Write(client_request)) {
        return false;
    }

    // Read the response
    if (streams_[partition_id]->Read(&response)) {
        if (response.has_not_leader()) {
            // If not a leader, update leader metadata and retry the request
            std::string new_leader_address = response.not_leader().leader_address();
            leader_addresses_[partition_id] = new_leader_address;  // Update leader address
            CleanupConnection(partition_id); // Close the stream with the old leader

            std::cout << "Redirected to new leader: " << new_leader_address << " for partition " << partition_id << std::endl;
            return RetryWithNewLeader(partition_id, client_request, response);
        }
        return true;

    } else {
        std::cerr << "Error: Failed to read response from leader for partition " << partition_id << ". Attempting to reconnect..." << std::endl;
        // Retry with another node in the partition, including potential new leader
        return RetryWithNewLeader(partition_id, client_request, response);
    }
    return false;
}


int kv739_init(const std::string &file_name) {
    // Ensure streams are not already initialized
    for (int i = 0; i < num_partitions; i++) {
        if (streams_[i]) {
            std::cerr << "Error: Client is already initialized for partition " << i << std::endl;
            return -1;
        }
    }

    // Read the list of service instances from the file and map them to partitions
    if (!ReadServiceInstancesFromFile(file_name)) {
        return -1;
    }

    // Initialize connection by selecting the first node of each partition
    for (int i = 0; i < num_partitions; i++) {
        leader_addresses_[i] = partition_instances_[i][0];  // Assume first node in partition group is the leader
        if (!ConnectToLeader(i)) {
            std::cerr << "Failed to initialize client with Raft leader for partition " << i << std::endl;
            return -1;
        }
    }
    return 0;
}


int kv739_shutdown() {
    for (int i = 0; i < num_partitions; i++) {
        if (!streams_[i]) {
            std::cerr << "Error: No active session for partition " << i << " to shut down." << std::endl;
            return -1;
        }

        ClientRequest client_request;
        ShutdownRequest shutdown_request;
        client_request.mutable_shutdown_request()->CopyFrom(shutdown_request);

        if (!streams_[i]->Write(client_request)) { // Send Shutdown request
            std::cerr << "Error: Failed to send shutdown request to partition " << i << std::endl;
            continue;  // If this stream fails, move on to the next partition
        }

        ServerResponse response; //@TODO: what if leader fails during shutdown request and what if shutdown operation failed
        if (streams_[i]->Read(&response) && response.has_shutdown_response() && response.shutdown_response().success()) {
            std::cout << "Shutdown successful for Raft leader of partition " << i << std::endl;
        } else {
            std::cerr << "Error: Failed to receive shutdown response from partition " << i << std::endl;
        }

        // Finish the stream and clean up the context
        CleanupConnection(i);
    }

    return 0;
}


int kv739_get(const std::string &key, std::string &value) {
    int partition_id = HashKey(key);  // Determine partition
    if (!streams_[partition_id]) {
        std::cerr << "Error: No active session for partition " << partition_id << std::endl;
        return -1;
    }

    ClientRequest client_request;
    GetRequest get_request;
    get_request.set_key(key);
    client_request.mutable_get_request()->CopyFrom(get_request);

    ServerResponse response;
    if (!HandleLeaderRedirection(partition_id, client_request, response)) {
        return -1;  // Communication failure
    }

    if (response.has_get_response()) {
        if (response.get_response().key_found()) {
            value = response.get_response().value();
            std::cout << "Get operation successful. Key: '" << key << "', Value: '" << value << "'." << std::endl;
            return 0;
        } else {
            std::cout << "Key '" << key << "' not found." << std::endl;
            return 1;  // Key not found
        }
    }

    std::cerr << "Error: Get operation failed for key: '" << key << "'." << std::endl;
    return -1;
}

int kv739_put(const std::string &key, const std::string &value, std::string &old_value) {
    int partition_id = HashKey(key);  // Determine partition
    if (!streams_[partition_id]) {
        std::cerr << "Error: No active session for partition " << partition_id << std::endl;
        return -1;
    }

    PutRequest put_request;
    put_request.set_key(key);
    put_request.set_value(value);

    ClientRequest client_request;
    client_request.mutable_put_request()->CopyFrom(put_request);

    ServerResponse response;
    if (!HandleLeaderRedirection(partition_id, client_request, response)) {
        return -2;  // Communication failure
    }

    if (response.has_put_response()) {
        if (response.put_response().key_found()) {
            old_value = response.put_response().old_value();
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