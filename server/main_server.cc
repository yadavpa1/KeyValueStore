#include "keyvaluestore_server.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <thread>

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <node_id> <cluster_id> <db_path> <peer_1> <peer_2> ..." << std::endl;
        return 1;
    }

    int node_id = std::stoi(argv[1]);
    int cluster_id = std::stoi(argv[2]);
    std::string db_path = argv[3];
    
    std::vector<std::string> peers;
    for (int i = 4; i < argc; ++i) {
        peers.push_back(argv[i]);
    }

    KeyValueStoreServer server(node_id, cluster_id, db_path, peers);
    
    // Run Raft node in a separate thread
    std::thread raft_thread([&server]() {
        server.GetRaftNode().Run();
    });

    // Calculate a unique port by combining cluster_id and node_id
    int base_port = 50050;
    int port_offset = (cluster_id * 5) + node_id;  // Each cluster has 5 nodes
    std::string server_address("0.0.0.0:" + std::to_string(base_port + port_offset));
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&server);
    std::unique_ptr<grpc::Server> grpc_server(builder.BuildAndStart());
    
    std::cout << "Key-Value Store server listening on " << server_address << std::endl;
    grpc_server->Wait();
    
    raft_thread.join();
    return 0;
}

