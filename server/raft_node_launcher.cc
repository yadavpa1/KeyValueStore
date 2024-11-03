#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include "raft_server.h"

void launchRaftServer(int group_index, const std::string &host_port) {
    int server_id = 0;  // Since each server has server_id set to 0
    std::vector<std::string> host_list = { host_port };

    std::string db_path = "raft_db_group_" + std::to_string(group_index) + "_server_" + std::to_string(server_id);
    std::string raft_log_db_path = "log_" + db_path;
    size_t cache_size = 20 * 1024 * 1024; // 20MB cache

    RaftServer raft_server(server_id, host_port, host_list, db_path, raft_log_db_path, cache_size);
    raft_server.StartServer();
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <group_index> <hostname:port>\n";
        return 1;
    }

    try {
        // Parse group_index and hostname:port from command-line arguments
        int group_index = std::stoi(argv[1]);
        std::string host_port = argv[2];

        std::cout << "Starting Raft server on " << host_port << " in group " << group_index << " with server_id 0" << std::endl;
        launchRaftServer(group_index, host_port);

    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << ". Please provide a valid <group_index> and <hostname:port>." << std::endl;
        return 1;
    }

    return 0;
}

