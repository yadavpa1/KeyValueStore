#include <iostream>
#include <string>
#include <vector>
#include <unistd.h>  // For fork(), getpid()
#include <sys/wait.h>
#include <csignal>
#include <cstdlib>   // For exit()

#include "raft_server.h"

// Function to trim whitespace from a string (for cleaner input handling)
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

// Function to convert the server name to server_id
int getServerIdFromName(const std::string& server_name) {
    std::string base = "localhost:50";
    if (server_name.rfind(base, 0) == 0) {
        // Extract the last 3 digits after 'localhost:50'
        std::string port_str = server_name.substr(11); // Remove 'localhost:50'
        int port_number = std::stoi(port_str);

        // Ensure the port number is in the valid range 50051 to 50150
        if (port_number >= 51 && port_number <= 150) {
            return port_number - 51; // server_id = port_number - 50051
        }
    }

    std::cerr << "Invalid server name format or out of range: " << server_name << std::endl;
    return -1;
}

// Function to get the server configuration based on the server_id and group size
std::vector<std::string> getServerConfig(int server_id, int group_size, const std::vector<std::string>& host_list) {
    int group_index = server_id / group_size;
    int local_server_id = server_id % group_size;

    std::vector<std::string> raft_group;
    int group_start = group_index * group_size;
    int group_end = std::min(group_start + group_size, static_cast<int>(host_list.size()));

    raft_group.insert(raft_group.end(), host_list.begin() + group_start, host_list.begin() + group_end);

    return raft_group;
}

int main() {
    int group_size = 5; // Each group consists of 5 consecutive servers

    // Prepare the list of all servers (from 50051 to 50150)
    std::vector<std::string> host_list;
    for (int port = 50051; port <= 50150; port++) {
        host_list.push_back("localhost:" + std::to_string(port));
    }

    // Run the process continuously and accept input to restart servers
    while (true) {
        std::string input;
        std::cout << "Enter server name to restart (e.g., 'localhost:50+input') or 'exit' to quit: ";
        std::getline(std::cin, input);
        input = "localhost:50" + input;
        input = trim(input);

        if (input == "exit") {
            std::cout << "Exiting..." << std::endl;
            break;
        }

        // Get server_id from server name
        int server_id = getServerIdFromName(input);
        if (server_id == -1 || server_id >= host_list.size()) {
            std::cerr << "Invalid server name. Must be between 'localhost:50051' and 'localhost:50150'." << std::endl;
            continue;
        }

        // Get the server configuration for the given server_id
        std::vector<std::string> raft_group = getServerConfig(server_id, group_size, host_list);
        int local_server_id = server_id % group_size;

        // Fork a new process to restart the server
        pid_t pid = fork();
        if (pid == 0) {
            // Child process: restart the RaftServer
            std::string db_path = "raft_db_group_" + std::to_string(server_id / group_size) + "_server_" + std::to_string(local_server_id);
            std::string raft_log_db_path = "log_" + db_path;
            size_t cache_size = 20 * 1024 * 1024; // 20MB cache for RocksDB
            
            RaftServer raft_server(local_server_id, raft_group[local_server_id], raft_group, db_path, raft_log_db_path, cache_size);

            // Reload state and restart the server
            raft_server.Run();
            exit(0);
        } else if (pid < 0) {
            std::cerr << "Error: Fork failed while trying to restart server " << local_server_id << std::endl;
        } else {
            std::cout << "Restarted server " << raft_group[local_server_id] << " with new pid: " << pid << std::endl;
        }

        // Parent process keeps running and waits for the next input
    }

    return 0;
}
