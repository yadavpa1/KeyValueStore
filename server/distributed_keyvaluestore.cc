#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <cctype>
#include <locale>
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fstream>

#include <grpcpp/grpcpp.h>
#include "raft_server.h"
#include "rocksdb_wrapper.h"

const int nodes_per_partition = 5;

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
std::vector<std::string> readConfigFile(const std::string &filepath) {
    std::vector<std::string> host_list;
    std::ifstream config_file(filepath);

    if (!config_file.is_open()) {
        std::cerr << "Error: Could not open config file: " << filepath << std::endl;
        return host_list;
    }

    std::string line;
    while (std::getline(config_file, line)) {
        line = trim(line);
        if (!line.empty()) {
            host_list.push_back(line);
        }
    }

    // Close the file
    config_file.close();

    if (host_list.empty()) {
        std::cerr << "Error: No hosts found in the config file." << std::endl;
    }
    // Randomly shuffle the host list with a fixed seed of 42
    std::srand(42);
    std::random_shuffle(host_list.begin(), host_list.end());
    return host_list;
}

// Construct groups of 5 hosts each
std::vector<std::vector<std::string>> constructGroups(const std::vector<std::string> &host_list) {
    int num_groups = host_list.size() / nodes_per_partition;
    std::vector<std::vector<std::string>> groups(num_groups);
    
    int partition_id = 0;
    for (int i = 0; i < host_list.size(); i += nodes_per_partition) {
        std::vector<std::string> partition(host_list.begin() + i, host_list.begin() + i + nodes_per_partition);
        groups[partition_id] = partition;
        partition_id++;
    }
    return groups;
}

int main(int argc, char** argv) {
    std::string config_file;

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (std::string(argv[i]) == "-c") {
            config_file = argv[i + 1];
            i++;
        }
    }

    // Ensure required arguments are provided
    if (config_file.empty()) {
        std::cerr << "Usage: " << argv[0] << " -c <config_file>" << std::endl;
        return 1;
    }

    // Read host list from file
    std::vector<std::string> host_list = readConfigFile(config_file);

    // Validate the host list
    if (host_list.empty()) {
        std::cerr << "Error: Host list is empty or file could not be read properly" << std::endl;
        return 1;
    }

    int group_size = 5;
    int num_groups = host_list.size() / group_size;

    // Construct groups of 5 hosts each
    std::vector<std::vector<std::string>> groups = constructGroups(host_list);

    for (int group_index = 0; group_index < num_groups; group_index++) {
        std::vector<std::string> raft_group = groups[group_index];

        // Fork processes for each server in the group
        for (int local_server_id = 0; local_server_id < raft_group.size(); local_server_id++) {
            pid_t pid = fork();

            if (pid == 0) {
                // Child process - instantiate a RaftServer for this server_id
                std::string db_path = "raft_db_group_" + std::to_string(group_index) + "_server_" + std::to_string(local_server_id);
                size_t cache_size = 20 * 1024 * 1024; // 20MB cache
                RaftServer raft_server(local_server_id, raft_group, db_path, cache_size);
                raft_server.Run();

                // Exit child process when done
                exit(0);
            } else if (pid < 0) {
                std::cerr << "Error: Fork failed for server " << local_server_id << std::endl;
                return 1;
            }
            // Print the pid of the child process with the port number
            std::cout << "Server " << raft_group[local_server_id] << " started with pid: " << pid << std::endl;
        }
    }

    // Parent process waits for all child processes to finish
    while (wait(nullptr) > 0);

    return 0;
}
