#include "lib739kv.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <cassert>
#include <thread>  // For std::this_thread::sleep_for
#include <chrono>  // For std::chrono::seconds
#include <iomanip> // For formatted table output
#include <unordered_map>
#include <string>
#include "consistent_hashing.h"

// int num_partitions;  // Number of partitions (based on server configuration)
// Hash function to map keys to Raft partitions
// ConsistentHashing *ch;

// Utility function to read server list from config file
std::vector<std::string> read_server_list(const std::string& config_file) {
    std::ifstream file(config_file);
    std::vector<std::string> server_list;
    std::string line;

    if (!file.is_open()) {
        std::cerr << "Error: Unable to open config file: " << config_file << std::endl;
        exit(1);
    }

    while (std::getline(file, line)) {
        if (!line.empty()) {
            server_list.push_back(line);
        }
    }

    return server_list;
}

void init_consistent_hashing(std::vector<std::string> server_list) {
    num_partitions = server_list.size()/5;
    std::vector<std::string> nodes(num_partitions);
    for(int i = 0; i < num_partitions; i++){
        nodes[i] = std::to_string(i);
    }
    ch = new ConsistentHashing(3, nodes);
    return;
}

// Function to track active servers in each group
void update_active_servers_count(std::vector<int>& active_servers_per_group, int server_id) {
    int group_id = server_id / 5;  // Each group consists of 5 servers
    active_servers_per_group[group_id]--;  // Decrement the active server count for this group
}

// Utility function to generate random strings (keys and values)
std::string generate_random_string(size_t length) {
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string random_string;
    // Seed the random number generator
    // Create random strings for each new run
    for (size_t i = 0; i < length; ++i) {
        random_string += charset[rand() % (sizeof(charset) - 1)];
    }

    return random_string;
}

// Extract the port number from the server string (e.g., "localhost:50051" -> 50051)
int extract_port_from_server(const std::string& server) {
    size_t colon_pos = server.find(':');
    if (colon_pos == std::string::npos) {
        throw std::invalid_argument("Invalid server format: " + server);
    }
    std::string port_str = server.substr(colon_pos + 1);
    return std::stoi(port_str);  // Convert port part to integer
}

// Function to run the availability test and track failed keys
void test_availability_with_failures_of_leaders(const std::string& config_file, int keys_to_test) {
    // Step 1: Read server list from config file
    std::vector<std::string> server_list = read_server_list(config_file);

    // Step 2: Initialize the client with all servers
    assert(kv739_init(config_file) == 0);
    init_consistent_hashing(server_list);

    // Step 3: Generate 100 random key-value pairs
    std::vector<std::pair<std::string, std::string>> test_data;

    // How to change random string with each iteration
    
    // But then all the strings generated are same
    // What else to do?
    
    for (int i = 0; i < keys_to_test; ++i) {
        std::string key = generate_random_string(8);
        std::string value = generate_random_string(16);
        test_data.emplace_back(key, value);
    }

    // Step 4: Perform `put` operations with all servers running
    std::cout << "Performing PUT operations with all servers running..." << std::endl;
    for (const auto& pair : test_data) {
        std::string old_value;
        int result = kv739_put(pair.first, pair.second, old_value);
        assert(result == 0 || result == 1);  // Allow both 0 (key existed) and 1 (key didn't exist)
    }
    std::cout << "PUT operations completed successfully." << std::endl;

    // Step 5: Start killing nodes one by one and track get failures
    std::vector<int> failed_get_counts;  // To store the number of failed `get` operations after each failure
    std::vector<std::string> failed_servers;  // To store the failed servers

    std::vector<int> leader_ids = {0, 5, 10, 15};

    for (size_t failures = 0; failures < leader_ids.size(); ++failures) {
        // Step 6: Randomly select a server to kill
        size_t server_to_kill = leader_ids[failures];
        std::string server = server_list[server_to_kill];

        // Kill the selected server
        std::cout << "Killing server: " << server << std::endl;
        // assert(kv739_die(server, 1) == 0);
        kv739_die(server, 1);
        // Remove the killed server from the list so it doesn't get killed again
        // server_list.erase(server_list.begin() + server_to_kill);

        failed_servers.push_back(server);

        // Wait for 3 seconds after killing the server
        std::this_thread::sleep_for(std::chrono::seconds(15));

        // Step 7: Perform `get` operations and count failures
        int failed_gets = 0;
        for (const auto& pair : test_data) {
            std::string retrieved_value;
            int get_result = kv739_get(pair.first, retrieved_value);

            // Count the number of failed gets
            if (get_result != 0 || retrieved_value != pair.second) {
                failed_gets++;
            }
        }

        // Store the count of failed gets for this round of failures
        failed_get_counts.push_back(failed_gets);
        std::cout << "Failed GET operations after " << (failures + 1) << " server failure(s): " << failed_gets << std::endl;

        // If all keys are failing, break out of the test early
        if (failed_gets == 100) {
            std::cout << "All GET operations failed. Stopping the test." << std::endl;
            break;
        }

        // Step 9: Display the results in a table format
        std::cout << "\nAvailability Test Results\n";
        std::cout << "-------------------------\n";
        std::cout << std::setw(15) << "Servers Failed" << std::setw(20) << "GET Failures" << std::setw(20) << "Failed Server\n";
        std::cout << "----------------------------------------\n";
        for (size_t i = 0; i < failed_get_counts.size(); ++i) {
            std::cout << std::setw(15) << (i + 1) << std::setw(20) << failed_get_counts[i] << std::setw(20) << failed_servers[i] << "\n";
        }
    }

    // Step 8: Shutdown the client
    assert(kv739_shutdown() == 0);

    // Step 9: Display the results in a table format
    std::cout << "\nAvailability Test Results\n";
    std::cout << "-------------------------\n";
    std::cout << std::setw(15) << "Servers Failed" << std::setw(20) << "GET Failures\n";
    std::cout << "----------------------------------------\n";
    for (size_t i = 0; i < failed_get_counts.size(); ++i) {
        std::cout << std::setw(15) << (i + 1) << std::setw(20) << failed_get_counts[i] << "\n";
    }
}


void test_availability_with_failures(const std::string& config_file, int keys_to_test) {
    // Step 1: Read server list from config file
    std::vector<std::string> server_list = read_server_list(config_file);
    size_t total_servers = server_list.size();

    // Step 2: Initialize the client with all servers
    assert(kv739_init(config_file) == 0);
    init_consistent_hashing(server_list);

    // Step 3: Generate 100 random key-value pairs
    std::vector<std::pair<std::string, std::string>> test_data;
    std::unordered_map<int, int> partition_key_count;  // To track the number of keys in each partition
    for (int i = 0; i < keys_to_test; ++i) {
        std::string key = generate_random_string(8);
        std::string value = generate_random_string(16);

        // Hash the key to determine the partition and update the count
        std::cout << "Key: " << key << std::endl;
        std:: cout << "Hashing key: " << ch->GetPartition(key) << std::endl;
        int partition_id = std::stoi(ch->GetPartition(key));
        partition_key_count[partition_id]++;

        test_data.emplace_back(key, value);
    }

    // Step 4: Perform `put` operations with all servers running
    std::cout << "Performing PUT operations with all servers running..." << std::endl;
    for (const auto& pair : test_data) {
        std::string old_value;
        int result = kv739_put(pair.first, pair.second, old_value);
        assert(result == 0 || result == 1);  // Allow both 0 (key existed) and 1 (key didn't exist)
    }
    std::cout << "PUT operations completed successfully." << std::endl;


    // Step x: Display the partition distribution
    std::cout << "\nKey Distribution Across Partitions\n";
    std::cout << "-----------------------------------\n";
    std::cout << std::setw(15) << "Partition" << std::setw(20) << "Keys Count\n";
    std::cout << "-----------------------------------\n";
    for (int partition_id = 0; partition_id < num_partitions; ++partition_id) {
        std::cout << std::setw(15) << partition_id << std::setw(20) << partition_key_count[partition_id] << "\n";
    }

    // Step Y: Initialize group tracking
    int num_groups = total_servers / 5;  // Assuming 5 servers per group
    std::vector<int> active_servers_per_group(num_groups, 5);  // Initially, all groups have 5 active servers

    // This will store the number of active servers in the group at the time of each failure
    std::vector<int> active_servers_at_failure;

    // Create an array to track which servers are active
    std::vector<bool> active_status(total_servers, true);  // All servers start as active

    // Step 5: Start killing nodes one by one and track get failures
    std::vector<int> failed_get_counts;  // To store the number of failed `get` operations after each failure
    std::vector<std::string> failed_servers;  // To store the failed servers

    for (size_t failures = 0; failures < total_servers; ++failures) {
        // Step 6: Randomly select a server to kill
        size_t server_to_kill;
        do {
            server_to_kill = rand() % server_list.size();  // Randomly choose an index
        } while (!active_status[server_to_kill]);  // Ensure the server is active

        std::string server = server_list[server_to_kill];

        // Kill the selected server
        std::cout << "Killing server: " << server << std::endl;
        // assert(kv739_die(server, 1) == 0);
        kv739_die(server, 1);

        // Track active servers in the group of the killed server
        int group_id = server_to_kill / 5;
        update_active_servers_count(active_servers_per_group, server_to_kill);  // Update the count for future failures
        active_servers_at_failure.push_back(active_servers_per_group[group_id]);  // Store the active server count after failure

        // Remove the killed server from the list so it doesn't get killed again
        // server_list.erase(server_list.begin() + server_to_kill);

        // Mark the server as inactive
        active_status[server_to_kill] = false;

        failed_servers.push_back(server);

        // Wait for 3 seconds after killing the server
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // Step 7: Perform `get` operations and count failures
        int failed_gets = 0;
        for (const auto& pair : test_data) {
            std::string retrieved_value;
            int get_result = kv739_get(pair.first, retrieved_value);

            // Count the number of failed gets
            if (get_result != 0 || retrieved_value != pair.second) {
                failed_gets++;
            }
        }

        // Store the count of failed gets for this round of failures
        failed_get_counts.push_back(failed_gets);
        std::cout << "Failed GET operations after " << (failures + 1) << " server failure(s): " << failed_gets << std::endl;

        // If all keys are failing, break out of the test early
        if (failed_gets == 100) {
            std::cout << "All GET operations failed. Stopping the test." << std::endl;
            break;
        }

        // Display the results in a table format
        std::cout << "\nAvailability Test Results\n";
        std::cout << "-------------------------\n";
        std::cout << std::setw(15) << "Servers Failed" << std::setw(20) << "GET Failures" << std::setw(20) << "Failed Server" << std::setw(25) << "Active Servers in Group\n";
        std::cout << "------------------------------------------------------------------------------------------------------------------------------------------------------\n";
        for (size_t i = 0; i < failed_get_counts.size(); ++i) {
            int failed_server_port = extract_port_from_server(failed_servers[i]);
            int group = (failed_server_port - 50051) / 5;
            std::cout << std::setw(15) << (i + 1) << std::setw(20) << failed_get_counts[i] << std::setw(20) << failed_servers[i] << std::setw(25) << active_servers_at_failure[i] << "\n";
        }
    }

    // Step 8: Shutdown the client
    assert(kv739_shutdown() == 0);

    // Display the results in a table format
    std::cout << "\nAvailability Test Results\n";
    std::cout << "-------------------------\n";
    std::cout << std::setw(15) << "Servers Failed" << std::setw(20) << "GET Failures" << std::setw(20) << "Failed Server" << std::setw(25) << "Active Servers in Group\n";
    std::cout << "-------------------------------------------------------------\n";
    for (size_t i = 0; i < failed_get_counts.size(); ++i) {
        int failed_server_id = std::stoi(failed_servers[i].substr(9)) - 50051;
        int group = failed_server_id / 5;
        std::cout << std::setw(15) << (i + 1) << std::setw(20) << failed_get_counts[i] << std::setw(20) << failed_servers[i] << std::setw(25) << active_servers_per_group[group] << "\n";
    }
}

void test_availability_with_leave_operations(const std::string& config_file, int keys_to_test){
    // Step 1: Read server list from config file
    std::vector<std::string> server_list = read_server_list(config_file);
    size_t total_servers = server_list.size();

    // Step 2: Initialize the client with all servers
    assert(kv739_init(config_file) == 0);
    init_consistent_hashing(server_list);
    std::cout << "Consistent Hashing Initialized" << std::endl;

    // Step 3: Generate 100 random key-value pairs
    std::vector<std::pair<std::string, std::string>> test_data;
    std::unordered_map<int, int> partition_key_count;  // To track the number of keys in each partition
    for (int i = 0; i < keys_to_test; ++i) {
        std::string key = generate_random_string(8);
        std::string value = generate_random_string(16);

        // Hash the key to determine the partition and update the 
        int partition_id = std::stoi(ch->GetPartition(key));
        std::cout << "Key: " << key << " Partition: " << partition_id << std::endl;
        partition_key_count[partition_id]++;

        test_data.emplace_back(key, value);
    }

    // Step 4: Perform `put` operations with all servers running
    std::cout << "Performing PUT operations with all servers running..." << std::endl;
    for (const auto& pair : test_data) {
        std::string old_value;
        int result = kv739_put(pair.first, pair.second, old_value);
        assert(result == 0 || result == 1);  // Allow both 0 (key existed) and 1 (key didn't exist)
    }
    std::cout << "PUT operations completed successfully." << std::endl;


    // Step x: Display the partition distribution
    std::cout << "\nKey Distribution Across Partitions\n";
    std::cout << "-----------------------------------\n";
    std::cout << std::setw(15) << "Partition" << std::setw(20) << "Keys Count\n";
    std::cout << "-----------------------------------\n";
    for (int partition_id = 0; partition_id < num_partitions; ++partition_id) {
        std::cout << std::setw(15) << partition_id << std::setw(20) << partition_key_count[partition_id] << "\n";
    }

    // Step Y: Initialize group tracking
    int num_groups = total_servers / 5;  // Assuming 5 servers per group
    std::vector<int> active_servers_per_group(num_groups, 5);  // Initially, all groups have 5 active servers

    // This will store the number of active servers in the group at the time of each failure
    std::vector<int> active_servers_at_failure;

    // Create an array to track which servers are active
    std::vector<bool> active_status(total_servers, true);  // All servers start as active

    // Step 5: Start leaving nodes one by one and track get failures
    std::vector<int> failed_get_counts;  // To store the number of failed `get` operations after each leave
    std::vector<std::string> left_servers;  // To store the left servers

    for (size_t failures = 0; failures < total_servers; ++failures) {
        // Step 6: Randomly select a server to leave
        size_t server_to_leave;
        do {
            server_to_leave = rand() % server_list.size();  // Randomly choose an index
        } while (!active_status[server_to_leave]);  // Ensure the server is active

        std::string server = server_list[server_to_leave];

        // Kill the selected server
        std::cout << "Leaving server: " << server << std::endl;
        // assert(kv739_leave(server, 1) == 0);
        kv739_leave(server, 1);

        // Track active servers in the group of the killed server
        int group_id = server_to_leave / 5;
        update_active_servers_count(active_servers_per_group, server_to_leave);  // Update the count for future failures
        active_servers_at_failure.push_back(active_servers_per_group[group_id]);  // Store the active server count after failure

        // Remove the killed server from the list so it doesn't get killed again
        // server_list.erase(server_list.begin() + server_to_kill);

        // Mark the server as inactive
        active_status[server_to_leave] = false;

        left_servers.push_back(server);

        // Wait for 3 seconds after killing the server
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // Step 7: Perform `get` operations and count failures
        int failed_gets = 0;
        for (const auto& pair : test_data) {
            std::string retrieved_value;
            int get_result = kv739_get(pair.first, retrieved_value);

            // Count the number of failed gets
            if (get_result != 0 || retrieved_value != pair.second) {
                failed_gets++;
            }
        }

        // Store the count of failed gets for this round of failures
        failed_get_counts.push_back(failed_gets);
        std::cout << "Failed GET operations after " << (failures + 1) << " server failure(s): " << failed_gets << std::endl;

        // If all keys are failing, break out of the test early
        if (failed_gets == 100) {
            std::cout << "All GET operations failed. Stopping the test." << std::endl;
            break;
        }

        // Display the results in a table format
        std::cout << "\nAvailability Test Results\n";
        std::cout << "-------------------------\n";
        std::cout << std::setw(15) << "Servers Failed" << std::setw(20) << "GET Failures" << std::setw(20) << "Failed Server" << std::setw(25) << "Active Servers in Group\n";
        std::cout << "------------------------------------------------------------------------------------------------------------------------------------------------------\n";
        for (size_t i = 0; i < failed_get_counts.size(); ++i) {
            int left_server_port = extract_port_from_server(left_servers[i]);
            int group = (left_server_port - 50051) / 5;
            std::cout << std::setw(15) << (i + 1) << std::setw(20) << failed_get_counts[i] << std::setw(20) << left_servers[i] << std::setw(25) << active_servers_at_failure[i] << "\n";
        }
    }

    // Step 8: Shutdown the client
    assert(kv739_shutdown() == 0);

    // Display the results in a table format
    std::cout << "\nAvailability Test Results\n";
    std::cout << "-------------------------\n";
    std::cout << std::setw(15) << "Servers Left" << std::setw(20) << "GET Failures" << std::setw(20) << "Left Server" << std::setw(25) << "Active Servers in Group\n";
    std::cout << "-------------------------------------------------------------\n";
    for (size_t i = 0; i < failed_get_counts.size(); ++i) {
        int failed_server_id = std::stoi(left_servers[i].substr(9)) - 50051;
        int group = failed_server_id / 5;
        std::cout << std::setw(15) << (i + 1) << std::setw(20) << failed_get_counts[i] << std::setw(20) << left_servers[i] << std::setw(25) << active_servers_per_group[group] << "\n";
    }
}


int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }
    int keys_to_test = 100;
    // srand(time(0));
    // Run availability test with server list from the provided config file
    // test_availability_with_failures_of_leaders(argv[1]);

    srand(time(0));
    // test_availability_with_failures_of_leaders(argv[1], keys_to_test);
    //test_availability_with_failures(argv[1], keys_to_test);
    test_availability_with_leave_operations(argv[1], keys_to_test);

    return 0;
}