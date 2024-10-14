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

// Utility function to generate random strings (keys and values)
std::string generate_random_string(size_t length) {
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string random_string;

    for (size_t i = 0; i < length; ++i) {
        random_string += charset[rand() % (sizeof(charset) - 1)];
    }

    return random_string;
}

// Function to run the availability test and track failed keys
void test_availability_with_failures_of_leaders(const std::string& config_file) {
    // Step 1: Read server list from config file
    std::vector<std::string> server_list = read_server_list(config_file);

    // Step 2: Initialize the client with all servers
    assert(kv739_init(config_file) == 0);

    // Step 3: Generate 100 random key-value pairs
    std::vector<std::pair<std::string, std::string>> test_data;
    for (int i = 0; i < 100; ++i) {
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


void test_availability_with_failures(const std::string& config_file) {
    // Step 1: Read server list from config file
    std::vector<std::string> server_list = read_server_list(config_file);
    size_t total_servers = server_list.size();

    // Step 2: Initialize the client with all servers
    assert(kv739_init(config_file) == 0);

    // Step 3: Generate 100 random key-value pairs
    std::vector<std::pair<std::string, std::string>> test_data;
    for (int i = 0; i < 100; ++i) {
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

    for (size_t failures = 0; failures < total_servers; ++failures) {
        // Step 6: Randomly select a server to kill
        size_t server_to_kill = rand() % server_list.size();
        std::string server = server_list[server_to_kill];

        // Kill the selected server
        std::cout << "Killing server: " << server << std::endl;
        // assert(kv739_die(server, 1) == 0);
        kv739_die(server, 1);
        // Remove the killed server from the list so it doesn't get killed again
        server_list.erase(server_list.begin() + server_to_kill);

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


int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }

    // Run availability test with server list from the provided config file
    // test_availability_with_failures(argv[1]);
    test_availability_with_failures_of_leaders(argv[1]);

    return 0;
}
