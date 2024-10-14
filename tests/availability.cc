#include "lib739kv.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <cassert>
#include <thread>  // For std::this_thread::sleep_for
#include <chrono>  // For std::chrono::seconds

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

// Test function to check availability after each failure
void test_availability(const std::string& config_file) {
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

    // Step 5: Start killing nodes one by one and check availability
    int failure_threshold = 0;
    // srand(time(0)); // Seed for randomness

    for (size_t failures = 1; failures <= total_servers; ++failures) {
        // Randomly select a server to kill
        size_t server_to_kill = rand() % server_list.size();
        std::string server = server_list[server_to_kill];

        // Kill the selected server
        std::cout << "Killing server: " << server << std::endl;
        assert(kv739_die(server, 1) == 0);

        // Remove the killed server from the list so it doesn't get killed again
        server_list.erase(server_list.begin() + server_to_kill);

        // Wait for 3 seconds after killing the server
        std::cout << "Waiting for 6 seconds after killing the server..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(6));

        // Step 6: Perform `get` operations to check system availability
        bool system_available = true;
        std::cout << "Performing GET operations after killing " << failures << " server(s)..." << std::endl;
        for (const auto& pair : test_data) {
            std::string retrieved_value;
            int get_result = kv739_get(pair.first, retrieved_value);

            // Check if the retrieved value is correct
            if (get_result != 0 || retrieved_value != pair.second) {
                system_available = false;
                std::cerr << "Error: Key mismatch or failed GET after " << failures
                          << " server failure(s). System unavailable!" << std::endl;
                failure_threshold = failures;
                break;
            }
        }

        // If system is no longer available, break out of the test
        if (!system_available) {
            break;
        }

        std::cout << "System still operational after " << failures << " server failure(s)." << std::endl;
    }

    // Step 7: Log the failure threshold
    if (failure_threshold > 0) {
        std::cout << "System started failing after " << failure_threshold << " server failure(s)." << std::endl;
    } else {
        std::cout << "System handled all node failures without issues." << std::endl;
    }

    // Step 8: Shutdown the client after tests
    assert(kv739_shutdown() == 0);
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }

    // Run availability test with server list from the provided config file
    test_availability(argv[1]);

    return 0;
}
