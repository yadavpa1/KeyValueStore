// Availability with Membership Changes:
// Scenario 1: One old node leaves and New node joins the group incrementally. Eventually all the old servers are replaced with new servers.
// Steps:
// Step 0: Print the membership details of each raft group.
//         Print config file in a readable format. To show the membership details. Group of consecutive 5 servers are considered one group.
// Step 1: Put X random keys to the the KV store.
// Step 2: Leave a server from one group and do get operation:
//         Sequentially leave servers from the config list in each iteration.
// Step 3: Start one server (should join the same group as it has less no. of servers) Sleep for some time and do get operation:
//         See how long it takes for the new server to join the group.
//         Start the server process using the server Run().
//         Call start and client will automatically assign it to the server will less no. of servers. 
// Step 4: Do Step 0
// Repeat Step 2 till Step 4 for all the servers in the config file.


// Scenario 2: A new partition (group of servers) is added.
// Steps:
// Step 0: Print the membership details of each raft group.
// Step 1: Put X random keys to the the KV store.
// Step 2: Start one , two , three servers and see if new group is formed.
//         Call start and client will automatically assign it to a new group since no group has less servers.
// Step 3: See if all gets succeed and get from new group is also successful.
//         Print the partition from where get was successful to see if new group was able to server the request.
// Repeat Steps 0 to Steps 3 for additional groups till 30 servers are present in the cluster.

#include "lib739kv.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <iomanip>
#include <unordered_map>

extern std::map<int, std::string> leader_addresses_;  // Maps partition IDs to current leader addresses
extern std::map<int, std::vector<std::string>> partition_instances_;  // Maps partition IDs to list of nodes
extern int num_partitions;  // Number of partitions (based on server configuration)
extern std::vector<std::string> service_instances_;  // List of service instances (host:port)
extern std::vector<std::string> free_hanging_nodes;
extern ConsistentHashing *ch;

std::vector<std::pair<std::string, std::string>> test_data;  // Stores the keys and values for later retrieval
int new_port_start = 50101;  // Starting port number for new instances

void printMembershipDetails() {
    std::cout << "\n=== Membership Details ===\n";
    std::cout << std::setw(15) << std::left << "Partition" << "Nodes\n";
    std::cout << std::string(40, '=') << "\n";
    
    for (int i = 0; i < num_partitions; i++) {
        std::cout << std::setw(15) << std::left << ("Partition " + std::to_string(i)) << "Size: " << partition_instances_[i].size() << "\n";
        
        // Print each node in the partition, separated by commas
        for (size_t j = 0; j < partition_instances_[i].size(); j++) {
            std::cout << partition_instances_[i][j];
            if (j < partition_instances_[i].size() - 1) {
                std::cout << ", ";
            }
        }
        std::cout << "\n";
    }

    std::cout << std::string(40, '=') << "\n";
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

void putRandomKeys(int num_keys) {
    for (int i = 0; i < num_keys; i++) {
        std::string key = generate_random_string(8);
        std::string value = generate_random_string(16);
        std::string old_value;
        
        // Attempt to put the key-value pair in the store
        if (kv739_put(key, value, old_value) != -1) {
            std::cout << "Put key: " << key << " value: " << value << std::endl;
            test_data.emplace_back(key, value);  // Store the key-value pair for later use
        } else {
            std::cerr << "Failed to put key: " << key << std::endl;
        }
    }
}

void performGetOperationForAllKeys(std::unordered_map<int, int> &partition_get_counts, int &failed_gets) {
    failed_gets = 0;
    partition_get_counts.clear();  // Reset counts for each partition

    for (const auto &pair : test_data) {
        std::string value;
        int partition_id = std::stoi(ch->GetPartition(pair.first));  // Get the partition ID for the key
        
        partition_get_counts[partition_id]++;  // Track which partition failed the get
        int status = kv739_get(pair.first, value);
        if (status != 0 || value != pair.second) {
            failed_gets++;  // Increment failed gets if status is not 0 or value mismatched
        }
    }
}

void printGetResults(int failed_gets, const std::unordered_map<int, int> &partition_failed_get_counts) {
    // Print table header
    std::cout << "\n=== Get Operation Summary ===\n";
    std::cout << std::setw(20) << "Partition ID" << std::setw(20) << "Get Count" << "\n";
    std::cout << std::string(40, '=') << "\n";
    
    for (const auto &entry : partition_failed_get_counts) {
        std::cout << std::setw(20) << entry.first << std::setw(20) << entry.second << "\n";
    }

    std::cout << "\nTotal failed gets: " << failed_gets << " out of " << test_data.size() << " attempts.\n";
    std::cout << std::string(40, '=') << "\n";
}

int startNewInstance(std::unordered_map<int, int> &partition_get_counts, int &failed_gets) {
    // Step 1: Determine the partition ID for the new server instance
    int partition_id = FindPartition();

    if(partition_id == -1){
        partition_id = num_partitions;
    }

    // Step 2: Determine the unique server address for the new instance within the specified range
    std::string new_instance = "localhost:" + std::to_string(new_port_start);
    new_port_start++;

    // Step 3: Fork a new process to simulate starting the server instance
    pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "Failed to fork process for new instance." << std::endl;
        return -1;
    } else if (pid == 0) {
        // Child process: Launch the Raft server instance
        // Replace the process with `raft_node_launcher` executable, passing partition_id and new_instance as arguments
        if (execl("./raft_node_launcher", "./raft_node_launcher", std::to_string(partition_id).c_str(), new_instance.c_str(), (char *)nullptr) == -1) {
            // If execl fails, log the error and exit the child process
            std::cerr << "Failed to launch raft_node_launcher for " << new_instance << " (errno: " << errno << ")" << std::endl;
            _exit(1);  // Use _exit to avoid flushing shared resources in the child process
        }
    } else {
        // Step 4: In the parent process, add the new instance to the kv store
        if (kv739_start(new_instance, 1) == 0) {
            // Perform get operations and print results
            performGetOperationForAllKeys(partition_get_counts, failed_gets);  
            printGetResults(failed_gets, partition_get_counts);
        } else {
            std::cerr << "Failed to add new instance: " << new_instance << std::endl;
            return -1;
        }
    }

    return 0;
}

void testMembershipChangesScenario1(int num_keys) {
    std::unordered_map<int, int> partition_get_counts;  // Track get counts by partition
    int failed_gets = 0;

    printMembershipDetails();
    putRandomKeys(num_keys);  // Populate the store with random keys

    for (const auto instance : service_instances_) {
        // Step 2: Leave a server from the group and perform get operations
        if (kv739_leave(instance, 1) == 0) {
            // std::cout << "Instance " << instance << " left successfully." << std::endl;
            performGetOperationForAllKeys(partition_get_counts, failed_gets);  // Check key availability
            printGetResults(failed_gets, partition_get_counts);  // Print get summary after server leave
        } else {
            std::cerr << "Failed to remove instance: " << instance << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::seconds(10));

        // Step 3: Start a new server instance using the function
        if (startNewInstance(partition_get_counts, failed_gets) != 0) {
            std::cerr << "Failed to start new instance." << std::endl;
            return;
        }

        std::this_thread::sleep_for(std::chrono::seconds(10));
        printMembershipDetails();  // Print updated membership details
    }
}


void testMembershipChangesScenario2(int num_keys) {
    std::unordered_map<int, int> partition_get_counts;  // Track get counts by partition
    int failed_gets = 0;
    int total_instances = service_instances_.size() + free_hanging_nodes.size();  // Initial number of servers
    const int max_servers = 30;  // Maximum number of servers to test

    // Step 0: Print initial membership details
    printMembershipDetails();
    
    // Step 1: Put random keys in the KV store
    putRandomKeys(num_keys);

    while (total_instances < max_servers) {
        // Step 2: Start adding new instances to form new groups as needed
        if (startNewInstance(partition_get_counts, failed_gets) != 0) {
            std::cerr << "Failed to start a new instance." << std::endl;
            return;
        }
        
        // Wait a bit to ensure the new instance is up and running
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
        // Increment the count of total instances
        total_instances++;
        
        // Print updated membership details to verify new grouping
        printMembershipDetails();

        // Step 3: Check if `get` operations succeed, including those from the new group
        performGetOperationForAllKeys(partition_get_counts, failed_gets);
        printGetResults(failed_gets, partition_get_counts);

        // Break if we have reached the desired maximum number of servers
        if (total_instances >= max_servers) {
            std::cout << "Reached maximum server count of " << max_servers << ". Stopping further additions." << std::endl;
            break;
        }

        // Optional sleep between iterations for better clarity in output
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }

    const std::string config_file = argv[1];
    const int num_keys = 10;

    if (kv739_init(config_file) != 0) {
        std::cerr << "Failed to initialize client." << std::endl;
        return -1;
    }

    std::cout << "\nRunning Scenario 1: Incremental Node Replacement\n";
    testMembershipChangesScenario1(num_keys);

    // std::cout << "\nRunning Scenario 2: Adding New Partition\n";
    // testMembershipChangesScenario2(num_keys);

    kv739_shutdown();

    if (ch != nullptr) {
        delete ch;  // Free the consistent hashing object if dynamically allocated
        ch = nullptr;
    }

    return 0;
}