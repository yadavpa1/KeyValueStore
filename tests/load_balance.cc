#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <chrono>
#include <thread>
#include <random>
#include <filesystem>
#include <unistd.h>
#include <mutex>
#include <algorithm> // Required for std::all_of
#include "lib739kv.h" // Include the kv-store library

// Function to generate a random key
std::string generate_random_key() {
    static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string key(10, ' ');
    for (int i = 0; i < 10; ++i) {
        key[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return key;
}

// Function to generate a random value
std::string generate_random_value() {
    return "value_" + generate_random_key(); // Just append a random key as a value
}

// Function to get the total CPU time from /proc/stat
long get_total_cpu_time() {
    std::ifstream stat_file("/proc/stat");
    std::string line;
    long total_time = 0;

    if (stat_file.is_open()) {
        getline(stat_file, line); // Read the first line (the "cpu" line)
        stat_file.close();

        std::istringstream ss(line);
        std::string cpu_label;
        long time_value;
        ss >> cpu_label; // Skip the "cpu" label

        // Sum the times (user, nice, system, idle, etc.)
        while (ss >> time_value) {
            total_time += time_value;
        }
    }

    return total_time;
}

// Function to read the CPU usage of a process from /proc/[pid]/stat
long get_process_cpu_time(pid_t pid) {
    std::ifstream stat_file("/proc/" + std::to_string(pid) + "/stat");
    std::string line;

    if (stat_file.is_open()) {
        getline(stat_file, line);
        stat_file.close();
        
        std::istringstream ss(line);
        std::vector<std::string> stat_fields;
        std::string field;
        
        // Split the line into fields
        while (ss >> field) {
            stat_fields.push_back(field);
        }

        long utime = std::stol(stat_fields[13]); // User time
        long stime = std::stol(stat_fields[14]); // System time
        return utime + stime; // Return total CPU time (user + system)
    }
    return -1;
}

// Function to check if one string is a prefix of another
bool is_prefix(const std::string &prefix, const std::string &str) {
    return str.substr(0, prefix.size()) == prefix;
}

// Function to find PIDs of processes with a name that starts with the given prefix
std::vector<pid_t> find_pids_by_name_prefix(const std::string& process_prefix) {
    std::vector<pid_t> pids;

    // Iterate over all process directories in /proc
    for (const auto& entry : std::filesystem::directory_iterator("/proc")) {
        if (entry.is_directory()) {
            const std::string pid_str = entry.path().filename().string();

            // Check if the directory name is a PID
            if (std::all_of(pid_str.begin(), pid_str.end(), ::isdigit)) {
                pid_t pid = std::stoi(pid_str);

                // Read the executable name from /proc/[pid]/comm
                std::ifstream comm_file("/proc/" + pid_str + "/comm");
                std::string comm_name;
                if (comm_file.is_open()) {
                    getline(comm_file, comm_name);
                    comm_file.close();

                    // If the executable name starts with the given prefix, add the PID to the list
                    if (is_prefix(process_prefix, comm_name)) {
                        pids.push_back(pid);
                    }
                }
            }
        }
    }

    return pids;
}

// Function to log CPU utilization percentage of up to 100 processes to a CSV file
void log_cpu_utilization_to_csv(const std::string& process_name, int interval_ms, int iterations, const std::string& output_file) {
    std::ofstream csv_file(output_file);

    if (!csv_file.is_open()) {
        std::cerr << "Error opening CSV output file: " << output_file << std::endl;
        return;
    }

    // Write the CSV header
    csv_file << "timestamp";
    for (int i = 1; i <= 100; ++i) {
        csv_file << ",pid" << i;
    }
    csv_file << std::endl;

    for (int i = 0; i < iterations; ++i) {
        // Find the PIDs of processes with the given name
        std::vector<pid_t> pids = find_pids_by_name_prefix(process_name);

        if (pids.empty()) {
            std::cerr << "No processes found with name: " << process_name << std::endl;
            return;
        }

        // Limit the number of processes to 100 (if there are more)
        if (pids.size() > 100) {
            pids.resize(100);
        }

        // Get the current total CPU time for the system
        long total_cpu_time_1 = get_total_cpu_time();

        // Get the current process CPU times
        std::vector<long> process_cpu_times_1;
        for (const pid_t& pid : pids) {
            process_cpu_times_1.push_back(get_process_cpu_time(pid));
        }

        // Sleep for the interval duration
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));

        // Get the new total CPU time for the system
        long total_cpu_time_2 = get_total_cpu_time();

        // Get the new process CPU times
        std::vector<long> process_cpu_times_2;
        for (const pid_t& pid : pids) {
            process_cpu_times_2.push_back(get_process_cpu_time(pid));
        }

        // Calculate CPU utilization percentage
        long total_cpu_diff = total_cpu_time_2 - total_cpu_time_1;

        // Get the current timestamp
        auto now = std::chrono::system_clock::now();
        auto now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_c);

        // Write the timestamp to the CSV file
        csv_file << std::put_time(now_tm, "%Y-%m-%d %H:%M:%S");

        // Calculate and log the CPU usage for each process
        for (size_t j = 0; j < pids.size(); ++j) {
            long process_cpu_diff = process_cpu_times_2[j] - process_cpu_times_1[j];
            double cpu_usage = (double(process_cpu_diff) / total_cpu_diff) * 100;
            csv_file << "," << cpu_usage;
        }

        // Fill remaining columns if there are fewer than 100 processes
        for (size_t j = pids.size(); j < 100; ++j) {
            csv_file << ",-1";
        }

        csv_file << std::endl;
        csv_file.flush(); // Ensure data is written to disk
    }

    csv_file.close();
}

// Function to perform random get/put operations on the key-value store for 1000 cycles
void perform_operations(int cycles) {
    std::string key, value, old_value;

    // store the keys previously put
    std::vector<std::string> keys;
    
    for (int i = 0; i < cycles; ++i) {
        int operation = rand() % 2; // Randomly choose between get (0) and put (1)

        if (operation == 0 && !keys.empty()) {
            key = keys[rand() % keys.size()];
            // Perform a get operation
            kv739_get(key, value);
        } else {
            // Perform a put operation
            key = generate_random_key();
            value = generate_random_value();
            kv739_put(key, value, old_value);
            keys.push_back(key);
        }
        // Print the iteration number and operation
        std::cout << "Iteration: " << i << ", Operation: " << (operation == 0 ? "GET" : "PUT") << ", Key: " << key << ", Value: " << value << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(50)); // Simulate time between operations
    }
}

int main() {
    // Name of the executable (as appears in /proc/[pid]/comm)
    std::string process_name = "python3";

    // Initialize the key-value store
    std::string config_file = "chain_config.txt";
    kv739_init(config_file);

    // Start a thread to monitor CPU utilization
    int interval_ms = 500;  // Monitor every 500ms
    int iterations = 2000;  // Monitor for 2000 cycles
    std::string output_file = "cpu_usage_log.csv";

    std::thread monitor_thread(log_cpu_utilization_to_csv, process_name, interval_ms, iterations, output_file);

    // Perform 1000 random get/put operations
    int operation_cycles = 1000;
    perform_operations(operation_cycles);

    // Wait for the monitoring thread to finish
    monitor_thread.join();

    // Shutdown the key-value store
    kv739_shutdown();

    return 0;
}
