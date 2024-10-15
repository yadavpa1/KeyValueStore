#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <atomic>
#include <cstdlib>
#include <csignal>
#include <unistd.h> // For `kill()`

#include <sys/wait.h>

#include "lib739kv.h"


#include <map>
#include <stdexcept>

using namespace std;

/*                                       RELIABILITY GUARANTEES
TEST CASE 2.1: Non-concurrent key access but multiple processes concurrent access to different keys
TEST CASE 2.2: Concurrent key access by multiple concurrent processes
*****************************************************************************************/

int passed_tests = 0;
int failed_tests = 0;

int TOTAL_CYCLES = 1000;
const std::string CONFIG_FILE="config";

void print_test_result(const std::string &test_name, bool passed)
{
    if (passed)
    {
        std::cout << "    PASS: " << test_name << "\n";
        ++passed_tests;
    }
    else
    {
        std::cerr << "    FAIL: " << test_name << "\n";
        ++failed_tests;
    }
}

void process_operations_non_concurrent(int process_num, const std::vector<std::string> &key_set, std::atomic<bool> &failure_flag){
    if (kv739_init(CONFIG_FILE) != 0) {
        std::cerr << "Failed to initialize client with server at " << CONFIG_FILE << std::endl;
        failure_flag = true;
        return;
    }

    std::string key1 = key_set[process_num - 1];

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;
    
    while (cycle <= TOTAL_CYCLES) {
        float random_num = dis(gen);
        std::string key = (random_num < 0.9) ? key1 : std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle * process_num);
        std::string old_value;

        if (kv739_put(key, value, old_value) == -1) {
            std::cerr << "Failed to put key-value pair: " << key << " -> " << value << std::endl;
            failure_flag = true;
            break;
        }
        if (random_num < 0.9) {
            cycle++;
        }
    }
    return;
}

void process_operations_concurrent(int process_num, const std::vector<std::string> &key_set, std::atomic<bool> &failure_flag){
    if (kv739_init(CONFIG_FILE) != 0) {
        std::cerr << "Failed to initialize client with server at " << CONFIG_FILE << std::endl;
        failure_flag = true;
        return;
    }

    std::string key1 = key_set[process_num - 1];
    std::string key2 = key_set[key_set.size()-process_num];

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;
    
    while (cycle <= TOTAL_CYCLES) {
        float random_num = dis(gen);
        float random_num2 = dis(gen);
        std::string key = (random_num < 0.9) ? (random_num2<0.5)? key1: key2 : std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle * process_num);
        std::string old_value;

        if (kv739_put(key, value, old_value) == -1) {
            std::cerr << "Failed to put key-value pair: " << key << " -> " << value << std::endl;
            failure_flag = true;
            break;
        }

        cout << process_num << " wrote to " << key << " value of " << value << endl;
        if (random_num < 0.9) {
            cycle++;
        }
    }

    return;
}

bool is_valid_key_value_pair_non_concurrent(const std::vector<std::string> &key_set){
    if (kv739_init(CONFIG_FILE) != 0) {
        return false;
    }

    for(const auto &key:key_set){
        std::string value;
        if(kv739_get(key, value) != 0){
            return false;
        }
        int key_num = std::stoi(key.substr(1));
        int value_num = std::stoi(value);
        if(value_num != key_num*TOTAL_CYCLES){
            cerr << "Key: " << key << " has incorrect value: " << value << endl;
            return false;
        }
    }
    return true;
}

bool is_valid_key_value_pair_concurrent(const std::vector<std::string> &key_set){
    if (kv739_init(CONFIG_FILE) != 0) {
        return false;
    }

    int total_keys = key_set.size();

    for(int i = 1;i<=total_keys; ++i){
        std::string key_current = key_set[i-1];
        std::string key_next = key_set[total_keys-i];
        std::string value_current, value_next;

        if(kv739_get(key_current, value_current)!=0 || kv739_get(key_next, value_next)){
            return false;
        }

        cout << key_current << ": " << value_current << endl;
        int value_num_current = std::stoi(value_current);
        int value_num_next = std::stoi(value_next);

        if (value_num_current != i * TOTAL_CYCLES &&
            value_num_next != i * TOTAL_CYCLES &&
            value_num_current != (total_keys-i+1)*TOTAL_CYCLES &&
            value_num_next !=(total_keys-i+1)*TOTAL_CYCLES) {
            std::cerr << i+1 << " process produced erroneous result" << endl; 
            return false;
        }
    }
    return true;
}

/* 
TESTS BEGIN HERE
*/

/*
############################# NON-CONCURRENT KEY ACCESS WITHOUT SERVER FAILURE #######################################
*/
void run_no_concurrent_key_access_wo_server_failure(int num_processes, std::vector<std::string> key_set){
    std::atomic<bool> failure_flag(false);

    // Fork child processes
    for (int i = 1; i <= num_processes; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            // Child process
            process_operations_non_concurrent(i, key_set, failure_flag);
            exit(0);
        }
    }

    // Wait for all child processes to finish
    for (int i = 0; i < num_processes; ++i) {
        int status;
        wait(&status);
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
            failure_flag = true;
        }
    }

    if(failure_flag){
        print_test_result("Test 2.1: No concurrent key access but multiple processes access to different keys", false);
        failed_tests++;
        return;
    }

    print_test_result("Test 2.1: No concurrent key access but multiple processes access to different keys", is_valid_key_value_pair_non_concurrent(key_set));
    return;
}

/*
############################# CONCURRENT KEY ACCESS WITHOUT SERVER FAILURE #######################################
*/
void run_concurrent_key_access_wo_server_failure(int num_processes, std::vector<std::string> key_set){
    std::atomic<bool> failure_flag(false);

    // Fork child processes
    for (int i = 1; i <= num_processes; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            // Child process
            process_operations_concurrent(i, key_set, failure_flag);
            exit(0);
        }
    }

    // Wait for all child processes to finish
    for (int i = 0; i < num_processes; ++i) {
        int status;
        wait(&status);
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
            failure_flag = true;
        }
    }

    if(failure_flag){
        print_test_result("Test 2.2: Concurrent key access but multiple processes access to different keys", false);
        failed_tests++;
        return;
    }

    print_test_result("Test 2.2: Concurrent key access by multiple concurrent processes", is_valid_key_value_pair_concurrent(key_set));
    return;
}

/*
TESTS END HERE
*/

std::map<std::string, std::string> parse_arguments(int argc, char* argv[]) {
    std::map<std::string, std::string> args;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-p" || arg == "--processes") {
            if (i + 1 < argc) {
                args["processes"] = argv[++i];
            } else {
                throw std::invalid_argument("Missing value for argument: " + arg);
            }
        } else {
            throw std::invalid_argument("Unknown argument: " + arg);
        }
    }

    return args;
}

int main(int argc, char* argv[]){
    try {
        passed_tests = 0;
        failed_tests = 0;
        auto cl_options = parse_arguments(argc, argv);

        int num_processes = std::stoi(cl_options["processes"]);
        std::vector<std::string> key_set;
        for(int i = 1;i<=num_processes;++i){
            key_set.push_back("k"+std::to_string(i));
        }

        // run_no_concurrent_key_access_wo_server_failure(num_processes, key_set);
        run_concurrent_key_access_wo_server_failure(num_processes, key_set);

        std::cout << "\n=== Test Summary ===\n";
        std::cout << "Total Passed Tests: " << passed_tests << "\n";
        std::cout << "Total Failed Tests: " << failed_tests << "\n";
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}