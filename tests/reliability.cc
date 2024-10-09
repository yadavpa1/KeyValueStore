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
#include "lib739kv.h"
#include <cxxopts.hpp>

using namespace std;

//  Path: tests/reliability_guarantees.cc
/*                                       RELIABILITY GUARANTEES        
This file contains the implementation of the first reliability test case.
This test case basically spins up multiple threads and each thread performs a series of operations on the key-value store.
The operations are such that the 90-10% rule is followed. 90% of the operations are to 10% of the keys.
....

Now after cycling through about 1000 operations, the server is shut down. This is done to test the reliability of the server.
The server is then restarted and the client checks if the keys contain the correct values.

The test case passes if all the keys contain the correct values at the end of the test case.
The test case fails if any of the keys contain incorrect values at the end of the test case.

End of the test case.
Arguments to the test case:
    1. The server address
    2. Name of the server file
    3. Number of threads to spawn
    4. Process ID of the server

TEST CASE 1.1: No concurrent key access but multiple threads concurrent access to different keys
TEST CASE 1.2: Concurrent key access by multiple concurrent threads
TEST CASE 1.3: Concurrent key access by multiple concurrent threads with server restart
*****************************************************************************************/

int passed_tests = 0;
int failed_tests = 0;

int TOTAL_CYCLES = 1000;

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

int get_pid_of_server(const char* server_name) {
    string command = "pidof -s ";
    command += server_name;
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) {
        cerr << "Error: popen failed." << endl;
        return -1;
    }
    char buffer[128];
    string result = "";
    while (!feof(pipe)) {
        if (fgets(buffer, 128, pipe) != NULL) {
            result += buffer;
        }
    }
    pclose(pipe);
    return stoi(result);
}

bool start_server(const char* server_name) {
    pid_t pid = fork();
    if (pid == -1) {
        cerr << "Error: Fork failed." << endl;
        return false;
    }
    if (pid == 0) {
        string command = "./";
        command += server_name;
        system(command.c_str());
        exit(EXIT_FAILURE);
    } else if (pid > 0) {
        sleep(1);
        return true;
    }
    std::cerr << "Error: Fork failed." << std::endl;
    return false;
}

bool shutdown_server(pid_t server_pid) {
    pid_t pid = fork();
    if (pid == -1) {
        cerr << "Error: Fork failed." << endl;
        return false;
    }
    if (pid == 0) {
        string command = "kill -9 ";
        command += to_string(server_pid);
        system(command.c_str());
        exit(EXIT_SUCCESS);
    }
    sleep(1);
    return true;
}

void thread_operations_1_1(int thread_num, const std::string &server_address, const std::vector<std::string> &key_set, std::atomic<bool> &failure_flag){
    if (kv739_init(server_address) != 0) {
        cerr << "Failed to initialize client with server at " << server_address << endl;
        failure_flag = true;
        return;
    }
    std::string key1 = key_set[thread_num-1];
    std::string key2 = key_set[thread_num%key_set.size()];

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;
    while(cycle<=TOTAL_CYCLES){
        // Generate a random number between 0 and 1
        float random_num = dis(gen);
        std::string key = (random_num < 0.9)?key1:std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle*thread_num);
        std::string old_value;

        if(kv739_put(key, value, old_value) == -1){
            cerr << "Failed to put key-value pair: " << key << " -> " << value << endl;
            failure_flag = true;
            break;
        }
        if(random_num<0.9){
            cycle++;
        }
    }
    // shutdown client
    kv739_shutdown();
    return;
}

void thread_operations_1_2(int thread_num, const std::string &server_address, const std::vector<std::string> &key_set, std::atomic<bool> &failure_flag){
    if (kv739_init(server_address) != 0) {
        failure_flag = true;
        return;
    }

    std::string key1 = key_set[std::ceil((float)thread_num/2)];

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;
    while(cycle<=TOTAL_CYCLES){
        float random_num = dis(gen);
        std::string key = (random_num < 0.9)?key1:std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle*thread_num);
        std::string old_value;

        if(kv739_put(key, value, old_value) == -1){
            failure_flag = true;
            break;
        }
        if(random_num<0.9){
            cycle++;
        }
    }
    kv739_shutdown();
    return;
}

void thread_operations_1_3(int thread_num, const std::string &server_address, const std::vector<std::string> &key_set, std::atomic<bool> &failure_flag){
    if (kv739_init(server_address) != 0) {
        failure_flag = true;
        return;
    }

    std::string key1 = key_set[std::ceil((float)thread_num/2)];

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;
    while(cycle<=TOTAL_CYCLES){
        float random_num = dis(gen);
        std::string key = (random_num < 0.9)?key1:std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle*thread_num);
        std::string old_value;

        if(kv739_put(key, value, old_value) == -1){
            failure_flag = true;
            break;
        }
        if(random_num<0.9){
            cycle++;
        }
    }
    // don't shutdown the client
    // as if server fails
    return;
}

bool is_valid_key_value_pair_1_1(const std::string &server_address, const std::vector<std::string> &key_set){
    if (kv739_init(server_address) != 0) {
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

bool is_valid_key_value_pair_1_2(const std::string &server_address, const std::vector<std::string> &key_set){
    if (kv739_init(server_address) != 0) {
        return false;
    }
    // any key kn should be 1000*(2n-1) or 1000*(2n)
    for(const auto &key:key_set){
        std::string value;
        if(kv739_get(key, value) != 0){
            return false;
        }
        int key_num = std::stoi(key.substr(1));
        int value_num = std::stoi(value);
        if(value_num != TOTAL_CYCLES*(2*key_num-1) && value_num != TOTAL_CYCLES*(2*key_num)){
            return false;
        }
    }
    return true;
}

bool is_valid_key_value_pair_1_3(const std::string &server_address, const std::vector<std::string> &key_set){
    if (kv739_init(server_address) != 0) {
        return false;
    }
    // any key kn should be 1000*(2n-1) or 1000*(2n)
    for(const auto &key:key_set){
        std::string value;
        if(kv739_get(key, value) != 0){
            return false;
        }
        int key_num = std::stoi(key.substr(1));
        int value_num = std::stoi(value);
        if(value_num != TOTAL_CYCLES*(2*key_num-1) && value_num != TOTAL_CYCLES*(2*key_num)){
            return false;
        }
    }
    return true;
}

/* 
TESTS BEGIN HERE
*/

void run_no_concurrent_key_access(std::string server_address, std::string server_name, int num_threads, std::vector<std::string> key_set){
    std::atomic<bool> failure_flag(false);

    std::vector<std::thread> threads;
    // Start the server
    if(start_server(server_name.c_str()) == false){
        print_test_result("Test 1.1: No concurrent key access but multiple threads concurrent access to different keys", false);
        failed_tests++;
        return;
    }
    // Get process ID of the server
    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        print_test_result("Test 1.1: No concurrent key access but multiple threads concurrent access to different keys", false);
        failed_tests++;
        return;
    }

    for(int i = 1;i<=num_threads;++i){
        threads.push_back(std::thread(thread_operations_1_1, i, server_address, key_set, std::ref(failure_flag)));
    }

    for(auto &thread:threads){
        thread.join();
    }

    if(failure_flag){
        shutdown_server(server_pid);
        print_test_result("Test 1.1: No concurrent key access but multiple threads concurrent access to different keys", false);
        failed_tests++;
        return;
    }
    print_test_result("Test 1.1: No concurrent key access but multiple threads concurrent access to different keys", is_valid_key_value_pair_1_1(server_address, key_set));
    shutdown_server(server_pid);
}

void run_concurrent_key_access(std::string server_address, std::string server_name, int num_threads, std::vector<std::string> key_set) {
    std::atomic<bool> failure_flag(false);

    std::vector<std::thread> threads;

    if(start_server(server_name.c_str()) == false){
        print_test_result("Test 1.2: Concurrent key access by multiple concurrent threads", false);
        failed_tests++;
        return;
    }

    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        print_test_result("Test 1.2: Concurrent key access by multiple concurrent threads", false);
        failed_tests++;
        return;
    }
    for(int i = 1;i<=num_threads;++i){
        threads.push_back(std::thread(thread_operations_1_2, i, server_address, key_set, std::ref(failure_flag)));
    }

    for(auto &thread:threads){
        thread.join();
    }

    if(failure_flag){
        shutdown_server(server_pid);
        print_test_result("Test 1.2: Concurrent key access by multiple concurrent threads", false);
        failed_tests++;
        return;
    }
    print_test_result("Test 1.2: Concurrent key access by multiple concurrent threads", is_valid_key_value_pair_1_2(server_address, key_set));
    shutdown_server(server_pid);
}

void run_concurrent_key_access_with_server_failure(std::string server_address, std::string server_name, int num_threads, std::vector<std::string> key_set) {
    std::atomic<bool> failure_flag(false);

    std::vector<std::thread> threads;

    if(start_server(server_name.c_str()) == false){
        print_test_result("Test 1.3: Concurrent key access by multiple concurrent threads with server restart", false);
        failed_tests++;
        return;
    }

    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        print_test_result("Test 1.3: Concurrent key access by multiple concurrent threads with server restart", false);
        failed_tests++;
        return;
    }
    for(int i = 1;i<=num_threads;++i){
        threads.push_back(std::thread(thread_operations_1_3, i, server_address, key_set, std::ref(failure_flag)));
    }

    for(auto &thread:threads){
        thread.join();
    }

    if(failure_flag){
        shutdown_server(server_pid);
        print_test_result("Test 1.3: Concurrent key access by multiple concurrent threads with server restart", false);
        failed_tests++;
        return;
    }
    // Shutdown the server
    shutdown_server(server_pid);
    // Restart the server
    if(start_server(server_name.c_str()) == false){
        print_test_result("Test 1.3: Concurrent key access by multiple concurrent threads with server restart", false);
        failed_tests++;
        return;
    }

    // Get the process ID of the server
    server_pid = get_pid_of_server(server_name.c_str());

    print_test_result("Test 1.3: Concurrent key access by multiple concurrent threads with server restart", is_valid_key_value_pair_1_3(server_address, key_set));
    shutdown_server(server_pid);
    return;
}

/*
TESTS END HERE
*/

int main(int argc, char* argv[]){
    cxxopts::Options options("reliability_guarantees", "Test the reliability guarantees of the key-value store");
    options.add_options()
        ("a,address", "Server address", cxxopts::value<std::string>())
        ("f,file", "Name of the server file", cxxopts::value<std::string>())
        ("t,threads", "Number of threads to spawn", cxxopts::value<int>());

    auto cl_options = options.parse(argc, argv);

    std::string server_address = cl_options["address"].as<std::string>();
    std::string server_name = cl_options["file"].as<std::string>();
    int num_threads = cl_options["threads"].as<int>();

    std::vector<std::string> key_set;
    for(int i = 1;i<=num_threads;++i){
        key_set.push_back("k"+std::to_string(i));
    }
    run_no_concurrent_key_access(server_address, server_name, num_threads, key_set);
    sleep(1);
    run_concurrent_key_access(server_address, server_name, num_threads, key_set);
    sleep(1);
    run_concurrent_key_access_with_server_failure(server_address, server_name, num_threads, key_set);
    std::cout << "\n=== Test Summary ===\n";
    std::cout << "Total Passed Tests: " << passed_tests << "\n";
    std::cout << "Total Failed Tests: " << failed_tests << "\n";
    return 0;
}