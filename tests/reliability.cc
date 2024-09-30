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
The operations are as follows:
Thread 1: Switches between writing and reading the keys k1 and k2 90% of the time and a random key 10% of the time
Thread 2: Switches between writing and reading the keys k2 and k3 90% of the time and a random key 10% of the time
Thread 3: Switches between writing and reading the keys k3 and k4 90% of the time and a random key 10% of the time
Thread 4: Switches between writing and reading the keys k4 and k5 90% of the time and a random key 10% of the time
....
Thread n: Switches between writing and reading the keys kn and k1 90% of the time and a random key 10% of the time
Now after cycling through about 1000 operations, the server is shut down. This is done to test the reliability of the server.
The server is now restarted and the same operations are performed again. This is done to test the persistence of the server.
The test case passes if all the keys contain the correct values at the end of the test case.
The test case fails if any of the keys contain incorrect values at the end of the test case.
End of the test case.
Arguments to the test case:
    1. The server address
    2. Name of the server file
    3. Number of threads to spawn
    4. Process ID of the server

Returns 0 if the test case passes, -1 if the test case fails
k1, k2, k3, k4.....kn [10, 100, 1000, 10000, 100000]
value iternum*threadnum
*****************************************************************************************/

bool restart_server(const char* server_name) {
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

void thread_operations(int thread_num, const std::string &server_address, const std::vector<std::string> &key_set, int total_cycles, std::atomic<bool> &failure_flag){
    KeyValueStoreClient client;
    if (client.kv739_init(server_address) != 0) {
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
    while(cycle<=total_cycles){
        // Generate a random number between 0 and 1
        float random_num = dis(gen);
        std::string key = (random_num < 0.9)?key1:std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle*thread_num);
        std::string old_value;

        if(client.kv739_put(key, value, old_value) == -1){
            cerr << "Failed to put key-value pair: " << key << " -> " << value << endl;
            failure_flag = true;
            break;
        }
        if(random_num<0.9){
            cycle++;
        }
    }
    // exit without shutting down the client
    // as if the server is down, the client will not be able to shut down
    return;
}

bool is_valid_key_value_pair(const std::string &server_address, const std::vector<std::string> &key_set){
    KeyValueStoreClient client;
    if (client.kv739_init(server_address) != 0) {
        cerr << "Failed to initialize client with server at " << server_address << endl;
        return false;
    }
    // Now each key should basically be a multiple of 1000
    // For any key kn, the multiple should either be n or n+1%key_set.size()
    for(const auto &key:key_set){
        std::string value;
        if(client.kv739_get(key, value) != 0){
            cerr << "Failed to get value for key: " << key << endl;
            return false;
        }
        int key_num = std::stoi(key.substr(1));
        int value_num = std::stoi(value);
        if(value_num%1000 != 0 || value_num/1000 != key_num){
            cerr << "Key: " << key << " has incorrect value: " << value << endl;
            return false;
        }
    }
    return true;
}

int main(int argc, char* argv[]){
    cxxopts::Options options("reliability_guarantees", "Test the reliability guarantees of the key-value store");
    options.add_options()
        ("a,address", "Server address", cxxopts::value<std::string>())
        ("f,file", "Name of the server file", cxxopts::value<std::string>())
        ("t,threads", "Number of threads to spawn", cxxopts::value<int>())
        ("p,pid", "Process ID of the server", cxxopts::value<int>());

    auto cl_options = options.parse(argc, argv);

    std::string server_address = cl_options["address"].as<std::string>();
    std::string server_name = cl_options["file"].as<std::string>();
    int num_threads = cl_options["threads"].as<int>();
    int server_pid = cl_options["pid"].as<int>();

    std::vector<std::string> key_set;
    for(int i = 1;i<=num_threads;++i){
        key_set.push_back("k"+std::to_string(i));
    }

    std::atomic<bool> failure_flag(false);
    int total_cycles = 1000;

    /************ I] Spawn threads *****************/
    std::vector<std::thread> threads;
    for(int i = 1;i<=num_threads;++i){
        threads.push_back(std::thread(thread_operations, i, server_address, key_set, total_cycles, std::ref(failure_flag)));
    }

    // Wait for all threads to finish
    for(auto &thread:threads){
        thread.join();
    }

    if(failure_flag){
        shutdown_server(server_pid);
        std::cerr << "Test case failed." << std::endl;
        return -1;
    }
    
    sleep(10);
    // II] Shutdown the server
    if(!shutdown_server(server_pid)){
        std::cerr << "You didn't provide the correct server process ID. Re-run after checking process id with `ps -ef | grep <server_name>`" << std::endl;
        return -1;
    }

    // III] Restart the server
    if(!restart_server(server_name.c_str())){
        std::cerr << "Failed to restart the server. Make sure that you have the executable in the same directory and don't pass any path." << std::endl;
        return -1;
    }

    // IV] Check if the server has persisted the data
    if(!is_valid_key_value_pair(server_address, key_set)){
        std::cerr << "Test case failed." << std::endl;
        return -1;
    }
    std::cout << "Test case passed." << std::endl;
    return 0;
}