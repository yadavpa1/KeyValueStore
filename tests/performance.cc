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
#include <chrono>

#include <sys/wait.h>

#include "lib739kv.h"


#include <map>
#include <stdexcept>

using namespace std;

/*                                       PERFORMANCE MEASURES
TEST CASE 3.1: Write Throughput test
TEST CASE 3.2: Write Latency test
TEST CASE 3.3: Read Throughput test
TEST CASE 3.4: Read Latency test
*****************************************************************************************/

int TOTAL_CYCLES = 1000;

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
        string command = std::string("./") + std::string(server_name);
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

void process_operations_write_tpt(int process_num, const std::string &server_address, std::atomic<bool> &failure_flag, double &throughput){
    if (kv739_init(server_address) != 0) {
        std::cerr << "Failed to initialize client with server at " << server_address << std::endl;
        failure_flag = true;
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;

    auto start_time = std::chrono::high_resolution_clock::now();
    
    while (cycle <= TOTAL_CYCLES) {
        std::string key = std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle * process_num);
        std::string old_value;

        if (kv739_put(key, value, old_value) == -1) {
            std::cerr << "Failed to put key-value pair: " << key << " -> " << value << std::endl;
            failure_flag = true;
            break;
        }
        cycle++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;

    throughput = TOTAL_CYCLES/elapsed_time.count();
    kv739_shutdown();
    return;
}

void process_operations_read_tpt(int process_num, const std::string &server_address, std::atomic<bool> &failure_flag, double &throughput){
    if (kv739_init(server_address) != 0) {
        std::cerr << "Failed to initialize client with server at " << server_address << std::endl;
        failure_flag = true;
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;

    auto start_time = std::chrono::high_resolution_clock::now();
    
    while (cycle <= TOTAL_CYCLES) {
        std::string key = std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle * process_num);
        std::string old_value;

        if (kv739_get(key, value)!=0) {
            std::cerr << "Failed to get key-value pair: " << key << " -> " << value << std::endl;
            failure_flag = true;
            break;
        }
        cycle++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;

    throughput = TOTAL_CYCLES/elapsed_time.count();
    kv739_shutdown();
    return;
}

void process_operations_write_lat(int process_num, const std::string &server_address, std::atomic<bool> &failure_flag, double &latency){
    if (kv739_init(server_address) != 0) {
        std::cerr << "Failed to initialize client with server at " << server_address << std::endl;
        failure_flag = true;
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;

    auto start_time = std::chrono::high_resolution_clock::now();
    
    while (cycle <= TOTAL_CYCLES) {
        std::string key = std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle * process_num);
        std::string old_value;

        if (kv739_put(key, value, old_value) == -1) {
            std::cerr << "Failed to put key-value pair: " << key << " -> " << value << std::endl;
            failure_flag = true;
            break;
        }
        cycle++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;

    latency = elapsed_time.count();
    kv739_shutdown();
    return;
}

void process_operations_read_lat(int process_num, const std::string &server_address, std::atomic<bool> &failure_flag, double &latency){
    if (kv739_init(server_address) != 0) {
        std::cerr << "Failed to initialize client with server at " << server_address << std::endl;
        failure_flag = true;
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;

    auto start_time = std::chrono::high_resolution_clock::now();
    
    while (cycle <= TOTAL_CYCLES) {
        std::string key = std::to_string(rnd_dis(gen));
        std::string value;

        if (kv739_get(key, value)!=0) {
            std::cerr << "Failed to get key-value pair: " << key << std::endl;
            failure_flag = true;
            break;
        }
        cycle++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;

    latency = elapsed_time.count();
    kv739_shutdown();
    return;
}

void process_operations_hot_keys(int process_num, const std::string &server_address, const std::vector<std::string> &key_set, std::atomic<bool> &failure_flag, bool is_server_fail){
    if (kv739_init(server_address) != 0) {
        std::cerr << "Failed to initialize client with server at " << server_address << std::endl;
        failure_flag = true;
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::uniform_int_distribution<> dis_key(0, key_set.size());
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;
    
    while (cycle <= TOTAL_CYCLES) {
        float random_num = dis(gen);
        float key_index = dis_key(gen);
        std::string key = (random_num < 0.9) ? key_set[key_index] : std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle * process_num);
        std::string old_value;

        if (kv739_put(key, value, old_value) == -1) {
            std::cerr << "Failed to put key-value pair: " << key << " -> " << value << std::endl;
            failure_flag = true;
            break;
        }
        cycle++;
    }

    kv739_shutdown();
    return;
}

void process_operations_write_tpthk(int process_num, const std::string &server_address, const std::vector<string> &key_set, std::atomic<bool> &failure_flag, double &throughput){
    if (kv739_init(server_address) != 0) {
        std::cerr << "Failed to initialize client with server at " << server_address << std::endl;
        failure_flag = true;
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::uniform_int_distribution<> dis_key(0, key_set.size()-1);
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;
    
    auto start_time = std::chrono::high_resolution_clock::now();

    while (cycle <= TOTAL_CYCLES) {
        float random_num = dis(gen);
        float key_index = dis_key(gen);
        std::string key = (random_num < 0.9) ? key_set[key_index] : std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle * process_num);
        std::string old_value;

        if (kv739_put(key, value, old_value) == -1) {
            std::cerr << "Failed to put key-value pair: " << key << " -> " << value << std::endl;
            failure_flag = true;
            break;
        }
        cycle++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;

    throughput = TOTAL_CYCLES/elapsed_time.count();
    kv739_shutdown();
    return;
}

void process_operations_write_lathk(int process_num, const std::string &server_address, const std::vector<string> &key_set, std::atomic<bool> &failure_flag, double &latency){
    if (kv739_init(server_address) != 0) {
        std::cerr << "Failed to initialize client with server at " << server_address << std::endl;
        failure_flag = true;
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::uniform_int_distribution<> dis_key(0, key_set.size()-1);
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;
    
    auto start_time = std::chrono::high_resolution_clock::now();

    while (cycle <= TOTAL_CYCLES) {
        float random_num = dis(gen);
        float key_index = dis_key(gen);
        std::string key = (random_num < 0.9) ? key_set[key_index] : std::to_string(rnd_dis(gen));
        std::string value = std::to_string(cycle * process_num);
        std::string old_value;

        if (kv739_put(key, value, old_value) == -1) {
            std::cerr << "Failed to put key-value pair: " << key << " -> " << value << std::endl;
            failure_flag = true;
            break;
        }
        cycle++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;

    latency = elapsed_time.count();
    kv739_shutdown();
    return;
}

void process_operations_read_tpthk(int process_num, const std::string &server_address, const std::vector<string> &key_set, std::atomic<bool> &failure_flag, double &throughput){
    if (kv739_init(server_address) != 0) {
        std::cerr << "Failed to initialize client with server at " << server_address << std::endl;
        failure_flag = true;
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::uniform_int_distribution<> dis_key(0, key_set.size()-1);
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;
    
    auto start_time = std::chrono::high_resolution_clock::now();

    while (cycle <= TOTAL_CYCLES) {
        float random_num = dis(gen);
        float key_index = dis_key(gen);
        std::string key = (random_num < 0.9) ? key_set[key_index] : std::to_string(rnd_dis(gen));
        std::string value;

        if (kv739_get(key, value) != 0) {
            std::cerr << "Failed to get key-value pair: " << key << std::endl;
            failure_flag = true;
            break;
        }
        cycle++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;

    throughput = TOTAL_CYCLES/elapsed_time.count();
    kv739_shutdown();
    return;
}

void process_operations_read_lathk(int process_num, const std::string &server_address, const std::vector<string> &key_set, std::atomic<bool> &failure_flag, double &latency){
    if (kv739_init(server_address) != 0) {
        std::cerr << "Failed to initialize client with server at " << server_address << std::endl;
        failure_flag = true;
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::uniform_int_distribution<> dis_key(0, key_set.size()-1);
    std::uniform_int_distribution<> rnd_dis(0, 1e5);
    int cycle = 1;
    
    auto start_time = std::chrono::high_resolution_clock::now();

    while (cycle <= TOTAL_CYCLES) {
        float random_num = dis(gen);
        float key_index = dis_key(gen);
        std::string key = (random_num < 0.9) ? key_set[key_index] : std::to_string(rnd_dis(gen));
        std::string value;

        if (kv739_get(key, value) != 0) {
            std::cerr << "Failed to get key-value pair: " << key << std::endl;
            failure_flag = true;
            break;
        }
        cycle++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;

    latency = elapsed_time.count();
    kv739_shutdown();
    return;
}

/* 
TESTS BEGIN HERE
*/

/*
############################# NORMAL ACCESS #######################################
*/
void run_throughput_norm_write(std::string server_address, std::string server_name, int num_processes){
    std::atomic<bool> failure_flag(false);

    // Start the server
    if(start_server(server_name.c_str()) == false){
        std::cerr << "Whoopsie!" << endl;
        return;
    }

    sleep(5);
    // Get process ID of the server
    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        cout << "FAILED TO GET PID OF SERVER" << endl;
        return;
    }

    int pipes[num_processes][2];
    for(int i = 0;i<num_processes;i++){
        if(pipe(pipes[i])==-1){
            std::cerr << "Pipe creation failed for process" << i+1 << endl;
        }
    }

    // Fork child processes
    for (int i = 1; i <= num_processes; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            close(pipes[i-1][0]);
            // Child process
            double throughput = 0.0;
            process_operations_write_tpt(i, server_address, failure_flag, throughput);
            if(write(pipes[i-1][1], &throughput, sizeof(throughput))==-1){
                std::cerr << "FAILED TO WRITE THROUGHPUT FROM PROCESS " << i << endl;
            }
            close(pipes[i-1][1]);
            exit(0);
        } else {
            close(pipes[i-1][1]);
        }
    }

    double total_throughput = 0.0;
    for(int i = 0;i<num_processes;i++){
        double throughput = 0.0;
        if(read(pipes[i][0], &throughput, sizeof(throughput))==-1){
            std::cerr << "FAILED TO READ THROUGHPUT FROM PROCESS " << i+1 << endl;
        }
        total_throughput += throughput;
        close(pipes[i][0]);
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
        shutdown_server(server_pid);
        cout << "FAILURE OCCURRED AT ONE OF THE CHILDREN" << endl;
        return;
    }

    std::cout << "Total throughput for " << num_processes << " processes: " << total_throughput << " operations per second" << std::endl;
    // Get the process ID of the server
    server_pid = get_pid_of_server(server_name.c_str());
    shutdown_server(server_pid);
    return;
}

void run_latency_norm_write(std::string server_address, std::string server_name, int num_processes){
    std::atomic<bool> failure_flag(false);

    // Start the server
    if(start_server(server_name.c_str()) == false){
        std::cerr << "Whoopsie!" << endl;
        return;
    }

    sleep(5);
    // Get process ID of the server
    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        cout << "FAILED TO GET PID OF SERVER" << endl;
        return;
    }

    int pipes[num_processes][2];
    for(int i = 0;i<num_processes;i++){
        if(pipe(pipes[i])==-1){
            std::cerr << "Pipe creation failed for process" << i+1 << endl;
        }
    }

    // Fork child processes
    for (int i = 1; i <= num_processes; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            close(pipes[i-1][0]);
            // Child process
            double latency = 0.0;
            process_operations_write_lat(i, server_address, failure_flag, latency);
            if(write(pipes[i-1][1], &latency, sizeof(latency))==-1){
                std::cerr << "FAILED TO WRITE LATENCY FROM PROCESS " << i << endl;
            }
            close(pipes[i-1][1]);
            exit(0);
        } else {
            close(pipes[i-1][1]);
        }
    }

    double total_latency = 0.0;
    for(int i = 0;i<num_processes;i++){
        double latency = 0.0;
        if(read(pipes[i][0], &latency, sizeof(latency))==-1){
            std::cerr << "FAILED TO READ LATENCY FROM PROCESS " << i+1 << endl;
        }
        total_latency += latency;
        close(pipes[i][0]);
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
        shutdown_server(server_pid);
        cout << "FAILURE OCCURRED AT ONE OF THE CHILDREN" << endl;
        return;
    }

    std::cout << "Total latency for " << num_processes << " processes: " << (double)(total_latency/(double)num_processes)/TOTAL_CYCLES << "s per operation" << std::endl;
    // Get the process ID of the server
    server_pid = get_pid_of_server(server_name.c_str());
    shutdown_server(server_pid);
    return;
}

void run_throughput_norm_read(std::string server_address, std::string server_name, int num_processes){
    std::atomic<bool> failure_flag(false);

    // Start the server
    if(start_server(server_name.c_str()) == false){
        std::cerr << "Whoopsie!" << endl;
        return;
    }

    sleep(5);
    // Get process ID of the server
    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        cout << "FAILED TO GET PID OF SERVER" << endl;
        return;
    }

    int pipes[num_processes][2];
    for(int i = 0;i<num_processes;i++){
        if(pipe(pipes[i])==-1){
            std::cerr << "Pipe creation failed for process" << i+1 << endl;
        }
    }

    // Fork child processes
    for (int i = 1; i <= num_processes; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            close(pipes[i-1][0]);
            // Child process
            double throughput = 0.0;
            process_operations_read_tpt(i, server_address, failure_flag, throughput);
            if(write(pipes[i-1][1], &throughput, sizeof(throughput))==-1){
                std::cerr << "FAILED TO WRITE THROUGHPUT FROM PROCESS " << i << endl;
            }
            close(pipes[i-1][1]);
            exit(0);
        } else {
            close(pipes[i-1][1]);
        }
    }

    double total_throughput = 0.0;
    for(int i = 0;i<num_processes;i++){
        double throughput = 0.0;
        if(read(pipes[i][0], &throughput, sizeof(throughput))==-1){
            std::cerr << "FAILED TO READ THROUGHPUT FROM PROCESS " << i+1 << endl;
        }
        total_throughput += throughput;
        close(pipes[i][0]);
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
        shutdown_server(server_pid);
        cout << "FAILURE OCCURRED AT ONE OF THE CHILDREN" << endl;
        return;
    }

    std::cout << "Total throughput for " << num_processes << " processes: " << total_throughput << " operations per second" << std::endl;
    // Get the process ID of the server
    server_pid = get_pid_of_server(server_name.c_str());
    shutdown_server(server_pid);
    return;
}

void run_latency_norm_read(std::string server_address, std::string server_name, int num_processes){
    std::atomic<bool> failure_flag(false);

    // Start the server
    if(start_server(server_name.c_str()) == false){
        std::cerr << "Whoopsie!" << endl;
        return;
    }

    sleep(5);
    // Get process ID of the server
    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        cout << "FAILED TO GET PID OF SERVER" << endl;
        return;
    }

    int pipes[num_processes][2];
    for(int i = 0;i<num_processes;i++){
        if(pipe(pipes[i])==-1){
            std::cerr << "Pipe creation failed for process" << i+1 << endl;
        }
    }

    // Fork child processes
    for (int i = 1; i <= num_processes; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            close(pipes[i-1][0]);
            // Child process
            double latency = 0.0;
            process_operations_read_lat(i, server_address, failure_flag, latency);
            if(write(pipes[i-1][1], &latency, sizeof(latency))==-1){
                std::cerr << "FAILED TO WRITE LATENCY FROM PROCESS " << i << endl;
            }
            close(pipes[i-1][1]);
            exit(0);
        } else {
            close(pipes[i-1][1]);
        }
    }

    double total_latency = 0.0;
    for(int i = 0;i<num_processes;i++){
        double latency = 0.0;
        if(read(pipes[i][0], &latency, sizeof(latency))==-1){
            std::cerr << "FAILED TO READ LATENCY FROM PROCESS " << i+1 << endl;
        }
        total_latency += latency;
        close(pipes[i][0]);
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
        shutdown_server(server_pid);
        cout << "FAILURE OCCURRED AT ONE OF THE CHILDREN" << endl;
        return;
    }

    std::cout << "Total latency for " << num_processes << " processes: " << (double)(total_latency/(double)num_processes)/TOTAL_CYCLES << "s per operation" << std::endl;
    // Get the process ID of the server
    server_pid = get_pid_of_server(server_name.c_str());
    shutdown_server(server_pid);
    return;
}

/*
############################## HOT ACCESS #########################################
*/
void run_throughput_hk_write(std::string server_address, std::string server_name, int num_processes){
    std::atomic<bool> failure_flag(false);

    std::vector<string> key_set;
    for(int i = 1;i<=num_processes;i++){
        key_set.push_back("k"+std::to_string(i));
    }

    // Start the server
    if(start_server(server_name.c_str()) == false){
        std::cerr << "Whoopsie!" << endl;
        return;
    }

    sleep(5);
    // Get process ID of the server
    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        cout << "FAILED TO GET PID OF SERVER" << endl;
        return;
    }

    int pipes[num_processes][2];
    for(int i = 0;i<num_processes;i++){
        if(pipe(pipes[i])==-1){
            std::cerr << "Pipe creation failed for process" << i+1 << endl;
        }
    }

    // Fork child processes
    for (int i = 1; i <= num_processes; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            close(pipes[i-1][0]);
            // Child process
            double throughput = 0.0;
            process_operations_write_tpthk(i, server_address, key_set, failure_flag, throughput);
            if(write(pipes[i-1][1], &throughput, sizeof(throughput))==-1){
                std::cerr << "FAILED TO WRITE THROUGHPUT FROM PROCESS " << i << endl;
            }
            close(pipes[i-1][1]);
            exit(0);
        } else {
            close(pipes[i-1][1]);
        }
    }

    double total_throughput = 0.0;
    for(int i = 0;i<num_processes;i++){
        double throughput = 0.0;
        if(read(pipes[i][0], &throughput, sizeof(throughput))==-1){
            std::cerr << "FAILED TO READ THROUGHPUT FROM PROCESS " << i+1 << endl;
        }
        total_throughput += throughput;
        close(pipes[i][0]);
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
        shutdown_server(server_pid);
        cout << "FAILURE OCCURRED AT ONE OF THE CHILDREN" << endl;
        return;
    }

    std::cout << "Total throughput for " << num_processes << " processes: " << total_throughput << " operations per second" << std::endl;
    // Get the process ID of the server
    server_pid = get_pid_of_server(server_name.c_str());
    shutdown_server(server_pid);
    return;
}

void run_latency_hk_write(std::string server_address, std::string server_name, int num_processes){
    std::atomic<bool> failure_flag(false);

    std::vector<string> key_set;
    for(int i = 1;i<=num_processes;i++){
        key_set.push_back("k"+std::to_string(i));
    }

    // Start the server
    if(start_server(server_name.c_str()) == false){
        std::cerr << "Whoopsie!" << endl;
        return;
    }

    sleep(5);
    // Get process ID of the server
    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        cout << "FAILED TO GET PID OF SERVER" << endl;
        return;
    }

    int pipes[num_processes][2];
    for(int i = 0;i<num_processes;i++){
        if(pipe(pipes[i])==-1){
            std::cerr << "Pipe creation failed for process" << i+1 << endl;
        }
    }

    // Fork child processes
    for (int i = 1; i <= num_processes; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            close(pipes[i-1][0]);
            // Child process
            double latency = 0.0;
            process_operations_write_lathk(i, server_address, key_set, failure_flag, latency);
            if(write(pipes[i-1][1], &latency, sizeof(latency))==-1){
                std::cerr << "FAILED TO WRITE LATENCY FROM PROCESS " << i << endl;
            }
            close(pipes[i-1][1]);
            exit(0);
        } else {
            close(pipes[i-1][1]);
        }
    }

    double total_latency = 0.0;
    for(int i = 0;i<num_processes;i++){
        double latency = 0.0;
        if(read(pipes[i][0], &latency, sizeof(latency))==-1){
            std::cerr << "FAILED TO READ LATENCY FROM PROCESS " << i+1 << endl;
        }
        total_latency += latency;
        close(pipes[i][0]);
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
        shutdown_server(server_pid);
        cout << "FAILURE OCCURRED AT ONE OF THE CHILDREN" << endl;
        return;
    }

    std::cout << "Average latency for " << num_processes << " processes: " << (float)(total_latency/(float)num_processes)/1000 << "s per operation" << std::endl;
    // Get the process ID of the server
    server_pid = get_pid_of_server(server_name.c_str());
    shutdown_server(server_pid);
    return;
}

void run_throughput_hk_read(std::string server_address, std::string server_name, int num_processes){
    std::atomic<bool> failure_flag(false);

    std::vector<string> key_set;
    for(int i = 1;i<=num_processes;i++){
        key_set.push_back("k"+std::to_string(i));
    }

    // Start the server
    if(start_server(server_name.c_str()) == false){
        std::cerr << "Whoopsie!" << endl;
        return;
    }

    sleep(5);
    // Get process ID of the server
    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        cout << "FAILED TO GET PID OF SERVER" << endl;
        return;
    }

    int pipes[num_processes][2];
    for(int i = 0;i<num_processes;i++){
        if(pipe(pipes[i])==-1){
            std::cerr << "Pipe creation failed for process" << i+1 << endl;
        }
    }

    // Fork child processes
    for (int i = 1; i <= num_processes; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            close(pipes[i-1][0]);
            // Child process
            double throughput = 0.0;
            process_operations_read_tpthk(i, server_address, key_set, failure_flag, throughput);
            if(write(pipes[i-1][1], &throughput, sizeof(throughput))==-1){
                std::cerr << "FAILED TO WRITE THROUGHPUT FROM PROCESS " << i << endl;
            }
            close(pipes[i-1][1]);
            exit(0);
        } else {
            close(pipes[i-1][1]);
        }
    }

    double total_throughput = 0.0;
    for(int i = 0;i<num_processes;i++){
        double throughput = 0.0;
        if(read(pipes[i][0], &throughput, sizeof(throughput))==-1){
            std::cerr << "FAILED TO READ THROUGHPUT FROM PROCESS " << i+1 << endl;
        }
        total_throughput += throughput;
        close(pipes[i][0]);
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
        shutdown_server(server_pid);
        cout << "FAILURE OCCURRED AT ONE OF THE CHILDREN" << endl;
        return;
    }

    std::cout << "Total throughput for " << num_processes << " processes: " << total_throughput << " operations per second" << std::endl;
    // Get the process ID of the server
    server_pid = get_pid_of_server(server_name.c_str());
    shutdown_server(server_pid);
    return;
}

void run_latency_hk_read(std::string server_address, std::string server_name, int num_processes){
    std::atomic<bool> failure_flag(false);

    std::vector<string> key_set;
    for(int i = 1;i<=num_processes;i++){
        key_set.push_back("k"+std::to_string(i));
    }

    // Start the server
    if(start_server(server_name.c_str()) == false){
        std::cerr << "Whoopsie!" << endl;
        return;
    }

    sleep(5);
    // Get process ID of the server
    pid_t server_pid = get_pid_of_server(server_name.c_str());
    if(server_pid == -1){
        cout << "FAILED TO GET PID OF SERVER" << endl;
        return;
    }

    int pipes[num_processes][2];
    for(int i = 0;i<num_processes;i++){
        if(pipe(pipes[i])==-1){
            std::cerr << "Pipe creation failed for process" << i+1 << endl;
        }
    }

    // Fork child processes
    for (int i = 1; i <= num_processes; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            close(pipes[i-1][0]);
            // Child process
            double latency = 0.0;
            process_operations_read_lathk(i, server_address, key_set, failure_flag, latency);
            if(write(pipes[i-1][1], &latency, sizeof(latency))==-1){
                std::cerr << "FAILED TO WRITE LATENCY FROM PROCESS " << i << endl;
            }
            close(pipes[i-1][1]);
            exit(0);
        } else {
            close(pipes[i-1][1]);
        }
    }

    double total_latency = 0.0;
    for(int i = 0;i<num_processes;i++){
        double latency = 0.0;
        if(read(pipes[i][0], &latency, sizeof(latency))==-1){
            std::cerr << "FAILED TO READ LATENCY FROM PROCESS " << i+1 << endl;
        }
        total_latency += latency;
        close(pipes[i][0]);
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
        shutdown_server(server_pid);
        cout << "FAILURE OCCURRED AT ONE OF THE CHILDREN" << endl;
        return;
    }

    std::cout << "Average latency for " << num_processes << " processes: " << (double)((double)total_latency/num_processes)/1000 << "s per operation" << std::endl;
    // Get the process ID of the server
    server_pid = get_pid_of_server(server_name.c_str());
    shutdown_server(server_pid);
    return;
}

/*
TESTS END HERE
*/

std::map<std::string, std::string> parse_arguments(int argc, char* argv[]) {
    std::map<std::string, std::string> args;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-a" || arg == "--address") {
            if (i + 1 < argc) {
                args["address"] = argv[++i];
            } else {
                throw std::invalid_argument("Missing value for argument: " + arg);
            }
        } else if (arg == "-f" || arg == "--file") {
            if (i + 1 < argc) {
                args["file"] = argv[++i];
            } else {
                throw std::invalid_argument("Missing value for argument: " + arg);
            }
        } else if (arg == "-p" || arg == "--processes") {
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
        auto cl_options = parse_arguments(argc, argv);

        std::string server_address = cl_options["address"];
        std::string server_name = cl_options["file"];
        int num_processes = std::stoi(cl_options["processes"]);
        std::vector<std::string> key_set;
        for(int i = 1;i<=num_processes;++i){
            key_set.push_back("k"+std::to_string(i));
        }

        // run_throughput_norm_write(server_address, server_name, num_processes);
        // run_latency_norm_write(server_address, server_name, num_processes);
        // run_latency_norm_read(server_address, server_name, num_processes);
        // run_throughput_norm_read(server_address, server_name, num_processes);
        // run_throughput_hk_write(server_address, server_name, num_processes);
        // run_latency_hk_write(server_address, server_name, num_processes);
        run_throughput_hk_read(server_address, server_name, num_processes);
        // run_latency_hk_read(server_address, server_name, num_processes);

        std::cout << "\n=== Test Summary ===\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
