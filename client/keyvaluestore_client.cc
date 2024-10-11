#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include "lib739kv.h"
#include <fstream>
#include <map>
#include <vector>

// Global variables to hold server instance data
std::map<int, std::string> leader_addresses_;  // Maps cluster IDs to current leader addresses
std::map<int, std::vector<std::string>> cluster_instances_;  // Maps cluster IDs to list of nodes
const int num_clusters = 20;  // Number of Raft clusters

void print_welcome()
{
    std::cout << "************************************************************\n";
    std::cout << "*          WELCOME TO CLUSTER CREW STORAGE SERVICES        *\n";
    std::cout << "************************************************************\n";
    std::cout << "*                                                          *\n";
    std::cout << "*  Store your data in one of the world's most durable      *\n";
    std::cout << "*  storage systems! Use the commands below to interact     *\n";
    std::cout << "*  with our services.                                      *\n";
    std::cout << "*                                                          *\n";
    std::cout << "************************************************************\n\n";

    std::cout << "Available Commands:\n";
    std::cout << "-------------------\n";
    std::cout << "  \033[1;32minit <service_instance_file>\033[0m  - Initialize connections to servers (eg. init service_instances.txt )\n";
    std::cout << "  \033[1;32mget <key>\033[0m                      - Retrieve value by key (eg. get 1 )\n";
    std::cout << "  \033[1;32mput\033[0m                            - Store a key-value pair (you will be prompted for input)\n";
    std::cout << "  \033[1;32mshutdown\033[0m                       - Shut down the servers\n";
    std::cout << "  \033[1;32mquit\033[0m                           - Exit the client\n";
}

void print_quit()
{
    std::cout << "\n**************************************************************\n";
    std::cout << "*   Thank you for choosing Cluster Crew for your storage     *\n";
    std::cout << "**************************************************************\n\n";
}

// Function to read service instances (host:port) from a file and map them to clusters
bool init_servers(const std::string &file_name)
{
    std::ifstream file(file_name);
    if (!file.is_open())
    {
        std::cerr << "Error: Unable to open service instance file: " << file_name << std::endl;
        return false;
    }

    std::string instance;
    int current_cluster = 0;
    int instance_count = 0;

    while (std::getline(file, instance))
    {
        if (!instance.empty())
        {
            cluster_instances_[current_cluster].push_back(instance);
            instance_count++;

            // Assign the first node in each cluster as the initial leader (can change dynamically later)
            if (cluster_instances_[current_cluster].size() == 1)
            {
                leader_addresses_[current_cluster] = instance;
            }

            // Move to the next cluster after adding 5 nodes
            if (instance_count % 5 == 0)
            {
                current_cluster++;
            }
        }
    }

    file.close();

    if (instance_count < num_clusters * 5)
    {
        std::cerr << "Error: Service instance file contains fewer instances than expected." << std::endl;
        return false;
    }

    // Initialize the kv739 client
    if (kv739_init(file_name) != 0)
    {
        std::cerr << "Failed to initialize client." << std::endl;
        return false;
    }

    return true;
}

// Function to hash the key and map it to the correct Raft cluster
int get_cluster_id(const std::string &key)
{
    std::hash<std::string> hasher;
    return hasher(key) % num_clusters;
}

int main(int argc, char **argv)
{
    std::string command, key, value;
    std::string old_value, retrieved_value;
    std::string service_instance_file = "service_instances.txt";

    print_welcome();

    while (true)
    {
        std::cout << "\n> ";
        std::getline(std::cin, command);

        // Parse the input command
        std::istringstream iss(command);
        std::string cmd;
        iss >> cmd;

        if (cmd == "init")
        {
            iss >> service_instance_file;
            if (!service_instance_file.empty())
            {
                if (!init_servers(service_instance_file))
                {
                    std::cerr << "Failed to initialize servers from file: " << service_instance_file << std::endl;
                }
            }
            else
            {
                std::cerr << "Usage: init <service_instance_file>" << std::endl;
            }
        }
        else if (cmd == "get")
        {
            // Extract the entire rest of the line as the key (handles spaces)
            key = command.substr(command.find(" ") + 1);
            if (!key.empty())
            {
                int cluster_id = get_cluster_id(key);  // Determine the cluster based on the key
                std::cout << "Sending get request to cluster: " << cluster_id << std::endl;
                
                if (kv739_get(key, retrieved_value) != 0)
                {
                    std::cerr << "Failed to get value for key: " << key << std::endl;
                }
                else
                {
                    std::cout << "Value retrieved: " << retrieved_value << std::endl;
                }
            }
            else
            {
                std::cerr << "Usage: get <key>" << std::endl;
            }
        }
        else if (cmd == "put")
        {
            // Prompt the user to enter the key and value interactively
            std::cout << "Enter key: ";
            std::getline(std::cin, key);
            if (key.empty())
            {
                std::cerr << "Error: Key cannot be empty." << std::endl;
                continue;
            }

            std::cout << "Enter value: ";
            std::getline(std::cin, value);
            if (value.empty())
            {
                std::cerr << "Error: Value cannot be empty." << std::endl;
                continue;
            }

            int cluster_id = get_cluster_id(key);  // Determine the cluster based on the key
            std::cout << "Sending put request to cluster: " << cluster_id << std::endl;

            if (kv739_put(key, value, old_value) == -1)
            {
                std::cerr << "Failed to put key-value pair: " << key << " -> " << value << std::endl;
            }
            else
            {
                std::cout << "Previous value: " << old_value << std::endl;
            }
        }
        else if (cmd == "shutdown")
        {
            if (kv739_shutdown() != 0)
            {
                std::cerr << "Failed to shut down the servers." << std::endl;
            }
        }
        else if (cmd == "quit")
        {
            std::cout << "Exiting client..." << std::endl;
            print_quit();
            break;
        }
        else
        {
            print_welcome();
        }
    }

    return 0;
}
