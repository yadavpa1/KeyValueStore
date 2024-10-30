#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include "lib739kv.h"

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
    std::cout << "  \033[1;32minit\033[0m                   - Initialize connection to the server (eg. init ) \n";
    std::cout << "  \033[1;32mget <key>\033[0m              - Retrieve value by key (eg. get 1 )\n";
    std::cout << "  \033[1;32mput\033[0m                    - Store a key-value pair (you will be prompted for input)\n";
    std::cout << "  \033[1;32mshutdown\033[0m               - Shut down the server\n";
    std::cout << "  \033[1;32mdie <server_name> <clean>\033[0m - Terminate a server (1 for clean shutdown, 0 for abrupt)\n";
    std::cout << "  \033[1;32mquit\033[0m                   - Exit the client\n";
    std::cout << "  \033[1;32mstart <instance_name> <new_instance>\033[0m - Start a new instance (1 for new instance, 0 for existing)\n";
}

void print_quit()
{
    std::cout << "\n**************************************************************\n";
    std::cout << "*   Thank you for choosing Cluster Crew for your storage     *\n";
    std::cout << "**************************************************************\n\n";
}

int main(int argc, char **argv)
{
    std::string command, key, value;
    std::string old_value, retrieved_value;
    std::string config_file = "config";

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
            if (kv739_init(config_file) != 0)
            {
                std::cerr << "Failed to initialize client with server" << std::endl;
            }
        }
        else if (cmd == "get")
        {
            // Extract the entire rest of the line as the key (handles spaces)
            key = command.substr(command.find(" ") + 1);
            if (!key.empty())
            {
                if (kv739_get(key, retrieved_value) != 0)
                {
                    std::cerr << "Failed to get value for key: " << key << std::endl;
                }
                else
                {
                    std::cout << "Retrieved value for key: " << key << " -> " << retrieved_value << std::endl;
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

            if (kv739_put(key, value, old_value) == -1)
            {
                std::cerr << "Failed to put key-value pair: " << key << " -> " << value << std::endl;
            }
        }
        else if (cmd == "shutdown")
        {
            if (kv739_shutdown() != 0)
            {
                std::cerr << "Failed to shut down the server." << std::endl;
            }
        }
        else if (cmd == "die")
        {
            // Handle the "die" command to terminate a server
            std::string server_name;
            int clean;

            // Extract the server_name and clean flag from the command
            iss >> server_name >> clean;

            if (server_name.empty() || (clean != 0 && clean != 1))
            {
                std::cerr << "Usage: die <server_name> <clean (1 for clean shutdown, 0 for abrupt)>" << std::endl;
                continue;
            }

            if (kv739_die(server_name, clean) != 0)
            {
                std::cerr << "Failed to terminate the server: " << server_name << std::endl;
            }
        }
        else if (cmd == "start") {
            std::string instance_name;
            int new_instance;

            // Extract the instance_name and new_instance flag from the command
            iss >> instance_name >> new_instance;

            if (instance_name.empty() || (new_instance != 0 && new_instance != 1))
            {
                std::cerr << "Usage: start <instance_name> <new_instance (1 for new instance, 0 for existing)>" << std::endl;
                continue;
            }

            if (kv739_start(instance_name, new_instance) != 0)
            {
                std::cerr << "Failed to start the server: " << instance_name << std::endl;
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