#include <iostream>
#include <sstream>
#include <memory>
#include <string>
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
    std::cout << "  \033[1;32minit <server_address>\033[0m  - Initialize connection to the server (eg. init localhost:50051 ) \n";
    std::cout << "  \033[1;32mget <key>\033[0m              - Retrieve value by key (eg. get myKey )\n";
    std::cout << "  \033[1;32mput\033[0m                    - Store a key-value pair (you will be prompted for input)\n";
    std::cout << "  \033[1;32mshutdown\033[0m               - Shut down the server\n";
    std::cout << "  \033[1;32mquit\033[0m                   - Exit the client\n";
}

void print_quit()
{
    std::cout << "\n**************************************************************\n";
    std::cout << "*   Thank you for choosing Cluster Crew for your storage     *\n";
    std::cout << "**************************************************************\n\n";
}

int main(int argc, char **argv)
{
    KeyValueStoreClient client;
    std::string command, key, value, server_address;
    std::string old_value, retrieved_value;

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
            iss >> server_address;
            if (!server_address.empty())
            {
                if (client.kv739_init(server_address) != 0)
                {
                    std::cerr << "Failed to initialize client with server at " << server_address << std::endl;
                }
            }
            else
            {
                std::cerr << "Usage: init <server_address>" << std::endl;
            }
        }
        else if (cmd == "get")
        {
            // Extract the entire rest of the line as the key (handles spaces)
            key = command.substr(command.find(" ") + 1);
            if (!key.empty())
            {
                if (client.kv739_get(key, retrieved_value) != 0)
                {
                    std::cerr << "Failed to get value for key: " << key << std::endl;
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

            if (client.kv739_put(key, value, old_value) != 0)
            {
                std::cerr << "Failed to put key-value pair: " << key << " -> " << value << std::endl;
            }
        }
        else if (cmd == "shutdown")
        {
            if (client.kv739_shutdown() != 0)
            {
                std::cerr << "Failed to shut down the server." << std::endl;
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