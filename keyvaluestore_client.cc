#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using keyvaluestore::ClientRequest;
using keyvaluestore::GetRequest;
using keyvaluestore::GetResponse;
using keyvaluestore::InitRequest;
using keyvaluestore::InitResponse;
using keyvaluestore::KeyValueStore;
using keyvaluestore::PutRequest;
using keyvaluestore::PutResponse;
using keyvaluestore::ServerResponse;
using keyvaluestore::ShutdownRequest;
using keyvaluestore::ShutdownResponse;

class KeyValueStoreClient
{
public:
    int kv739_init(const std::string &server_address)
    {
        channel_ = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
        stub_ = KeyValueStore::NewStub(channel_);

        InitRequest init_request;
        init_request.set_server_name(server_address);

        ClientRequest client_request;
        client_request.mutable_init_request()->CopyFrom(init_request);

        stream_ = stub_->manage_session(&context_); // Open the bidirectional stream

        // Send Init request
        stream_->Write(client_request);

        // Read Init response
        ServerResponse response;
        if (stream_->Read(&response) && response.has_init_response())
        {
            if (response.init_response().success())
            {
                std::cout << "Client successfully initialized with server: " << server_address << std::endl;
                return 0;
            }
        }

        std::cerr << "Error: Failed to initialize the client." << std::endl;
        return -1;
    }

    int kv739_shutdown()
    {
        if (!stream_)
        {
            std::cerr << "Error: No active session to shut down." << std::endl;
            return -1;
        }

        ShutdownRequest shutdown_request;
        ClientRequest client_request;
        client_request.mutable_shutdown_request()->CopyFrom(shutdown_request);

        // Send Shutdown request
        stream_->Write(client_request);
        stream_->WritesDone(); // Close the stream for writing

        // Read Shutdown response before the stream is fully closed
        ServerResponse response;
        if (stream_->Read(&response) && response.has_shutdown_response())
        {
            if (response.shutdown_response().success())
            {
                std::cout << "Client successfully shut down." << std::endl;
                return 0;
            }
        }

        std::cerr << "Error: Failed to shut down the client." << std::endl;
        return -1;
    }

    int kv739_get(const std::string &key, std::string &value)
    {
        if (!stream_)
        {
            std::cerr << "Error: No active session to perform Get." << std::endl;
            return -1;
        }

        GetRequest get_request;
        get_request.set_key(key);

        ClientRequest client_request;
        client_request.mutable_get_request()->CopyFrom(get_request);

        // Send Get request
        stream_->Write(client_request);

        // Read Get response
        ServerResponse response;
        if (stream_->Read(&response) && response.has_get_response())
        {
            if (response.get_response().key_found())
            {
                value = response.get_response().value();
                std::cout << "Get operation successful. Key: '" << key << "', Value: '" << value << "'." << std::endl;
                return 0;
            }
            else
            {
                std::cout << "Key '" << key << "' not found." << std::endl;
                return 1;
            }
        }

        std::cerr << "Error: Get operation failed for key: '" << key << "'." << std::endl;
        return -1;
    }

    int kv739_put(const std::string &key, const std::string &value, std::string &old_value)
    {
        if (!stream_)
        {
            std::cerr << "Error: No active session to perform Put." << std::endl;
            return -1;
        }

        PutRequest put_request;
        put_request.set_key(key);
        put_request.set_value(value);

        ClientRequest client_request;
        client_request.mutable_put_request()->CopyFrom(put_request);

        // Send Put request
        stream_->Write(client_request);

        // Read Put response
        ServerResponse response;
        if (stream_->Read(&response) && response.has_put_response())
        {
            if (response.put_response().key_found())
            {
                old_value = response.put_response().old_value();
                std::cout << "Put operation successful. Old value for key: '" << key << "' was: '" << old_value << "'." << std::endl;
                return 0;
            }
            else
            {
                std::cout << "Put operation successful. No old value existed for key: '" << key << "'." << std::endl;
                return 1;
            }
        }

        std::cerr << "Error: Put operation failed for key: '" << key << "'." << std::endl;
        return -1;
    }

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<KeyValueStore::Stub> stub_;
    std::shared_ptr<grpc::ClientReaderWriter<ClientRequest, ServerResponse> > stream_;
    ClientContext context_; // Keep the context alive for the whole session
};

void print_help()
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

    print_help();

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
            print_help();
        }
    }

    return 0;
}