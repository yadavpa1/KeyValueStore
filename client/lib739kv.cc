#include "lib739kv.h"
#include <iostream>
#include <thread>

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

// Global variables to hold the gRPC objects
std::shared_ptr<grpc::Channel> channel_;
std::unique_ptr<keyvaluestore::KeyValueStore::Stub> stub_;
std::shared_ptr<grpc::ClientReaderWriter<keyvaluestore::ClientRequest, keyvaluestore::ServerResponse>> stream_;
std::unique_ptr<grpc::ClientContext> context_;
std::string connected_server_name;

// Channel arguments for automatic reconnection and keepalive
grpc::ChannelArguments GetChannelArguments()
{
    grpc::ChannelArguments args;
    // Set reconnection policies
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 1000); // Initial backoff 1 second
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 20000);    // Max backoff 20 seconds
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);           // Keepalive ping every 10 seconds
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);         // Timeout after 5 seconds if no response
    return args;
}

// Helper function to reset the gRPC stream and context
bool InitializeStream(const std::string &server_name)
{
    context_ = std::make_unique<grpc::ClientContext>();
    stream_ = stub_->ManageSession(context_.get());

    InitRequest init_request;
    init_request.set_server_name(server_name);

    ClientRequest client_request;
    client_request.mutable_init_request()->CopyFrom(init_request);

    if (!stream_->Write(client_request)) // Send Init request
    {
        return false;
    }

    ServerResponse response;
    if (stream_->Read(&response) && response.has_init_response() && response.init_response().success())
    {
        std::cout << "Client successfully initialized with server: " << server_name << std::endl;
        return true;
    }

    std::cerr << "Error: Failed to initialize the client." << std::endl;
    return false;
}

// Helper function to reset the channel and stream if connection fails
bool Reconnect()
{
    std::cerr << "Reconnecting to server..." << std::endl;
    while (!InitializeStream(connected_server_name))
    {
        std::cerr << "Reconnection failed. Retrying..." << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(5000)); // Retry every 5 seconds
    }
    std::cout << "Reconnected successfully." << std::endl;
    return true;
}

int kv739_init(const std::string &server_name)
{
    if (stream_)
    {
        std::cerr << "Error: Client is already initialized at server: " << connected_server_name << std::endl;
        return -1;
    }

    // Create a channel with reconnection support
    channel_ = grpc::CreateCustomChannel(server_name, grpc::InsecureChannelCredentials(), GetChannelArguments());
    stub_ = KeyValueStore::NewStub(channel_);

    if (!InitializeStream(server_name))
    {
        return -1;
    }

    connected_server_name = server_name;
    return 0;
}

int kv739_shutdown()
{
    if (!stream_)
    {
        std::cerr << "Error: No active session to shut down." << std::endl;
        return -1;
    }

    ShutdownRequest shutdown_request;
    ServerResponse response;

    ClientRequest client_request;
    client_request.mutable_shutdown_request()->CopyFrom(shutdown_request);

    if (!stream_->Write(client_request)) // Send Shutdown request
    {
        std::cerr << "Error: Failed to write shutdown request. Reconnecting..." << std::endl;
        if (!Reconnect())
        {
            return -1;
        }
        stream_->Write(client_request); // Retry after reconnecting
    }
    stream_->WritesDone(); // Close the stream

    if (stream_->Read(&response) && response.has_shutdown_response() && response.shutdown_response().success())
    {
        std::cout << "Client successfully shut down." << std::endl;

        grpc::Status status = stream_->Finish();
        if (!status.ok())
        {
            std::cerr << "Error: Shutdown stream failed with status: " << status.error_message() << std::endl;
            return -1;
        }

        stream_.reset(); // Clear the stream pointer only on success
        context_.reset();
        return 0;
    }

    std::cerr << "Error: Shutdown operation failed, unable to read server response." << std::endl;
    return -1;
}

int kv739_get(const std::string &key, std::string &value)
{
    if (!stream_)
    {
        std::cerr << "Error: No active session to perform Get." << std::endl;
        return -1;
    }

    ClientRequest client_request;
    GetRequest get_request;
    get_request.set_key(key);
    client_request.mutable_get_request()->CopyFrom(get_request);

    if (!stream_->Write(client_request)) // Send Get request
    {
        if (!Reconnect())
        {
            return -1;
        }
        stream_->Write(client_request); // Retry after reconnecting
    }

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

    if (!stream_->Write(client_request)) // Send Put request
    {
        if (!Reconnect())
        {
            return -1;
        }
        stream_->Write(client_request); // Retry after reconnecting
    }

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