#include <iostream>
#include "lib739kv.h"
#include <grpcpp/grpcpp.h>

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

int KeyValueStoreClient::kv739_init(const std::string &server_address)
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

int KeyValueStoreClient::kv739_shutdown()
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

int KeyValueStoreClient::kv739_get(const std::string &key, std::string &value)
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

int KeyValueStoreClient::kv739_put(const std::string &key, const std::string &value, std::string &old_value)
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