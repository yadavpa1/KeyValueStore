#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using keyvaluestore::GetRequest;
using keyvaluestore::GetResponse;
using keyvaluestore::InitRequest;
using keyvaluestore::InitResponse;
using keyvaluestore::KeyValueStore;
using keyvaluestore::PutRequest;
using keyvaluestore::PutResponse;
using keyvaluestore::ShutdownResponse;

class KeyValueStoreClient
{
public:
    KeyValueStoreClient(std::shared_ptr<Channel> channel)
        : stub_(KeyValueStore::NewStub(channel)) {}

    // initialize the key-value store with a server name.
    bool init(const std::string &server_name)
    {
        InitRequest request;
        request.set_server_name(server_name);

        InitResponse response;
        ClientContext context;

        Status status = stub_->init(&context, request, &response);
        if (!status.ok())
        {
            std::cerr << "init RPC failed: " << status.error_message() << std::endl;
            return false;
        }

        std::cout << "Connected to Server: " << server_name << std::endl;
        return response.success();
    }

    // shut down the key-value store.
    bool shutdown()
    {
        google::protobuf::Empty empty;
        ShutdownResponse response;
        ClientContext context;

        Status status = stub_->shutdown(&context, empty, &response);
        if (!status.ok())
        {
            std::cerr << "shutdown RPC failed: " << status.error_message() << std::endl;
            return false;
        }

        std::cout << "Shutting down server!" << std::endl;
        return response.success();
    }

    // get a value associated with a key.
    std::pair<std::string, bool> get(const std::string &key)
    {
        GetRequest request;
        request.set_key(key);

        GetResponse response;
        ClientContext context;

        Status status = stub_->get(&context, request, &response);
        if (!status.ok())
        {
            std::cerr << "get RPC failed: " << status.error_message() << std::endl;
            return {"", false};
        }

        std::cout << "get key: " << key << ", value: " << response.value() << std::endl;
        return {response.value(), response.key_found()};
    }

    // put a key-value pair into the store.
    std::pair<std::string, bool> put(const std::string &key, const std::string &value)
    {
        PutRequest request;
        request.set_key(key);
        request.set_value(value);

        PutResponse response;
        ClientContext context;

        Status status = stub_->put(&context, request, &response);
        if (!status.ok())
        {
            std::cerr << "put RPC failed: " << status.error_message() << std::endl;
            return {"", false};
        }
        std::cout << "put key: " << key << ", value: " << value << ", old_value: " << response.old_value() << std::endl;
        return {response.old_value(), response.key_found()};
    }

private:
    // gRPC stub to access the service
    std::unique_ptr<KeyValueStore::Stub> stub_;
};

int main(int argc, char **argv)
{
    KeyValueStoreClient client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));

    client.init("TestServer");
    client.put("key100", "value100");
    client.get("key1");
    client.shutdown();

    return 0;
}
