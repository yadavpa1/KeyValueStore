#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using keyvaluestore::InitRequest;
using keyvaluestore::InitResponse;
using keyvaluestore::GetRequest;
using keyvaluestore::GetResponse;
using keyvaluestore::PutRequest;
using keyvaluestore::PutResponse;
using keyvaluestore::ShutdownResponse;
using keyvaluestore::KeyValueStore;

class KeyValueStoreClient {
    public:
    KeyValueStoreClient(std::shared_ptr<Channel> channel)
    : stub_(KeyValueStore::NewStub(channel)) {}
    
    // Initialize the key-value store with a server name.
    bool Init(const std::string& server_name) {
        InitRequest request;
        request.set_server_name(server_name);

        InitResponse response;
        ClientContext context;

        Status status = stub_->Init(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Init RPC failed: " << status.error_message() << std::endl;
            return false;
        }
        return response.success();
    }

    // Shut down the key-value store.
    bool Shutdown() {
        google::protobuf::Empty empty;
        ShutdownResponse response;
        ClientContext context;

        Status status = stub_->Shutdown(&context, empty, &response);
        if (!status.ok()) {
            std::cerr << "Shutdown RPC failed: " << status.error_message() << std::endl;
            return false;
        }
        return response.success();
    }

    // Get a value associated with a key.
    std::pair<std::string, bool> Get(const std::string& key) {
        GetRequest request;
        request.set_key(key);

        GetResponse response;
        ClientContext context;

        Status status = stub_->Get(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Get RPC failed: " << status.error_message() << std::endl;
            return {"", false};
        }
        return {response.value(), response.key_found()};
    }

    // Put a key-value pair into the store.
    std::pair<std::string, bool> Put(const std::string& key, const std::string& value) {
        PutRequest request;
        request.set_key(key);
        request.set_value(value);

        PutResponse response;
        ClientContext context;

        Status status = stub_->Put(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Put RPC failed: " << status.error_message() << std::endl;
            return {"", false};
        }
        return {response.old_value(), response.key_found()};
    }

    private:
    // gRPC stub to access the service
    std::unique_ptr<KeyValueStore::Stub> stub_;
    
};

int main(int argc, char** argv) {
    KeyValueStoreClient client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));

    client.Init("TestServer");
    client.Put("key1", "value1");
    client.Get("key1");
    client.Shutdown();

    return 0;
}

