#include "lib739kv.h"
#include <iostream>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientReaderWriter;
using keyvaluestore::KeyValueStore;
using keyvaluestore::ClientRequest;
using keyvaluestore::ServerResponse;
using keyvaluestore::InitRequest;
using keyvaluestore::InitResponse;
using keyvaluestore::GetRequest;
using keyvaluestore::GetResponse;
using keyvaluestore::PutRequest;
using keyvaluestore::PutResponse;
using keyvaluestore::ShutdownRequest;
using keyvaluestore::ShutdownResponse;

int KeyValueStoreClient::kv739_init(const std::string& server_name) {
    channel_ = grpc::CreateChannel(server_name, grpc::InsecureChannelCredentials());
    stub_ = KeyValueStore::NewStub(channel_);

    InitRequest init_request;
    init_request.set_server_name(server_name);

    ClientRequest client_request;
    client_request.mutable_init_request()->CopyFrom(init_request);

    stream_ = stub_->ManageSession(&context_); // Open the bidirectional stream
    stream_->Write(client_request); // Send Init request

    ServerResponse response;

    // Read Init response
    if (stream_->Read(&response) && response.has_init_response()) {
        if (response.init_response().success()) {
            std::cout << "Client successfully initialized with server: " << server_name << std::endl;
            return 0;
        }
    }

    std::cerr << "Error: Failed to initialize the client." << std::endl;
    return -1;
}

int KeyValueStoreClient::kv739_shutdown() {
    ShutdownRequest shutdown_request;
    ServerResponse response;

    ClientRequest client_request;
    client_request.mutable_shutdown_request()->CopyFrom(shutdown_request);

    stream_->Write(client_request); // Send Shutdown request
    stream_->WritesDone(); // Close the stream

    if (stream_->Read(&response) && response.has_shutdown_response()) {
        if (response.shutdown_response().success()) {
            std::cout << "Client successfully shut down." << std::endl;
        } else {
            std::cerr << "Shutdown request failed on the server." << std::endl;
            return -1;
        }
    }

    grpc::Status status = stream_->Finish();
    if (!status.ok()) {
        std::cerr << "Error: Shutdown stream failed with status: " << status.error_message() << std::endl;
        return -1;
    }
    return 0;
}

int KeyValueStoreClient::kv739_get(const std::string& key, std::string& value) {
    ClientRequest client_request;
    GetRequest get_request;
    get_request.set_key(key);
    client_request.mutable_get_request()->CopyFrom(get_request);

    stream_->Write(client_request);  // Send Get request

    ServerResponse response;
    if (stream_->Read(&response) && response.has_get_response()) {
        if (response.get_response().key_found()) {
            value = response.get_response().value();
            std::cout << "Get operation successful. Key: '" << key << "', Value: '" << value << "'." << std::endl;
            return 0;
        } else {
            std::cout << "Key '" << key << "' not found." << std::endl;
            return 1;
        }
    }

    std::cerr << "Error: Get operation failed for key: '" << key << "'." << std::endl;
    return -1;
}

int KeyValueStoreClient::kv739_put(const std::string& key, const std::string& value, std::string& old_value) {
    PutRequest put_request;
    put_request.set_key(key);
    put_request.set_value(value);

    ClientRequest client_request;
    client_request.mutable_put_request()->CopyFrom(put_request);

    stream_->Write(client_request);  // Send Put request

    ServerResponse response;
    if (stream_->Read(&response) && response.has_put_response()) {
        if (response.put_response().key_found()) {
            old_value = response.put_response().old_value();
            std::cout << "Put operation successful. Old value for key: '" << key << "' was: '" << old_value << "'." << std::endl;
            return 0;
        } else {
            std::cout << "Put operation successful. No old value existed for key: '" << key << "'." << std::endl;
            return 1;
        }
    }

    std::cerr << "Error: Put operation failed for key: '" << key << "'." << std::endl;
    return -1;
}