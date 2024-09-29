#include <iostream>
#include <string>

#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"
#include "rocksdb_wrapper.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReaderWriter;
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

class KeyValueStoreServiceImpl final : public KeyValueStore::Service {
    public:
        KeyValueStoreServiceImpl(const std::string& db_path) : db_(db_path) {}

        Status ManageSession(ServerContext* context, ServerReaderWriter<ServerResponse, ClientRequest>* stream) override {
            ClientRequest client_request;
            while (stream->Read(&client_request)) {
                if (client_request.has_init_request()) {
                    HandleInitRequest(client_request.init_request(), stream);

                } else if (client_request.has_get_request()) {
                    HandleGetRequest(client_request.get_request(), stream);

                } else if (client_request.has_put_request()) {
                    HandlePutRequest(client_request.put_request(), stream);

                } else if (client_request.has_shutdown_request()) {
                    HandleShutdownRequest(stream);
                    break;
                }
            }
            return Status::OK;
        }

    private:
    RocksDBWrapper db_;  // RocksDBWrapper instance for database operations

    void HandleInitRequest(const InitRequest& request, ServerReaderWriter<ServerResponse, ClientRequest>* stream) {
        std::cout << "Received InitRequest for Server: " << request.server_name() << std::endl;

        InitResponse init_response;
        init_response.set_success(true);

        ServerResponse response;
        *response.mutable_init_response() = init_response;
        stream->Write(response);
    }

    void HandleGetRequest(const GetRequest& request, ServerReaderWriter<ServerResponse, ClientRequest>* stream) {
        std::cout << "Received GetRequest for key: " << request.key() << std::endl;

        ServerResponse response;
        GetResponse get_response;

        std::string value;
        if (db_.Get(request.key(), value)) {
            get_response.set_value(value);
            get_response.set_key_found(true);
        } else {
            get_response.set_key_found(false);
        }

        *response.mutable_get_response() = get_response;
        stream->Write(response);
    }

    void HandlePutRequest(const PutRequest& request, ServerReaderWriter<ServerResponse, ClientRequest>* stream) {
        std::cout << "Received PutRequest for key: " << request.key() << " with value: " << request.value() << std::endl;

        ServerResponse response;
        PutResponse put_response;

        std::string old_value;
        if (db_.Get(request.key(), old_value)) {
            put_response.set_old_value(old_value);
            put_response.set_key_found(true);
        } else {
            put_response.set_key_found(false);
        }

        db_.Put(request.key(), request.value());

        *response.mutable_put_response() = put_response;
        stream->Write(response);
    }

    void HandleShutdownRequest(ServerReaderWriter<ServerResponse, ClientRequest>* stream) {
        std::cout << "Received ShutdownRequest. Shutting down server..." << std::endl;

        ShutdownResponse shutdown_response;
        shutdown_response.set_success(true);

        ServerResponse response;
        *response.mutable_shutdown_response() = shutdown_response;
        stream->Write(response);
    }
};

void RunServer(const std::string& server_address, const std::string& db_path) {
    KeyValueStoreServiceImpl service(db_path);

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    std::string server_address("0.0.0.0:50051");
    std::string db_path = "keyvaluestore.db";
    RunServer(server_address, db_path);
    return 0;
}
