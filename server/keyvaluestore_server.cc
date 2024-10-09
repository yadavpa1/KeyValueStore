#include <iostream>
#include <string>

#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"
#include "rocksdb_wrapper.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
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

const size_t CACHE_SIZE = 20*1024*1024; // Cache size is 20MB covering a db size of 200MB

class KeyValueStoreServiceImpl final : public KeyValueStore::Service
{
    public:
    KeyValueStoreServiceImpl(const std::string &db_path, size_t cache_size)
        : db_(db_path, cache_size) {}

    Status ManageSession(ServerContext *context, ServerReaderWriter<ServerResponse, ClientRequest> *stream) override
    {
        ClientRequest client_request;
        while (stream->Read(&client_request))
        {
            if (client_request.has_init_request())
            {
                HandleInitRequest(client_request.init_request(), stream);
            }
            else if (client_request.has_get_request())
            {
                HandleGetRequest(client_request.get_request(), stream);
            }
            else if (client_request.has_put_request())
            {
                HandlePutRequest(client_request.put_request(), stream);
            }
            else if (client_request.has_shutdown_request())
            {
                HandleShutdownRequest(stream);
                break;
            }
        }
        return Status::OK;
    }

    private:
    RocksDBWrapper db_; // RocksDBWrapper instance for database operations

    void HandleInitRequest(const InitRequest &request, ServerReaderWriter<ServerResponse, ClientRequest> *stream)
    {
        std::cout << "Received InitRequest for Server: " << request.server_name() << std::endl;

        InitResponse init_response;
        init_response.set_success(true);

        ServerResponse response;
        *response.mutable_init_response() = init_response;
        stream->Write(response);
    }

    void HandleGetRequest(const GetRequest &request, ServerReaderWriter<ServerResponse, ClientRequest> *stream)
    {
        std::cout << "Received GetRequest for key: " << request.key() << std::endl;

        ServerResponse response;
        GetResponse get_response;

        std::string value;

        if (db_.Get(request.key(), value))
        {
            get_response.set_value(value);
            get_response.set_key_found(true);
        }
        else
        {
            std::cout << "Key not found: " << request.key() << std::endl;
            get_response.set_key_found(false);
        }

        *response.mutable_get_response() = get_response;
        stream->Write(response);
    }

    void HandlePutRequest(const PutRequest &request, ServerReaderWriter<ServerResponse, ClientRequest> *stream)
    {
        std::cout << "Received PutRequest for key: " << request.key() << " with value: " << request.value() << std::endl;

        ServerResponse response;
        PutResponse put_response;

        std::string old_value;
        // Use RocksDB transactions with partitioning for the Put operation
        int result = db_.Put(request.key(), request.value(), old_value);

        if (result == 0)
        {
            put_response.set_old_value(old_value);
            put_response.set_key_found(true);
            std::cout << "Updated key: " << request.key() << " with old value: " << old_value << std::endl;
        }
        else if (result == 1)
        {
            put_response.set_key_found(false);
            std::cout << "Inserted new key: " << request.key() << " with value: " << request.value() << std::endl;
        }
        else
        {
            std::cerr << "Error updating key: " << request.key() << std::endl;
        }

        *response.mutable_put_response() = put_response;
        stream->Write(response);
    }

    void HandleShutdownRequest(ServerReaderWriter<ServerResponse, ClientRequest> *stream)
    {
        std::cout << "Received ShutdownRequest. Shutting down server..." << std::endl;

        ShutdownResponse shutdown_response;
        shutdown_response.set_success(true);

        ServerResponse response;
        *response.mutable_shutdown_response() = shutdown_response;
        stream->Write(response);
    }
};

void RunServer(const std::string &server_address, const std::string &db_path, const size_t cache_size)
{
    KeyValueStoreServiceImpl service(db_path, cache_size);

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char **argv)
{
    std::string server_address("0.0.0.0:50051");
    std::string db_path = "keyvaluestore.db";
    RunServer(server_address, db_path, CACHE_SIZE);
    return 0;
}
