#ifndef LIB739KV_H
#define LIB739KV_H

#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"
#include <string>

class KeyValueStoreClient
{
public:
    int kv739_init(const std::string &server_name);
    int kv739_shutdown();
    int kv739_get(const std::string &key, std::string &value);
    int kv739_put(const std::string &key, const std::string &value, std::string &old_value);

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<keyvaluestore::KeyValueStore::Stub> stub_;
    std::shared_ptr<grpc::ClientReaderWriter<keyvaluestore::ClientRequest, keyvaluestore::ServerResponse>> stream_;
    std::unique_ptr<grpc::ClientContext> context_;  // Use a smart pointer for the context
    std::string connected_server_name;
};

#endif