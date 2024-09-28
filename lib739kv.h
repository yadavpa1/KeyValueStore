#ifndef KEYVALUESTORE_CLIENT_H
#define KEYVALUESTORE_CLIENT_H

#include <memory>
#include <string>
#include "keyvaluestore.grpc.pb.h"

class KeyValueStoreClient
{
public:
    int kv739_init(const std::string &server_address);
    int kv739_shutdown();
    int kv739_get(const std::string &key, std::string &value);
    int kv739_put(const std::string &key, const std::string &value, std::string &old_value);

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<keyvaluestore::KeyValueStore::Stub> stub_;
    std::shared_ptr<grpc::ClientReaderWriter<keyvaluestore::ClientRequest, keyvaluestore::ServerResponse>> stream_;
    grpc::ClientContext context_;
};

#endif
