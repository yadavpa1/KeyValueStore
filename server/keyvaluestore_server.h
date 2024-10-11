#ifndef KEYVALUESTORE_SERVER_H
#define KEYVALUESTORE_SERVER_H

#include <memory>
#include "rocksdb_wrapper.h"
#include "raft_server.h"
#include "keyvaluestore.grpc.pb.h"

class KeyValueStoreServer : public keyvaluestore::KeyValueStore::Service {
public:
    KeyValueStoreServer(int node_id, int cluster_id, const std::string& db_path, const std::vector<std::string>& peers);

    grpc::Status ManageSession(grpc::ServerContext* context, grpc::ServerReaderWriter<keyvaluestore::ServerResponse, keyvaluestore::ClientRequest>* stream) override;

    // Getter for RaftNode
    RaftNode& GetRaftNode() { return raft_node_; }

private:
    RocksDBWrapper db_;               // RocksDB instance
    RaftNode raft_node_;              // Raft node for consensus
};

#endif
