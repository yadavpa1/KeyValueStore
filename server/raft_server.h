#ifndef RAFT_SERVER_H
#define RAFT_SERVER_H

#include <memory>
#include <vector>
#include <string>
#include <mutex>
#include <condition_variable>
#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"
#include "rocksdb_wrapper.h"

// Log entry structure used by Raft nodes
struct LogEntry {
    std::string key;
    std::string value;
    int64_t term;
};

// Raft Node class for handling consensus in each Raft cluster
class RaftNode : public raft::RaftService::Service {
public:
    RaftNode(int node_id, int cluster_id, const std::vector<std::string>& peer_addresses, const std::string& db_path);

    grpc::Status RequestVote(grpc::ServerContext* context, const raft::RequestVoteRequest* request, raft::RequestVoteResponse* response) override;
    grpc::Status AppendEntries(grpc::ServerContext* context, const raft::AppendEntriesRequest* request, raft::AppendEntriesResponse* response) override;
    grpc::Status Heartbeat(grpc::ServerContext* context, const raft::HeartbeatRequest* request, raft::HeartbeatResponse* response) override;

    // Getter for current term
    int64_t GetCurrentTerm() const { return current_term_; }

    // Raft methods for leader election and log replication
    void StartElection();
    void BecomeLeader();
    void AppendLog(const LogEntry& entry);

    // Raft communication loop
    void Run();
    
    // Apply logs to RocksDB
    void ApplyLogsToDB();

private:
    int node_id_;                               // ID of this node
    int cluster_id_;                            // ID of the Raft cluster
    int64_t current_term_;                      // Current term number
    int64_t commit_index_;                      // Index of the highest log entry known to be committed
    bool is_leader_;                            // True if this node is the leader
    std::vector<LogEntry> log_;                 // The log of commands
    std::vector<std::string> peer_addresses_;   // List of peer Raft node addresses
    std::unique_ptr<grpc::Server> server_;      // gRPC server instance
    RocksDBWrapper db_;                         // RocksDB instance for key-value storage
    std::mutex mtx_;                            // Mutex for synchronizing Raft state
    std::condition_variable cv_;                // Condition variable for leader election

    // Private helper functions for election, heartbeats, etc.
    void HeartbeatLoop();
    void ElectionTimeout();
    bool IsLeader() const;
};

#endif
