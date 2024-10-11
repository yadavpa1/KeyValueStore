#ifndef RAFT_SERVER_H
#define RAFT_SERVER_H

#include <memory>
#include <vector>
#include <mutex>
#include <atomic>

// GRPC stuff
#include <grpc/grpc.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc++/security/server_credentials.h>
#include "keyvaluestore.grpc.pb.h"

// RocksDB wrapper
#include "rocksdb_wrapper.h"

using grpc::Status;
using grpc::Server;
using grpc::ServerContext;
using raft_group::Raft;
using raft_group::LogEntry;
using raft_group::AppendEntriesRequest;
using raft_group::AppendEntriesResponse;
using raft_group::RequestVoteRequest;
using raft_group::RequestVoteResponse;
using raft_group::HeartbeatRequest;
using raft_group::HeartbeatResponse;

using raft_group::InitRequest;
using raft_group::InitResponse;
using raft_group::GetRequest;
using raft_group::GetResponse;
using raft_group::PutRequest;
using raft_group::PutResponse;
using raft_group::ShutdownRequest;
using raft_group::ShutdownResponse;

enum class RaftState { 
    FOLLOWER,
    CANDIDATE,
    LEADER 
};

class RaftServer final : public raft_group::Raft::Service, public raft_group::KeyValueStore::Service {
    public:
        RaftServer(
            int server_id, 
            const std::vector<std::string>& host_list, 
            const std::string &db_path,
            size_t cache_size    
        );
        
        // Run raft server
        void Run();

        // wait for server to shutdown
        void Wait();

        // Client requests for KV-Store operations
        Status Init(
            ServerContext* context, 
            const InitRequest* request, 
            InitResponse* response
        ) override;

        Status Get(
            ServerContext* context, 
            const GetRequest* request, 
            GetResponse* response
        ) override;

        Status Put(
            ServerContext* context, 
            const PutRequest* request, 
            PutResponse* response
        ) override;

        Status Shutdown(
            ServerContext* context, 
            const ShutdownRequest* request, 
            ShutdownResponse* response
        ) override;

        // Raft RPCs
        Status AppendEntries(
            ServerContext* context,
            const AppendEntriesRequest* request,
            AppendEntriesResponse* response
        ) override;

        Status RequestVote(
            ServerContext* context,
            const RequestVoteRequest* request,
            RequestVoteResponse* response
        ) override;

        Status Heartbeat(
            ServerContext* context,
            const HeartbeatRequest* request,
            HeartbeatResponse* response
        ) override;

    private:

        // Raft operations
        void StartElection();
        void BecomeLeader();
        void BecomeFollower(int leader_id);
        void ReplicateLogEntries();
        void InvokeRequestVote(int peer_id, std::atomic<int>* votes_gained);
        void InvokeAppendEntries(int peer_id);
        void SendHeartbeat();
        void ResetElectionTimeout();

        int server_id;
        std::mutex state_mutex;
        RaftState state;

        // Persistent state-make this persistent!
        int current_term;
        int voted_for;
        std::vector<LogEntry> raft_log;

        // Volatile state on all servers
        int64_t commit_index;
        int last_applied;

        // Volatile state on leader
        std::vector<int> next_index;
        std::vector<int> match_index;

        int current_leader;

        // Network related members
        std::vector<std::unique_ptr<Raft::Stub>> peer_stubs;
        std::unique_ptr<Server> server;
        const std::vector<std::string> host_list;

        RocksDBWrapper db_;

        // Election timeout and heartbeat
        int election_timeout;
        static const int min_election_timeout;
        static const int max_election_timeout;
        static const int heartbeat_interval;
};

#endif
