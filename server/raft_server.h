#ifndef RAFT_SERVER_H
#define RAFT_SERVER_H

#include <memory>
#include <vector>
#include <mutex>
#include <atomic>
#include <chrono>
#include <queue>
#include <thread>
#include <condition_variable>
#include <functional>

// ThreadPool class
#include "thread_pool.h"

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
using keyvaluestore::Raft;
using keyvaluestore::LogEntry;
using keyvaluestore::AppendEntriesRequest;
using keyvaluestore::AppendEntriesResponse;
using keyvaluestore::RequestVoteRequest;
using keyvaluestore::RequestVoteResponse;

using keyvaluestore::InitRequest;
using keyvaluestore::InitResponse;
using keyvaluestore::GetRequest;
using keyvaluestore::GetResponse;
using keyvaluestore::PutRequest;
using keyvaluestore::PutResponse;
using keyvaluestore::ShutdownRequest;
using keyvaluestore::ShutdownResponse;
using keyvaluestore::DieRequest;
using keyvaluestore::DieResponse;
using keyvaluestore::StartRequest;
using keyvaluestore::StartResponse;
using keyvaluestore::LeaveRequest;
using keyvaluestore::LeaveResponse;
using keyvaluestore::PartitionChangeRequest;
using keyvaluestore::PartitionChangeResponse;
using keyvaluestore::KeyRange;
using keyvaluestore::NewRaftGroupRequest;
using keyvaluestore::NewRaftGroupResponse;

enum class RaftState { 
    FOLLOWER,
    CANDIDATE,
    LEADER 
};

class RaftServer final : public keyvaluestore::Raft::Service, public keyvaluestore::KeyValueStore::Service {
    public:
        RaftServer(
            int server_id,
            const std::string &server_name,
            std::vector<std::string>& host_list, 
            const std::string &db_path,
            const std::string &raft_log_db_path,
            size_t cache_size    
        );

        virtual ~RaftServer();
        
        // Run raft server
        void Run();

        void StartServer();

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

        Status Die(
            ServerContext* context, 
            const DieRequest* request, 
            DieResponse* response
        ) override;

        Status PartitionChange(
            ServerContext* context, 
            const PartitionChangeRequest* request, 
            PartitionChangeResponse* response
        ) override;

        Status NotifyNewRaftGroup(
            ServerContext* context,
            const NewRaftGroupRequest* request,
            NewRaftGroupResponse* response
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

        Status Start(
            ServerContext* context,
            const StartRequest* request,
            StartResponse* response
        ) override;

        Status Leave(
            ServerContext* context,
            const LeaveRequest* request,
            LeaveResponse* response
        ) override;

        // Raft operations
        void StartElection();
        void BecomeLeader();
        void BecomeFollower(int leader_id);
        void ReplicateLogEntries();
        void InvokeRequestVote(int peer_id, std::atomic<int>* votes_gained);
        void InvokeAppendEntries(int peer_id);
        void SendHeartbeat();

        void HandleAlarm();
        void SetElectionAlarm(int timeout_ms);
        void ResetElectionTimeout();

        void StartPersistenceThread();
        void EnqueuePersistenceTask(const std::function<void()> &task);
        void PersistRaftStateInBackground(const AppendEntriesRequest* request);
        void LoadRaftState();
        void PersistRaftState();

        // Configuration serialization/deserialization
        static std::string SerializeHostList(std::vector<std::string>& host_list);
        static std::vector<std::string> DeserializeHostList(const std::string& serialized_host_list);

    private:
        int server_id;
        std::string server_name;
        std::mutex state_mutex;
        RaftState state;

        // Persistent state
        int current_term;
        int voted_for;
        std::vector<LogEntry> raft_log;

        // Volatile state on all servers
        int64_t commit_index;
        int last_applied;

        ThreadPool thread_pool;

        std::queue<std::function<void()>> persistence_queue;
        std::mutex persistence_mutex;
        std::condition_variable persistence_condition;

        std::thread persistence_thread;
        bool stop_persistence_thread = false;

        void HandlePersistenceTasks();

        // Volatile state on leader
        std::vector<int> next_index;
        std::vector<int> match_index;

        int current_leader;

        // Network related members
        std::vector<std::unique_ptr<Raft::Stub>> peer_stubs;
        std::unique_ptr<Server> server;
        std::vector<std::string> host_list;

        RocksDBWrapper db_;          // For key-value store operations
        RocksDBWrapper raft_log_db_; // For Raft log and state persistence

        // Election timeout and heartbeat
        int election_timeout;
        static const int min_election_timeout;
        static const int max_election_timeout;
        static const int heartbeat_interval;
};

#endif
