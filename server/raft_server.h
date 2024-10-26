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

// GRPC includes
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
using keyvaluestore::HeartbeatRequest;
using keyvaluestore::HeartbeatResponse;

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

// Simple ThreadPool class for asynchronous tasks
class ThreadPool {
public:
    ThreadPool(size_t num_threads);
    ~ThreadPool();

    // Add a task to the queue
    void enqueue(std::function<void()> task);

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;

    void worker_thread();
};

// Enum representing the state of a Raft node
enum class RaftState { 
    FOLLOWER,
    CANDIDATE,
    LEADER 
};

class RaftServer final : public keyvaluestore::Raft::Service, public keyvaluestore::KeyValueStore::Service {
    public:
        RaftServer(
            int server_id, 
            const std::vector<std::string>& host_list, 
            const std::string &db_path,
            const std::string &raft_log_db_path,
            size_t cache_size    
        );

        virtual ~RaftServer();

        void StartPersistenceThread();
        void EnqueuePersistenceTask(const std::function<void()> &task);
        void PersistRaftStateInBackground(const AppendEntriesRequest* request);
        
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

        Status Die(
            ServerContext* context, 
            const DieRequest* request, 
            DieResponse* response
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

        void LoadRaftState();
        void PersistRaftState();

        // Snapshot-related operations
        void CreateSnapshot();
        void RestoreSnapshot();
        void TruncateLog();

    private:
        int server_id;
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
        const std::vector<std::string> host_list;

        RocksDBWrapper db_;          // For key-value store operations
        RocksDBWrapper raft_log_db_; // For Raft log and state persistence

        // Election timeout and heartbeat
        int election_timeout;
        static const int min_election_timeout;
        static const int max_election_timeout;
        static const int heartbeat_interval;

        // Snapshot-related variables
        int last_snapshot_index = -1; // Index of the last log entry included in the snapshot
        int last_snapshot_term = 0;   // Term of the last log entry in the snapshot
        std::string snapshot_path;    // Path for storing snapshot files
        std::mutex snapshot_mutex;    // Mutex for snapshot access control
};

#endif
