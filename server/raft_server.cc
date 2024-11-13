#include "raft_server.h"
#include <iostream>
#include <thread>
#include <atomic>
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <csignal>
#include <sys/time.h>
#include <cerrno>
#include <fstream>


using grpc::Status;
using grpc::ServerBuilder;
using grpc::ServerContext;

const int RaftServer::min_election_timeout = 1000;
const int RaftServer::max_election_timeout = 2000;
const int RaftServer::heartbeat_interval = 80;

RaftServer* alarm_handler_server;

void SignalHandler(int signum) {
    if (alarm_handler_server) {
        alarm_handler_server->HandleAlarm();
    }
}

// Utility functions for serialization and deserialization of log entries
std::string serializeLogEntry(const LogEntry& entry) {
    return entry.SerializeAsString();
}

LogEntry deserializeLogEntry(const std::string& data) {
    LogEntry entry;
    entry.ParseFromString(data);
    return entry;
}

RaftServer::RaftServer(int server_id, const std::string &server_name, std::vector<std::string>& host_list, const std::string &db_path, const std::string &raft_log_db_path, size_t cache_size)
    : server_id(server_id),
      server_name(server_name),
      host_list(host_list),
      state(RaftState::FOLLOWER),
      current_term(0),
      voted_for(-1),
      commit_index(-1),
      last_applied(-1),
      current_leader(-1),
      thread_pool(4),
      db_(db_path, cache_size),
      raft_log_db_(raft_log_db_path, cache_size)
{
    // Load persisted Raft state from the Raft log database
    LoadRaftState();

    StartPersistenceThread();
    
    // Set a random election timeout between min and max election timeouts
    election_timeout = min_election_timeout + (rand() % (max_election_timeout - min_election_timeout));
}

RaftServer::~RaftServer() {
    stop_persistence_thread = true;
    persistence_condition.notify_one();  // Notify the persistence thread to stop

    if (persistence_thread.joinable()) {
        persistence_thread.join();
    }
}

void RaftServer::StartPersistenceThread() {
    persistence_thread = std::thread(&RaftServer::HandlePersistenceTasks, this);
}

void RaftServer::EnqueuePersistenceTask(const std::function<void()>& task) {
    {
        std::lock_guard<std::mutex> lock(persistence_mutex);
        persistence_queue.push(task);
    }
    persistence_condition.notify_one();  // Notify the persistence thread
}

void RaftServer::HandlePersistenceTasks() {
    while (!stop_persistence_thread) {
        std::function<void()> task;

        {
            std::unique_lock<std::mutex> lock(persistence_mutex);
            persistence_condition.wait(lock, [this]() { return !persistence_queue.empty() || stop_persistence_thread; });

            if (stop_persistence_thread && persistence_queue.empty()) {
                break;
            }

            // Get the next task from the queue
            task = persistence_queue.front();
            persistence_queue.pop();
        }

        // Execute the persistence task
        task();
    }
}

void RaftServer::LoadRaftState() {
    std::string term_str, voted_for_str, log_size_str, commit_index_str, old_config_str;

    // Load current term
    if (raft_log_db_.Get("current_term", term_str)) {
        current_term = std::stoi(term_str);
    }

    // Load voted_for
    if (raft_log_db_.Get("voted_for", voted_for_str)) {
        voted_for = std::stoi(voted_for_str);
    }

    // Load commit_index
    if (raft_log_db_.Get("commit_index", commit_index_str)) {
        commit_index = std::stoi(commit_index_str);
    } else {
        commit_index = -1; // Default if not found
    }

    // Load Raft log entries
    std::vector<std::string> log_entries;
    if (raft_log_db_.LoadLogEntries("log_", log_entries)) {
        for (const auto& entry_str : log_entries) {
            raft_log.push_back(deserializeLogEntry(entry_str));
        }
    }

    // sleep for 1 second to allow the persistence thread to load the state
    std::this_thread::sleep_for(std::chrono::seconds(1));
}

void RaftServer::PersistRaftState() {
    // Enqueue the persistence task
    EnqueuePersistenceTask([this]() {
        rocksdb::WriteBatch batch;

        // Persist current term and voted_for in the same batch
        batch.Put("current_term", std::to_string(current_term));
        batch.Put("voted_for", std::to_string(voted_for));

        // Persist commit_index
        batch.Put("commit_index", std::to_string(commit_index));

        // Persist log entries
        batch.Put("log_size", std::to_string(raft_log.size()));
        for (size_t i = 0; i < raft_log.size(); ++i) {
            batch.Put("log_" + std::to_string(i), serializeLogEntry(raft_log[i]));
        }

        // Write all changes atomically
        rocksdb::Status status = raft_log_db_.Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok()) {
            std::cerr << "Failed to persist Raft state: " << status.ToString() << std::endl;
        }
    });
}

void RaftServer::PersistRaftStateInBackground(const AppendEntriesRequest* request) {
    // Enqueue the persistence task for RocksDB (raft_log_db_)
    EnqueuePersistenceTask([this, request]() {
        rocksdb::WriteOptions write_options;
        write_options.sync = true;
        rocksdb::WriteBatch batch;

        // Persist current term and log size in the same batch
        batch.Put("current_term", std::to_string(current_term));
        batch.Put("log_size", std::to_string(raft_log.size()));

        // If the commit index is updated, persist it too
        if (request->leader_commit() > commit_index) {
            batch.Put("commit_index", std::to_string(commit_index));
        }

        // Persist new log entries to RocksDB
        for (size_t i = 0; i < raft_log.size(); ++i) {
            batch.Put("log_" + std::to_string(i), serializeLogEntry(raft_log[i]));
        }

        // Write the batch to RocksDB asynchronously
        rocksdb::Status status = raft_log_db_.Write(write_options, &batch);
        if (!status.ok()) {
            std::cerr << "Failed to persist Raft state: " << status.ToString() << std::endl;
        }
    });
}

void RaftServer::Run() {
    signal(SIGALRM, &SignalHandler);
    alarm_handler_server = this;

    ServerBuilder builder;
    builder.AddListeningPort(host_list[server_id], grpc::InsecureServerCredentials());

    // Register Raft and KeyValueStore services
    builder.RegisterService(static_cast<keyvaluestore::Raft::Service*>(this));
    builder.RegisterService(static_cast<keyvaluestore::KeyValueStore::Service*>(this));

    server = builder.BuildAndStart();
    // std::cout << "Raft server listening on " << host_list[server_id] << std::endl;

    // Initialize gRPC stubs for peer nodes
    for (int i = 0; i < host_list.size(); i++) {
        if (i != server_id) {
            peer_stubs.push_back(Raft::NewStub(grpc::CreateChannel(host_list[i], grpc::InsecureChannelCredentials())));
        } else {
            peer_stubs.push_back(nullptr);
        }
    }

    ResetElectionTimeout();
 
    if(server_id == 0){
        std::thread(&RaftServer::StartElection, this).detach();
    }
    SetElectionAlarm(election_timeout);
    server->Wait();
}


void RaftServer::StartServer() {
    signal(SIGALRM, &SignalHandler);
    alarm_handler_server = this;

    ServerBuilder builder;
    builder.AddListeningPort(host_list[server_id], grpc::InsecureServerCredentials());

    // Register Raft and KeyValueStore services
    builder.RegisterService(static_cast<keyvaluestore::Raft::Service*>(this));
    builder.RegisterService(static_cast<keyvaluestore::KeyValueStore::Service*>(this));

    server = builder.BuildAndStart();
    // std::cout << "Raft server listening on " << host_list[server_id] << std::endl;
    server->Wait();
}

Status RaftServer::NotifyNewRaftGroup(
    ServerContext* context,
    const NewRaftGroupRequest* request,
    NewRaftGroupResponse* response
) {
    server_name = request->server_name();
    
    auto it = std::find(request->server_instances().begin(), request->server_instances().end(), server_name);
    if (it == request->server_instances().end()) {
        response->set_success(false);
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "Server name not found in new raft group instances");
    }
    server_id = std::distance(request->server_instances().begin(), it);

    host_list.assign(request->server_instances().begin(), request->server_instances().end());
    peer_stubs.clear();

    // Initialize gRPC stubs for each peer, excluding self
    for (int i = 0; i < host_list.size(); i++) {
        if (i != server_id) {
            peer_stubs.push_back(Raft::NewStub(grpc::CreateChannel(host_list[i], grpc::InsecureChannelCredentials())));
        } else {
            peer_stubs.push_back(nullptr);
        }
    }

    // Reset the election timeout and, if this server is the initial leader, start the election process
    ResetElectionTimeout();
    if (server_id == 0) {
        std::thread(&RaftServer::StartElection, this).detach();
    }
    SetElectionAlarm(election_timeout);

    response->set_success(true);
    response->set_leader_server("");
    return Status::OK;
}


void RaftServer::Wait() {
    server->Wait();
}

Status RaftServer::AppendEntries(
    ServerContext* context,
    const AppendEntriesRequest* request,
    AppendEntriesResponse* response
) {
    std::lock_guard<std::mutex> lock(state_mutex);

    // Set the current term in the response
    response->set_term(current_term);
    response->set_success(false);

    // Step 1: If the term in the request is smaller than the current term, reject the request
    if (request->term() < current_term) {
        return Status::OK;
    }

    // Step 2: Update the current term if the request has a higher term
    if (request->term() > current_term) {
        current_term = request->term();
        BecomeFollower(request->leader_id());

        // Persist the updated term and follower state
        PersistRaftState();
    }

    // Step 3: Handle configuration change if detected in the entries
    // Case 1: When it is a start request -> each follower should identify the new instance using the host_list and the new config & create a gRPC channel with it.
    // They should also update the old_config/host_list.
    // New instance -> should create a gRPC channel with leader and everyone in the new config except itself. It should also set its host_list here.

    // Case 2: When it is a leave request ->  each follower should identify the leaving instance using the host_list and the new config & remove the gRPC stub with it.
    // They should also update the old_config/host_list.
    if (!request->new_config().empty()) {
        std::vector<std::string> new_config;
        for (const auto& entry : request->new_config()) {
            new_config.push_back(entry);
        }

        bool is_new_instance = (host_list.size() == 1 && host_list[0] == server_name);
        if (new_config.size() > host_list.size()) {
            // Case 1: Handle Start Request - Adding a new node
            if (is_new_instance) {
                host_list = new_config;
                peer_stubs.resize(new_config.size());

                for (size_t i = 0; i < new_config.size(); ++i) {
                    if (new_config[i] != server_name) {
                        peer_stubs[i] = Raft::NewStub(grpc::CreateChannel(new_config[i], grpc::InsecureChannelCredentials()));
                        std::cout << "New instance created gRPC channel with instance: " << new_config[i] << std::endl;
                    } else {
                        // Set itself as nullptr to avoid self-communication
                        peer_stubs[i] = nullptr;
                    }
                }
            } else {
                // Existing instances
                for (const auto& instance : new_config) {
                    if (std::find(host_list.begin(), host_list.end(), instance) == host_list.end()) {
                        // New instance found; create gRPC channel with it
                        peer_stubs.push_back(Raft::NewStub(grpc::CreateChannel(instance, grpc::InsecureChannelCredentials())));
                        std::cout << "New gRPC channel created for instance: " << instance << std::endl;
                    }
                }
                host_list = new_config;
            }
            
        } else if (new_config.size() < host_list.size()) {
            // Case 2: Handle Leave Request - Removing a node
            for (size_t i = 0; i < host_list.size(); ++i) {
                if (std::find(new_config.begin(), new_config.end(), host_list[i]) == new_config.end()) {
                    // Instance to remove found
                    if (i != request->leader_id()) {
                        // Clear the gRPC stub for the removed instance
                        peer_stubs[i].reset();  // Close the gRPC channel
                        std::cout << "Removed gRPC channel for instance: " << host_list[i] << std::endl;

                        host_list.erase(host_list.begin() + i);
                        peer_stubs.erase(peer_stubs.begin() + i);
                    }
                }
            }

        }

    }

    SetElectionAlarm(election_timeout);

    // Step 3: Check if the previous log index is out of bounds or the log is empty
    if (raft_log.empty()) {
        // If the log is empty, ensure that prev_log_index is -1 (indicating no prior logs)
        if (request->prev_log_index() != -1) {
            return Status::OK;  // Mismatch: follower's log is empty, but prev_log_index isn't -1
        }
    } else {
        // If the log is not empty, ensure the prev_log_index is valid
        if (request->prev_log_index() >= raft_log.size() || 
            (request->prev_log_index() != -1 && raft_log[request->prev_log_index()].term() != request->prev_log_term())) {
            return Status::OK;  // Mismatch between the log's term and the request's prev_log_term
        }
    }

    // Step 4: Remove conflicting entries in the follower's log, if necessary
    if (request->prev_log_index() != -1 && request->prev_log_index() < raft_log.size() - 1) {
        raft_log.resize(request->prev_log_index() + 1);
    }

    // Step 5: Append new log entries from the leader
    for (const auto& entry : request->entries()) {
        raft_log.push_back(entry);
    }

    // Step 6: Update commit index and apply to the state machine
    if (request->leader_commit() > commit_index) {
        commit_index = std::min(request->leader_commit(), static_cast<int64_t>(raft_log.size() - 1));
        PersistRaftStateInBackground(request);

        // Apply the committed log entries to the state machine
        while (last_applied < commit_index) {
            last_applied++;
            if (raft_log[last_applied].key() != "config_change") {
                std::string old_value;
                // Apply the log entry to the state machine
                // std::cout << "Applying log entry: " << raft_log[last_applied].key() << " -> " << raft_log[last_applied].value() << std::endl;
                db_.Put(raft_log[last_applied].key(), raft_log[last_applied].value(), old_value);

            }
        }

    } else {
        // Write the batch to RocksDB
        PersistRaftStateInBackground(request);
    }

    current_leader = request->leader_id();

    // Step 7: Reset the election timeout
    ResetElectionTimeout();

    // Step 8: Set success to true and return
    response->set_success(true);
    return Status::OK;
}

Status RaftServer::RequestVote(
    ServerContext* context,
    const RequestVoteRequest* request,
    RequestVoteResponse* response
) {
    std::lock_guard<std::mutex> lock(state_mutex);

    response->set_term(current_term);
    response->set_vote_granted(false);

    // If the term in the request is older, reject the vote request
    if (request->term() < current_term) {
        return Status::OK;
    }

    // If the term is newer, update the current term and become a follower
    if (request->term() > current_term) {
        current_term = request->term();
        BecomeFollower(-1);

        // Persist the updated term and follower state
        ResetElectionTimeout();
        PersistRaftState();
    }

    // Reset the election timeout since we're considering a vote
    SetElectionAlarm(election_timeout);
    
    // Check if the candidate's log is at least as up-to-date as ours
    bool log_ok = false;
    if (raft_log.empty()) {
        log_ok = true;
    } else {
        if (request->last_log_term() > raft_log.back().term()) {
            log_ok = true;
        } else if (request->last_log_term() == raft_log.back().term() && request->last_log_index() >= raft_log.size() - 1) {
            log_ok = true;
        }
    }

    // Grant vote if we haven't already voted and the candidate's log is up-to-date
    if ((voted_for == -1 || voted_for == request->candidate_id()) && log_ok) {
        voted_for = request->candidate_id();
        response->set_vote_granted(true);

        // Persist the fact that we voted for this candidate
        PersistRaftState();
    }

    return Status::OK;
}

void RaftServer::BecomeFollower(int leader_id) {
    current_leader = leader_id;
    state = RaftState::FOLLOWER;
    voted_for = -1;

    // Persist the new follower state and reset the voted_for field
    PersistRaftState();
}

void RaftServer::StartElection() {
    state = RaftState::CANDIDATE;
    current_term++;
    int initial_term = current_term;
    voted_for = server_id;

    std::atomic<int> votes_gained(1);

    // Persist current term and voted_for
    PersistRaftState();

    ResetElectionTimeout();
    SetElectionAlarm(election_timeout);

    // Send RequestVote RPCs to all peers
    for (int i = 0; i < host_list.size(); ++i) {
        if (i != server_id) {
            std::thread(&RaftServer::InvokeRequestVote, this, i, &votes_gained).detach();
        }
    }

    // Wait for a majority of votes
    while (votes_gained <= host_list.size() / 2 && state == RaftState::CANDIDATE && current_term == initial_term) {
    }

    if (votes_gained > host_list.size() / 2) {
        BecomeLeader();
    }
    else {
        BecomeFollower(-1);
    }
}

void RaftServer::BecomeLeader() {
    state = RaftState::LEADER;
    current_leader = server_id;

    ResetElectionTimeout();
    next_index.assign(host_list.size(), raft_log.size());
    match_index.assign(host_list.size(), -1);

    std::cout << "Server " << host_list[server_id] << " became the leader" << std::endl;
    SendHeartbeat();
    SetElectionAlarm(heartbeat_interval);
}

void RaftServer::SendHeartbeat() {
    while (state == RaftState::LEADER) {
        for (int i = 0; i < host_list.size(); ++i) {
            if (i != server_id) {
                std::thread(&RaftServer::InvokeAppendEntries, this, i).detach();
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_interval));
    }
}

void RaftServer::ReplicateLogEntries() {
    SetElectionAlarm(election_timeout);

    for (int i = 0; i < host_list.size(); ++i) {
        if (i != server_id) {
            std::thread(&RaftServer::InvokeAppendEntries, this, i).detach();
        }
    }
}

void RaftServer::InvokeAppendEntries(int peer_id) {
    thread_pool.enqueue([this, peer_id] { 
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(heartbeat_interval));
        AppendEntriesRequest request;
        AppendEntriesResponse response;

        // Set the current term and leader ID
        request.set_term(current_term);
        request.set_leader_id(server_id);

        // Handle the case where the log is empty
        if (raft_log.empty()) {
            request.set_prev_log_index(-1);
            request.set_prev_log_term(-1);
        } else {
            // Set the previous log index and term
            request.set_prev_log_index(next_index[peer_id] - 1);
            request.set_prev_log_term((next_index[peer_id] - 1 == -1) ? -1 : raft_log[next_index[peer_id] - 1].term());

            // Add log entries to the request starting from the next index for this peer
            for (int i = next_index[peer_id]; i < raft_log.size(); ++i) {
                *request.add_entries() = raft_log[i];
            }
        }

        // Set the leader's commit index
        request.set_leader_commit(commit_index);

        // Send the AppendEntries RPC to the peer
        auto status = peer_stubs[peer_id]->AppendEntries(&context, request, &response);
        if(!status.ok()){
            return;
        }

        // Update the next_index and match_index based on the response
        if (response.success()) {
            next_index[peer_id] = raft_log.size();
            match_index[peer_id] = raft_log.size() - 1;
        } else {
            // Decrement the next_index to retry with the previous entry
            next_index[peer_id] = std::max(0, next_index[peer_id] - 1);
        }
    });
}

void RaftServer::InvokeRequestVote(int peer_id, std::atomic<int>* votes_gained) {
    thread_pool.enqueue([this, peer_id, votes_gained] {
        grpc::ClientContext context;
        RequestVoteRequest request;
        RequestVoteResponse response;

        request.set_term(current_term);
        request.set_candidate_id(server_id);

        if (raft_log.empty()) {
            request.set_last_log_index(-1);
            request.set_last_log_term(-1);
        } else {
            request.set_last_log_index(raft_log.size() - 1);
            request.set_last_log_term(raft_log.back().term());
        }

        // Send the RequestVote RPC
        peer_stubs[peer_id]->RequestVote(&context, request, &response);

        // Increment the votes_gained if the vote was granted
        if (response.vote_granted()) {
            (*votes_gained)++;
        }
    });
}


void RaftServer::ResetElectionTimeout() {
    election_timeout = min_election_timeout + (rand() % (max_election_timeout - min_election_timeout));
}

void RaftServer::SetElectionAlarm(int timeout_ms) {
    struct itimerval timer;

    timer.it_value.tv_sec = timeout_ms / 1000;
    timer.it_value.tv_usec = (timeout_ms % 1000) * 1000;

    timer.it_interval.tv_sec = 0;
    timer.it_interval.tv_usec = 0;

    if (setitimer(ITIMER_REAL, &timer, nullptr) == -1) {
        perror("Error setting timer");
    }
}

void RaftServer::HandleAlarm() {
    if (state == RaftState::LEADER) {
        ReplicateLogEntries();
    } else {
        StartElection();
    }
}

Status RaftServer::Init(
    ServerContext* context,
    const InitRequest* request,
    InitResponse* response
){  

    // print current leader
    // if(current_leader != -1){
    //     std::cout << "Current leader at server side: " << host_list[current_leader] << std::endl;
    // } else {
    //     std::cout << "Current leader: None" << std::endl;
    // }

    if(state != RaftState::LEADER){
        response->set_success(false);
        if(current_leader != -1){
            response->set_leader_server(host_list[current_leader]);
        } else {
            response->set_leader_server("");
        }
        return Status::OK;
    }

    response->set_success(true);
    if(current_leader != -1){
        response->set_leader_server(host_list[current_leader]);
    } else {
        response->set_leader_server("");
    }
    return Status::OK;
}

Status RaftServer::Get(
    ServerContext* context,
    const GetRequest* request,
    GetResponse* response
) {
    if(state != RaftState::LEADER){
        response->set_key_found(false);
        response->set_value("");
        if(current_leader != -1){
            response->set_leader_server(host_list[current_leader]);
        } else {
            response->set_leader_server("");
        }
        return Status::OK;
    }

    std::string value;
    bool key_found = db_.Get(request->key(), value);
    if (key_found) {
        response->set_value(value);
        response->set_key_found(true);
    } else {
        // std::cout << "Key not found: " << request->key() << std::endl;
        response->set_key_found(false);
    }
    if(current_leader != -1){
        response->set_leader_server(host_list[current_leader]);
    } else {
        response->set_leader_server("");
    }
    return Status::OK;
}

Status RaftServer::Put(
    ServerContext* context, 
    const PutRequest* request, 
    PutResponse* response
) {
    if(state != RaftState::LEADER){
        response->set_key_found(false);
        response->set_leader_server(host_list[current_leader]);
        return Status::OK;
    }

    LogEntry entry;
    entry.set_term(current_term);
    entry.set_key(request->key());
    entry.set_value(request->value());

    // 1. Append the new entry to the leader's own log
    raft_log.push_back(entry);

    // 2. Replicate the log entry to all followers
    ReplicateLogEntries();

    // 3. Wait until a majority of followers replicate the entry
    // Track how many followers have replicated the entry
    int majority_count = host_list.size() / 2 + 1;
    int replicated_count = 1; // Leader has already replicated
    const int poll_interval_ms = 50;  // Poll every 50 milliseconds
    // const int timeout_ms = 5000;      // Timeout after 5 seconds
    // auto start_time = std::chrono::steady_clock::now();

    while (state == RaftState::LEADER) {
        replicated_count = 1;

        for (int i = 0; i < host_list.size(); i++) {
            if (match_index[i] >= raft_log.size() - 1) {
                replicated_count++;
            }
        }

        // If a majority of followers have replicated, break the loop
        if (replicated_count >= majority_count) {
            break;
        }

        // Check if the polling has timed out
        // auto elapsed_time = std::chrono::steady_clock::now() - start_time;
        // if (std::chrono::duration_cast<std::chrono::milliseconds>(elapsed_time).count() > timeout_ms) {
        //     std::cerr << "Put operation timed out while waiting for majority replication." << std::endl;
        //     response->set_key_found(false);
        //     return Status::CANCELLED;
        // }

        // Sleep for the polling interval before checking again
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
    }

    if(state != RaftState::LEADER){
        response->set_key_found(false);
        response->set_leader_server(host_list[current_leader]);
        return Status::OK;
    }

    std::string old_value;
    int result = -1;

    // 4. If a majority of followers have replicated, commit the entry
    {
        std::lock_guard<std::mutex> lock(state_mutex);
        commit_index = raft_log.size() - 1;
    }

    PersistRaftState();

    // Apply the committed log entry to the state machine (key-value store)
    while (last_applied < commit_index) {
        last_applied++;
        result = db_.Put(raft_log[last_applied].key(), raft_log[last_applied].value(), old_value);
    }
    last_applied = commit_index;

    // 5. Prepare the response based on the result of applying the log
    if (result == 0) {
        response->set_old_value(old_value);
        response->set_key_found(true);
    } else {
        response->set_key_found(false);
    }
    if(current_leader != -1){
        response->set_leader_server(host_list[current_leader]);
    } else {
        response->set_leader_server("");
    }

    return Status::OK;
}

Status RaftServer::Shutdown(
    ServerContext* context, 
    const ShutdownRequest* request, 
    ShutdownResponse* response
) {
    if(state != RaftState::LEADER){
        response->set_success(false);
        return Status::OK;
    }
    response->set_success(true);
    return Status::OK;
}

Status RaftServer::Die(ServerContext* context, const DieRequest* request, DieResponse* response) {
    std::string server_name = request->server_name();
    bool clean = request->clean();

    // Send the success response to the client before shutting down
    response->set_success(true);

    // Spawn a new thread to handle the shutdown after the response is sent
    std::thread([this, clean]() {
        if (clean) {
            // Graceful shutdown without deadline
            std::cout << "Server performing graceful shutdown." << std::endl;
            server->Shutdown();  // Gracefully shutdown, wait for ongoing RPCs to complete
            server->Wait();      // Wait for the server to complete all operations (optional)
            std::exit(0);        // Normal exit after graceful shutdown
        } else {
            // Forceful shutdown: immediately terminate the process
            std::cout << "Server performing forceful kill." << std::endl;
            std::exit(1);  // Immediate, abnormal exit
        }
    }).detach();  // Detach the thread to allow shutdown in the background

    return Status::OK;
}

Status RaftServer::Start(
    ServerContext* context,
    const StartRequest* request,
    StartResponse* response
) {
    if (state != RaftState::LEADER) {
        response->set_success(false);
        response->set_leader_server(host_list[current_leader]);
        return Status::OK;
    }

    // Check if the instance name in the request is empty
    if (request->instance_name().empty()) {
        response->set_success(false);
        return Status::OK;
    }

    // Add the new node to the new configuration
    std::vector<std::string> new_config = host_list;
    new_config.push_back(request->instance_name());

    // Create a configuration change entry in the log
    LogEntry config_entry;
    config_entry.set_term(current_term);
    config_entry.set_key("config_change");
    config_entry.set_value(SerializeHostList(new_config));
    config_entry.set_config_change(true);  // Mark as a configuration change

    // Append the entry to the Raft log and replicate it to followers
    raft_log.push_back(config_entry);
    // Create a gRPC channel of the leader with the new instance
    peer_stubs.push_back(Raft::NewStub(grpc::CreateChannel(request->instance_name(), grpc::InsecureChannelCredentials())));
    ReplicateLogEntries();

    // 3. Wait until a majority of followers replicate the entry
    // Track how many followers have replicated the entry
    int old_config_majority_count = host_list.size() / 2 + 1;
    int new_config_majority_count = new_config.size() / 2 + 1;
    int old_config_replicated_count = 1; // Leader has already replicated
    int new_config_replicated_count = 1;
    const int poll_interval_ms = 50;

    while (state == RaftState::LEADER) {
        old_config_replicated_count = 1;
        new_config_replicated_count = 1;

        for (int i = 0; i < host_list.size(); i++) {
            if (match_index[i] >= raft_log.size() - 1) {
                old_config_replicated_count++;
            }
        }

        for (int i = 0; i < new_config.size(); i++) {
            if (match_index[i] >= raft_log.size() - 1) {
                new_config_replicated_count++;
            }
        }

        // If a majority of followers have replicated, break the loop
        if (old_config_replicated_count >= old_config_majority_count &&
            new_config_replicated_count >= new_config_majority_count) {
            break;
        }

        // Sleep for a short duration before checking again
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
    }

    if (state != RaftState::LEADER) {
        response->set_success(false);
        response->set_leader_server(host_list[current_leader]);
        return Status::OK;
    }

    // If a majority of followers have replicated, commit the entry
    {
        std::lock_guard<std::mutex> lock(state_mutex);
        commit_index = raft_log.size() - 1;
    }

    PersistRaftState();

    // Apply the committed log entry to the state machine (key-value store)
    while (last_applied < commit_index) {
        last_applied++;
        if (raft_log[last_applied].key() == "config_change") {
            // Update the configuration
            host_list = DeserializeHostList(raft_log[last_applied].value());
        } else {
            // Apply the log entry to the state machine
            std::string old_value;
            db_.Put(raft_log[last_applied].key(), raft_log[last_applied].value(), old_value);
        }
    }
    last_applied = commit_index;

    // Respond with success and current leader information
    response->set_success(true);
    response->set_leader_server(host_list[current_leader]);
    return Status::OK;
}


Status RaftServer::Leave(
    ServerContext* context,
    const LeaveRequest* request,
    LeaveResponse* response
) {
    if (state != RaftState::LEADER) {
        response->set_success(false);
        response->set_leader_server(host_list[current_leader]);
        return Status::OK;
    }

    // Check if the instance name in the request is empty
    if (request->instance_name().empty()) {
        response->set_success(false);
        return Status::OK;
    }

    // Remove the node from the new configuration
    std::vector<std::string> new_config = host_list;
    new_config.erase(std::remove(new_config.begin(), new_config.end(), request->instance_name()), new_config.end());

    // Create a configuration change entry in the log
    LogEntry config_entry;
    config_entry.set_term(current_term);
    config_entry.set_key("config_change");
    config_entry.set_value(SerializeHostList(new_config));
    config_entry.set_config_change(true);  // Mark as a configuration change

    // Append the entry to the Raft log and replicate it to followers
    raft_log.push_back(config_entry);

    // Erase the gRPC channel of the leaving instance, unless it's the leader
    if (request->instance_name() != server_name) {
        auto it = std::find(host_list.begin(), host_list.end(), request->instance_name());
        if (it != host_list.end()) {
            size_t index = std::distance(host_list.begin(), it);
            peer_stubs[index].reset();  // Clear the gRPC channel for the leaving instance
        }
    }

    bool leader_removed = (request->instance_name() == host_list[current_leader]);

    ReplicateLogEntries();

    // 3. Wait until a majority of followers replicate the entry
    // Track how many followers have replicated the entry
    int old_config_majority_count = host_list.size() / 2 + 1;
    int new_config_majority_count = new_config.size() / 2 + 1;
    int old_config_replicated_count = 1; // Leader has already replicated
    int new_config_replicated_count = leader_removed ? 0 : 1; // Exclude leader's vote if it’s removed
    const int poll_interval_ms = 50;

    while (state == RaftState::LEADER) {
        old_config_replicated_count = 1;
        new_config_replicated_count = leader_removed ? 0 : 1;

        for (int i = 0; i < host_list.size(); i++) {
            if (match_index[i] >= raft_log.size() - 1) {
                old_config_replicated_count++;
            }
        }

        for (int i = 0; i < new_config.size(); i++) {
            if (match_index[i] >= raft_log.size() - 1) {
                new_config_replicated_count++;
            }
        }

        // If a majority of followers have replicated, break the loop
        if (old_config_replicated_count >= old_config_majority_count &&
            new_config_replicated_count >= new_config_majority_count) {
            break;
        }

        // Sleep for a short duration before checking again
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
    }

    if (state != RaftState::LEADER) {
        response->set_success(false);
        response->set_leader_server(host_list[current_leader]);
        return Status::OK;
    }

    // If a majority of followers have replicated, commit the entry
    {
        std::lock_guard<std::mutex> lock(state_mutex);
        commit_index = raft_log.size() - 1;
    }

    PersistRaftState();

    // Check if the leader needs to step down
    if (leader_removed) {
        // Prepare response for client before shutdown
        response->set_success(true);
        response->set_leader_server("");

        // Start a separate thread to handle shutdown after responding to client
        std::thread([this]() {
            BecomeFollower(-1);  // Step down from leader role

            // Clear all gRPC channels, as this node is no longer the leader
            for (auto& stub : peer_stubs) {
                stub.reset();
            }

            std::cout << "Leader removed from configuration, shutting down process." << std::endl;
            std::exit(0);  // Terminate the process
        }).detach();  // Detach the thread to allow it to run independently

        return Status::OK;
    }

    // Apply the committed log entry to the state machine (key-value store)
    while (last_applied < commit_index) {
        last_applied++;
        if (raft_log[last_applied].key() == "config_change") {
            // Update the configuration
            host_list = DeserializeHostList(raft_log[last_applied].value());
        } else {
            // Apply the log entry to the state machine
            std::string old_value;
            db_.Put(raft_log[last_applied].key(), raft_log[last_applied].value(), old_value);
        }
    }
    last_applied = commit_index;

    // Respond with success and current leader information
    response->set_success(true);
    response->set_leader_server(host_list[current_leader]);
    return Status::OK;
}

Status RaftServer::PartitionChange(
    ServerContext* context,
    const PartitionChangeRequest* request,
    PartitionChangeResponse* response
) {
    if (state != RaftState::LEADER) {
        response->set_success(false);
        response->set_leader_server(host_list[current_leader]);
        return Status::OK;
    }

    std::vector<std::string> keys;
    db_.GetAllKeys(keys);
    std::vector<std::pair<unsigned long, unsigned long>> key_ranges;
    for (const auto& key_range : request->key_ranges()) {
        key_ranges.emplace_back(key_range.key_start(), key_range.key_end());
    }
    
    for (const auto &key: keys) {
        for (const auto& key_range : key_ranges) {
            // Compute the hash of the key
            std::hash<std::string> hash_fn;
            unsigned long hash = hash_fn(key);
            if(hash >= key_range.first && hash <= key_range.second){
                // Get the value for the key
                std::string value;
                db_.Get(key, value);

                auto* key_value_pair = response->add_key_values();
                key_value_pair->set_key(key);
                key_value_pair->set_value(value);
            }
        }
    }

    // Respond with success and current leader information
    response->set_success(true);
    response->set_leader_server(host_list[current_leader]);

    return Status::OK;
}

std::string RaftServer::SerializeHostList(std::vector<std::string>& host_list) {
    std::string serialized_host_list;
    for (const auto& instance_name : host_list) {
        serialized_host_list += instance_name + ",";  // Add each instance name followed by a comma
    }
    if (!serialized_host_list.empty()) {
        serialized_host_list.pop_back();  // Remove the trailing comma
    }
    return serialized_host_list;
}

std::vector<std::string> RaftServer::DeserializeHostList(const std::string& serialized_host_list) {
    std::vector<std::string> host_list;
    size_t start = 0;
    size_t end = serialized_host_list.find(',');

    while (end != std::string::npos) {
        host_list.push_back(serialized_host_list.substr(start, end - start));
        start = end + 1;
        end = serialized_host_list.find(',', start);
    }

    // Add the last host in the list (or the only one if no commas were found)
    if (start < serialized_host_list.size()) {
        host_list.push_back(serialized_host_list.substr(start));
    }

    return host_list;
}
