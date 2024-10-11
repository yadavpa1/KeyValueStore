#include "raft_server.h"
#include <iostream>
#include <fstream>
#include <thread>
#include <atomic>
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <algorithm>
#include <cctype>
#include <locale>

using grpc::Status;
using grpc::ServerBuilder;
using grpc::ServerContext;

const int RaftServer::min_election_timeout = 800;
const int RaftServer::max_election_timeout = 1600;
const int RaftServer::heartbeat_interval = 50;

RaftServer::RaftServer(int server_id, const std::vector<std::string>& host_list, const std::string &db_path, size_t cache_size)
    : server_id(server_id),
      host_list(host_list),
      state(RaftState::FOLLOWER),
      current_term(0),
      voted_for(-1),
      commit_index(0),
      last_applied(0),
      current_leader(-1),
      db_(db_path, cache_size)
{}

void RaftServer::Run() {
    ServerBuilder builder;
    builder.AddListeningPort(host_list[server_id], grpc::InsecureServerCredentials());

    builder.RegisterService(static_cast<raft_group::Raft::Service*>(this));

    builder.RegisterService(static_cast<raft_group::KeyValueStore::Service*>(this));

    server = builder.BuildAndStart();
    std::cout << "Raft server listening on " << host_list[server_id] << std::endl;

    for (int i = 0; i < host_list.size(); i++) {
        if (i != server_id) {
            peer_stubs.push_back(Raft::NewStub(grpc::CreateChannel(host_list[i], grpc::InsecureChannelCredentials())));
        } else {
            peer_stubs.push_back(nullptr);
        }
    }

    ResetElectionTimeout();
    std::thread(&RaftServer::SendHeartbeat, this).detach();

    if(server_id==0){
        std::thread(&RaftServer::StartElection, this).detach();
    }

    server->Wait();
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
    }

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

    // Step 4: Append new entries from the leader to the log
    for (const auto& entry : request->entries()) {
        raft_log.push_back(entry);
    }

    // Step 5: Update the commit index if necessary
    if (request->leader_commit() > commit_index) {
        commit_index = std::min(request->leader_commit(), static_cast<int64_t>(raft_log.size() - 1));

        // Apply the committed log entries to the state machine
        while (last_applied < commit_index) {
            last_applied++;
            std::string old_value;
            db_.Put(raft_log[last_applied].key(), raft_log[last_applied].value(), old_value);
        }
    }

    // Step 6: Set success to true and return
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

    if (request->term() < current_term) {
        return Status::OK;
    }

    if (request->term() > current_term) {
        current_term = request->term();
        BecomeFollower(request->candidate_id());
    }

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

    if ((voted_for == -1 || voted_for == request->candidate_id()) && log_ok) {
        voted_for = request->candidate_id();
        response->set_vote_granted(true);
    }

    return Status::OK;
}

Status RaftServer::Heartbeat(
    ServerContext* context,
    const HeartbeatRequest* request,
    HeartbeatResponse* response
) {
    std::lock_guard<std::mutex> lock(state_mutex);
    response->set_term(current_term);
    response->set_success(false);

    if (request->term() < current_term) {
        return Status::OK;
    }

    if (request->term() > current_term) {
        current_term = request->term();
        BecomeFollower(request->leader_id());
    }

    // Reset the election timeout when heartbeats are received
    ResetElectionTimeout();
    response->set_success(true);

    return Status::OK;
}

void RaftServer::BecomeFollower(int leader_id) {
    current_leader = leader_id;
    state = RaftState::FOLLOWER;
    voted_for = -1;
}

void RaftServer::StartElection() {
    state = RaftState::CANDIDATE;
    current_term++;
    voted_for = server_id;

    std::atomic<int> votes_gained(1);

    for (int i = 0; i < host_list.size(); ++i) {
        if (i != server_id) {
            std::thread(&RaftServer::InvokeRequestVote, this, i, &votes_gained).detach();
        }
    }

    // Wait for majority of votes
    while (votes_gained <= host_list.size() / 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (votes_gained > host_list.size() / 2) {
        BecomeLeader();
    }
}

void RaftServer::BecomeLeader() {
    state = RaftState::LEADER;
    current_leader = server_id;

    ResetElectionTimeout();
    next_index.assign(host_list.size(), raft_log.size());
    match_index.assign(host_list.size(), -1);

    SendHeartbeat();
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
    for (int i = 0; i < host_list.size(); ++i) {
        if (i != server_id) {
            std::thread(&RaftServer::InvokeAppendEntries, this, i).detach();
        }
    }
}

void RaftServer::InvokeAppendEntries(int peer_id) {
    grpc::ClientContext context;
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
    peer_stubs[peer_id]->AppendEntries(&context, request, &response);

    // Update the next_index and match_index based on the response
    if (response.success()) {
        next_index[peer_id] = raft_log.size();
        match_index[peer_id] = raft_log.size() - 1;
    } else {
        // Decrement the next_index to retry with the previous entry
        next_index[peer_id] = std::max(0, next_index[peer_id] - 1);
    }
}

void RaftServer::InvokeRequestVote(int peer_id, std::atomic<int>* votes_gained) {
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
}


void RaftServer::ResetElectionTimeout() {
    election_timeout = min_election_timeout + (rand() % (max_election_timeout - min_election_timeout));
}

Status RaftServer::Init(
    ServerContext* context,
    const InitRequest* request,
    InitResponse* response
){
    if(state!=RaftState::LEADER){
        response->set_success(false);
        if(current_leader!=-1){
            response->set_leader_server(host_list[current_leader]);
        } else {
            response->set_leader_server("");
        }
        return Status::OK;
    }

    response->set_success(true);
    return Status::OK;
}

Status RaftServer::Get(
    ServerContext* context,
    const GetRequest* request,
    GetResponse* response
) {
    if(state!=RaftState::LEADER){
        response->set_key_found(false);
        response->set_value("");
        if(current_leader!=-1){
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
        std::cout << "Key not found: " << request->key() << std::endl;
        response->set_key_found(false);
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

    raft_log.push_back(entry);
    ReplicateLogEntries();

    std::string old_value;
    int result = db_.Put(request->key(), request->value(), old_value);

    if (result == 0) {
        response->set_old_value(old_value);
        response->set_key_found(true);
    } else {
        response->set_key_found(false);
    }
    return Status::OK;
}

Status RaftServer::Shutdown(
    ServerContext* context, 
    const ShutdownRequest* request, 
    ShutdownResponse* response
) {
    if(state!=RaftState::LEADER){
        response->set_success(false);
        return Status::OK;
    }
    response->set_success(true);
    return Status::OK;
}

static inline std::string trim(const std::string &s) {
    auto start = s.begin();
    while (start != s.end() && std::isspace(*start)) {
        start++;
    }

    auto end = s.end();
    do {
        end--;
    } while (std::distance(start, end) > 0 && std::isspace(*end));

    return std::string(start, end + 1);
}

std::vector<std::string> readConfigFile(const std::string &filepath) {
    std::vector<std::string> host_list;
    std::ifstream config_file(filepath);

    // Check if the file is opened successfully
    if (!config_file.is_open()) {
        std::cerr << "Error: Could not open config file: " << filepath << std::endl;
        return host_list; // return empty list to avoid undefined behavior
    }

    std::string line;
    while (std::getline(config_file, line)) {
        // Trim and skip empty lines (also remove any trailing or leading spaces)
        line = trim(line);
        if (!line.empty()) {
            host_list.push_back(line);
        }
    }

    // Check if hosts were loaded
    if (host_list.empty()) {
        std::cerr << "Error: No hosts found in the config file." << std::endl;
    }

    return host_list;
}

int main(int argc, char** argv) {
    std::string config_file;
    int server_id = -1;

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (std::string(argv[i]) == "-c") {
            config_file = argv[i + 1];
            i++;
        } else if (std::string(argv[i]) == "-i") {
            server_id = std::stoi(argv[i + 1]);
            i++;
        }
    }

    // Check if required arguments are provided
    if (config_file.empty() || server_id == -1) {
        std::cerr << "Usage: " << argv[0] << " -c <config_file> -i <server_id>" << std::endl;
        return 1;
    }

    // Read host list from file
    std::vector<std::string> host_list = readConfigFile(config_file);

    // Check if host_list is valid
    if (host_list.empty()) {
        std::cerr << "Error: Host list is empty or file could not be read properly" << std::endl;
        return 1;
    }

    // Validate server_id against the host list size
    if (server_id < 0 || server_id >= host_list.size()) {
        std::cerr << "Error: Invalid server_id. Must be between 0 and " << host_list.size() - 1 << std::endl;
        return 1;
    }

    // Start the Raft server
    std::string db_path = "raft_db";
    size_t cache_size = 20 * 1024 * 1024; // 20MB cache

    RaftServer raft_server(server_id, host_list, db_path, cache_size);
    raft_server.Run();

    return 0;
}