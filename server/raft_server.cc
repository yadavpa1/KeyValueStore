#include "raft_server.h"
#include <iostream>
#include <fstream>
#include <thread>
#include <atomic>
#include <grpcpp/grpcpp.h>
#include <chrono>

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
    response->set_term(current_term);
    response->set_success(false);

    if (request->term() < current_term) {
        return Status::OK;
    }

    if (request->term() > current_term) {
        current_term = request->term();
        BecomeFollower();
    }

    if (request->prev_log_index() >= raft_log.size() ||
        (request->prev_log_index() != -1 && raft_log[request->prev_log_index()].term() != request->prev_log_term())) {
        return Status::OK;
    }

    for (const auto& entry : request->entries()) {
        raft_log.push_back(entry);
    }

    if (request->leader_commit() > commit_index) {
        commit_index = std::min(request->leader_commit(), static_cast<int64_t>(raft_log.size() - 1));
        while (last_applied < commit_index) {
            last_applied++;
            std::string old_value;
            db_.Put(raft_log[last_applied].key(), raft_log[last_applied].value(), old_value);
        }
    }

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
        BecomeFollower();
    }

    if ((voted_for == -1 || voted_for == request->candidate_id()) &&
        (request->last_log_term() > raft_log.back().term() ||
        (request->last_log_term() == raft_log.back().term() && request->last_log_index() >= raft_log.size() - 1))) {
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
        BecomeFollower();
    }

    // Reset the election timeout when heartbeats are received
    ResetElectionTimeout();
    response->set_success(true);

    return Status::OK;
}

void RaftServer::BecomeFollower() {
    state = RaftState::FOLLOWER;
    voted_for = -1;
}

void RaftServer::StartElection() {
    state = RaftState::CANDIDATE;
    current_term++;
    voted_for = server_id;

    std::atomic<int> votes_gained(1);  // Self-vote
    for (int i = 0; i < host_list.size(); ++i) {
        if (i != server_id) {
            std::thread(&RaftServer::InvokeRequestVote, this, i, &votes_gained).detach();
        }
    }

    // Wait for majority of votes
    while (votes_gained <= host_list.size() / 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    BecomeLeader();
}

void RaftServer::BecomeLeader() {
    state = RaftState::LEADER;
    current_leader = server_id;

    // Initialize nextIndex and matchIndex for log replication
    next_index.assign(host_list.size(), raft_log.size());
    match_index.assign(host_list.size(), -1);

    // Send heartbeats to followers
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

    request.set_term(current_term);
    request.set_leader_id(server_id);
    request.set_prev_log_index(next_index[peer_id] - 1);
    request.set_prev_log_term((next_index[peer_id] - 1 == -1) ? -1 : raft_log[next_index[peer_id] - 1].term());
    for (int i = next_index[peer_id]; i < raft_log.size(); ++i) {
        *request.add_entries() = raft_log[i];
    }
    request.set_leader_commit(commit_index);

    peer_stubs[peer_id]->AppendEntries(&context, request, &response);

    if (response.success()) {
        next_index[peer_id] = raft_log.size();
        match_index[peer_id] = raft_log.size() - 1;
    } else {
        next_index[peer_id]--;
    }
}

void RaftServer::InvokeRequestVote(int peer_id, std::atomic<int>* votes_gained) {
    grpc::ClientContext context;
    RequestVoteRequest request;
    RequestVoteResponse response;

    request.set_term(current_term);
    request.set_candidate_id(server_id);
    request.set_last_log_index(raft_log.size() - 1);
    request.set_last_log_term(raft_log.empty() ? -1 : raft_log.back().term());

    peer_stubs[peer_id]->RequestVote(&context, request, &response);

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

std::vector<std::string> readConfigFile(const std::string &filepath) {
    std::vector<std::string> host_list;
    std::ifstream config_file(filepath);
    
    if (!config_file) {
        std::cerr << "Error: Could not open config file: " << filepath << std::endl;
        exit(1);
    }

    std::string line;
    while (std::getline(config_file, line)) {
        if (!line.empty()) {
            host_list.push_back(line);
        }
    }
    
    return host_list;
}

int main(int argc, char** argv) {
    std::string config_file;
    int server_id = -1;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-f" && i + 1 < argc) {
            config_file = argv[i + 1];
            i++;
        } else if (arg == "-i" && i + 1 < argc) {
            server_id = std::stoi(argv[i + 1]);
            i++;
        } else {
            std::cerr << "Usage: " << argv[0] << " -f <config_filepath> -i <server_id>" << std::endl;
            return 1;
        }
    }

    if (config_file.empty() || server_id == -1) {
        std::cerr << "Error: Missing -f <config_filepath> or -i <server_id>" << std::endl;
        return 1;
    }

    std::vector<std::string> host_list = readConfigFile(config_file);
    
    if (server_id < 0 || server_id >= host_list.size()) {
        std::cerr << "Error: Invalid server_id. Must be between 0 and " << host_list.size() - 1 << std::endl;
        return 1;
    }

    std::string db_path = "raft_db";
    size_t cache_size = 20 * 1024 * 1024; // 20MB cache

    RaftServer raft_server(server_id, host_list, db_path, cache_size);
    raft_server.Run();

    return 0;
}