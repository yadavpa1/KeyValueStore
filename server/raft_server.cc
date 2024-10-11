#include "raft_server.h"
#include <grpcpp/grpcpp.h>
#include <thread>
#include <iostream>

RaftNode::RaftNode(int node_id, int cluster_id, const std::vector<std::string>& peer_addresses, const std::string& db_path)
    : node_id_(node_id), cluster_id_(cluster_id), current_term_(0), commit_index_(0), is_leader_(false), peer_addresses_(peer_addresses), db_(db_path, 1024) {}

grpc::Status RaftNode::RequestVote(grpc::ServerContext* context, const raft::RequestVoteRequest* request, raft::RequestVoteResponse* response) {
    std::lock_guard<std::mutex> lock(mtx_);
    
    if (request->term() > current_term_) {
        current_term_ = request->term();
        response->set_vote_granted(true);
    } else {
        response->set_vote_granted(false);
    }
    
    response->set_term(current_term_);
    return grpc::Status::OK;
}

grpc::Status RaftNode::AppendEntries(grpc::ServerContext* context, const raft::AppendEntriesRequest* request, raft::AppendEntriesResponse* response) {
    std::lock_guard<std::mutex> lock(mtx_);
    
    if (request->term() < current_term_) {
        response->set_success(false);
    } else {
        current_term_ = request->term();
        response->set_success(true);
        
        // Append new entries to log
        for (const auto& entry : request->entries()) {
            log_.push_back({entry.key(), entry.value(), entry.term()});
        }
        
        commit_index_ = request->leader_commit();
    }
    
    response->set_term(current_term_);
    return grpc::Status::OK;
}

grpc::Status RaftNode::Heartbeat(grpc::ServerContext* context, const raft::HeartbeatRequest* request, raft::HeartbeatResponse* response) {
    std::lock_guard<std::mutex> lock(mtx_);
    
    current_term_ = request->term();
    response->set_term(current_term_);
    return grpc::Status::OK;
}

void RaftNode::StartElection() {
    std::unique_lock<std::mutex> lock(mtx_);
    
    current_term_++;
    int votes = 1; // Vote for self

    // Send RequestVote RPCs to all peers
    for (const auto& peer : peer_addresses_) {
        raft::RequestVoteRequest request;
        request.set_term(current_term_);
        request.set_candidate_id(node_id_);

        raft::RequestVoteResponse response;
        
        // RPC call to the peer
        grpc::ClientContext context;
        auto stub = raft::RaftService::NewStub(grpc::CreateChannel(peer, grpc::InsecureChannelCredentials()));
        stub->RequestVote(&context, request, &response);
        
        if (response.vote_granted()) {
            votes++;
        }
    }
    
    if (votes > peer_addresses_.size() / 2) {
        BecomeLeader();
    }
}

void RaftNode::BecomeLeader() {
    is_leader_ = true;
    
    std::cout << "Node " << node_id_ << " became the leader in cluster " << cluster_id_ << " for term " << current_term_ << std::endl;
    
    // Start sending heartbeats
    std::thread([this]() { this->HeartbeatLoop(); }).detach();
}

void RaftNode::AppendLog(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(mtx_);
    
    log_.push_back(entry);
    commit_index_ = log_.size() - 1;

    // Apply logs to RocksDB
    ApplyLogsToDB();
}

void RaftNode::ApplyLogsToDB() {
    for (const auto& entry : log_) {
        std::string old_value;
        db_.Put(entry.key, entry.value, old_value);
    }
}

void RaftNode::Run() {
    grpc::ServerBuilder builder;
    
    std::string server_address("0.0.0.0:" + std::to_string(50050 + node_id_));
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    
    server_ = builder.BuildAndStart();
    
    std::cout << "Raft node " << node_id_ << " listening on " << server_address << std::endl;
    server_->Wait();
}

void RaftNode::HeartbeatLoop() {
    while (is_leader_) {
        for (const auto& peer : peer_addresses_) {
            raft::HeartbeatRequest request;
            request.set_term(current_term_);
            request.set_leader_id(node_id_);
            
            raft::HeartbeatResponse response;
            grpc::ClientContext context;
            
            auto stub = raft::RaftService::NewStub(grpc::CreateChannel(peer, grpc::InsecureChannelCredentials()));
            stub->Heartbeat(&context, request, &response);
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
}

bool RaftNode::IsLeader() const {
    return is_leader_;
}
