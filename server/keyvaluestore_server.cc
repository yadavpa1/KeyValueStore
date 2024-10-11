#include "keyvaluestore_server.h"
#include <iostream>

KeyValueStoreServer::KeyValueStoreServer(int node_id, int cluster_id, const std::string& db_path, const std::vector<std::string>& peers)
    : db_(db_path, 1024), raft_node_(node_id, cluster_id, peers, db_path) {}

grpc::Status KeyValueStoreServer::ManageSession(grpc::ServerContext* context, grpc::ServerReaderWriter<keyvaluestore::ServerResponse, keyvaluestore::ClientRequest>* stream) {
    keyvaluestore::ClientRequest request;
    
    while (stream->Read(&request)) {
        if (request.has_get_request()) {
            std::string value;
            keyvaluestore::GetResponse get_response;

            if (db_.Get(request.get_request().key(), value)) {
                get_response.set_value(value);
                get_response.set_key_found(true);
            } else {
                get_response.set_key_found(false);
            }
            
            keyvaluestore::ServerResponse response;
            response.mutable_get_response()->CopyFrom(get_response);
            stream->Write(response);
        }
        
        if (request.has_put_request()) {
            std::string old_value;
            std::string key = request.put_request().key();
            std::string value = request.put_request().value();
            
            // Append the put operation to Raft log for consensus
            LogEntry log_entry = {key, value, raft_node_.GetCurrentTerm()};
            raft_node_.AppendLog(log_entry);
            
            db_.Put(key, value, old_value);
            
            keyvaluestore::PutResponse put_response;
            put_response.set_old_value(old_value);
            put_response.set_key_found(true);
            
            keyvaluestore::ServerResponse response;
            response.mutable_put_response()->CopyFrom(put_response);
            stream->Write(response);
        }
    }
    
    return grpc::Status::OK;
}
