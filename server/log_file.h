#ifndef LOGFILE_H
#define LOGFILE_H
#include <fstream>
#include <string>
#include <vector>
#include "raft.grpc.pb.h"

class log_file {
    public:
        log_file(const std::string &file_path);
        int LoadCurrentTerm();
        int LoadVotedFor();
        std::vector<raft::LogEntry> LoadLog();

        void SaveCurrentTerm(int term);
        void SaveVotedFor(int candidate_id);
        void AppendLogEntry(const raft::LogEntry &entry);

    private:
        std::string file_path;
        std::ofstream log_output;
        std::ifstream log_input;

        std::string SerializeLogEntry(const raft::LogEntry &entry);
        raft::LogEntry DeserializeLogEntry(const std::string &entry_str);

        void OpenLogForWrite();
        void OpenLogForRead();
};

#endif