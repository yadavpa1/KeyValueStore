#include "log_file.h"
#include <iostream>
#include <sstream>

log_file::log_file(const std::string& file_path){
    this->file_path = file_path;
}

void log_file::OpenLogForWrite() {
    log_output.open(file_path, std::ios::app);
    if(!log_output.is_open()){
        throw std::runtime_error("Failed to open log for writing");
    }
}

void log_file::OpenLogForRead() {
    log_input.open(file_path);
    if (!log_input.is_open()) {
        throw std::runtime_error("Failed to open log file for reading");
    }
}

int log_file::LoadCurrentTerm() {
  OpenLogForRead();
  std::string line;
  int current_term = 0;

  while (std::getline(log_input, line)) {
    if (line.find("term:") == 0) {
      current_term = std::stoi(line.substr(5));
    }
  }
  log_input.close();
  return current_term;
}

int log_file::LoadVotedFor() {
  OpenLogForRead();
  std::string line;
  int voted_for = -1;
  while (std::getline(log_input, line)) {
    if (line.find("voted_for:") == 0) {
      voted_for = std::stoi(line.substr(10));
    }
  }
  log_input.close();
  return voted_for;
}

std::vector<raft::LogEntry> log_file::LoadLog() {
  OpenLogForRead();
  std::vector<raft::LogEntry> log_entries;
  std::string line;
  while (std::getline(log_input, line)) {
    if (line.find("log:") == 0) {
      raft::LogEntry entry = DeserializeLogEntry(line.substr(4));
      log_entries.push_back(entry);
    }
  }
  log_input.close();
  return log_entries;
}

void log_file::SaveCurrentTerm(int term) {
  OpenLogForWrite();
  log_output << "term:" << term << "\n";
  log_output.close();
}

void log_file::SaveVotedFor(int candidate_id) {
  OpenLogForWrite();
  log_output << "voted_for:" << candidate_id << "\n";
  log_output.close();
}

void log_file::AppendLogEntry(const raft::LogEntry& entry) {
  OpenLogForWrite();
  log_output << "log:" << SerializeLogEntry(entry) << "\n";
  log_output.close();
}

std::string log_file::SerializeLogEntry(const raft::LogEntry& entry) {
  std::string serialized;
  entry.SerializeToString(&serialized);
  return serialized;
}

raft::LogEntry log_file::DeserializeLogEntry(const std::string& entry_str) {
  raft::LogEntry entry;
  entry.ParseFromString(entry_str);
  return entry;
}