#ifndef LIB739KV_H
#define LIB739KV_H

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <map>
#include <thread>
#include <algorithm>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include "consistent_hashing.h"


std::map<int, std::string> leader_addresses_;  // Maps partition IDs to current leader addresses
std::map<int, std::vector<std::string>> partition_instances_;  // Maps partition IDs to list of nodes

int num_partitions;  // Number of partitions (based on server configuration)

std::vector<std::string> service_instances_;  // List of service instances (host:port)

std::vector<std::string> free_hanging_nodes;

// Consistent hashing object to map keys to Raft partitions
ConsistentHashing *ch;

int FindPartition();

int kv739_init(const std::string &config_file);

int kv739_shutdown();

int kv739_get(const std::string &key, std::string &value);

int kv739_put(const std::string &key, const std::string &value, std::string &old_value);

int kv739_die(const std::string &server_name, int clean);

int kv739_start(const std::string &instance_name, int new_instance);

int kv739_leave(const std::string &instance_name, int clean);
#endif