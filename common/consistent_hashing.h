#ifndef CONSISTENT_HASHING_H
#define CONSISTENT_HASHING_H

#include <string>
#include <vector>
#include <map>
#include <functional>
#include <iostream>
#include <algorithm>
#include <cmath>

class ConsistentHashing {
public:
    ConsistentHashing(
        int num_replicas,
        const std::vector<std::string>& partitions
    );

    std::string GetPartition(
        const std::string& key
    );

    unsigned long GetKeyHash(
        const std::string& key
    );

    void AddPartition(
        const std::string& partition
    );

    void RemovePartition(
        const std::string& partition
    );

    void PrintHashRing();

    std::vector<std::pair<unsigned long, unsigned long>> GetKeySpaceToTransfer(
        const std::string& current_partition,
        const std::string& new_partition
    );

private:
    int num_replicas;
    std::vector<std::string> partitions;
    std::map<unsigned long, std::string> hash_ring;
};

#endif // CONSISTENT_HASHING_H