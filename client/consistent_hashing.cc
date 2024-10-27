#include "consistent_hashing.h"

ConsistentHashing::ConsistentHashing(int num_replicas, const std::vector<std::string>& nodes)
    : num_replicas(num_replicas), partitions(nodes) {
    for (const auto& partition : partitions) {
        for (int i = 0; i < num_replicas; i++) {
            std::string virtual_partition = partition + std::to_string(i);
            unsigned long hash = std::hash<std::string>{}(virtual_partition);
            hash_ring[hash] = partition;
        }
    }
}

std::string ConsistentHashing::GetPartition(const std::string& key) {
    unsigned long hash = std::hash<std::string>{}(key);
    auto it = hash_ring.lower_bound(hash);
    if (it == hash_ring.end()) {
        return hash_ring.begin()->second;
    }
    return it->second;
}

void ConsistentHashing::AddPartition(const std::string& partition) {
    partitions.push_back(partition);
    for (int i = 0; i < num_replicas; i++) {
        std::string virtual_partition = partition + std::to_string(i);
        unsigned long hash = std::hash<std::string>{}(virtual_partition);
        hash_ring[hash] = partition;
    }
}

void ConsistentHashing::RemovePartition(const std::string& partition) {
    partitions.erase(std::remove(partitions.begin(), partitions.end(), partition), partitions.end());
    for (int i = 0; i < num_replicas; i++) {
        std::string virtual_partition = partition + std::to_string(i);
        unsigned long hash = std::hash<std::string>{}(virtual_partition);
        hash_ring.erase(hash);
    }
}

void ConsistentHashing::PrintHashRing() {
    for (const auto& entry : hash_ring) {
        std::cout << entry.first << " -> " << entry.second << std::endl;
    }
}
