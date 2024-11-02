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

unsigned long ConsistentHashing::GetKeyHash(const std::string& key){
    unsigned long hash = std::hash<std::string>{}(key);
    return hash;
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

std::vector<std::pair<unsigned long, unsigned long>> ConsistentHashing::GetKeySpaceToTransfer(
    const std::string& current_partition,
    const std::string& new_partition
) {
    std::vector<std::pair<unsigned long, unsigned long>> key_space;
    std::vector<unsigned long> sorted_hashes;
    
    for (const auto& entry : hash_ring) {
        sorted_hashes.push_back(entry.first);
    }
    std::sort(sorted_hashes.begin(), sorted_hashes.end());

    for (size_t i = 0; i < sorted_hashes.size(); i++) {
        if (hash_ring[sorted_hashes[i]] == current_partition) {
            size_t prev_index = (i - 1 + sorted_hashes.size()) % sorted_hashes.size();
            // Get prev_prev_index as long it is not the same as new_partition
            size_t prev_prev_index = prev_index;
            do {
                prev_prev_index = (prev_prev_index - 1 + sorted_hashes.size()) % sorted_hashes.size();
            } while (hash_ring[sorted_hashes[prev_prev_index]] == new_partition);
            
            if (hash_ring[sorted_hashes[prev_index]] == new_partition) {
                key_space.push_back({
                    sorted_hashes[prev_prev_index],
                    sorted_hashes[prev_index]
                });
            }
        }
    }
    for (const auto& range : key_space) {
        // Print key space as well as old and new partition
        std::cout << range.first << " -> " << range.second << " : " << current_partition << " -> " << new_partition << std::endl;
    }

    return key_space;
}

void ConsistentHashing::PrintHashRing() {
    // Print the sorted hash ring
    // sort the hash ring by hash value
    std::vector<unsigned long> sorted_hashes;
    for (const auto& entry : hash_ring) {
        sorted_hashes.push_back(entry.first);
    }

    std::sort(sorted_hashes.begin(), sorted_hashes.end());
    
    for (const auto& hash : sorted_hashes) {
        std::cout << hash << " -> " << hash_ring[hash] << std::endl;
    }
}