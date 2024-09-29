#ifndef ROCKSDB_WRAPPER_H
#define ROCKSDB_WRAPPER_H

#include <string>
#include <rocksdb/db.h>

class RocksDBWrapper {
    public:
        // Constructor initializes the RocksDB database at the given path.
        RocksDBWrapper(const std::string& db_path);

        // Destructor to close the RocksDB instance.
        ~RocksDBWrapper();
        
        bool Get(const std::string& key, std::string& value) const;
        
        bool Put(const std::string& key, const std::string& value);

    private:
        rocksdb::DB* db_;  // Pointer to the RocksDB instance.
        rocksdb::Options options_;  // Options for RocksDB configuration.
};

#endif