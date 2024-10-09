#ifndef ROCKSDB_WRAPPER_H
#define ROCKSDB_WRAPPER_H

#include <string>
#include <rocksdb/db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

class RocksDBWrapper {
    public:
        // Constructor initializes the RocksDB database at the given path.
        RocksDBWrapper(const std::string& db_path, int num_partitions);

        // Destructor to close the RocksDB instance.
        ~RocksDBWrapper();
        
        bool Get(const std::string& key, std::string& value) const;
        
        int Put(const std::string& key, const std::string& value, std::string& old_value);

    private:
        std::vector<rocksdb::TransactionDB*> db_partitions_; // Vector of RocksDB transactional instances for each partition
        int num_partitions_;
        rocksdb::Options options_;  // Options for RocksDB configuration.
        rocksdb::TransactionDBOptions txn_options_; // Transaction-specific options.
};

#endif