#ifndef ROCKSDB_WRAPPER_H
#define ROCKSDB_WRAPPER_H

#include <string>
#include <rocksdb/db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

class RocksDBWrapper
{
public:
    // Constructor initializes the RocksDB database at the given path.
    RocksDBWrapper(const std::string &db_path);

    // Destructor to close the RocksDB instance.
    ~RocksDBWrapper();

    bool Get(const std::string &key, std::string &value) const;

    int Put(const std::string &key, const std::string &value, std::string &old_value);

    // Create a snapshot for isolation.
    const rocksdb::Snapshot *GetSnapshot() const;

    // Release the snapshot.
    void ReleaseSnapshot(const rocksdb::Snapshot *snapshot);

private:
    rocksdb::TransactionDB *db_;                // Pointer to the RocksDB transactional instance.
    rocksdb::Options options_;                  // Options for RocksDB configuration.
    rocksdb::TransactionDBOptions txn_options_; // Transaction-specific options.
};

#endif