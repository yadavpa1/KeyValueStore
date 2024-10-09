#include "rocksdb_wrapper.h"
#include <iostream>
#include <thread> // For std::this_thread::sleep_for
#include <chrono> // For delay between retries

RocksDBWrapper::RocksDBWrapper(const std::string &db_path, int num_partitions) : num_partitions_(num_partitions)
{
    options_.create_if_missing = true;

    // Set a lock timeout (in ms) for pessimistic transactions.
    txn_options_.default_lock_timeout = 1000;

    for (int i = 0; i < num_partitions_; ++i)
    {
        std::string partition_path = db_path + "_partition_" + std::to_string(i);
        rocksdb::TransactionDB *db = nullptr;
        rocksdb::Status status = rocksdb::TransactionDB::Open(options_, txn_options_, partition_path, &db);
        if (!status.ok())
        {
            std::cerr << "Error: Failed to open DB partition at path: " << partition_path << "\n"
                      << status.ToString() << std::endl;
        }
        else
        {
            db_partitions_.push_back(db);
            std::cout << "DB partition " << i << " initialized at path: " << partition_path << std::endl;
        }
    }
}

// Helper function to determine partition based on key
static int GetPartitionForKey(const std::string &key, int num_partitions)
{
    std::hash<std::string> hash_fn;
    return hash_fn(key) % num_partitions;
}

// Helper function to create a snapshot for isolation.
const rocksdb::Snapshot *CreateSnapshot(rocksdb::TransactionDB *db)
{
    return db->GetSnapshot();
}

// Helper function to release the snapshot.
void ReleaseSnapshot(rocksdb::TransactionDB *db, const rocksdb::Snapshot *snapshot)
{
    db->ReleaseSnapshot(snapshot);
}

bool RocksDBWrapper::Get(const std::string &key, std::string &value) const
{
    if (db_partitions_.empty())
    {
        std::cerr << "Error: DB partitions not initialized." << std::endl;
        return false;
    }

    int partition_index = GetPartitionForKey(key, num_partitions_);
    rocksdb::TransactionDB *db = db_partitions_[partition_index];

    // Create a read transaction using the current snapshot for isolation.
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot *snapshot = CreateSnapshot(db);
    read_options.snapshot = snapshot;

    rocksdb::Status status = db->Get(read_options, key, &value);

    // Release the snapshot after the read.
    ReleaseSnapshot(db, snapshot);

    return status.ok(); // Returns true if the key was found
}

int RocksDBWrapper::Put(const std::string &key, const std::string &value, std::string &old_value)
{
    if (db_partitions_.empty())
    {
        std::cerr << "Error: DB partitions not initialized." << std::endl;
        return -1;
    }

    int partition_index = GetPartitionForKey(key, num_partitions_);
    rocksdb::TransactionDB *db = db_partitions_[partition_index];

    // Create a pessimistic transaction.
    rocksdb::WriteOptions write_options;
    rocksdb::TransactionOptions txn_options;
    rocksdb::Transaction *txn = db->BeginTransaction(write_options, txn_options);

    // Lock the key for pessimistic access.
    txn->SetLockTimeout(1000);

    // Read the old value within the transaction to ensure isolation.
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot *snapshot = CreateSnapshot(db); // Use snapshot for consistent reads
    read_options.snapshot = snapshot;

    rocksdb::Status get_status = txn->Get(read_options, key, &old_value);
    bool key_found = get_status.ok();

    // Perform the Put operation inside the transaction.
    rocksdb::Status put_status = txn->Put(key, value);
    if (!put_status.ok())
    {
        std::cerr << "Error updating key: " << key << std::endl;
        delete txn;
        ReleaseSnapshot(db, snapshot); // Ensure snapshot is released on failure
        return -1;
    }

    // Commit the transaction.
    rocksdb::Status commit_status = txn->Commit();
    if (!commit_status.ok())
    {
        std::cerr << "Error committing transaction for key: " << key << std::endl;
        delete txn;
        ReleaseSnapshot(db, snapshot); // Ensure snapshot is released on failure
        return -1;
    }

    // Clean up the transaction.
    delete txn;
    ReleaseSnapshot(db, snapshot); // Ensure snapshot is released after use
    return key_found ? 0 : 1;
}

RocksDBWrapper::~RocksDBWrapper()
{
    for (rocksdb::TransactionDB *db : db_partitions_)
    {
        delete db;
    }
}