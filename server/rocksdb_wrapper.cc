#include "rocksdb_wrapper.h"
#include<rocksdb/table.h>
#include <rocksdb/iterator.h>
#include <iostream>
#include <thread> // For std::this_thread::sleep_for
#include <chrono> // For delay between retries

RocksDBWrapper::RocksDBWrapper(const std::string &db_path, const size_t cache_size)
{
    options_.create_if_missing = true;

    // Set a lock timeout (in ms) for pessimistic transactions.
    txn_options_.default_lock_timeout = 1000;

    // Set up block-based table options with a cache.
    cache_ = rocksdb::NewLRUCache(cache_size);
    table_options_.block_cache = cache_;  // Associate the cache with table options
    options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options_));

    // Open the RocksDB transactional database.
    rocksdb::Status status = rocksdb::TransactionDB::Open(options_, txn_options_, db_path, &db_);
    if (!status.ok())
    {
        std::cerr << "Error: Failed to open DB at path: " << db_path << "\n"
                  << status.ToString() << std::endl;
        db_ = nullptr;
    }
    // std::cout << "DB initialized at path: " << db_path << std::endl;
}

bool RocksDBWrapper::Get(const std::string &key, std::string &value) const
{
    if (!db_)
    {
        std::cerr << "Error: DB not initialized." << std::endl;
        return false;
    }

    // Create a read transaction using the current snapshot for isolation.
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot *snapshot = db_->GetSnapshot();
    read_options.snapshot = snapshot;

    rocksdb::Status status = db_->Get(read_options, key, &value);

    // Release the snapshot after the read.
    db_->ReleaseSnapshot(snapshot);

    return status.ok(); // Returns true if the key was found
}

int RocksDBWrapper::Put(const std::string &key, const std::string &value, std::string &old_value)
{
    if (!db_)
    {
        std::cerr << "Error: DB not initialized." << std::endl;
        return -1;
    }

    // Create a pessimistic transaction.
    rocksdb::WriteOptions write_options;
    rocksdb::TransactionOptions txn_options;
    rocksdb::Transaction *txn = db_->BeginTransaction(write_options, txn_options);

    // Lock the key for pessimistic access.
    txn->SetLockTimeout(1000);

    // Read the old value within the transaction to ensure isolation.
    rocksdb::ReadOptions read_options;
    read_options.snapshot = txn->GetSnapshot(); // Use snapshot for consistent reads
    rocksdb::Status get_status = txn->Get(read_options, key, &old_value);

    bool key_found = get_status.ok();

    // Perform the Put operation inside the transaction.
    rocksdb::Status put_status = txn->Put(key, value);
    if (!put_status.ok())
    {
        std::cerr << "Error updating key: " << key << std::endl;
        delete txn;
        return -1;
    }

    // Commit the transaction.
    rocksdb::Status commit_status = txn->Commit();
    if (!commit_status.ok())
    {
        std::cerr << "Error committing transaction for key: " << key << std::endl;
        delete txn;
        return -1;
    }

    // Clean up the transaction.
    delete txn;
    return key_found ? 0 : 1;
}

rocksdb::Status RocksDBWrapper::Write(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *batch) {
    return db_->Write(options, batch);  // Perform the batch write
}

bool RocksDBWrapper::LoadLogEntries(const std::string &prefix, std::vector<std::string> &entries) const
{
    if (!db_)
    {
        std::cerr << "Error: DB not initialized." << std::endl;
        return false;
    }

    // Create an iterator to traverse log entries.
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));

    // Seek to the first key that matches the prefix.
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        entries.push_back(it->value().ToString());
    }

    if (!it->status().ok()) {
        std::cerr << "Error loading log entries: " << it->status().ToString() << std::endl;
        return false;
    }

    return true;
}

std::string RocksDBWrapper::SerializeStateUpTo(int64_t commit_index) const
{
    std::string serialized_state;
    if (!db_)
    {
        std::cerr << "Error: DB not initialized." << std::endl;
        return serialized_state;
    }

    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));

    int64_t current_index = 0;
    for (it->SeekToFirst(); it->Valid() && current_index <= commit_index; it->Next())
    {
        serialized_state += it->key().ToString() + "=" + it->value().ToString() + "\n";
        current_index++;
    }

    if (!it->status().ok())
    {
        std::cerr << "Error during serialization: " << it->status().ToString() << std::endl;
    }

    return serialized_state;
}

bool RocksDBWrapper::DeserializeState(const std::string &snapshot_data)
{
    if (!db_)
    {
        std::cerr << "Error: DB not initialized." << std::endl;
        return false;
    }

    // Clear the existing database.
    rocksdb::WriteBatch batch;
    batch.Clear();

    // Copy snapshot_data to a mutable string
    std::string mutable_data = snapshot_data;

    // Split the snapshot data by lines, then by key-value pairs.
    size_t pos = 0;
    std::string line;
    while ((pos = mutable_data.find('\n')) != std::string::npos)
    {
        line = mutable_data.substr(0, pos);
        size_t delim_pos = line.find('=');
        if (delim_pos != std::string::npos)
        {
            std::string key = line.substr(0, delim_pos);
            std::string value = line.substr(delim_pos + 1);
            batch.Put(key, value);
        }
        mutable_data.erase(0, pos + 1);
    }

    rocksdb::WriteOptions write_options;
    rocksdb::Status status = db_->Write(write_options, &batch);
    if (!status.ok())
    {
        std::cerr << "Error during deserialization: " << status.ToString() << std::endl;
        return false;
    }

    return true;
}

const rocksdb::Snapshot *RocksDBWrapper::GetSnapshot() const
{
    return db_->GetSnapshot();
}

void RocksDBWrapper::ReleaseSnapshot(const rocksdb::Snapshot *snapshot)
{
    db_->ReleaseSnapshot(snapshot);
}

RocksDBWrapper::~RocksDBWrapper()
{
    if (db_)
    {
        delete db_;
    }
}