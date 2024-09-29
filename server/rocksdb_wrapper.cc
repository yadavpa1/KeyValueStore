#include "rocksdb_wrapper.h"
#include <iostream>

RocksDBWrapper::RocksDBWrapper(const std::string& db_path) {
    options_.create_if_missing = true;

    // Open the RocksDB database.
    rocksdb::Status status = rocksdb::DB::Open(options_, db_path, &db_);
    if (!status.ok()) {
        std::cerr << "Error: Failed to open RocksDB at path: " << db_path << "\n"
                  << status.ToString() << std::endl;
        db_ = nullptr;
    }
    std::cout << "RocksDB initialized at path: " << db_path << std::endl;
}


bool RocksDBWrapper::Get(const std::string& key, std::string& value) const {
    if (!db_) {
        std::cerr << "Error: RocksDB not initialized." << std::endl;
        return false;
    }
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
    return status.ok();  // Returns true if the key was found
}


bool RocksDBWrapper::Put(const std::string& key, const std::string& value) {
    if (!db_) {
        std::cerr << "Error: RocksDB not initialized." << std::endl;
        return false;
    }
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
    return status.ok();  // Returns true if the key was found
}


RocksDBWrapper::~RocksDBWrapper() {
    if (db_) {
        delete db_;
    }
}