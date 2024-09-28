#ifndef KEYVALUESTORE_CLIENT_H
#define KEYVALUESTORE_CLIENT_H

#include<string>

class KeyValueStoreClient
{
public:
    int kv739_init(const std::string &server_address);
    int kv739_shutdown();
    int kv739_get(const std::string &key, std::string &value);
    int kv739_put(const std::string &key, const std::string &value, std::string &old_value);
};

#endif // KEYVALUESTORE_CLIENT_H