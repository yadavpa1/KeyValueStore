#ifndef LIB739KV_H
#define LIB739KV_H

#include <string>
#include <vector>

int kv739_init(const std::string &config_file);

int kv739_shutdown();

int kv739_get(const std::string &key, std::string &value);

int kv739_put(const std::string &key, const std::string &value, std::string &old_value);

#endif