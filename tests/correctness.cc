#include "lib739kv.h"
#include <iostream>
#include <string>

int passed_tests = 0;
int failed_tests = 0;

void print_test_result(const std::string &test_name, bool passed)
{
    if (passed)
    {
        std::cout << "    PASS: " << test_name << "\n";
        ++passed_tests;
    }
    else
    {
        std::cerr << "    FAIL: " << test_name << "\n";
        ++failed_tests;
    }
}

void run_initialization_tests(KeyValueStoreClient &client)
{
    std::cout << "\n=== Initialization Tests ===\n";

    // Test 1.1: Valid Server Initialization
    int init_result = client.kv739_init("localhost:50051");
    print_test_result("Test 1.1: Valid Server Initialization", init_result == 0);

    // Test 1.2: Invalid Server Initialization
    init_result = client.kv739_init("invalid_host:50051");
    print_test_result("Test 1.2: Invalid Server Initialization", init_result == -1);
}

void run_get_put_tests(KeyValueStoreClient &client)
{
    std::cout << "\n=== Get/Put Operation Tests ===\n";

    // Test 2.1: Put a New Key-Value Pair
    std::string old_value;
    int put_result = client.kv739_put("key1", "value1", old_value);
    print_test_result("Test 2.1: Put a New Key-Value Pair", put_result == 1);

    // Test 2.2: Get Existing Key
    std::string value;
    int get_result = client.kv739_get("key1", value);
    print_test_result("Test 2.2: Get Existing Key", get_result == 0 && value == "value1");

    // Test 2.3: Overwrite an Existing Key
    put_result = client.kv739_put("key1", "value2", old_value);
    print_test_result("Test 2.3: Overwrite an Existing Key", put_result == 0 && old_value == "value1");

    // Test 2.4: Get Non-Existent Key
    get_result = client.kv739_get("non_existent_key", value);
    print_test_result("Test 2.4: Get Non-Existent Key", get_result == 1);
}

void run_shutdown_tests(KeyValueStoreClient &client)
{
    std::cout << "\n=== Shutdown Tests ===\n";

    // Test 3.1: Shutdown After Initialization
    int shutdown_result = client.kv739_shutdown();
    print_test_result("Test 3.1: Shutdown After Initialization", shutdown_result == 0);

    // Test 3.2: Shutdown Without Initialization
    shutdown_result = client.kv739_shutdown();
    print_test_result("Test 3.2: Shutdown Without Initialization", shutdown_result == -1);
}

void run_invalid_key_value_tests(KeyValueStoreClient &client)
{
    std::cout << "\n=== Invalid Key/Value Tests ===\n";

    // Test 4.1: Key with Special Characters
    std::string old_value;
    int put_result = client.kv739_put("invalid!key", "value1", old_value);
    print_test_result("Test 4.1: Key with Special Characters", put_result == -1);

    // Test 4.2: Key with '[' or ']'
    put_result = client.kv739_put("key[1]", "value1", old_value);
    print_test_result("Test 4.2: Key with '[' or ']'", put_result == -1);

    // Test 4.3: Value with Special Characters
    put_result = client.kv739_put("key2", "invalid!value", old_value);
    print_test_result("Test 4.3: Value with Special Characters", put_result == -1);

    // Test 4.4: Value with '[' or ']'
    put_result = client.kv739_put("key3", "value[1]", old_value);
    print_test_result("Test 4.4: Value with '[' or ']'", put_result == -1);

    // Test 4.5: Key Exceeding Maximum Length
    std::string long_key(129, 'a'); // 129 bytes, exceeds 128-byte limit
    put_result = client.kv739_put(long_key, "value1", old_value);
    print_test_result("Test 4.5: Key Exceeding Maximum Length", put_result == -1);

    // Test 4.6: Value Exceeding Maximum Length
    std::string long_value(2049, 'b'); // 2049 bytes, exceeds 2048-byte limit
    put_result = client.kv739_put("key4", long_value, old_value);
    print_test_result("Test 4.6: Value Exceeding Maximum Length", put_result == -1);

    // Test 4.7: Put/Get with Empty Key
    put_result = client.kv739_put("", "value", old_value);
    print_test_result("Test 4.7: Put with Empty Key", put_result == 1);

    int get_result = client.kv739_get("", old_value);
    print_test_result("Test 4.7: Get with Empty Key", get_result == 0 && old_value == "value");
}

void run_error_handling_tests(KeyValueStoreClient &client)
{
    std::cout << "\n=== Error Handling Tests ===\n";

    // Test 5.1: Get Without Initialization
    std::string value;
    int get_result = client.kv739_get("test_key", value);
    print_test_result("Test 5.1: Get Without Initialization", get_result == -1);
}

int main()
{
    KeyValueStoreClient client;

    std::cout << "\n=== Running Correctness Tests ===\n";

    // Run the tests
    run_initialization_tests(client);
    run_get_put_tests(client);
    run_invalid_key_value_tests(client);
    run_error_handling_tests(client);
    run_shutdown_tests(client);

    std::cout << "\n=== Test Summary ===\n";
    std::cout << "Total Passed Tests: " << passed_tests << "\n";
    std::cout << "Total Failed Tests: " << failed_tests << "\n";

    return 0;
}
