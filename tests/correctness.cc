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

void run_initialization_tests()
{
    std::cout << "\n=== Initialization Tests ===\n";

    // Test 1.1: Valid Server Initialization
    int init_result = kv739_init("localhost:50051");
    print_test_result("Test 1.1: Valid Server Initialization", init_result == 0);

    // Test 1.2: Invalid Server Initialization
    init_result = kv739_init("invalid_host:50051");
    print_test_result("Test 1.2: Invalid Server Initialization", init_result == -1);
}

void run_get_put_tests()
{
    std::cout << "\n=== Get/Put Operation Tests ===\n";

    // Test 2.1: Put a New Key-Value Pair
    std::string old_value;
    int put_result = kv739_put("key1", "value1", old_value);
    print_test_result("Test 2.1: Put a New Key-Value Pair", put_result == 1);

    // Test 2.2: Get Existing Key
    std::string value;
    int get_result = kv739_get("key1", value);
    print_test_result("Test 2.2: Get Existing Key", get_result == 0 && value == "value1");

    // Test 2.3: Overwrite an Existing Key
    put_result = kv739_put("key1", "value2", old_value);
    print_test_result("Test 2.3: Overwrite an Existing Key", put_result == 0 && old_value == "value1");

    // Test 2.4: Get Non-Existent Key
    get_result = kv739_get("non_existent_key", value);
    print_test_result("Test 2.4: Get Non-Existent Key", get_result == 1);

    // Test 2.5: Put with Empty Key
    put_result = kv739_put("", "value", old_value);
    print_test_result("Test 2.5: Put with Empty Key", put_result == 1);

    get_result = kv739_get("", old_value);
    print_test_result("Test 2.5: Get with Empty Key", get_result == 0 && old_value == "value");
}

void run_shutdown_tests()
{
    std::cout << "\n=== Shutdown Tests ===\n";

    // Test 3.1: Shutdown After Initialization
    int shutdown_result = kv739_shutdown();
    print_test_result("Test 3.1: Shutdown After Initialization", shutdown_result == 0);

    // Test 3.2: Shutdown Without Initialization
    shutdown_result = kv739_shutdown();
    print_test_result("Test 3.2: Shutdown Without Initialization", shutdown_result == -1);
}

void run_error_handling_tests()
{
    std::cout << "\n=== Error Handling Tests ===\n";

    // Test 5.1: Get Without Initialization
    std::string value;
    int get_result = kv739_get("test_key", value);
    print_test_result("Test 5.1: Get Without Initialization", get_result == -1);
}

int main()
{
    std::cout << "\n=== Running Correctness Tests ===\n";

    // Run the tests
    run_initialization_tests();
    run_get_put_tests();
    run_error_handling_tests();
    run_shutdown_tests();

    std::cout << "\n=== Test Summary ===\n";
    std::cout << "Total Passed Tests: " << passed_tests << "\n";
    std::cout << "Total Failed Tests: " << failed_tests << "\n";

    return 0;
}
