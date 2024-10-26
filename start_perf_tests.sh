# This tests runs the performance_v2 tests for a set number of processes (clients)
# The processes vary from {1, 2, 4, 8, 16, 32, 64, 128, 256}

# Start the distributed_keyvaluestore in the background
./distributed_keyvaluestore -c config &
sleep 10

for i in 1 2 4 8 16 32 64 128 256
do
    echo "Running performance_v2 test for $i processes"
    ./performance_v2 -p $i
    sleep 2
    # After every test, kill the distributed_keyvaluestore
    pkill distributed_keyvaluestore
    # remove the log_raft_* directory and raft_db_* directory
    rm -rf log_raft_*
    rm -rf raft_db_*
    # Wait for user to press enter
    read -p "Press enter to continue"
    # Start the distributed_keyvaluestore in the background
    ./distributed_keyvaluestore -c config &
    sleep 10
done