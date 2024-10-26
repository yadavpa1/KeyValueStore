#ifndef THREAD_SERVER_H
#define THREAD_SERVER_H

#include <vector>
#include <mutex>
#include <atomic>
#include <queue>
#include <thread>
#include <condition_variable>
#include <functional>

// Simple ThreadPool class for asynchronous tasks
class ThreadPool {
public:
    ThreadPool(size_t num_threads);
    ~ThreadPool();

    // Add a task to the queue
    void enqueue(std::function<void()> task);

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;

    void worker_thread();
};

#endif // THREAD_SERVER_H