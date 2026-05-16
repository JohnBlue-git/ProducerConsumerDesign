#pragma once

#include <vector>
#include <mutex>
#include <condition_variable>

// Fixed-size Ring Buffer Class
class RingBuffer {
private:
    std::vector<int> buffer;
    size_t capacity;
    size_t head = 0;
    size_t tail = 0;
    size_t count = 0;

    std::mutex mtx;
    std::condition_variable cv_not_full;  // Wait if buffer is full
    std::condition_variable cv_not_empty; // Wait if buffer is empty
    bool done = false;

public:
    explicit RingBuffer(size_t size) : buffer(size), capacity(size) {}

    // Push item to the ring buffer (Producer blocks if full).
    // Returns false if buffer is closed while waiting or before push.
    bool push(int item) {
        std::unique_lock<std::mutex> lock(mtx);
        
        // Wait until there is space in the buffer
        cv_not_full.wait(lock, [this]() { return done || count < capacity; });

        if (done) {
            return false;
        }

        buffer[tail] = item;
        tail = (tail + 1) % capacity;
        count++;

        // Notify a blocked consumer
        cv_not_empty.notify_one();
        return true;
    }

    // Mark buffer as done and wake blocked producer/consumer threads.
    void close() {
        std::lock_guard<std::mutex> lock(mtx);
        done = true;
        cv_not_empty.notify_all();
        cv_not_full.notify_all();
    }

    // Pop item from the ring buffer.
    // Returns false when buffer is closed and fully drained.
    bool pop(int& item) {
        std::unique_lock<std::mutex> lock(mtx);

        // Wait until there is an item to consume
        cv_not_empty.wait(lock, [this]() { return done || count > 0; });

        if (count == 0) {
            return false;
        }

        item = buffer[head];
        head = (head + 1) % capacity;
        count--;

        // Notify a blocked producer
        cv_not_full.notify_one();
        return true;
    }
};
