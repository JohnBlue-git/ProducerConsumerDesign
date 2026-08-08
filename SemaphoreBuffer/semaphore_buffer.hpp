#pragma once

#include <vector>
#include <semaphore>
#include <cstddef>

// Fixed-size Ring Buffer Class (semaphore version)
class SemRingBuffer {
private:
    std::vector<int> buffer;
    size_t capacity;
    std::atomic<size_t> head{0};
    std::atomic<size_t> tail{0};
    std::atomic<size_t> count{0};
    std::atomic<bool> done{false};

    // Counts free slots in the ring buffer. Producers wait here when the buffer is full.
    std::counting_semaphore<> sem_not_full;

    // Counts available items in the ring buffer. Consumers wait here when the buffer is empty.
    std::counting_semaphore<> sem_not_empty;

public:
    explicit SemRingBuffer(size_t size)
        : buffer(size),
          capacity(size),
          sem_not_full(static_cast<std::ptrdiff_t>(size)),
          sem_not_empty(0) {}

    // Push item to the ring buffer (Producer blocks if full).
    // Returns false if buffer is closed before push.
    bool push(int item) {
        sem_not_full.acquire();

        if (done.load(std::memory_order_acquire)) {
            sem_not_full.release();
            return false;
        }

        size_t pos = tail.fetch_add(1, std::memory_order_relaxed) % capacity;
        buffer[pos] = item;
        count.fetch_add(1, std::memory_order_release);

        sem_not_empty.release();
        return true;
    }

    // Mark buffer as done and wake blocked consumer threads.
    void close(int consumer_count) {
        bool expected = false;
        if (!done.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            return;
        }

        sem_not_empty.release(consumer_count);
    }

    // Pop item from the ring buffer.
    // Returns false when buffer is closed and fully drained.
    bool pop(int& item) {
        sem_not_empty.acquire();

        while (true) {
            size_t current = count.load(std::memory_order_acquire);
            if (current == 0) {
                return false;
            }
            if (count.compare_exchange_strong(current, current - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                size_t pos = head.fetch_add(1, std::memory_order_relaxed) % capacity;
                item = buffer[pos];
                sem_not_full.release();
                return true;
            }
        }
    }
};
