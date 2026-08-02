#pragma once

#include <vector>
#include <semaphore>
#include <cstddef>

// Fixed-size Ring Buffer Class (semaphore version)
class SemRingBuffer {
private:
    std::vector<int> buffer;
    size_t capacity;
    size_t head = 0;
    size_t tail = 0;
    size_t count = 0;
    bool done = false;

    // Counts free slots in the ring buffer. Producers wait here when the buffer is full.
    std::counting_semaphore<> sem_not_full;

    // Counts available items in the ring buffer. Consumers wait here when the buffer is empty.
    std::counting_semaphore<> sem_not_empty;

    // Guards access to the shared ring-buffer state.
    std::binary_semaphore mtx;

public:
    explicit SemRingBuffer(size_t size)
        : buffer(size),
          capacity(size),
          sem_not_full(static_cast<std::ptrdiff_t>(size)),
          sem_not_empty(0),
          mtx(1) {}

    // Push item to the ring buffer (Producer blocks if full).
    // Returns false if buffer is closed while waiting or before push.
    bool push(int item) {
        sem_not_full.acquire();

        mtx.acquire();
        if (done) {
            mtx.release();
            sem_not_full.release();
            return false;
        }

        buffer[tail] = item;
        tail = (tail + 1) % capacity;
        count++;
        mtx.release();

        sem_not_empty.release();
        return true;
    }

    // Mark buffer as done and wake blocked consumer threads.
    void close(int consumer_count) {
        mtx.acquire();
        if (done) {
            mtx.release();
            return;
        }
        done = true;
        mtx.release();

        sem_not_empty.release(consumer_count);
    }

    // Pop item from the ring buffer.
    // Returns false when buffer is closed and fully drained.
    bool pop(int& item) {
        sem_not_empty.acquire();

        mtx.acquire();
        if (count == 0) {
            mtx.release();
            return false;
        }

        item = buffer[head];
        head = (head + 1) % capacity;
        count--;
        mtx.release();

        sem_not_full.release();
        return true;
    }
};
