#pragma once

#include <atomic>
#include <thread>

#if __has_include("concurrentqueue.h")
#include "concurrentqueue.h"
#elif __has_include(<concurrentqueue/concurrentqueue.h>)
#include <concurrentqueue/concurrentqueue.h>
#else
#error "concurrentqueue.h not found. Please add moodycamel::ConcurrentQueue header to include path."
#endif

namespace moody_demo {

// Adapter that wraps moodycamel::ConcurrentQueue<int> for the demos.
class ConcurrentQueueAdapter {
public:
    explicit ConcurrentQueueAdapter(int /*producer_count*/ = 4) {}

    void push_for_producer(int /*producer_id*/, int item) {
        // Non-blocking enqueue; rely on external coordination for termination.
        queue_.enqueue(item);
    }

    void close() {
        // No-op for non-blocking adapter. Application-level ProducerCountTracker
        // should coordinate shutdown and consumers should stop when producers are done
        // and the queue is empty.
    }

    // Non-blocking pop: returns true if an item was dequeued, false otherwise.
    bool pop(int& item) {
        return queue_.try_dequeue(item);
    }

private:
    moodycamel::ConcurrentQueue<int> queue_;
};

} // namespace moody_demo
