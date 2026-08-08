#pragma once

#if !__has_include(<folly/MPMCQueue.h>)
#error "Folly was requested but <folly/MPMCQueue.h> was not found. Install Folly before building this example."
#endif

#include <folly/MPMCQueue.h>

namespace mpmc_demo {

class MPMCQueueAdapter {
public:
    explicit MPMCQueueAdapter(size_t capacity = 1024)
        : queue_(capacity) {}

    bool push(int item) {
        return queue_.write(item);
    }

    void close() {
        // Folly's MPMCQueue does not expose a close operation in this minimal demo.
        // The application-level producer counter still coordinates shutdown.
    }

    bool pop(int& item) {
        return queue_.read(item);
    }

private:
    folly::MPMCQueue<int> queue_;
};

} // namespace mpmc_demo
