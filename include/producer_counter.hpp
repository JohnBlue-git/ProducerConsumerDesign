#pragma once

#include <atomic>

class ProducerCountTracker {
public:
    explicit ProducerCountTracker(int initial_count) : count_(initial_count) {}

    bool mark_finished() noexcept {
        return count_.fetch_sub(1, std::memory_order_acq_rel) == 1;
    }

    int remaining() const noexcept {
        return count_.load(std::memory_order_acquire);
    }

private:
    std::atomic<int> count_;
};
