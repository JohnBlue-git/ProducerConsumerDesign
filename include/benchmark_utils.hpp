#pragma once

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <string>

#ifdef __unix__
#include <sys/resource.h>
#endif

namespace bench {

inline double current_cpu_seconds() {
#ifdef __unix__
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        return usage.ru_utime.tv_sec + usage.ru_utime.tv_usec * 1e-6 +
               usage.ru_stime.tv_sec + usage.ru_stime.tv_usec * 1e-6;
    }
#endif
    return 0.0;
}

class StartGate {
public:
    explicit StartGate(int total_threads) : total_threads_(total_threads) {}

    void arrive_and_wait() {
        ready_count_.fetch_add(1, std::memory_order_acq_rel);
        while (!go_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    void wait_for_all_ready() const {
        while (ready_count_.load(std::memory_order_acquire) < total_threads_) {
            std::this_thread::yield();
        }
    }

    void release() {
        go_.store(true, std::memory_order_release);
    }

private:
    const int total_threads_;
    std::atomic<int> ready_count_{0};
    std::atomic<bool> go_{false};
};

inline void print_json_result(const std::string& implementation,
                              double runtime_s,
                              double cpu_s,
                              uint64_t operations_processed) {
    std::cout << "{\"implementation\":\"" << implementation << "\",";
    std::cout << "\"runtime_s\":" << std::fixed << runtime_s << ",";
    std::cout << "\"cpu_s\":" << std::fixed << cpu_s << ",";
    std::cout << "\"operations\":" << operations_processed << "}" << std::endl;
}

} // namespace bench
