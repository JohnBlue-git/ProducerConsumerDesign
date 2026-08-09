#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

#include "../include/benchmark_utils.hpp"
#include "../include/producer_counter.hpp"
#include "concurrent_queue_adapter.hpp"
#include "../include/logging.hpp"

namespace {

void producer(moody_demo::ConcurrentQueueAdapter& queue,
              int producer_id,
              int start_value,
              int items_per_producer,
              ProducerCountTracker& producers_left,
              bench::StartGate& gate,
              std::atomic<uint64_t>& operations_processed) {
    gate.arrive_and_wait();

    for (int i = 0; i < items_per_producer; ++i) {
        int item = start_value + i;
        queue.push_for_producer(producer_id, item);
        operations_processed.fetch_add(1, std::memory_order_relaxed);
        PC_PRINT("[Producer %d] Pushed: %d\n", producer_id, item);
    }

    if (producers_left.mark_finished()) {
        queue.close();
    }
}

void consumer(moody_demo::ConcurrentQueueAdapter& queue,
              int consumer_id,
              ProducerCountTracker& producers_left,
              bench::StartGate& gate) {
    gate.arrive_and_wait();
    int item = 0;
    while (true) {
        if (queue.pop(item)) {
            PC_PRINT("[Consumer %d] Popped: %d\n", consumer_id, item);
            continue;
        }
        if (producers_left.remaining() == 0) {
            break;
        }
        std::this_thread::yield();
    }
}

} // namespace

int main(int argc, char** argv) {
    int PRODUCER_COUNT = 3;
    int CONSUMER_COUNT = 2;
    int ITEMS_PER_PRODUCER = 10;

    if (argc >= 4) {
        PRODUCER_COUNT = std::stoi(argv[1]);
        CONSUMER_COUNT = std::stoi(argv[2]);
        ITEMS_PER_PRODUCER = std::stoi(argv[3]);
    }
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--quiet") {
            pc::enable_logging.store(false, std::memory_order_relaxed);
        }
    }

    moody_demo::ConcurrentQueueAdapter queue(PRODUCER_COUNT);
    bench::StartGate gate(PRODUCER_COUNT + CONSUMER_COUNT);
    std::atomic<uint64_t> operations_processed{0};
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    ProducerCountTracker producers_left(PRODUCER_COUNT);

    for (int i = 0; i < PRODUCER_COUNT; ++i) {
        int start_value = i * ITEMS_PER_PRODUCER + 1;
        producers.emplace_back(producer,
                               std::ref(queue),
                               i + 1,
                               start_value,
                               ITEMS_PER_PRODUCER,
                               std::ref(producers_left),
                               std::ref(gate),
                               std::ref(operations_processed));
    }

    for (int i = 0; i < CONSUMER_COUNT; ++i) {
        consumers.emplace_back(consumer,
                               std::ref(queue),
                               i + 1,
                               std::ref(producers_left),
                               std::ref(gate));
    }

    gate.wait_for_all_ready();
    auto start_wall = std::chrono::steady_clock::now();
    double start_cpu = bench::current_cpu_seconds();
    gate.release();

    for (auto& thread : producers) {
        thread.join();
    }
    for (auto& thread : consumers) {
        thread.join();
    }

    auto end_wall = std::chrono::steady_clock::now();
    double end_cpu = bench::current_cpu_seconds();
    double runtime_s = std::chrono::duration<double>(end_wall - start_wall).count();
    double cpu_s = end_cpu - start_cpu;
    bench::print_json_result("moodycamel", runtime_s, cpu_s, operations_processed.load());
    return 0;
}
