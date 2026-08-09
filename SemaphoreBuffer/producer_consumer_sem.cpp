#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

#include "semaphore_buffer.hpp"
#include "../include/benchmark_utils.hpp"
#include "../include/producer_counter.hpp"
#include "../include/logging.hpp"

void producer(
    SemRingBuffer& rb,
    int producer_id,
    int start_value,
    int items_per_producer,
    ProducerCountTracker& producers_left,
    int consumer_count,
    bench::StartGate& gate,
    std::atomic<uint64_t>& operations_processed
) {
    gate.arrive_and_wait();

    for (int i = 0; i < items_per_producer; ++i) {
        int item = start_value + i;
        if (!rb.push(item)) {
            break;
        }
        operations_processed.fetch_add(1, std::memory_order_relaxed);
        PC_PRINT("[Producer %d] Pushed: %d\n", producer_id, item);
    }

    if (producers_left.mark_finished()) {
        rb.close(consumer_count);
    }
}

void consumer(SemRingBuffer& rb, int consumer_id, bench::StartGate& gate) {
    gate.arrive_and_wait();
    int item = 0;
    while (rb.pop(item)) {
        PC_PRINT("[Consumer %d] Popped: %d\n", consumer_id, item);
    }
}

int main(int argc, char** argv) {
    size_t BUFFER_SIZE = 5;
    int PRODUCER_COUNT = 3;
    int CONSUMER_COUNT = 2;
    int ITEMS_PER_PRODUCER = 10;

    if (argc >= 4) {
        PRODUCER_COUNT = std::stoi(argv[1]);
        CONSUMER_COUNT = std::stoi(argv[2]);
        ITEMS_PER_PRODUCER = std::stoi(argv[3]);
    }
    if (argc >= 5) {
        BUFFER_SIZE = static_cast<size_t>(std::stoul(argv[4]));
    }
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--quiet") {
            pc::enable_logging.store(false, std::memory_order_relaxed);
        }
    }

    SemRingBuffer rb(BUFFER_SIZE);
    bench::StartGate gate(PRODUCER_COUNT + CONSUMER_COUNT);
    std::atomic<uint64_t> operations_processed{0};
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    ProducerCountTracker producers_left(PRODUCER_COUNT);

    for (int i = 0; i < PRODUCER_COUNT; ++i) {
        int start_value = i * ITEMS_PER_PRODUCER + 1;
        producers.emplace_back(producer,
                               std::ref(rb),
                               i + 1,
                               start_value,
                               ITEMS_PER_PRODUCER,
                               std::ref(producers_left),
                               CONSUMER_COUNT,
                               std::ref(gate),
                               std::ref(operations_processed));
    }

    for (int i = 0; i < CONSUMER_COUNT; ++i) {
        consumers.emplace_back(consumer, std::ref(rb), i + 1, std::ref(gate));
    }

    gate.wait_for_all_ready();
    auto start_wall = std::chrono::steady_clock::now();
    double start_cpu = bench::current_cpu_seconds();
    gate.release();

    for (auto& t : producers) {
        t.join();
    }
    for (auto& t : consumers) {
        t.join();
    }

    auto end_wall = std::chrono::steady_clock::now();
    double end_cpu = bench::current_cpu_seconds();
    double runtime_s = std::chrono::duration<double>(end_wall - start_wall).count();
    double cpu_s = end_cpu - start_cpu;
    bench::print_json_result("semaphore", runtime_s, cpu_s, operations_processed.load());
    return 0;
}
