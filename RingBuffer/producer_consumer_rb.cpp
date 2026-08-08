#include <iostream>
#include <vector>
#include <thread>
#include <chrono>

#include "ring_buffer.hpp"
#include "../producer_counter.hpp"
#include "../logging.hpp"

void producer(RingBuffer& rb, int producer_id, int start_value, int items_per_producer, ProducerCountTracker& producers_left) {
    for (int i = 0; i < items_per_producer; ++i) {
        int item = start_value + i;

        // Simulate manufacturing time (disabled for benchmarks)
        // std::this_thread::sleep_for(std::chrono::milliseconds(6));

        // Push to ring buffer
        if (!rb.push(item)) {
            break;
        }
        std::printf("[Producer %d] Pushed: %d\n", producer_id, item);
    }

    if (producers_left.mark_finished()) {
        rb.close();
    }
}

void consumer(RingBuffer& rb, int consumer_id) {
    int item = 0;
    while (rb.pop(item)) {
        // Pop from ring buffer
        std::printf("[Consumer %d] Popped: %d\n", consumer_id, item);
        
        // Simulate processing time (disabled for benchmarks)
        // std::this_thread::sleep_for(std::chrono::milliseconds(9));
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

    RingBuffer rb(BUFFER_SIZE);
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    ProducerCountTracker producers_left(PRODUCER_COUNT);

    for (int i = 0; i < PRODUCER_COUNT; ++i) {
        int start_value = i * ITEMS_PER_PRODUCER + 1;
        producers.emplace_back(producer, std::ref(rb), i + 1, start_value, ITEMS_PER_PRODUCER, std::ref(producers_left));
    }

    for (int i = 0; i < CONSUMER_COUNT; ++i) {
        consumers.emplace_back(consumer, std::ref(rb), i + 1);
    }

    for (auto& t : producers) {
        t.join();
    }
    for (auto& t : consumers) {
        t.join();
    }

    std::cout << "MPMC ring-buffer streaming finished successfully." << std::endl;
    return 0;
}
