#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>

#include "semaphore_buffer.hpp"
#include "../producer_counter.hpp"

void producer(
    SemRingBuffer& rb,
    int producer_id,
    int start_value,
    int items_per_producer,
    ProducerCountTracker& producers_left,
    int consumer_count
) {
    for (int i = 0; i < items_per_producer; ++i) {
        int item = start_value + i;

        // Simulate manufacturing time
        std::this_thread::sleep_for(std::chrono::milliseconds(6));

        // Push to ring buffer (semaphore version)
        if (!rb.push(item)) {
            break;
        }
        std::printf("[Producer %d] Pushed: %d\n", producer_id, item);
    }

    if (producers_left.mark_finished()) {
        rb.close(consumer_count);
    }
}

void consumer(SemRingBuffer& rb, int consumer_id) {
    int item = 0;
    while (rb.pop(item)) {
        // Pop from ring buffer (semaphore version)
        std::printf("[Consumer %d] Popped: %d\n", consumer_id, item);

        // Simulate processing time
        std::this_thread::sleep_for(std::chrono::milliseconds(9));
    }
}

int main() {
    const size_t BUFFER_SIZE = 5;
    const int PRODUCER_COUNT = 3;
    const int CONSUMER_COUNT = 2;
    const int ITEMS_PER_PRODUCER = 10;

    SemRingBuffer rb(BUFFER_SIZE);
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    ProducerCountTracker producers_left(PRODUCER_COUNT);

    for (int i = 0; i < PRODUCER_COUNT; ++i) {
        int start_value = i * ITEMS_PER_PRODUCER + 1;
        producers.emplace_back(producer, std::ref(rb), i + 1, start_value, ITEMS_PER_PRODUCER, std::ref(producers_left), CONSUMER_COUNT);
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

    std::cout << "MPMC semaphore ring-buffer streaming finished successfully." << std::endl;
    return 0;
}
