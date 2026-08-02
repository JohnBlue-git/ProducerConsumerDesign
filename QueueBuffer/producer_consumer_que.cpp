#include <iostream>
#include <vector>
#include <thread>
#include <chrono>

#include "queue_buffer.hpp"
#include "../producer_counter.hpp"

void producer(QueBuffer& rb, int producer_id, int start_value, int items_per_producer, ProducerCountTracker& producers_left) {
    for (int i = 0; i < items_per_producer; ++i) {
        int item = start_value + i;

        // Simulate manufacturing time
        std::this_thread::sleep_for(std::chrono::milliseconds(6));

        // Push to queue buffer
        if (!rb.push(item)) {
            break;
        }
        std::printf("[Producer %d] Pushed: %d\n", producer_id, item);
    }

    if (producers_left.mark_finished()) {
        rb.close();
    }
}

void consumer(QueBuffer& rb, int consumer_id) {
    int item = 0;
    while (rb.pop(item)) {
        // Pop from queue buffer
        std::printf("[Consumer %d] Popped: %d\n", consumer_id, item);
        
        // Simulate processing time
        std::this_thread::sleep_for(std::chrono::milliseconds(9));
    }
}

int main() {
    const int PRODUCER_COUNT = 3;
    const int CONSUMER_COUNT = 2;
    const int ITEMS_PER_PRODUCER = 10;

    QueBuffer que;
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    ProducerCountTracker producers_left(PRODUCER_COUNT);

    for (int i = 0; i < PRODUCER_COUNT; ++i) {
        int start_value = i * ITEMS_PER_PRODUCER + 1;
        producers.emplace_back(producer, std::ref(que), i + 1, start_value, ITEMS_PER_PRODUCER, std::ref(producers_left));
    }

    for (int i = 0; i < CONSUMER_COUNT; ++i) {
        consumers.emplace_back(consumer, std::ref(que), i + 1);
    }

    for (auto& t : producers) {
        t.join();
    }
    for (auto& t : consumers) {
        t.join();
    }

    std::cout << "MPMC queue streaming finished successfully." << std::endl;
    return 0;
}
