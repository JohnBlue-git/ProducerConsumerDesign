#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>

#include "mpmc_queue.hpp"
#include "../producer_counter.hpp"

namespace {

void producer(mpmc_demo::MPMCQueueAdapter& queue, int producer_id, int start_value,
              int items_per_producer, ProducerCountTracker& producers_left) {
    for (int i = 0; i < items_per_producer; ++i) {
        int item = start_value + i;
        std::this_thread::sleep_for(std::chrono::milliseconds(6));

        if (!queue.push(item)) {
            break;
        }
        std::printf("[Producer %d] Pushed: %d\n", producer_id, item);
    }

    if (producers_left.mark_finished()) {
        queue.close();
    }
}

void consumer(mpmc_demo::MPMCQueueAdapter& queue, int consumer_id) {
    int item = 0;
    while (queue.pop(item)) {
        std::printf("[Consumer %d] Popped: %d\n", consumer_id, item);
        std::this_thread::sleep_for(std::chrono::milliseconds(9));
    }
}

} // namespace

int main() {
    constexpr int PRODUCER_COUNT = 3;
    constexpr int CONSUMER_COUNT = 2;
    constexpr int ITEMS_PER_PRODUCER = 10;

    mpmc_demo::MPMCQueueAdapter queue;
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    ProducerCountTracker producers_left(PRODUCER_COUNT);

    for (int i = 0; i < PRODUCER_COUNT; ++i) {
        int start_value = i * ITEMS_PER_PRODUCER + 1;
        producers.emplace_back(producer, std::ref(queue), i + 1, start_value,
                               ITEMS_PER_PRODUCER, std::ref(producers_left));
    }

    for (int i = 0; i < CONSUMER_COUNT; ++i) {
        consumers.emplace_back(consumer, std::ref(queue), i + 1);
    }

    for (auto& thread : producers) {
        thread.join();
    }
    for (auto& thread : consumers) {
        thread.join();
    }

    std::cout << "MPMC queue streaming finished successfully." << std::endl;
    return 0;
}
