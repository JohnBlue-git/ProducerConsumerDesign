#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>

#include "mpmc_queue.hpp"
#include "../include/producer_counter.hpp"
#include "../include/logging.hpp"

namespace {

void producer(mpmc_demo::MPMCQueueAdapter& queue, int producer_id, int start_value,
              int items_per_producer, ProducerCountTracker& producers_left) {
    for (int i = 0; i < items_per_producer; ++i) {
        int item = start_value + i;
        // std::this_thread::sleep_for(std::chrono::milliseconds(6));

        if (!queue.push(item)) {
            break;
        }
        PC_PRINT("[Producer %d] Pushed: %d\n", producer_id, item);
    }

    if (producers_left.mark_finished()) {
        queue.close();
    }
}

void consumer(mpmc_demo::MPMCQueueAdapter& queue, int consumer_id, ProducerCountTracker& producers_left) {
    int item = 0;
    while (true) {
        if (queue.pop(item)) {
            PC_PRINT("[Consumer %d] Popped: %d\n", consumer_id, item);
            continue;
        }

        if (producers_left.remaining() == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

} // namespace

int main(int argc, char** argv) {
    int PRODUCER_COUNT = 3;
    int CONSUMER_COUNT = 2;
    int ITEMS_PER_PRODUCER = 10;
    size_t BUFFER_SIZE = 1024;

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

    mpmc_demo::MPMCQueueAdapter queue(BUFFER_SIZE);
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    ProducerCountTracker producers_left(PRODUCER_COUNT);

    for (int i = 0; i < PRODUCER_COUNT; ++i) {
        int start_value = i * ITEMS_PER_PRODUCER + 1;
        producers.emplace_back(producer, std::ref(queue), i + 1, start_value,
                               ITEMS_PER_PRODUCER, std::ref(producers_left));
    }

    for (int i = 0; i < CONSUMER_COUNT; ++i) {
        consumers.emplace_back(consumer, std::ref(queue), i + 1, std::ref(producers_left));
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
