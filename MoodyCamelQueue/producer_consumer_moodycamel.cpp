#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>

#include "../producer_counter.hpp"
#include "concurrent_queue_adapter.hpp"
#include "../logging.hpp"

namespace {

void producer(moody_demo::ConcurrentQueueAdapter& queue, int producer_id, int start_value,
              int items_per_producer, ProducerCountTracker& producers_left) {
    for (int i = 0; i < items_per_producer; ++i) {
        int item = start_value + i;
        // std::this_thread::sleep_for(std::chrono::milliseconds(6));

        queue.push_for_producer(producer_id, item);
        PC_PRINT("[Producer %d] Pushed: %d\n", producer_id, item);
    }

    if (producers_left.mark_finished()) {
        queue.close();
    }
}

void consumer(moody_demo::ConcurrentQueueAdapter& queue, int consumer_id, ProducerCountTracker& producers_left) {
    int item = 0;
    while (true) {
        if (queue.pop(item)) {
            PC_PRINT("[Consumer %d] Popped: %d\n", consumer_id, item);
            // std::this_thread::sleep_for(std::chrono::milliseconds(9));
            continue;
        }

        // No item available. If all producers finished, exit; otherwise back off briefly.
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

    std::cout << "MoodyCamel-style queue streaming finished successfully." << std::endl;
    return 0;
}
