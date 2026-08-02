# Folly Queue Synchronization Concepts

This note explains why Folly's queue implementations can be very efficient in concurrent code. The core idea is that they avoid turning every operation into a global lock contention point.

> The example below focuses on `folly::ProducerConsumerQueue`, which is one of the clearest demonstrations of the same design philosophy behind Folly's high-performance queues.

## 1. Single-writer invariant

In a true multi-producer, multi-consumer queue, threads often need to update shared head and tail pointers using expensive read-modify-write operations such as compare-and-swap (CAS).

```cpp
// MPMC requires a CAS loop because multiple threads fight over the same index
 do {
     old_head = head.load();
 } while (!head.compare_exchange_weak(old_head, old_head + 1));
```

`folly::ProducerConsumerQueue` avoids this by restricting access to exactly one producer and one consumer.

- The producer updates only the write index.
- The consumer updates only the read index.
- Because each index has a single writer, the queue does not need a CAS loop for normal progress.

That removes a major source of contention and makes the fast path much cheaper.

## 2. Acquire-release memory ordering

Instead of relying on the strongest and most expensive ordering (`std::memory_order_seq_cst`), Folly uses lightweight acquire-release semantics.

```cpp
// Producer pushing an item
const auto currentWrite = writeIndex_.load(std::memory_order_relaxed);
auto nextWrite = currentWrite + 1;
if (nextWrite == capacity_) nextWrite = 0;

// Check whether the queue is full
if (nextWrite == readIndex_.load(std::memory_order_acquire)) {
    return false;
}

// Write the payload and publish the new index
records_[currentWrite] = std::move(item);
writeIndex_.store(nextWrite, std::memory_order_release);
```

### Why this helps

- `std::memory_order_release` ensures that the payload write becomes visible before the new index is published.
- `std::memory_order_acquire` ensures that the consumer sees the payload when it reads the updated index.

This gives the queue correct visibility guarantees without paying the full cost of stronger global ordering.

## 3. Cache-line padding to avoid false sharing

Modern CPUs keep data in cache lines. If two threads update variables that live on the same cache line, the CPU may repeatedly invalidate and reload that line, which is called false sharing.

Folly avoids this by padding key variables so they sit on separate cache lines.

```cpp
// Simplified layout idea
alignas(folly::hardware_destructive_interference_size)
std::atomic<uint32_t> writeIndex_{0};

alignas(folly::hardware_destructive_interference_size)
std::atomic<uint32_t> readIndex_{0};
```

This helps the producer and consumer update their own counters without constantly invalidating each other's cache line.

## Summary

Folly's queue design can be very fast because it:

- avoids CAS loops by using a single-writer ownership model,
- uses acquire-release atomics instead of heavier ordering guarantees,
- and reduces false sharing through cache-line padding.

That combination makes the queue much more scalable than a simple mutex-protected `std::queue` under heavy concurrent access.
