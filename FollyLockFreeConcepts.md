# Folly Queue (SISO) Synchronization Concepts

This note explains why Folly's queue implementations can be very efficient in concurrent code. The core idea is that they avoid turning every operation into a global lock contention point.

> The example below focuses on `folly::ProducerConsumerQueue`, which is one of the clearest demonstrations of the same design philosophy behind Folly's high-performance queues.

## 1. Single-writer invariant

In a true multi-producer, multi-consumer queue, threads often need to update shared head and tail pointers using expensive read-modify-write operations such as std::atomic::compare_exchange compare-and-swap (CAS) .

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


# Folly Queue (MIMO)

`folly::MPMCQueue` keeps the benefits of cache line padding, memory alignment, and bounded memory pre-allocation, but its lock-free algorithm works differently** because it has to handle multiple producers and multiple consumers.

## 1. Do `pushTicket_` and `popTicket_` use `alignas` padding?

**Yes.** In an MPMC model, instead of simple `writeIndex` and `readIndex`, Folly uses atomic "ticket dispensers": `pushTicket_` (for producers) and `popTicket_` (for consumers).

Because multiple producer threads write to `pushTicket_` while multiple consumer threads write to `popTicket_`, Folly isolates them onto separate 128-byte cache lines using explicit alignment directives:

```cpp
// Inside folly::MPMCQueue header:
alignas(hardware_destructive_interference_size) Atom pushTicket_;
alignas(hardware_destructive_interference_size) Atom popTicket_;

```

* **Producer Threads** contention is isolated to the `pushTicket_` cache line.
* **Consumer Threads** contention is isolated to the `popTicket_` cache line.
* **Result:** Producers grabbing tickets will **never** invalidate the consumer's ticket-dispensing cache line.

---

## 2. Advanced Cache Line Padding: The "Stride" Trick

In the SPSC version, elements in the internal ring buffer array sit next to each other. Because only one producer and consumer exist, sequential elements stay local to one thread at a time.

In **MPMC**, Thread A could get Ticket #1 (Slot 1) and Thread B could get Ticket #2 (Slot 2). If Slot 1 and Slot 2 share a 64-byte cache line, Thread A and Thread B will suffer **false sharing while writing their payload data**.

`folly::MPMCQueue` solves this using a **Stride Calculation**:

```
Instead of writing sequentially:
[Slot 0] -> [Slot 1] -> [Slot 2] -> [Slot 3] ... (False sharing between adjacent threads!)

Folly strides indices across different cache lines:
[Slot 0] -------------> [Slot STRIDE] -------------> [Slot 2*STRIDE] ...

```

By calculating a mathematical `stride_` offset between consecutive tickets, Folly guarantees that concurrently active threads land on **slots located on completely different cache lines**, avoiding array-level false sharing without wasting megabytes of padding.

---

## 3. Atomic Fetch-and-Add vs. CAS Loops

In an MPMC environment, you *cannot* completely eliminate multi-writer contention. However, `folly::MPMCQueue` avoids traditional, slow Compare-And-Swap (CAS) loops on the queue indices.

1. **Ticket Allocation:** Threads obtain a slot by doing `pushTicket_.fetch_add(1, std::memory_order_relaxed)`. On modern CPUs (x86/ARM), `fetch_add` maps to a single hardware atomic instruction (`LOCK XADD`), which is much faster than spinning in a `CAS` loop.
2. **Slot Synchronization:** Once a thread holds a ticket, it navigates to its assigned `SingleElementQueue` slot and executes a lightweight atomic synchronization to write/read the payload.

---

## Summary Comparison

| Optimization | `folly::ProducerConsumerQueue` (SPSC) | `folly::MPMCQueue` (MPMC) |
| --- | --- | --- |
| **Index Separation (`alignas`)** | Separates `writeIndex` and `readIndex` | Separates `pushTicket_` and `popTicket_` |
| **Array Data False Sharing** | Prevented by design (single-writer) | Prevented using **`stride_` index mapping** |
| **Contention Strategy** | Zero contention (Single Producer/Consumer) | Reduced contention via `fetch_add` ticket dispensing |
| **Blocking Fallback** | Non-blocking / Returns `false` | Adaptive spinning with `futex` fallback for sleeping threads |
