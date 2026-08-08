# `moodycamel::ConcurrentQueue` Concept

While both are high-performance, multi-producer/multi-consumer (MPMC) lock-free C++ queues that utilize cache line padding and lock-free primitives, they rely on completely different architectural paradigms to handle thread contention.

## 1. Core Architectural Difference

### `folly::MPMCQueue` — The "Global Ring Buffer" Approach

`folly::MPMCQueue` is a **single, unified, fixed-capacity array (ring buffer)**.

* **Mechanism:** All producer threads contend for a single global `pushTicket_` counter using atomic operations. Once a thread obtains a ticket, it writes directly to that slot using stride calculations to prevent false sharing.
* **Ordering:** Guarantees strict **Global FIFO** (First-In, First-Out) across all threads.

```
  Producer 1 ──┐
  Producer 2 ──┼──> [ Global Ticket Counter ] ──> [ Fixed-Size Ring Buffer Array ]
  Producer 3 ──┘

```

---

### `moodycamel::ConcurrentQueue` — The "Distributed Sub-Queues" Approach

`moodycamel::ConcurrentQueue` eliminates cross-thread contention on enqueue by avoiding a single shared ring buffer entirely.

* **Mechanism:** It treats an MPMC queue as a collection of independent **Single-Producer, Multi-Consumer (SPMC)** sub-queues. Each producer thread gets its own dedicated, thread-local sub-queue.
* **Enqueueing:** When Producer 1 enqueues, it writes *only* to its own sub-queue. It never interacts with Producer 2 or Producer 3, resulting in **zero producer-side contention**.
* **Dequeueing:** Consumer threads use internal heuristics to round-robin or steal elements across the various SPMC sub-queues.
* **Ordering:** **Relaxed FIFO**. Ordering is strictly preserved for elements pushed by *the same thread*, but interleaving between different producer threads is permitted.

```
  Producer 1 ──> [ Sub-Queue 1 (SPMC) ] ──┐
  Producer 2 ──> [ Sub-Queue 2 (SPMC) ] ──┼──> Consumers (Round-Robin / Steal)
  Producer 3 ──> [ Sub-Queue 3 (SPMC) ] ──┘

```

---

## 2. Feature & Performance Trade-offs

| Feature | `folly::MPMCQueue` | `moodycamel::ConcurrentQueue` |
| --- | --- | --- |
| **Architectural Model** | Single MPMC Ring Buffer | Distributed SPMC Sub-Queues |
| **Ordering Guarantee** | **Strict FIFO** across all threads | **Per-producer FIFO** (relaxed overall) |
| **Capacity** | Bounded (Fixed at initialization) | **Unbounded** (Dynamically grows chunks on heap) |
| **Producer Contention** | Low (uses atomic `fetch_add`) | **Zero** (threads write to thread-local queues) |
| **Bulk Operations** | Item-by-item loop | **Native Bulk Enqueue/Dequeue** (blazingly fast) |
| **Memory Allocation** | Zero allocations after setup | Dynamic allocations as memory usage grows |
| **Header Dependency** | Requires Facebook Folly framework | **Header-only**, 1 file, zero dependencies |

---

## 3. When to Choose Which?

* **Choose `moodycamel::ConcurrentQueue` if:**
* You need **unbounded/dynamically growing** capacity.
* You perform **bulk operations** (pushing or popping 10–100 elements at a time).
* You want maximum multi-producer throughput and can tolerate **relaxed cross-producer ordering**.
* You want a standalone, dependency-free `concurrentqueue.h` header file you can drop into any C++ project.

### Note on blocking vs non-blocking

`moodycamel::ConcurrentQueue` is a non-blocking, lock-free queue providing `enqueue` and `try_dequeue` operations. It does not perform blocking waits by itself — if your application needs consumers to block while waiting for items, either use the separate `BlockingConcurrentQueue` (`blockingconcurrentqueue.h`) or implement an external coordination mechanism (such as a producer counter + brief backoff). The demo in this repo keeps the moodycamel queue fully non-blocking and uses `ProducerCountTracker` plus short sleeps to terminate consumers cleanly once all producers finish.


* **Choose `folly::MPMCQueue` if:**
* You require **strict global FIFO ordering** (e.g., event logs or strict task scheduling).
* You want a **hard cap on memory** and deterministic zero-allocation guarantees after initial setup.
* You are already using Facebook's Folly stack in your codebase.
