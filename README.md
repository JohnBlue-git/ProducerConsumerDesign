# ProducerConsumerDesign

This project demonstrates three Producer-Consumer designs in C++:

- Fixed-size Ring Buffer
- Unbounded Queue (queue without size limit)
- Semaphore-based Ring Buffer

All versions are thread-safe. For MPMC shutdown, producer counting is handled in application code (.cpp) with `std::atomic<int> producers_left(PRODUCER_COUNT);`.

## Project Structure

```text
ProducerConsumerDesign/
├── producer_consumer_rb.cpp    # Producer/Consumer demo using RingBuffer
├── producer_consumer_que.cpp   # Producer/Consumer demo using QueBuffer
├── producer_consumer_sem.cpp   # Producer/Consumer demo using semaphore + ring buffer
├── ring_buffer.hpp             # Fixed-size blocking ring buffer
├── queue_buffer.hpp            # Unbounded blocking queue
├── semaphore_buffer.hpp        # Fixed-size semaphore-based ring buffer
└── README.md
```

## Design 1: Ring Buffer (Fixed Size)

File:

- `ring_buffer.hpp`

Key behaviors:

- Uses a fixed capacity circular array.
- Producer blocks when buffer is full (blocked by `RingBuffer::push(int)`).
- Consumer blocks when buffer is empty (blocked by `RingBuffer::pop(int&)`).
- Uses two condition variables:
	- `cv_not_full`: wake producers when space becomes available.
	- `cv_not_empty`: wake consumers when data becomes available.

Done flag behavior:

- `close()` sets done flag and wakes all waiting threads.
- `push(int)` returns `false` if buffer is already closed.
- `pop(int&)` returns `false` when buffer is closed and drained.

When to use:

- You need bounded memory usage.
- Backpressure is required when producer is faster than consumer.

## Design 2: Queue Without Size Limit (Unbounded)

File:

- `queue_buffer.hpp`

Key behaviors:

- Uses `std::queue<int>` with no explicit capacity limit.
- Producer does not block for capacity.
- Consumer blocks only when queue is empty (blocked by `QueBuffer::pop(int&)`).
- Uses one condition variable:
	- `cv_not_empty`: wake consumers when data arrives.

Done flag behavior:

- `close()` sets done flag and wakes all waiting consumers.
- `push(int)` returns `false` if queue is already closed.
- `pop(int&)` returns `false` when queue is closed and drained.

When to use:

- You want simple throughput-oriented design.
- Temporary growth in queue size is acceptable.

## Design 3: Semaphore + Ring Buffer (C++20)

File:

- `semaphore_buffer.hpp`
- `producer_consumer_sem.cpp`

Key behaviors:

- Uses fixed-size circular storage with three semaphores:
	- `slots`: available capacity for producers.
	- `items`: available produced items for consumers.
	- `mtx`: binary semaphore for critical section protection.
- Producer blocks when ring buffer is full (`slots.acquire()`).
- Consumer blocks when ring buffer is empty (`items.acquire()`).
- Last producer calls `close(consumer_count)` to wake blocked consumers for clean exit.

When to use:

- You want bounded memory with explicit semaphore synchronization.
- You are targeting C++20 and want a condition-variable-free synchronization variant.

## Producer/Consumer Flow in All Demos

1. Main creates shared producer counter:

```cpp
std::atomic<int> producers_left(PRODUCER_COUNT);
```

2. Each producer pushes items in sequence.
3. When a producer finishes, it decrements the shared counter.
4. Only the last producer closes the buffer.
5. Consumers repeatedly call `pop(item)` in a loop.
6. When `pop` returns `false` (closed and drained), each consumer exits cleanly.

This avoids hard-coding consumer loop count and keeps producer-lifecycle logic outside `*Buffer` classes.

## MPMC Shutdown Pattern (in .cpp)

All demos use this producer-count coordination pattern in application code:

```cpp
if (producers_left.fetch_sub(1, std::memory_order_acq_rel) == 1) {
	// RingBuffer / QueBuffer
	rb.close();

	// Semaphore RingBuffer
	// rb.close(CONSUMER_COUNT);
}
```

Why this works:

- `fetch_sub` returns the previous value.
- If the previous value is `1`, current producer is the last producer.
- Only that last producer calls `close(...)`, so consumers drain remaining items before exiting.

## Build and Run

Build Queue version:

```bash
g++ -std=c++17 -pthread producer_consumer_que.cpp -o producer_consumer_que
./producer_consumer_que
```

Build Ring Buffer version:

```bash
g++ -std=c++17 -pthread producer_consumer_rb.cpp -o producer_consumer_rb
./producer_consumer_rb
```

Build Semaphore Ring Buffer version (C++20):

```bash
g++ -std=c++20 -pthread producer_consumer_sem.cpp -o producer_consumer_sem
./producer_consumer_sem
```

## Summary

- `RingBuffer`: fixed capacity, stronger memory control, producer can block when full.
- `QueBuffer`: unbounded growth, simpler producer path, consumer blocks only when empty.
- `Semaphore RingBuffer`: fixed capacity with semaphore-based coordination (C++20).
- All support clean termination through `close(...)`, and MPMC producer coordination is implemented in `.cpp` via `std::atomic<int> producers_left(PRODUCER_COUNT);`.
