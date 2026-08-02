# ProducerConsumerDesign

This project demonstrates three Producer-Consumer designs in C++:

- Fixed-size Ring Buffer
- Unbounded Queue (queue without size limit)
- Semaphore-based Ring Buffer

All versions are thread-safe. For MPMC shutdown, producer counting is handled by the shared utility in `producer_counter.hpp` through `ProducerCountTracker`.

## Project Structure

```text
ProducerConsumerDesign/
├── README.md
├── producer_counter.hpp
├── QueueBuffer/
│   ├── producer_consumer_que.cpp
│   └── queue_buffer.hpp
├── RingBuffer/
│   ├── producer_consumer_rb.cpp
│   └── ring_buffer.hpp
└── SemaphoreBuffer/
    ├── producer_consumer_sem.cpp
    └── semaphore_buffer.hpp
```

Each design folder contains its own implementation source, while the shared shutdown helper lives at the repository root.

## Design 1: Ring Buffer (Fixed Size)

File:

- `RingBuffer/ring_buffer.hpp`
- `RingBuffer/producer_consumer_rb.cpp`

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

- `QueueBuffer/queue_buffer.hpp`
- `QueueBuffer/producer_consumer_que.cpp`

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

- `SemaphoreBuffer/semaphore_buffer.hpp`
- `SemaphoreBuffer/producer_consumer_sem.cpp`

Key behaviors:

- Uses a fixed-size circular buffer synchronized with three semaphores:
	- `sem_not_full`: counts the number of free positions, so producers block when the buffer is full.
	- `sem_not_empty`: counts the number of available values, so consumers block when the buffer is empty.
	- `mtx`: provides mutual exclusion around the shared ring-buffer state.
- Producers wait on `sem_not_full.acquire()` before writing and release it after a value is stored.
- Consumers wait on `sem_not_empty.acquire()` before reading and release it after a value is consumed.
- The last producer calls `close(consumer_count)` so waiting consumers are released and can exit cleanly.

When to use:

- You want bounded memory with explicit semaphore synchronization.
- You are targeting C++20 and want a condition-variable-free synchronization variant.

## Design: Shared Shutdown and Flow Pattern

1. Main creates shared producer counter:

```cpp
ProducerCountTracker producers_left(PRODUCER_COUNT);
```

2. Each producer pushes items in sequence.
3. When a producer finishes, it decrements the shared counter.
4. Only the last producer closes the buffer.
5. Consumers repeatedly call `pop(item)` in a loop.
6. When `pop` returns `false` (closed and drained), each consumer exits cleanly.

This avoids hard-coding consumer loop count and keeps producer-lifecycle logic outside `*Buffer` classes.

All demos use the same producer-count coordination pattern in application code:

```cpp
ProducerCountTracker producers_left(PRODUCER_COUNT);

if (producers_left.mark_finished()) {
    // RingBuffer / QueBuffer
    rb.close();

    // Semaphore RingBuffer
    // rb.close(CONSUMER_COUNT);
}
```

Why this works:

- `mark_finished()` decrements the shared counter and reports whether this producer was the last one.
- If the return value is `true`, the current producer is the final producer.
- Only that last producer calls `close(...)`, so consumers drain the remaining items before exiting.

## Build and Run

Build Queue version:

```bash
g++ -std=c++17 -pthread QueueBuffer/producer_consumer_que.cpp -o QueueBuffer/producer_consumer_que
./QueueBuffer/producer_consumer_que
```

Build Ring Buffer version:

```bash
g++ -std=c++17 -pthread RingBuffer/producer_consumer_rb.cpp -o RingBuffer/producer_consumer_rb
./RingBuffer/producer_consumer_rb
```

Build Semaphore Ring Buffer version (C++20):

```bash
g++ -std=c++20 -pthread SemaphoreBuffer/producer_consumer_sem.cpp -o SemaphoreBuffer/producer_consumer_sem
./SemaphoreBuffer/producer_consumer_sem
```

## Summary

- `RingBuffer`: fixed capacity, stronger memory control, producer can block when full.
- `QueBuffer`: unbounded growth, simpler producer path, consumer blocks only when empty.
- `Semaphore RingBuffer`: fixed capacity with semaphore-based coordination (C++20).
- All support clean termination through `close(...)`, and MPMC producer coordination is implemented through the shared helper in `producer_counter.hpp`.
