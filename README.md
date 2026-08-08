# ProducerConsumerDesign

This project demonstrates four Producer-Consumer designs in C++:

- Fixed-size Ring Buffer
- Unbounded Queue (queue without size limit)
- Semaphore-based Ring Buffer
- Folly-based MPMC Queue

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
├── SemaphoreBuffer/
│   ├── producer_consumer_sem.cpp
│   └── semaphore_buffer.hpp
└── MPMCQueue/
    ├── producer_consumer_mpmc.cpp
    └── mpmc_queue.hpp
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

## Design 4: Folly MPMC Queue Comparison

This design adds a modern queue option based on Folly's multi-producer, multi-consumer queue implementations. It compares three approaches:

- `std::queue` + `std::mutex`
- `folly::ProducerConsumerQueue`
- `folly::MPMCQueue`

Why Folly is attractive here is that its queue implementations are built around lock-free or wait-free style synchronization techniques instead of relying on a single global mutex for every enqueue and dequeue. In practical terms, that means the implementation often uses techniques such as atomic state updates, carefully partitioned read/write indices, and buffer slots that can be consumed without forcing every thread through the same lock. Those techniques reduce the number of blocking points and help multiple producers and consumers make progress in parallel. A mutex-protected `std::queue` is simpler, but it forces access through one lock, so contention grows quickly as the number of threads increases.

### Dependency installation

Ubuntu / Debian:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build pkg-config \
  libdouble-conversion-dev libgoogle-glog-dev libgflags-dev libevent-dev \
  libboost-all-dev libfmt-dev libssl-dev libzstd-dev libsnappy-dev \
  liblz4-dev libunwind-dev
```

Folly from source:

```bash
cd /tmp
rm -rf fast_float folly

git clone https://github.com/fastfloat/fast_float.git
cd fast_float
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
sudo cmake --install build

cd /tmp
git clone https://github.com/facebook/folly.git
cd folly
git checkout v2024.11.04.00
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DBUILD_EXAMPLES=OFF
cmake --build build -j"$(nproc)"
sudo cmake --install build
```

### MoodyCamel demo (non-blocking)

This repo includes a MoodyCamel-style demo using `moodycamel::ConcurrentQueue` placed under `MoodyCamelQueue/`. The demo keeps the queue non-blocking and uses `ProducerCountTracker` for clean shutdown.

Build and run:

```bash
g++ -std=c++17 -O2 -pthread -IMoodyCamelQueue MoodyCamelQueue/producer_consumer_moodycamel.cpp -o MoodyCamelQueue/moody_demo
./MoodyCamelQueue/moody_demo
```

### Benchmark Results (quick summary)

Automated benchmarks were run with unified parameters: `PRODUCER_COUNT=4`, `CONSUMER_COUNT=4`, `ITEMS_PER_PRODUCER=20000`, `BUFFER_SIZE=1024`.

| Implementation | Runtime (s) | Peak RSS (bytes) | Notes |
| --- | ---: | ---: | --- |
| moodycamel (ConcurrentQueue) | 0.05089521408081055 | 319,488 | Non-blocking adapter |
| queuebuffer (mutex queue) | 0.05179309844970703 | 1,564,672 | Std::queue + mutex |
| ringbuffer (SPSC ring) | 0.05288362503051758 | 1,851,392 | Ring buffer implementation |
| semaphore (SemRingBuffer) | 0.050911903381347656 | 307,200 | C++20 semaphore implementation |
| mpmc (Folly MPMCQueue) | 0.0509343147277832 | 16,384 | Folly MPMCQueue implementation |

Full JSON results are available at `tests/results.json`.

### Comparison table

| Design | Concurrency model | Complexity | Throughput under heavy contention | Best fit |
| --- | --- | --- | --- | --- |
| `std::queue` + `std::mutex` | Single lock around shared container | Low | Lower | Simple educational and low-contention code |
| `folly::ProducerConsumerQueue` | Single producer / single consumer | Medium | High for 1P1C | Streaming pipelines with one writer and one reader |
| `folly::MPMCQueue` | Multiple producers / multiple consumers | Medium/High | Highest for MPMC | High-throughput concurrent systems |

### Recommendation

If the goal is to demonstrate a modern and scalable producer-consumer design, `folly::MPMCQueue` is the best choice. If the workload is simple and the priority is readability, `std::queue` + `std::mutex` is still a strong starting point. If the system is strictly one-producer/one-consumer, `folly::ProducerConsumerQueue` is the more specialized and efficient option.

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

Build the Folly-backed MPMC demo:

```bash
g++ -std=c++17 -pthread MPMCQueue/producer_consumer_mpmc.cpp -o MPMCQueue/producer_consumer_mpmc \
  $(pkg-config --cflags --libs --static libfolly) -lfmt
./MPMCQueue/producer_consumer_mpmc
```

If Folly is not available and the build is attempted with the Folly flag enabled, the compile stops immediately with a clear error message telling you to install Folly or build without the flag.

## Summary

- `RingBuffer`: fixed capacity, stronger memory control, producer can block when full.
- `QueBuffer`: unbounded growth, simpler producer path, consumer blocks only when empty.
- `Semaphore RingBuffer`: fixed capacity with semaphore-based coordination (C++20).
- `MPMCQueue`: modern Folly-backed option for multi-producer, multi-consumer workloads.
- All support clean termination through `close(...)`, and MPMC producer coordination is implemented through the shared helper in `producer_counter.hpp`.
