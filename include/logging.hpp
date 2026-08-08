#pragma once

#include <atomic>
#include <cstdio>

namespace pc {
inline std::atomic<bool> enable_logging{true};
}

#define PC_PRINT(...) \
    do { if (pc::enable_logging.load(std::memory_order_relaxed)) std::printf(__VA_ARGS__); } while (0)
