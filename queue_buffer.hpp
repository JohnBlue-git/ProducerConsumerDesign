#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>

// Thread-safe unbounded queue for producer-consumer.
class QueBuffer {
private:
	std::queue<int> buffer;
	std::mutex mtx;
	std::condition_variable cv_not_empty;
	bool done = false;

public:
	// Push item to queue. Returns false if queue is already closed.
	bool push(int item) {
		std::unique_lock<std::mutex> lock(mtx);
		if (done) {
			return false;
		}
		buffer.push(item);
		lock.unlock();
		cv_not_empty.notify_one();
		return true;
	}

	// Mark queue as done and wake all blocked consumers.
	void close() {
		std::lock_guard<std::mutex> lock(mtx);
		done = true;
		cv_not_empty.notify_all();
	}

	// Pop item from queue.
	// Returns false when queue is closed and fully drained.
	bool pop(int& item) {
		std::unique_lock<std::mutex> lock(mtx);
		cv_not_empty.wait(lock, [this]() { return done || !buffer.empty(); });

		if (buffer.empty()) {
			return false;
		}

		item = buffer.front();
		buffer.pop();
		return true;
	}
};
