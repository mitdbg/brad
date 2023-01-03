#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

namespace cirrus {

// Shared state used to coordinate benchmark execution.
class BenchmarkState {
 public:
  static std::shared_ptr<BenchmarkState> Create() {
    return std::make_shared<BenchmarkState>();
  }

  BenchmarkState()
      : num_ready_(0),
        keep_running_(true),
        future_(promise_.get_future().share()) {}

  void WaitUntilAllReady(uint64_t expected) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (num_ready_ < expected) {
      not_ready_.wait(lock);
    }
  }

  void BumpReady() {
    std::unique_lock<std::mutex> lock(mutex_);
    ++num_ready_;
    // Ideally we notify only after `num_ready_` reaches `expected`. But
    // `expected` is a local variable. This approach is fine since we are just
    // using it to coordinate starting a workload.
    not_ready_.notify_all();
  }

  void WaitToStart() { future_.get(); }

  void AllowStart() { promise_.set_value(); }

  bool KeepRunning() const { return keep_running_; }

  void SetStopRunning() { keep_running_ = false; }

 private:
  std::atomic<bool> keep_running_;

  mutable std::mutex mutex_;
  std::condition_variable not_ready_;
  uint64_t num_ready_;

  std::promise<void> promise_;
  std::shared_future<void> future_;
};

}  // namespace cirrus
