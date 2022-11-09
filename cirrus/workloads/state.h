#pragma once

#include <atomic>
#include <future>
#include <memory>

class BenchmarkState {
 public:
  static std::shared_ptr<BenchmarkState> Create() {
    return std::make_shared<BenchmarkState>();
  }

  BenchmarkState()
      : num_ready_(0),
        keep_running_(true),
        future_(promise_.get_future().share()) {}

  void SpinWaitUntilAllReady(uint64_t expected) {
    while (num_ready_ < expected) {
    }
  }

  void BumpReady() { ++num_ready_; }

  void WaitToStart() { future_.get(); }

  void AllowStart() { promise_.set_value(); }

  bool KeepRunning() const { return keep_running_; }

  void SetStopRunning() { keep_running_ = false; }

  void SetMaxDatetime(uint64_t value) {
    max_datetime_.store(value, std::memory_order::memory_order_release);
  }

  uint64_t GetMaxDatetime() const { return max_datetime_; }

 private:
  std::atomic<uint64_t> num_ready_;
  std::atomic<bool> keep_running_;

  // Updated by the writer, read by the reader.
  std::atomic<uint64_t> max_datetime_;

  std::promise<void> promise_;
  std::shared_future<void> future_;
};
