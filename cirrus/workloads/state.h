#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

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

  void SetSimulatedETLTimes(std::chrono::milliseconds freshness,
                            std::chrono::milliseconds sim_etl_time) {
    std::unique_lock<std::mutex> lock(mutex_);
    freshness_ = freshness;
    sim_etl_time_ = sim_etl_time;
    last_sync_ = std::chrono::steady_clock::now();
  }

  void MaybeRunSimulatedETL() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (std::chrono::steady_clock::now() - last_sync_ > freshness_) {
      // Need to run ETL. This thead will fall asleep with this lock. All
      // analytical clients call this method, so they will block and wait if
      // they arrive while the ETL is in progress.
      std::this_thread::sleep_for(sim_etl_time_);
      last_sync_ = std::chrono::steady_clock::now();
    }
  }

 private:
  std::atomic<bool> keep_running_;

  mutable std::mutex mutex_;
  std::condition_variable not_ready_;
  uint64_t num_ready_;

  // Used for simulating ETLs.
  std::chrono::steady_clock::time_point last_sync_;
  std::chrono::milliseconds freshness_;
  std::chrono::milliseconds sim_etl_time_;

  std::promise<void> promise_;
  std::shared_future<void> future_;
};
