#include "thread_pool.h"

#include <cassert>

#include "affinity.h"

namespace cirrus {

ThreadPool::ThreadPool(size_t num_threads, std::function<void()> run_on_start,
                       std::function<void()> run_on_finish)
    : shutdown_(false),
      run_on_start_(std::move(run_on_start)),
      run_on_finish_(std::move(run_on_finish)) {
  assert(num_threads > 0);
  for (size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back(&ThreadPool::ThreadMain, this);
  }
}

ThreadPool::ThreadPool(size_t num_threads,
                       const std::vector<size_t>& thread_to_core)
    : shutdown_(false) {
  assert(num_threads > 0);
  assert(num_threads == thread_to_core.size());
  for (size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back(&ThreadPool::ThreadMainOnCore, this,
                          thread_to_core[i]);
  }
}

ThreadPool::~ThreadPool() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    shutdown_ = true;
  }
  cv_.notify_all();
  for (auto& thread : threads_) {
    thread.join();
  }
}

void ThreadPool::ThreadMainOnCore(size_t core_id) {
  PinToCore(core_id);
  ThreadMain();
}

void ThreadPool::ThreadMain() {
  if (run_on_start_) {
    run_on_start_();
  }
  std::unique_ptr<Task> next_job = nullptr;
  while (true) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      // Need a loop here to handle spurious wakeup
      while (!shutdown_ && work_queue_.empty()) {
        cv_.wait(lock);
      }
      if (shutdown_ && work_queue_.empty()) break;
      next_job.reset(work_queue_.front().release());
      work_queue_.pop();
    }
    (*next_job)();
    next_job.reset(nullptr);
  }
  if (run_on_finish_) {
    run_on_finish_();
  }
}

}  // namespace cirrus
