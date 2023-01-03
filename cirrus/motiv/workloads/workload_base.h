#pragma once

#include <memory>
#include <thread>

#include "latency_manager.h"
#include "state.h"

namespace cirrus {

class WorkloadBase {
 public:
  WorkloadBase(std::shared_ptr<BenchmarkState> state);
  virtual ~WorkloadBase();
  void Wait();

  void SortLatency();
  std::chrono::milliseconds LatencyP50() const;
  std::chrono::milliseconds LatencyP99() const;

 protected:
  void Start();
  void WarmedUpAndReadyToRun();
  bool KeepRunning() const;
  void AddLatency(std::chrono::nanoseconds latency);
  const std::shared_ptr<BenchmarkState>& GetState() const;

 private:
  void Run();
  virtual void RunImpl() = 0;

  bool joined_;
  std::thread thread_;
  std::shared_ptr<BenchmarkState> state_;
  LatencyManager latency_;
};

}  // namespace cirrus
