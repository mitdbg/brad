#pragma once

#include <memory>
#include <thread>

#include "state.h"
#include "utils/latency_manager.h"

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
  std::shared_ptr<BenchmarkState>& GetState();

 private:
  void Run();
  virtual void RunImpl() = 0;

  bool joined_;
  std::thread thread_;
  std::shared_ptr<BenchmarkState> state_;
  utils::LatencyManager latency_;
};
