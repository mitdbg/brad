#pragma once

#include <memory>
#include <thread>

#include "state.h"

class WorkloadBase {
 public:
  WorkloadBase(std::shared_ptr<BenchmarkState> state);
  virtual ~WorkloadBase();
  void Wait();

 protected:
  void WarmedUpAndReadyToRun();
  bool KeepRunning() const;

 private:
  void Run();
  virtual void RunImpl() = 0;

  bool joined_;
  std::thread thread_;
  std::shared_ptr<BenchmarkState> state_;
};
