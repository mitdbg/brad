#include "workload_base.h"

WorkloadBase::WorkloadBase(std::shared_ptr<BenchmarkState> state)
    : joined_(false), state_(std::move(state)) {
  thread_ = std::thread(&WorkloadBase::Run, this);
}

WorkloadBase::~WorkloadBase() {
  if (joined_) return;
  Wait();
}

void WorkloadBase::Wait() {
  if (joined_) return;
  thread_.join();
}

void WorkloadBase::WarmedUpAndReadyToRun() {
  state_->BumpReady();
  state_->WaitToStart();
}

bool WorkloadBase::KeepRunning() const {
  return state_->KeepRunning();
}

void WorkloadBase::Run() { RunImpl(); }
