#include "workload_base.h"

WorkloadBase::WorkloadBase(std::shared_ptr<BenchmarkState> state)
    : joined_(false), state_(std::move(state)) {}

WorkloadBase::~WorkloadBase() {
  if (joined_) return;
  Wait();
}

void WorkloadBase::Start() {
  joined_ = false;
  thread_ = std::thread(&WorkloadBase::Run, this);
}

void WorkloadBase::Wait() {
  if (joined_) return;
  thread_.join();
  joined_ = true;
}

void WorkloadBase::WarmedUpAndReadyToRun() {
  state_->BumpReady();
  state_->WaitToStart();
}

bool WorkloadBase::KeepRunning() const { return state_->KeepRunning(); }

void WorkloadBase::Run() { RunImpl(); }
