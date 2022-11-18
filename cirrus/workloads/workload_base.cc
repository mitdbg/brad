#include "workload_base.h"

WorkloadBase::WorkloadBase(std::shared_ptr<BenchmarkState> state)
    : joined_(false), state_(std::move(state)), latency_(1000) {}

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

void WorkloadBase::SortLatency() { latency_.Sort(); }

std::chrono::milliseconds WorkloadBase::LatencyP50() const {
  return latency_.GetPercentile<std::chrono::milliseconds>(0.5);
}
std::chrono::milliseconds WorkloadBase::LatencyP99() const {
  return latency_.GetPercentile<std::chrono::milliseconds>(0.99);
}

void WorkloadBase::AddLatency(std::chrono::nanoseconds latency) {
  latency_.Add(latency);
}

std::shared_ptr<BenchmarkState>& WorkloadBase::GetState() { return state_; }
