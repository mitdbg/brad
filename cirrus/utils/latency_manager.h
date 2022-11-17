#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <vector>

namespace utils {

class LatencyManager {
 public:
  LatencyManager(uint32_t num_samples) : num_samples_(num_samples), next_(0) {
    samples_.reserve(num_samples);
  }

  void Add(std::chrono::nanoseconds measurement) {
    // TODO: Samples is a misnomer.
    if (samples_.size() < num_samples_) {
      samples_.push_back(measurement);
      return;
    }
    if (next_ >= num_samples_) {
      next_ = 0;
    }
    samples_[next_] = measurement;
    ++next_;
  }

  void Sort() {
    std::sort(samples_.begin(), samples_.end());
  }

  template <typename Units>
  Units GetPercentile(double pct) const {
    const size_t index = static_cast<size_t>(samples_.size() * pct);
    return std::chrono::duration_cast<Units>(samples_[index]);
  }

 private:
  uint32_t num_samples_;
  uint32_t next_;
  std::vector<std::chrono::nanoseconds> samples_;
};

}  // namespace utils
