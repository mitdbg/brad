#pragma once

#include <immintrin.h>

#include <random>

namespace utils {

// Used for implementing randomized exponential backoff.
class BackoffManager {
 public:
  explicit BackoffManager() : attempts_(0) {}

  void Wait() {
    if (attempts_ < 12) {
      ++attempts_;
    }

    const uint32_t max_spin_cycles = 10 * (1UL << attempts_);
    std::uniform_int_distribution<uint32_t> dist(0, max_spin_cycles);

    uint32_t spin_for = dist(prng_);
    while (spin_for > 0) {
      _mm_pause();
      --spin_for;
    }
  }

 private:
  static thread_local std::mt19937 prng_;
  uint32_t attempts_;
};

inline thread_local std::mt19937 BackoffManager::prng_{};

}  // namespace utils
