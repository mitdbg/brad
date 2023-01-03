#pragma once

#include <cstdint>
#include <mutex>
#include <queue>
#include <condition_variable>

namespace cirrus {

class TableVersion {
 public:
  TableVersion();

  void BumpLatestKnown(uint64_t version);
  void BumpUpdatedTo(uint64_t version);

  uint64_t LatestKnown() const;

  // Blocks the calling thread until this version has been updated to at least
  // `version`. This method returns whether or not the thread actually had to
  // wait, and the updated to version.
  std::pair<bool, uint64_t> WaitUntilAtLeast(uint64_t version);

 private:
  mutable std::mutex mutex_;
  std::condition_variable wait_;

  // Invariant: `updated_to_ <= latest_known_`
  uint64_t updated_to_;
  uint64_t latest_known_;
};

}  // namespace cirrus
