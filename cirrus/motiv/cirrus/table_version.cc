#include "table_version.h"

#include <condition_variable>
#include <iostream>
#include <stdexcept>

namespace cirrus {

TableVersion::TableVersion() : updated_to_(0), latest_known_(0) {}

void TableVersion::BumpLatestKnown(uint64_t version) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (version < latest_known_) {
    std::cerr << "ERROR: BumpLatestKnown - latest: " << latest_known_
              << " version: " << version << std::endl;
    throw std::runtime_error("Invalid table version change! (BumpLatestKnown)");
  }
  latest_known_ = version;
  wait_.notify_all();
}

void TableVersion::BumpUpdatedTo(uint64_t version) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (version < updated_to_ || version > latest_known_) {
    std::cerr << "ERROR: BumpUpdatedTo - latest: " << latest_known_
              << " updated_to: " << updated_to_ << " version: " << version
              << std::endl;
    throw std::runtime_error("Invalid table version change! (BumpUpdatedTo)");
  }
  updated_to_ = version;
  wait_.notify_all();
}

uint64_t TableVersion::LatestKnown() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return latest_known_;
}

std::pair<bool, uint64_t> TableVersion::WaitUntilAtLeast(uint64_t version) {
  std::unique_lock<std::mutex> lock(mutex_);
  bool had_to_wait = false;
  while (version > updated_to_) {
    had_to_wait = true;
    wait_.wait(lock);
  }
  return {had_to_wait, updated_to_};
}

}  // namespace cirrus
