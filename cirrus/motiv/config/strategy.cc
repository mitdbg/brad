#include "cirrus/strategy.h"

#include <unordered_map>
#include <stdexcept>

namespace cirrus {

Strategy StrategyFromString(const std::string& strategy) {
  static const std::unordered_map<std::string, Strategy> kStrategies = {
      {"all_on_one", Strategy::kAllOnOne},
      {"stream_no_mv", Strategy::kLatestStreamNoMV},
      {"stream_mv", Strategy::kLatestStreamWithMV},
      {"hot_no_mv", Strategy::kHotPlacementNoMV},
      {"hot_mv", Strategy::kHotPlacementWithMV},
      {"wide_write", Strategy::kWideAllOnWrite},
      {"wide_read", Strategy::kWideAllOnRead},
      {"wide_hot", Strategy::kWideHotPlacement}};
  const auto it = kStrategies.find(strategy);
  if (it == kStrategies.end()) {
    throw std::runtime_error("Unrecognized strategy: " + strategy);
  }
  return it->second;
}

bool StrategyUsesMaterializedView(const Strategy& s) {
  switch (s) {
    case Strategy::kLatestStreamWithMV:
    case Strategy::kHotPlacementWithMV:
      return true;
  }
  return false;
}

}  // namespace cirrus
