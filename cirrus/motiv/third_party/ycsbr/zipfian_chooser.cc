#include "ycsbr/zipfian_chooser.h"

#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

namespace {

// A thread-safe `zeta(n)` cache (to reduce recomputation latency for large item
// counts).
class ZetaCache {
 public:
  static ZetaCache& Instance() {
    static ZetaCache instance;
    return instance;
  }

  using Theta = double;
  using ItemCount = size_t;
  using ZetaN = double;

  // Finds a `zeta(n)` value for a given `item_count` (or for a smaller
  // `item_count` if the exact `item_count` is not in the cache).
  std::optional<std::pair<ItemCount, ZetaN>> FindStartingPoint(
      const size_t item_count, const double theta) const {
    std::unique_lock<std::mutex> lock(mutex_);
    auto theta_map_it = cache_.find(theta);
    if (theta_map_it == cache_.end() || theta_map_it->second.empty()) {
      return std::optional<std::pair<ItemCount, ZetaN>>();
    }

    const auto& theta_map = theta_map_it->second;
    const auto it = theta_map.lower_bound(item_count);
    if (it == theta_map.end()) {
      return *std::prev(theta_map.end());
    } else {
      if (it->first == item_count) {
        // Exact match.
        return *it;
      } else if (it == theta_map.begin()) {
        // No previous values.
        return std::optional<std::pair<size_t, double>>();
      } else {
        // Not an exact match, so the starting point should be the first zeta
        // computed with a smaller item count.
        return *std::prev(theta_map.end());
      }
    }
  }

  void Add(const size_t item_count, const double theta, const double zeta) {
    std::unique_lock<std::mutex> lock(mutex_);
    // Will create a map for the `theta` value if one does not already exist.
    auto& theta_map = cache_[theta];
    // N.B. If an entry for `item_count` already exists, this insert will be an
    // effective no-op.
    theta_map.insert(std::make_pair(item_count, zeta));
  }

  ZetaCache(ZetaCache&) = delete;
  ZetaCache& operator=(ZetaCache&) = delete;

 private:
  // Singleton class - use `ZetaCache::Instance()` instead.
  ZetaCache() = default;

  mutable std::mutex mutex_;

  // Caches (item_count, zeta) pairs for a given `theta`. It is okay to key the
  // map by a `double` here because the `theta` values are parsed from a
  // configuration file (i.e., they do not come from calculations).
  std::unordered_map<Theta, std::map<ItemCount, ZetaN>> cache_;
};

}  // namespace

namespace ycsbr {
namespace gen {

void ZipfianChooser::UpdateZetaNWithCaching() {
  ZetaCache& cache = ZetaCache::Instance();
  auto result = cache.FindStartingPoint(item_count_, theta_);
  if (result.has_value() && result->first == item_count_) {
    // We computed zeta(n) for this `item_count` and `theta` before.
    zeta_n_ = result->second;
    return;
  }
  size_t prev_item_count = 0;
  double prev_zeta_n = 0.0;
  if (result.has_value()) {
    prev_item_count = result->first;
    prev_zeta_n = result->second;
    assert(prev_item_count < item_count_);
  }
  zeta_n_ = ComputeZetaN(item_count_, theta_, prev_item_count, prev_zeta_n);
  // N.B. Multiple threads may end up computing zeta(n) for the same
  // `item_count`, but we consider this case acceptable because it cannot lead
  // to incorrect zeta(n) values.
  cache.Add(item_count_, theta_, zeta_n_);
}

}  // namespace gen
}  // namespace ycsbr
