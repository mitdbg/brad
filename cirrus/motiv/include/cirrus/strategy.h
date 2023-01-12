#pragma once

#include <string>

namespace cirrus {

// These strategies are set up for the "store" workload. Across all strategies,
// we ensure "peak freshness".
enum class Strategy {
  // Run analytical and write queries against one physical DB (the "write store"
  // (usually PostgreSQL)).
  //
  // Using an MV here will not be beneficial because PostgreSQL does not support
  // incremental view maintenance.
  kAllOnOne,

  // Run write queries against the "write store" and analytical queries against
  // the "read store". This strategy is a peak freshness strategy, so writes are
  // streamed over to the data warehouse.
  kLatestStreamNoMV,

  // Same as above, but we use a materialized view (it is refreshed on updates).
  kLatestStreamWithMV,

  // Our strategy that keeps hot data in the write store.
  kHotPlacementNoMV,

  // The same strategy, but now we also use a materialized view.
  kHotPlacementWithMV,

  // TODO: Also explore batched updates? I think the latency will highly likely
  // be worse than the "latest stream" approaches, but perhaps the throughput
  // will be better.

  // These strategies are for the "wide inventory" workload.
  kWideAllOnWrite,
  kWideAllOnRead,
  kWideHotPlacement,
};

Strategy StrategyFromString(const std::string& strategy);
bool StrategyUsesMaterializedView(const Strategy& s);

}  // namespace cirrus
