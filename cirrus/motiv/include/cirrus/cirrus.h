#pragma once

#include <filesystem>
#include <memory>
#include <string>

#include "cirrus/config.h"
#include "cirrus/strategy.h"
#include "cirrus/workloads.h"

namespace cirrus {

// The Cirrus in-process API.
class Cirrus {
 public:
  virtual ~Cirrus() = default;

  static std::unique_ptr<Cirrus> Open(std::shared_ptr<CirrusConfig> config,
                                      Strategy strategy = Strategy::kAllOnOne);

  // Must be called by each thread that will call any Run*Query() methods.
  virtual void EstablishThreadLocalConnections() = 0;

  // Retrieve Cirrus' version.
  virtual std::string GetVersion() const = 0;

  // Store dataset specific methods follow. Later on, these interfaces need to
  // be generalized.

  // Used to refresh the materialized views.
  virtual void SetUpViews() = 0;

  virtual void SyncTableVersions() = 0;

  // Write methods.
  virtual void NotifyUpdateInventory(NotifyInventoryUpdate inventory) = 0;
  virtual void NotifyInsertSales(NotifySalesInsert sales) = 0;

  // Read methods.
  // These methods will block until the query completes.
  virtual size_t RunReportingQuery(uint64_t datetime_start,
                                   uint64_t datetime_end) = 0;
  virtual size_t RunStockFeatureQuery() = 0;

  virtual uint64_t GetMaxDatetime() const = 0;

  ////////////////////////////////////////////////////////////////////////////

  // Inventory-wide dataset specific methods follow.

  virtual void NotifyUpdateInventoryWide(NotifyInventoryUpdate inventory) = 0;
  virtual size_t RunCategoryStockQuery() = 0;
  virtual void RunETLSync(uint64_t sequence_num, uint64_t max_synced_version) = 0;
  virtual uint64_t GetMaxSyncedInv() = 0;
};

}  // namespace cirrus
