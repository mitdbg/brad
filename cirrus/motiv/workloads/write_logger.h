#pragma once

#include <filesystem>
#include <fstream>

#include "cirrus/cirrus.h"

namespace cirrus {

// Logs all the writes that a transactional client makes.
class CirrusWriteLogger : public Cirrus {
 public:
  CirrusWriteLogger(const std::filesystem::path& out_dir);

  void EstablishThreadLocalConnections() override {}
  // Retrieve Cirrus' version.
  std::string GetVersion() const override;

  // Store dataset specific methods follow. Later on, these interfaces need to
  // be generalized.

  void SetUpViews() override {}
  void SyncTableVersions() override {}

  void NotifyUpdateInventory(NotifyInventoryUpdate inventory) override;
  void NotifyInsertSales(NotifySalesInsert sales) override;

  size_t RunReportingQuery(uint64_t datetime_start,
                           uint64_t datetime_end) override;
  size_t RunStockFeatureQuery() override;

  uint64_t GetMaxDatetime() const override { return 0; }

  void NotifyUpdateInventoryWide(NotifyInventoryUpdate inventory) override {}
  size_t RunCategoryStockQuery() override { return 0; }

  void RunETLSync(uint64_t sequence_num, uint64_t max_synced_version) override {}
  uint64_t GetMaxSyncedInv() override { return 0; }

 private:
  std::ofstream inventory_out_, sales_out_;
};

}  // namespace cirrus
