#include <cstdint>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "cirrus/stats.h"
#include "cirrus_impl.h"

#define MULTILINE(...) #__VA_ARGS__

namespace {

// Based on a zipfian, theta = 0.9, scale factor 10, workload (10 million keys).
// TODO: Improve this code.
static const std::vector<uint64_t> kHotIds = {
    6593012, 5382871, 9013293, 7803152, 1752450, 542309,  2962590, 6274137,
    4172731, 1433575, 7484277, 8694418, 223434,  1114700, 3853856, 5063996,
    8375543, 2643715, 5955262, 4745121, 7165402, 4426246, 7868512, 3534981,
    5448231, 2324840, 795824,  6846527, 2005965, 9904558, 5636386, 9078653,
    9969918, 8440902, 1817810, 6658372, 8056667, 1498935, 7549637, 5252151,
    8759778, 3027950, 3216105, 9585683, 1621730, 4491606, 3600341, 4614401,
    4238091, 9839198, 7991307, 288794,  7230762, 3281465, 2390200, 9332168,
    8122027, 6339497, 1180060, 2709075, 5824542, 8751853, 4998636, 6020622,
    9651043, 861184,  5129356,
};

static const std::unordered_set<uint64_t> kHotIdSet(kHotIds.begin(),
                                                    kHotIds.end());

static const std::string kHotIdString = ([](const std::vector<uint64_t>& ids) {
  std::stringstream result;
  result << "(";
  for (size_t i = 0; i < ids.size(); ++i) {
    result << ids[i];
    if (i != ids.size() - 1) {
      result << ", ";
    }
  }
  result << ")";
  return result.str();
})(kHotIds);

// clang-format off
static const std::string kAllOnOneQuery = MULTILINE(
  SELECT
    i_category,
    SUM(i_stock) AS total
  FROM
    inventory_wide
  GROUP BY
    i_category
);
// clang-format on

static const std::string kHotQueryWrite = ([]() {
  std::stringstream query;
  query << "SELECT i_category, SUM(i_stock) AS total FROM inventory_wide WHERE "
           "i_id IN ";
  query << kHotIdString;
  query << " GROUP BY i_category";
  return query.str();
})();

static const std::string kHotQueryRead = ([]() {
  std::stringstream query;
  // Note the negation.
  query << "SELECT i_category, SUM(i_stock) AS total FROM inventory_wide WHERE "
           "i_id NOT IN ";
  query << kHotIdString;
  query << " GROUP BY i_category";
  return query.str();
})();

static const std::string kUpdateInventory =
    "UPDATE inventory_wide SET i_stock = ? WHERE i_id = ?";

static const std::string kExtractHot = ([]() {
  std::stringstream query;
  query << "SELECT * from aws_s3.query_export_to_s3(";
  query << "'SELECT * FROM inventory_wide";
  query << " WHERE i_id IN " << kHotIdString << "'";
  query << ", aws_commons.create_s3_uri('geoffxy-research', "
           "'etl/inventory_wide.tbl', 'us-east-1'), options :='FORMAT text, "
           "DELIMITER ''|''');";
  return query.str();
})();

std::string GenerateImportQuery(const std::string& iam_role) {
  std::stringstream query;
  query << "COPY inventory_wide_hot"
        << " FROM 's3://geoffxy-research/etl/inventory_wide.tbl'"
        << " IAM_ROLE '" << iam_role << "' REGION 'us-east-1'";
  return query.str();
}

// clang-format off
static const std::string kHotQueryReadWithImport = MULTILINE(
  WITH full_results AS (
    SELECT i_id, i_category, i_stock FROM inventory_wide
  ),
  new_results AS (
    SELECT
      i_id AS n_id,
      i_category AS n_category,
      i_stock AS n_stock
    FROM
      inventory_wide_hot
  ),
  combined AS (
    SELECT
      (CASE
        WHEN ISNULL(i_id) THEN n_id
        WHEN ISNULL(n_id) THEN i_id
        ELSE n_id
      END) AS i_id,
      (CASE
        WHEN ISNULL(i_id) THEN n_category
        WHEN ISNULL(n_id) THEN i_category
        ELSE n_category
      END) AS i_category,
      (CASE
        WHEN ISNULL(i_id) THEN n_stock
        WHEN ISNULL(n_id) THEN i_stock
        ELSE n_stock
      END) AS i_stock
    FROM
      full_results FULL OUTER JOIN new_results
      ON i_id = n_id
  )
  SELECT
    i_category,
    SUM(i_stock) AS total
  FROM combined
  GROUP BY i_category
);

static const std::string kHotQueryReadDisjointWithImport = MULTILINE(
  WITH combined AS (
    SELECT i_id, i_category, i_stock FROM inventory_wide
    UNION ALL
    SELECT i_id, i_category, i_stock FROM inventory_wide_hot
  )
  SELECT
    i_category,
    SUM(i_stock) AS total
  FROM combined
  GROUP BY i_category
);
// clang-format on

static const std::string kCreateImportTable = MULTILINE(
    CREATE TABLE IF NOT EXISTS inventory_wide_hot(LIKE inventory_wide));

static const std::string kTruncateImportTable =
    MULTILINE(TRUNCATE TABLE inventory_wide_hot);

}  // namespace

namespace cirrus {

void CirrusImpl::NotifyUpdateInventoryWide(NotifyInventoryUpdate inventory) {
  Stats::Local().BumpInventoryNotifications();
  if (strategy_ == Strategy::kWideAllOnWrite) return;

  if (strategy_ == Strategy::kWideHotPlacement) {
    // Check if the key is hot. If it is, we drop the update! We never modify
    // the hot set in this implementation, so we do not need to take any locks.
    // We ignore the versioning - we use it only for tracking updates that
    // actually made to the table in the read store.
    if (kHotIdSet.count(inventory.i_id) == 0) {
      Stats::Local().BumpHotInventoryDrops();
      return;
    }
  }

  inventory_version_.BumpLatestKnown(inventory.i_phys_id);

  bg_workers_.SubmitNoWait([this, inventory = std::move(inventory)]() {
    auto& read_store = connections_.read();
    nanodbc::statement stmt(read_store);
    nanodbc::prepare(stmt, kUpdateInventory);
    stmt.bind(0, &(inventory.i_stock), 1);
    stmt.bind(1, &(inventory.i_id), 1);
    nanodbc::execute(stmt);

    // This is a naive approach. Updates currently aren't guaranteed to run in
    // order (unless we only use one background worker).
    inventory_version_.BumpUpdatedTo(inventory.i_phys_id);
  });
}

size_t CirrusImpl::RunCategoryStockQuery() {
  if (strategy_ == Strategy::kWideAllOnRead ||
      strategy_ == Strategy::kWideAllOnWrite) {
    return WideAllOnOne();
  } else if (strategy_ == Strategy::kWideHotPlacement) {
    return WideHotPlacement();
  } else if (strategy_ == Strategy::kWideExtractImport) {
    return WideExtractImport();
  } else {
    throw std::runtime_error("Unsupported strategy.");
  }
  return 0;
}

size_t CirrusImpl::WideAllOnOne() {
  if (strategy_ == Strategy::kWideAllOnWrite) {
    // No need to wait for the latest result to become available.
    Stats::Local().BumpReadWithoutPause();
    auto& write_store = connections_.write();
    auto all_results = nanodbc::execute(write_store, kAllOnOneQuery);
    size_t num_results = 0;
    while (all_results.next()) {
      ++num_results;
    }
    return num_results;

  } else {
    // May need to wait.
    const auto latest_inventory = inventory_version_.LatestKnown();
    const auto [inv_waited, _] =
        inventory_version_.WaitUntilAtLeast(latest_inventory);
    if (inv_waited) {
      Stats::Local().BumpReadWithPause();
    } else {
      Stats::Local().BumpReadWithoutPause();
    }

    auto& read_store = connections_.read();
    auto all_results = nanodbc::execute(read_store, kAllOnOneQuery);
    size_t num_results = 0;
    while (all_results.next()) {
      ++num_results;
    }
    return num_results;
  }
}

size_t CirrusImpl::WideHotPlacement() {
  // May need to wait.
  const auto latest_inventory = inventory_version_.LatestKnown();
  const auto [inv_waited, _] =
      inventory_version_.WaitUntilAtLeast(latest_inventory);
  // We assume the write store is always up to date. But we may need to wait for
  // the read store.
  // TODO: We need stronger transactional consistency guarantees. This benchmark
  // approach is sloppy. We should add a versioning column and filter for it.
  if (inv_waited) {
    Stats::Local().BumpReadWithPause();
  } else {
    Stats::Local().BumpReadWithoutPause();
  }

  // TODO: Ideally we run both queries in parallel.
  auto& read_store = connections_.read();
  auto& write_store = connections_.write();

  auto read_result = nanodbc::execute(read_store, kHotQueryRead);
  auto write_result = nanodbc::execute(write_store, kHotQueryWrite);

  // TODO: Properly merge the results.
  // Used to simulate merging.
  std::unordered_map<uint64_t, uint64_t> merged;
  while (read_result.next()) {
    const uint64_t category = read_result.get<uint64_t>(0);
    const uint64_t stock = read_result.get<uint64_t>(1);
    const auto it = merged.find(category);
    if (it == merged.end()) {
      merged.insert({category, stock});
    } else {
      it->second += stock;
    }
  }
  while (write_result.next()) {
    const uint64_t category = write_result.get<uint64_t>(0);
    const uint64_t stock = write_result.get<uint64_t>(1);
    const auto it = merged.find(category);
    if (it == merged.end()) {
      merged.insert({category, stock});
    } else {
      it->second += stock;
    }
  }

  return merged.size();
}

size_t CirrusImpl::WideExtractImport() {
  // May need to wait.
  const auto latest_inventory = inventory_version_.LatestKnown();
  const auto [inv_waited, _] =
      inventory_version_.WaitUntilAtLeast(latest_inventory);
  // We assume the write store is always up to date. But we may need to wait for
  // the read store.
  // TODO: We need stronger transactional consistency guarantees. This benchmark
  // approach is sloppy. We should add a versioning column and filter for it.
  if (inv_waited) {
    Stats::Local().BumpReadWithPause();
  } else {
    Stats::Local().BumpReadWithoutPause();
  }

  // Pull out the hot data
  auto& write_store = connections_.write();
  nanodbc::execute(write_store, kExtractHot);

  // Import the hot data
  auto& read_store = connections_.read();
  nanodbc::execute(read_store, kCreateImportTable);
  nanodbc::execute(read_store, kTruncateImportTable);
  nanodbc::execute(read_store, GenerateImportQuery(config_->iam_role()));

  // Run the query
  auto result = nanodbc::execute(read_store, kHotQueryReadWithImport);

  size_t num_results = 0;
  while (result.next()) {
    ++num_results;
  }

  return num_results;
}

}  // namespace cirrus
