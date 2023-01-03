#include "cirrus_impl.h"

#include <cstdint>
#include <iostream>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "cirrus/cirrus.h"
#include "cirrus/stats.h"

#define MULTILINE(...) #__VA_ARGS__

namespace {

// Based on a zipfian, theta = 0.9, scale factor 10, workload.
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

static const std::string kRatioHotFullQuery =
    ([](const std::string& hot_id_string) {
      std::stringstream query;
      // clang-format off
      query << "WITH volume AS (";
      query << MULTILINE(
          SELECT
            s_i_id AS l_i_id,
            SUM(s_quantity) AS l_volume
          FROM
            sales
          WHERE
            s_i_id IN
      );
      query << " " << hot_id_string << " ";
      query << "GROUP BY s_i_id) ";
      query << MULTILINE(
        SELECT
          i_id,
          i_stock / l_volume
        FROM
          inventory,
          volume
        WHERE
          i_id = l_i_id
      );
      // clang-format on
      return query.str();
    })(kHotIdString);

// clang-format off
static const std::string kRatioHotMVQuery = MULTILINE(
  SELECT
    i_id,
    i_stock / l_volume
  FROM
    inventory,
    volume_hot_mv
  WHERE
    i_id = l_i_id
);
// clang-format on

// clang-format off
static const std::string kRatioAllFullQuery = MULTILINE(
  WITH volume AS (
    SELECT
      s_i_id AS l_i_id,
      SUM(s_quantity) AS l_volume
    FROM
      sales
    GROUP BY s_i_id
  )
  SELECT
    i_id,
    i_stock / l_volume
  FROM
    inventory,
    volume
  WHERE
    i_id = l_i_id
);

static const std::string kRatioAllMVQuery = MULTILINE(
  SELECT
    i_id,
    i_stock / l_volume
  FROM
    inventory,
    volume_mv
  WHERE
    i_id = l_i_id
);
// clang-format on

std::string GenerateMaintenanceQuery(uint64_t phys_id_lower,
                                     uint64_t phys_id_upper) {
  std::stringstream query;
  // clang-format off
  query << "WITH latest_volumes AS (";
  query << MULTILINE(
    SELECT
      s_i_id AS l_i_id,
      SUM(s_quantity) AS l_volume
    FROM
      sales
    WHERE
      s_i_id IN
  );
  query << " " << kHotIdString;
  query << " AND s_phys_id >= " << phys_id_lower;
  query << " AND s_phys_id <= " << phys_id_upper;
  query << " GROUP BY s_i_id),";
  query << MULTILINE(
    combined AS (
      SELECT * FROM latest_volumes UNION ALL SELECT * FROM volume_hot_mv
    )
    SELECT
      l_i_id,
      SUM(l_volume) AS l_volume
    FROM
      combined
    GROUP BY
      l_i_id
  );
  // clang-format on
  return query.str();
}

static const std::string kUpdateInventory =
    "UPDATE inventory SET i_stock = ?, i_phys_id = ? WHERE i_id = ?";

static const std::string kInsertSales =
    "INSERT INTO sales (s_id, s_datetime, s_i_id, s_quantity, s_price, "
    "s_phys_id) VALUES (?, ?, ?, ?, ?, ?)";

static const std::string kGetMaxSalesPhysId =
    "SELECT MAX(s_phys_id) FROM sales";

}  // namespace

namespace cirrus {

thread_local Connector CirrusImpl::connections_;

std::unique_ptr<Cirrus> Cirrus::Open(std::shared_ptr<CirrusConfig> config,
                                     Strategy strategy) {
  return std::make_unique<CirrusImpl>(std::move(config), strategy);
}

CirrusImpl::CirrusImpl(std::shared_ptr<CirrusConfig> config, Strategy strategy)
    : config_(std::move(config)),
      strategy_(strategy),
      bg_workers_(
          config_->bg_workers(),
          [this]() { EstablishThreadLocalConnections(); },
          []() { Stats::Local().PostToGlobal(); }),
      last_updated_sales_id_(0) {}

std::string CirrusImpl::GetVersion() const { return "0.1.0+dev"; }

void CirrusImpl::EstablishThreadLocalConnections() {
  connections_.Connect(config_);
}

void CirrusImpl::SetUpViews() {
  // clang-format off
  // Computes, for each item, the quantity sold in the most recent sale.
  static const std::string kFullMV = MULTILINE(
    CREATE MATERIALIZED VIEW volume_mv AS
    SELECT
      s_i_id AS l_i_id,
      SUM(s_quantity) AS l_volume
    FROM
      sales
    GROUP BY s_i_id
  );
  // Same as above, but used for popular items only.
  static const std::string kManualHotMV = ([]() {
    std::stringstream query;
    query << "CREATE TABLE volume_hot_mv AS (";
    query << MULTILINE(
      SELECT
        s_i_id AS l_i_id,
        SUM(s_quantity) AS l_volume
      FROM
        sales
      WHERE
        s_i_id IN
    );
    query << " " << kHotIdString;
    query << " GROUP BY s_i_id)";
    return query.str();
  })();
  // clang-format on

  auto& read_store = connections_.read();
  auto& write_store = connections_.write_writer();

  std::cerr << "> Setting up the read store's MV." << std::endl;
  try {
    nanodbc::execute(read_store, kFullMV);
  } catch (nanodbc::database_error& ex) {
    // No-op. If the view already exists, we will get an error. Redshift
    // does not seem to support a "IF NOT EXISTS" clause.
  }
  nanodbc::execute(read_store, "REFRESH MATERIALIZED VIEW volume_mv");

  std::cerr << "> Setting up the write store's MV." << std::endl;
  nanodbc::execute(write_store, "DROP TABLE IF EXISTS volume_hot_mv");
  nanodbc::execute(write_store, kManualHotMV);

  std::cerr << "> Retrieving peak physical ID..." << std::endl;
  auto result = nanodbc::execute(write_store, kGetMaxSalesPhysId);
  result.next();
  last_updated_sales_id_ = result.get<uint64_t>(0);
  std::cerr << "> MV set up complete." << std::endl;
}

void CirrusImpl::NotifyUpdateInventory(NotifyInventoryUpdate inventory) {
  Stats::Local().BumpInventoryNotifications();
  if (strategy_ == Strategy::kAllOnOne) return;

  if (strategy_ == Strategy::kHotPlacementNoMV ||
      strategy_ == Strategy::kHotPlacementWithMV) {
    // Check if the key is hot. If it is, we drop the update! We never modify
    // the hot set in this implementation, so we do not need to take any locks.
    // We ignore the versioning - we use it only for tracking updates actually
    // made to the table in the read store.
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
    stmt.bind(1, &(inventory.i_phys_id), 1);
    stmt.bind(2, &(inventory.i_id), 1);
    nanodbc::execute(stmt);

    // This is a naive approach. Updates currently aren't guaranteed to run in
    // order (unless we only use one background worker).
    inventory_version_.BumpUpdatedTo(inventory.i_phys_id);
  });
}

void CirrusImpl::NotifyInsertSales(NotifySalesInsert sales) {
  Stats::Local().BumpSalesNotifications();
  if (strategy_ == Strategy::kAllOnOne) return;

  if (strategy_ == Strategy::kHotPlacementNoMV ||
      strategy_ == Strategy::kHotPlacementWithMV) {
    // Check if the key is hot. If it is, we drop the update! We never modify
    // the hot set in this implementation, so we do not need to take any locks.
    // We ignore the versioning - we use it only for tracking updates actually
    // made to the table in the read store.
    if (kHotIdSet.count(sales.s_i_id) == 0) {
      Stats::Local().BumpHotSalesDrops();
      return;
    }
  }

  sales_version_.BumpLatestKnown(sales.s_phys_id);
  bg_workers_.SubmitNoWait([this, sales = std::move(sales)]() {
    auto& read_store = connections_.read();
    {
      nanodbc::statement stmt(read_store);
      nanodbc::prepare(stmt, kInsertSales);
      stmt.bind(0, &(sales.s_id), 1);
      stmt.bind(1, &(sales.s_datetime), 1);
      stmt.bind(2, &(sales.s_i_id), 1);
      stmt.bind(3, &(sales.s_quantity), 1);
      stmt.bind(4, &(sales.s_price), 1);
      stmt.bind(5, &(sales.s_phys_id), 1);
      nanodbc::execute(stmt);
    }

    // This is a naive approach. Updates currently aren't guaranteed to run in
    // order (unless we only use one background worker).
    sales_version_.BumpUpdatedTo(sales.s_phys_id);
  });
}

size_t CirrusImpl::RunReportingQuery(uint64_t datetime_start,
                                     uint64_t datetime_end) {
  auto& read_store = connections_.read();
  const std::string query =
      GenerateReportingQuery(datetime_start, datetime_end);
  auto result = nanodbc::execute(read_store, query);
  size_t num_results = 0;
  while (result.next()) {
    ++num_results;
  }
  return num_results;
}

size_t CirrusImpl::RunStockFeatureQuery() {
  switch (strategy_) {
    case Strategy::kAllOnOne:
      return StockFeatureAllOnOne();

    case Strategy::kLatestStreamNoMV:
    case Strategy::kLatestStreamWithMV:
      return StockFeatureLatestStream();

    case Strategy::kHotPlacementNoMV:
    case Strategy::kHotPlacementWithMV:
      return StockFeatureHotPlacement();
  }

  // Should not reach here.
  return 0;
}

std::string CirrusImpl::GenerateReportingQuery(uint64_t datetime_start,
                                               uint64_t datetime_end) const {
  std::stringstream query;
  // clang-format off
  query << MULTILINE(
    SELECT
      i_name,
      i_category,
      SUM(s_price * s_quantity) AS gross_sales
    FROM
      sales,
      inventory
    WHERE
      i_id = s_i_id AND
  );
  // clang-format on
  query << "s_datetime >= " << datetime_start << " AND ";
  query << "s_datetime <= " << datetime_end;
  query << MULTILINE(GROUP BY i_id, i_name, i_category);
  return query.str();
}

uint64_t CirrusImpl::GetMaxDatetime() const {
  auto& connection = connections_.read();
  auto result =
      nanodbc::execute(connection, "SELECT MAX(s_datetime) FROM sales");
  result.next();
  return result.get<uint64_t>(0);
}

// In this strategy we read directly on the write store.
size_t CirrusImpl::StockFeatureAllOnOne() {
  auto& write_store = connections_.write();
  auto all_results = nanodbc::execute(write_store, kRatioAllFullQuery);
  size_t num_results = 0;
  while (all_results.next()) {
    ++num_results;
  }
  return num_results;
}

// In this strategy, we read directly from the read store.
size_t CirrusImpl::StockFeatureLatestStream() {
  const auto latest_sales = sales_version_.LatestKnown();
  const auto latest_inventory = inventory_version_.LatestKnown();

  // TODO: We need stronger transactional consistency guarantees. This benchmark
  // approach is sloppy.
  const auto [sales_waited, _1] = sales_version_.WaitUntilAtLeast(latest_sales);
  const auto [inv_waited, _2] =
      inventory_version_.WaitUntilAtLeast(latest_inventory);
  if (sales_waited || inv_waited) {
    Stats::Local().BumpReadWithPause();
  } else {
    Stats::Local().BumpReadWithoutPause();
  }

  auto& read_store = connections_.read();
  if (StrategyUsesMaterializedView(strategy_)) {
    // Refresh the Redshift MV when it is needed.
    const auto s = std::chrono::steady_clock::now();
    nanodbc::execute(read_store, "REFRESH MATERIALIZED VIEW volume_mv");
    const auto e = std::chrono::steady_clock::now();
    Stats::Local().BumpViewMaintInits();
    std::cerr
        << "> Redshift view refresh "
        << std::chrono::duration_cast<std::chrono::milliseconds>(e - s).count()
        << " ms" << std::endl;
  }

  auto all_results = strategy_ == Strategy::kLatestStreamNoMV
                         ? nanodbc::execute(read_store, kRatioAllFullQuery)
                         : nanodbc::execute(read_store, kRatioAllMVQuery);
  size_t num_results = 0;
  while (all_results.next()) {
    ++num_results;
  }
  return num_results;
}

// This strategy is the federated approach. We run part of the query on each
// system and then merge the results.
size_t CirrusImpl::StockFeatureHotPlacement() {
  // TODO: Ideally we run both queries in parallel.
  auto& read_store = connections_.read();
  auto& write_store = connections_.write();

  // We assume the write store is always up to date. But our query also needs to
  // update the local MV.
  // TODO: We need stronger transactional consistency guarantees. This benchmark
  // approach is sloppy.
  const auto latest_sales = sales_version_.LatestKnown();
  const auto [waited, _] = sales_version_.WaitUntilAtLeast(latest_sales);
  if (waited) {
    Stats::Local().BumpReadWithPause();
  } else {
    Stats::Local().BumpReadWithoutPause();
  }

  if (strategy_ == Strategy::kHotPlacementWithMV) {
    const auto s = std::chrono::steady_clock::now();
    RunWriteStoreMVUpdate();
    const auto e = std::chrono::steady_clock::now();
    std::cerr
        << "> Manual view refresh "
        << std::chrono::duration_cast<std::chrono::milliseconds>(e - s).count()
        << " ms" << std::endl;
  }
  if (StrategyUsesMaterializedView(strategy_)) {
    // Refresh the Redshift MV when it is needed.
    const auto s = std::chrono::steady_clock::now();
    nanodbc::execute(read_store, "REFRESH MATERIALIZED VIEW volume_mv");
    const auto e = std::chrono::steady_clock::now();
    Stats::Local().BumpViewMaintInits();
    std::cerr
        << "> Redshift view refresh "
        << std::chrono::duration_cast<std::chrono::milliseconds>(e - s).count()
        << " ms" << std::endl;
  }

  auto all_results = strategy_ == Strategy::kHotPlacementNoMV
                         ? nanodbc::execute(read_store, kRatioAllFullQuery)
                         : nanodbc::execute(read_store, kRatioAllMVQuery);
  auto hot_results = strategy_ == Strategy::kHotPlacementNoMV
                         ? nanodbc::execute(write_store, kRatioHotFullQuery)
                         : nanodbc::execute(write_store, kRatioHotMVQuery);

  // TODO: Properly merge the results.
  size_t num_results = 0;
  while (all_results.next()) {
    ++num_results;
  }
  while (hot_results.next()) {
    ++num_results;
  }

  return num_results;
}

void CirrusImpl::RunWriteStoreMVUpdate() {
  auto& write_store = connections_.write_writer();
  uint64_t next_max;
  {
    auto result = nanodbc::execute(write_store, kGetMaxSalesPhysId);
    result.next();
    next_max = result.get<uint64_t>(0);
  }

  // No writes since the last time this ran.
  if (next_max == last_updated_sales_id_) return;

  const std::string update_query =
      GenerateMaintenanceQuery(last_updated_sales_id_ + 1, next_max);

  nanodbc::transaction txn(write_store);
  nanodbc::execute(write_store,
                   "CREATE TABLE volume_hot_mv_new AS (" + update_query + ")");
  nanodbc::execute(write_store,
                   "ALTER TABLE volume_hot_mv RENAME TO volume_hot_mv_old");
  nanodbc::execute(write_store,
                   "ALTER TABLE volume_hot_mv_new RENAME TO volume_hot_mv");
  nanodbc::execute(write_store, "DROP TABLE volume_hot_mv_old");
  txn.commit();
  last_updated_sales_id_ = next_max + 1;
  Stats::Local().BumpManualViewMaints();
}

void CirrusImpl::SyncTableVersions() {
  auto& read_store = connections_.read();
  {
    auto result =
        nanodbc::execute(read_store, "SELECT MAX(i_phys_id) FROM inventory;");
    result.next();
    const uint64_t inventory_version = result.get<uint64_t>(0);
    inventory_version_.BumpLatestKnown(inventory_version);
    inventory_version_.BumpUpdatedTo(inventory_version);
  }
  {
    auto result =
        nanodbc::execute(read_store, "SELECT MAX(s_phys_id) FROM sales;");
    result.next();
    const uint64_t sales_version = result.get<uint64_t>(0);
    sales_version_.BumpLatestKnown(sales_version);
    sales_version_.BumpUpdatedTo(sales_version);
  }
}

}  // namespace cirrus
