#include "store.h"

#include <iostream>
#include <random>
#include <string>

StoreDataset::StoreDataset(nanodbc::connection& connection)
    : connection_(connection) {}

void StoreDataset::CreateTables() {
  nanodbc::transaction txn(connection_);
  nanodbc::execute(
      connection_,
      "CREATE TABLE IF NOT EXISTS inventory (i_id INT, i_name TEXT, "
      "i_category INT, i_stock INT, i_price INT)");
  // NOTE: s_datetime represents a timestamp. For now, we use an integer for
  // simplicity.
  nanodbc::execute(
      connection_,
      "CREATE TABLE IF NOT EXISTS sales (s_id INT, s_datetime INT, s_i_id INT, "
      "s_quantity INT, s_price INT)");
  txn.commit();
}

void StoreDataset::LoadSimple() {
  nanodbc::statement insert_inventory(connection_);
  nanodbc::prepare(insert_inventory,
                   "INSERT INTO inventory (i_id, i_name, i_category, i_stock, "
                   "i_price) VALUES (?, ?, ?, ?, ?);");
  // Inventory columns (for batching).
  std::vector<uint64_t> i_id = {0};
  std::vector<std::string> i_name = {"test"};
  std::vector<uint64_t> i_category = {0};
  std::vector<uint64_t> i_stock = {0};
  std::vector<uint64_t> i_price = {0};

  insert_inventory.bind(0, i_id.data(), i_id.size());
  insert_inventory.bind_strings(1, i_name);
  insert_inventory.bind(2, i_category.data(), i_category.size());
  insert_inventory.bind(3, i_stock.data(), i_stock.size());
  insert_inventory.bind(4, i_price.data(), i_price.size());
  nanodbc::execute(insert_inventory, i_id.size());
}

void StoreDataset::LoadData(uint32_t scale_factor, uint32_t seed) {
  // inventory:  sf * 10,000
  // sales:      sf * 1,000,000
  //
  // categories: 10
  // datetime:   monotonically increasing; uniformly spaced gaps of 1-10

  const uint64_t inventory_cardinality = InventoryBaseCardinality(scale_factor);
  const uint64_t sales_cardinality = SalesBaseCardinality(scale_factor);
  const uint64_t category_cardinality = 3;
  const uint64_t max_stock = 10'000ULL;
  const uint64_t max_price = 2000ULL;
  const uint64_t max_quantity = 20ULL;
  const uint64_t max_datetime_gap = 10ULL;
  const uint64_t batch_size = 10000ULL;

  using dist = std::uniform_int_distribution<uint64_t>;
  dist category_dist(0, category_cardinality - 1);
  dist stock_dist(0, max_stock);
  dist price_dist(1, max_price);
  dist quantity_dist(1, max_quantity);
  dist datetime_gap_dist(1, max_datetime_gap);
  dist item_id_dist(0, inventory_cardinality - 1);

  nanodbc::transaction txn(connection_);
  nanodbc::statement stmt(connection_);

  std::mt19937 prng(seed);

  // Inventory columns (for batching).
  std::vector<uint64_t> i_id;
  std::vector<std::string> i_name;
  std::vector<uint64_t> i_category;
  std::vector<uint64_t> i_stock;
  std::vector<uint64_t> i_price;

  i_id.reserve(batch_size);
  i_name.reserve(batch_size);
  i_category.reserve(batch_size);
  i_stock.reserve(batch_size);
  i_price.reserve(batch_size);

  const auto write_inventory_batch = [&]() {
    nanodbc::prepare(
        stmt,
        "INSERT INTO inventory (i_id, i_name, i_category, i_stock, "
        "i_price) VALUES (?, ?, ?, ?, ?);");
    stmt.bind(0, i_id.data(), i_id.size());
    stmt.bind_strings(1, i_name);
    stmt.bind(2, i_category.data(), i_category.size());
    stmt.bind(3, i_stock.data(), i_stock.size());
    stmt.bind(4, i_price.data(), i_price.size());
    nanodbc::execute(stmt, i_id.size());

    i_id.clear();
    i_name.clear();
    i_category.clear();
    i_stock.clear();
    i_price.clear();
  };

  // Generate inventory.
  std::cerr << "Loading inventory..." << std::endl;
  for (uint64_t id = 0; id < inventory_cardinality; ++id) {
    i_id.emplace_back(id);
    i_name.emplace_back("I" + std::to_string(id));
    i_category.emplace_back(category_dist(prng));
    i_stock.emplace_back(stock_dist(prng));
    i_price.emplace_back(price_dist(prng));

    if (i_id.size() >= batch_size) {
      write_inventory_batch();
    }
  }
  if (i_id.size() > 0) {
    write_inventory_batch();
  }

  // Sales columns (for batching).
  std::vector<uint64_t> s_id;
  std::vector<uint64_t> s_datetime;
  std::vector<uint64_t> s_i_id;
  std::vector<uint64_t> s_quantity;
  std::vector<uint64_t> s_price;

  s_id.reserve(batch_size);
  s_datetime.reserve(batch_size);
  s_i_id.reserve(batch_size);
  s_quantity.reserve(batch_size);
  s_price.reserve(batch_size);

  const auto write_sales_batch = [&]() {
    nanodbc::prepare(stmt,
                     "INSERT INTO sales (s_id, s_datetime, s_i_id, "
                     "s_quantity, s_price) VALUES (?, ?, ?, ?, ?);");
    stmt.bind(0, s_id.data(), s_id.size());
    stmt.bind(1, s_datetime.data(), s_datetime.size());
    stmt.bind(2, s_i_id.data(), s_i_id.size());
    stmt.bind(3, s_quantity.data(), s_quantity.size());
    stmt.bind(4, s_price.data(), s_price.size());
    nanodbc::execute(stmt, s_id.size());

    s_id.clear();
    s_datetime.clear();
    s_i_id.clear();
    s_quantity.clear();
    s_price.clear();
  };

  // Generate sales.
  std::cerr << "Loading sales..." << std::endl;
  uint64_t last_datetime = 1;
  for (uint64_t id = 0; id < sales_cardinality; ++id) {
    uint64_t next_datetime = last_datetime + datetime_gap_dist(prng);
    s_id.emplace_back(id);
    s_datetime.emplace_back(next_datetime);
    s_i_id.emplace_back(item_id_dist(prng));
    s_quantity.emplace_back(quantity_dist(prng));
    s_price.emplace_back(price_dist(prng));
    last_datetime = next_datetime;

    if (s_id.size() >= batch_size) {
      write_sales_batch();
    }
  }
  if (s_id.size() > 0) {
    write_sales_batch();
  }

  txn.commit();
  std::cerr << "Done!" << std::endl;
}

void StoreDataset::DropAll() {
  nanodbc::transaction txn(connection_);
  nanodbc::execute(connection_, "DROP MATERIALIZED VIEW IF EXISTS mv");
  nanodbc::execute(connection_, "DROP TABLE IF EXISTS mv_table");
  nanodbc::execute(connection_, "DROP TABLE IF EXISTS mv_delta_table");
  nanodbc::execute(connection_, "DROP TABLE IF EXISTS inventory");
  nanodbc::execute(connection_, "DROP TABLE IF EXISTS sales");
  txn.commit();
}

uint64_t StoreDataset::GetMaxDatetime() const {
  auto result = nanodbc::execute(connection_, "SELECT MAX(s_datetime) FROM sales;");
  result.next();
  return result.get<uint64_t>(0);
}

uint64_t StoreDataset::SalesBaseCardinality(uint32_t scale_factor) const {
  return 1'000'000ULL;
}

uint64_t StoreDataset::InventoryBaseCardinality(uint32_t scale_factor) const {
  return scale_factor * 100'000ULL;
}
