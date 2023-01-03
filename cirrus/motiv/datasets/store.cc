#include "datasets/store.h"

#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>

namespace cirrus {

StoreDataset::StoreDataset(uint32_t scale_factor)
    : scale_factor_(scale_factor) {}

void StoreDataset::CreateTables(nanodbc::connection& connection,
                                bool for_postgres_extraction) {
  std::stringstream inventory, sales;
  inventory << "CREATE TABLE IF NOT EXISTS inventory";
  inventory << " (i_id BIGINT, i_name TEXT, i_category BIGINT, i_stock BIGINT, "
               "i_price BIGINT,";
  if (for_postgres_extraction) {
    inventory << " i_phys_id BIGSERIAL,";
  } else {
    inventory << " i_phys_id BIGINT,";
  }
  inventory << " PRIMARY KEY (i_id));";

  sales << "CREATE TABLE IF NOT EXISTS sales";
  // NOTE: s_datetime represents a timestamp. For now, we use an integer for
  // simplicity.
  sales << " (s_id BIGINT, s_datetime BIGINT, s_i_id BIGINT, s_quantity "
           "BIGINT, s_price BIGINT,";
  if (for_postgres_extraction) {
    sales << " s_phys_id BIGSERIAL,";
  } else {
    // For Redshift.
    sales << " s_phys_id BIGINT,";
  }
  sales << " PRIMARY KEY (s_id));";

  nanodbc::transaction txn(connection);
  nanodbc::execute(connection, inventory.str());
  nanodbc::execute(connection, sales.str());
  if (for_postgres_extraction) {
    std::stringstream phys_id_index;
    phys_id_index << "CREATE INDEX IF NOT EXISTS sales_phys_id";
    phys_id_index << " ON sales USING btree (s_phys_id);";
    nanodbc::execute(connection, phys_id_index.str());

    // This index helps accelerate the analytical queries that run against
    // PostgreSQL.
    std::stringstream datetime_index;
    datetime_index << "CREATE INDEX IF NOT EXISTS sales_datetime";
    datetime_index << " ON sales USING btree (s_datetime);";
    nanodbc::execute(connection, datetime_index.str());

    std::stringstream inventory_phys_id_index;
    inventory_phys_id_index << "CREATE INDEX IF NOT EXISTS inventory_phys_id";
    inventory_phys_id_index << " ON inventory USING btree (i_phys_id);";
    nanodbc::execute(connection, inventory_phys_id_index.str());
  }
  txn.commit();
}

void StoreDataset::DropAll(nanodbc::connection& connection) {
  nanodbc::transaction txn(connection);
  nanodbc::execute(connection, "DROP TABLE IF EXISTS inventory");
  nanodbc::execute(connection, "DROP TABLE IF EXISTS sales");
  txn.commit();
}

uint64_t StoreDataset::SalesBaseCardinality(uint32_t scale_factor) {
  return scale_factor * 16'000'000ULL;
}

uint64_t StoreDataset::InventoryBaseCardinality(uint32_t scale_factor) {
  return scale_factor * 1'000'000ULL;
}

void StoreDataset::GenerateAndLoad(nanodbc::connection& connection,
                                   uint32_t seed) {
  // inventory:  sf * 100,000
  // sales:      sf * 1,000,000
  //
  // categories: 10
  // datetime:   monotonically increasing; uniformly spaced gaps of 1-10

  const uint64_t batch_size = 10000ULL;
  nanodbc::statement stmt(connection);
  std::mt19937 prng(seed);

  std::stringstream inventory_builder, sales_builder;
  inventory_builder << "INSERT INTO inventory";
  inventory_builder << " (i_id, i_name, i_category, i_stock, i_price) VALUES "
                       "(?, ?, ?, ?, ?);";
  sales_builder << "INSERT INTO sales";
  sales_builder << " (s_id, s_datetime, s_i_id, s_quantity, s_price) VALUES "
                   "(?, ?, ?, ?, ?);";

  const std::string inventory = inventory_builder.str();
  const std::string sales = sales_builder.str();

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
    nanodbc::prepare(stmt, inventory);
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

  const auto handle_inventory = [&](auto inventory_row) {
    auto [id, name, category, stock, price, phys_id] = inventory_row;
    i_id.emplace_back(id);
    i_name.emplace_back(std::move(name));
    i_category.emplace_back(category);
    i_stock.emplace_back(stock);
    i_price.emplace_back(price);
    // We rely on PostgreSQL to automatically choose the physical ID. So it is
    // unused here.

    if (i_id.size() >= batch_size) {
      write_inventory_batch();
    }
  };

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
    nanodbc::prepare(stmt, sales);
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

  const auto handle_sales = [&](auto sales_row) {
    auto [id, datetime, i_id, quantity, price, phys_id] = sales_row;
    s_id.emplace_back(id);
    s_datetime.emplace_back(datetime);
    s_i_id.emplace_back(i_id);
    s_quantity.emplace_back(quantity);
    s_price.emplace_back(price);
    // We rely on PostgreSQL to automatically choose the physical ID. So it is
    // unused here.

    if (s_id.size() >= batch_size) {
      write_sales_batch();
    }
  };

  nanodbc::transaction txn(connection);
  GenerateData(scale_factor_, seed, handle_inventory, handle_sales);
  if (!i_id.empty()) {
    write_inventory_batch();
  }
  if (!s_id.empty()) {
    write_sales_batch();
  }
  txn.commit();
}

void StoreDataset::GenerateDataFiles(std::filesystem::path out, uint32_t seed) {
  std::ofstream inventory(out / "inventory.tbl");
  std::ofstream sales(out / "sales.tbl");

  const auto handle_inventory = [&](auto inventory_row) {
    auto [id, name, category, stock, price, phys_id] = inventory_row;
    inventory << id << "|";
    inventory << name << "|";
    inventory << category << "|";
    inventory << stock << "|";
    inventory << price << "|";
    inventory << phys_id << std::endl;
  };

  const auto handle_sales = [&](auto sales_row) {
    auto [id, datetime, i_id, quantity, price, phys_id] = sales_row;
    sales << id << "|";
    sales << datetime << "|";
    sales << i_id << "|";
    sales << quantity << "|";
    sales << price << "|";
    sales << phys_id << std::endl;
  };

  GenerateData(scale_factor_, seed, handle_inventory, handle_sales);
}

void StoreDataset::GenerateData(uint32_t scale_factor, uint32_t seed,
                                const InventoryCallback& handle_inventory,
                                const SalesCallback& handle_sales) {
  // inventory:  sf * 100,000
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

  // Generate inventory.
  for (uint64_t id = 0; id < inventory_cardinality; ++id) {
    handle_inventory(std::make_tuple(id, "I" + std::to_string(id),
                                     category_dist(prng), stock_dist(prng),
                                     price_dist(prng), id));
  }

  // Generate sales.
  uint64_t last_datetime = 1;
  for (uint64_t id = 0; id < sales_cardinality; ++id) {
    uint64_t next_datetime = last_datetime + datetime_gap_dist(prng);
    handle_sales(std::make_tuple(id, next_datetime, item_id_dist(prng),
                                 quantity_dist(prng), price_dist(prng),
                                 /*s_phys_id=*/id));
    last_datetime = next_datetime;
  }
}

void StoreDataset::UpdateMaxStats(nanodbc::connection& connection) {
  auto result =
      nanodbc::execute(connection, "SELECT MAX(s_datetime) FROM sales");
  result.next();
  max_s_datetime_ = result.get<uint64_t>(0);

  result = nanodbc::execute(connection, "SELECT MAX(i_id) FROM inventory");
  result.next();
  max_i_id_ = result.get<uint64_t>(0);
}

uint64_t StoreDataset::MaxDatetime() const { return max_s_datetime_; }
uint64_t StoreDataset::MaxId() const { return max_i_id_; }

void StoreDataset::DropWorkloadGeneratedRecords(
    nanodbc::connection& connection) {
  // The generator generates IDs sequentially in the range `[0, num_sales)`.
  const uint64_t num_sales = SalesBaseCardinality(scale_factor_);
  nanodbc::execute(connection, "DELETE FROM sales WHERE s_id >= " +
                                   std::to_string(num_sales));
  // Ideally we reset the item counts in `inventory` too, but this is trickier
  // to do.
}

void StoreDataset::ResetPhysIdSequence(nanodbc::connection& connection) {
  // Makes sure that newly inserted rows have a `phys_id` greater than all
  // previous rows. This change is used to extract new rows.
  std::stringstream reset_cmd1;
  reset_cmd1 << "ALTER SEQUENCE sales_s_phys_id_seq RESTART WITH ";
  reset_cmd1 << (SalesBaseCardinality(scale_factor_) + 1);
  nanodbc::execute(connection, reset_cmd1.str());

  uint64_t max_i_phys_id = 0;
  {
    auto result =
        nanodbc::execute(connection, "SELECT MAX(i_phys_id) FROM inventory");
    result.next();
    max_i_phys_id = result.get<uint64_t>(0);
  }
  std::stringstream reset_cmd2;
  reset_cmd2 << "ALTER SEQUENCE inventory_i_phys_id_seq RESTART WITH ";
  reset_cmd2 << (max_i_phys_id + 1);
  nanodbc::execute(connection, reset_cmd2.str());
}

}  // namespace cirrus
