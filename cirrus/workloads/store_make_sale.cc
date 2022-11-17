#include <random>

#include "store.h"
#include "utils/connection.h"
#include "utils/sf.h"

MakeSale::MakeSale(uint32_t scale_factor, uint64_t num_warmup, uint32_t client_id,
                   std::shared_ptr<BenchmarkState> state)
    : WorkloadBase(std::move(state)),
      num_warmup_(num_warmup),
      num_txns_(0),
      scale_factor_(scale_factor),
      client_id_(client_id),
      next_id_(0),
      connection_(utils::GetConnection()) {}

uint64_t MakeSale::NumTxnsRun() const { return num_txns_; }

void MakeSale::RunImpl() {
  const uint64_t max_id = GetMaxItemId();

  std::mt19937 prng(client_id_);
  // TODO: We should have a skewed workload.
  std::uniform_int_distribution<uint64_t> item_id(0, max_id);
  std::uniform_int_distribution<uint32_t> num_items(1, 3);

  // NOTE: This is PostgreSQL-specific syntax. We need serializable isolation
  // because this transaction simulates a purchase transaction and we want to
  // ensure we only sell items that are available.
  nanodbc::execute(connection_,
                   "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
                   "SERIALIZABLE");

  const std::string select_inventory =
      "SELECT i_quantity, i_price FROM inventory_" +
      PaddedScaleFactor(scale_factor_) + " WHERE i_id = ?;";
  const std::string update_inventory = "UPDATE inventory_" +
                                       PaddedScaleFactor(scale_factor_) +
                                       " SET i_quantity = ? WHERE i_id = ?;";
  const std::string insert_sales =
      "INSERT INTO sales_" + PaddedScaleFactor(scale_factor_) +
      " (s_id, s_datetime, s_i_id, s_quantity, s_price) VALUES (?, ?, ?, ?, ?)";

  const auto run_txn = [&]() {
    // For simplicity, we buy one item.
    const uint64_t id = item_id(prng);
    const uint32_t quantity = num_items(prng);

    nanodbc::transaction txn(connection_);
    nanodbc::statement stmt(connection_);

    // Check the item we want to buy to make sure there is a sufficient
    // quantity.
    nanodbc::prepare(stmt, select_inventory);
    stmt.bind(0, &id, 1);
    auto result = nanodbc::execute(stmt);
    result.next();
    const uint32_t i_quantity = result.get<uint32_t>(0);
    const uint32_t i_price = result.get<uint32_t>(1);
    if (i_quantity < quantity) {
      // Not enough stock to make a sale.
      txn.commit();
      return;
    }

    // Make the purchase.
    const uint64_t new_quantity = i_quantity - quantity;
    nanodbc::prepare(stmt, update_inventory);
    stmt.bind(0, &new_quantity, 1);
    stmt.bind(1, &id, 1);
    nanodbc::execute(stmt);
    txn.commit();

    // Insert into sales. This does not need to run as part of the transaction.
    nanodbc::prepare(stmt, insert_sales);
    const uint32_t datetime = 1; // TODO: Generate something more plausible.
    const uint32_t sale_id = GenerateSaleId();
    stmt.bind(0, &sale_id, 1);
    stmt.bind(1, &datetime, 1);
    stmt.bind(2, &id, 1);
    stmt.bind(3, &quantity, 1);  // quantity purchased
    stmt.bind(4, &i_price, 2);   // purchase price
    nanodbc::execute(stmt);
  };

  for (uint64_t i = 0; i < num_warmup_; ++i) {
    run_txn();
  }

  WarmedUpAndReadyToRun();

  while (KeepRunning()) {
    run_txn();
    ++num_txns_;
  }
}

uint64_t MakeSale::GetMaxItemId() const {
  auto result =
      nanodbc::execute(connection_, "SELECT MAX(i_id) FROM inventory;");
  result.next();
  return result.get<uint64_t>(0);
}

uint32_t MakeSale::GenerateSaleId() {
  // To generate unique IDs without clashing with other transactions, we reserve
  // the most significant byte for the client ID.
  const uint32_t id = ((client_id_ & 0xFF) << 28) | next_id_;
  ++next_id_;
  return id;
}
