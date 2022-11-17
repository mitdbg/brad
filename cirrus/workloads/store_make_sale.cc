#include <random>

#include "store.h"
#include "utils/connection.h"

MakeSale::MakeSale(uint64_t num_warmup, uint32_t seed,
                   std::shared_ptr<BenchmarkState> state)
    : WorkloadBase(std::move(state)),
      num_warmup_(num_warmup),
      num_txns_(0),
      seed_(seed),
      connection_(utils::GetConnection()) {}

uint64_t MakeSale::NumTxnsRun() const { return num_txns_; }

void MakeSale::RunImpl() {
  const uint64_t max_id = GetMaxItemId();

  std::mt19937 prng(seed_);
  // TODO: We should have a skewed workload.
  std::uniform_int_distribution<uint64_t> item_id(0, max_id);
  std::uniform_int_distribution<uint32_t> num_items(1, 3);

  // NOTE: This is PostgreSQL-specific syntax.
  nanodbc::execute(connection_,
                   "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
                   "SERIALIZABLE");

  const auto run_txn = [&]() {
    // For simplicity, we buy one item.
    const uint64_t id = item_id(prng);
    const uint32_t quantity = num_items(prng);

    nanodbc::transaction txn(connection_);
    nanodbc::statement stmt(connection_);

    // Check the item we want to buy to make sure there is a sufficient
    // quantity.
    nanodbc::prepare(stmt,
                     "SELECT i_quantity FROM i_inventory WHERE i_id = ?;");
    stmt.bind(0, &id, 1);
    auto result = nanodbc::execute(stmt);
    result.next();
    const uint32_t quantity_left = result.get<uint32_t>(0);
    if (quantity_left < quantity) {
      txn.commit();
      return;
    }

    // Make the purchase.
    const uint64_t new_quantity = quantity_left - quantity;
    nanodbc::prepare(stmt,
                     "UPDATE i_inventory SET i_quantity = ? WHERE i_id = ?;");
    stmt.bind(0, &new_quantity, 1);
    stmt.bind(1, &id, 1);
    nanodbc::execute(stmt);
    txn.commit();
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

uint64_t MakeSale::GetMaxItemId() {
  auto result =
      nanodbc::execute(connection_, "SELECT MAX(i_id) FROM inventory;");
  result.next();
  return result.get<uint64_t>(0);
}
