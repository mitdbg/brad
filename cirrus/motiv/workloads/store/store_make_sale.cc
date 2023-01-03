#include <chrono>
#include <iostream>
#include <random>

#include "../state.h"
#include "cirrus/stats.h"
#include "store.h"
#include "utils/sf.h"

namespace cirrus {

MakeSale::MakeSale(MakeSaleOptions options, nanodbc::connection connection,
                   std::shared_ptr<Cirrus> cirrus,
                   std::shared_ptr<BenchmarkState> state)
    : WorkloadBase(std::move(state)),
      options_(options),
      num_txns_(0),
      num_aborts_(0),
      next_id_(0),
      next_datetime_(options.max_s_datetime),
      cirrus_(std::move(cirrus)),
      connection_(std::move(connection)),
      // We assume IDs are densely assigned (which should be the case for our
      // generated dataset).
      item_id_chooser_(options.max_i_id, options.theta) {
  Start();
}

uint64_t MakeSale::NumTxnsRun() const { return num_txns_; }

uint64_t MakeSale::NumAborts() const { return num_aborts_; }

void MakeSale::RunImpl() {
  const uint64_t max_id = options_.max_i_id;

  std::mt19937 prng(42 ^ options_.client_id);
  std::uniform_int_distribution<uint64_t> num_items(1, 3);
  std::uniform_int_distribution<uint64_t> datetime_gap(1, 10);

  // NOTE: This is PostgreSQL-specific syntax. We need serializable isolation
  // because this transaction simulates a purchase transaction and we want to
  // ensure we only sell items that are available.
  nanodbc::execute(connection_,
                   "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
                   "SERIALIZABLE");

  const std::string select_inventory =
      "SELECT i_stock, i_price FROM inventory WHERE i_id = ?;";
  const std::string update_inventory =
      "UPDATE inventory SET i_stock = ?, i_phys_id = DEFAULT WHERE i_id = ? "
      "RETURNING i_id, i_stock, i_phys_id;";
  const std::string insert_sales =
      "INSERT INTO sales (s_id, s_datetime, s_i_id, s_quantity, s_price) "
      "VALUES (?, ?, ?, ?, ?) RETURNING s_id, s_datetime, s_i_id, s_quantity, "
      "s_price, s_phys_id;";

  const auto run_txn = [&]() {
    // For simplicity, we buy one item.
    // Item IDs are chosen from a skewed distribution; note that doing this will
    // introduce contention when there are many clients.
    const uint64_t id = item_id_chooser_.Next(prng);
    const uint64_t quantity = num_items(prng);

    nanodbc::transaction txn(connection_);
    nanodbc::statement stmt(connection_);

    // Check the item we want to buy to make sure there is a sufficient
    // quantity.
    nanodbc::prepare(stmt, select_inventory);
    stmt.bind(0, &id, 1);
    // TODO: This assumes the item always exists.
    auto result = nanodbc::execute(stmt);
    result.next();
    const uint64_t i_stock = result.get<uint64_t>(0);
    const uint64_t i_price = result.get<uint64_t>(1);
    if (i_stock < quantity) {
      // Not enough stock to make a sale.
      txn.commit();
      return;
    }

    // Make the purchase.
    const uint64_t new_quantity = i_stock - quantity;
    nanodbc::prepare(stmt, update_inventory);
    stmt.bind(0, &new_quantity, 1);
    stmt.bind(1, &id, 1);
    result = nanodbc::execute(stmt);
    result.next();
    NotifyInventoryUpdate notify_inv;
    notify_inv.i_id = result.get<uint64_t>(0);
    notify_inv.i_stock = result.get<uint64_t>(1);
    notify_inv.i_phys_id = result.get<uint64_t>(2);
    txn.commit();

    cirrus_->NotifyUpdateInventory(notify_inv);

    // Insert into sales. This does not need to run as part of the transaction.
    nanodbc::prepare(stmt, insert_sales);
    const uint64_t datetime = next_datetime_ + datetime_gap(prng);
    next_datetime_ = datetime;
    const uint64_t sale_id = GenerateSaleId();
    stmt.bind(0, &sale_id, 1);
    stmt.bind(1, &datetime, 1);
    stmt.bind(2, &id, 1);
    stmt.bind(3, &quantity, 1);  // quantity purchased
    stmt.bind(4, &i_price, 2);   // purchase price
    result = nanodbc::execute(stmt);
    result.next();
    NotifySalesInsert notify_sales;
    notify_sales.s_id = result.get<uint64_t>(0);
    notify_sales.s_datetime = result.get<uint64_t>(1);
    notify_sales.s_i_id = result.get<uint64_t>(2);
    notify_sales.s_quantity = result.get<uint64_t>(3);
    notify_sales.s_price = result.get<uint64_t>(4);
    notify_sales.s_phys_id = result.get<uint64_t>(5);

    cirrus_->NotifyInsertSales(notify_sales);
  };

  for (uint64_t i = 0; i < options_.num_warmup; ++i) {
    while (true) {
      try {
        run_txn();
        break;
      } catch (nanodbc::database_error& ex) {
        // Forced abort. We will retry.
        // TODO: Aborts via an exception are not the best idea, according to the
        // discussion in "Opportunities for Optimism in Contended Main-Memory
        // Multicore Transactions (VLDB 2020)."
      }
    }
  }

  WarmedUpAndReadyToRun();

  while (KeepRunning()) {
    const auto start = std::chrono::steady_clock::now();
    while (true) {
      try {
        run_txn();
        break;
      } catch (nanodbc::database_error& ex) {
        // Forced abort. We will retry.
        // TODO: Aborts via an exception are not the best idea, according to the
        // discussion in "Opportunities for Optimism in Contended Main-Memory
        // Multicore Transactions (VLDB 2020)."
        ++num_aborts_;
      }
    }
    const auto end = std::chrono::steady_clock::now();
    ++num_txns_;
    AddLatency(end - start);
  }

  Stats::Local().PostToGlobal();
}

uint64_t MakeSale::GenerateSaleId() {
  // To generate unique IDs without clashing with other transactions, we reserve
  // the most significant byte for the client ID.
  const uint64_t id =
      (((static_cast<uint64_t>(options_.client_id) + 1) & 0xFF) << 56) |
      next_id_;
  ++next_id_;
  return id;
}

}  // namespace cirrus
