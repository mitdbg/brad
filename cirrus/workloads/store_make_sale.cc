#include <chrono>
#include <iostream>
#include <random>

#include "store.h"
#include "utils/backoff_manager.h"
#include "utils/connection.h"
#include "utils/sf.h"

MakeSale::MakeSale(uint32_t scale_factor, uint64_t num_warmup,
                   uint32_t client_id, std::shared_ptr<BenchmarkState> state)
    : WorkloadBase(std::move(state)),
      num_warmup_(num_warmup),
      num_txns_(0),
      num_aborts_(0),
      scale_factor_(scale_factor),
      client_id_(client_id),
      next_id_(0),
      next_datetime_(0),
      connection_(utils::GetConnection()) {
  Start();
}

uint64_t MakeSale::NumTxnsRun() const { return num_txns_; }

uint64_t MakeSale::NumAborts() const { return num_aborts_; }

void MakeSale::RunImpl() {
  const uint64_t max_id = GetMaxItemId();
  next_datetime_ = GetMaxSaleDatetime();

  std::mt19937 prng(42 ^ client_id_);
  // TODO: We should have a skewed workload.
  std::uniform_int_distribution<uint64_t> item_id(0, max_id);
  std::uniform_int_distribution<uint64_t> num_items(1, 3);
  std::uniform_int_distribution<uint64_t> datetime_gap(1, 10);

  // NOTE: This is PostgreSQL-specific syntax. We need serializable isolation
  // because this transaction simulates a purchase transaction and we want to
  // ensure we only sell items that are available.
  nanodbc::execute(connection_,
                   "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
                   "SERIALIZABLE");

  const std::string select_inventory =
      "SELECT i_stock, i_price FROM inventory_" +
      PaddedScaleFactor(scale_factor_) + " WHERE i_id = ?;";
  const std::string update_inventory = "UPDATE inventory_" +
                                       PaddedScaleFactor(scale_factor_) +
                                       " SET i_stock = ? WHERE i_id = ?;";
  const std::string insert_sales =
      "INSERT INTO sales_" + PaddedScaleFactor(scale_factor_) +
      " (s_id, s_datetime, s_i_id, s_quantity, s_price) VALUES (?, ?, ?, ?, ?)";

  const auto run_txn = [&]() {
    // For simplicity, we buy one item.
    const uint64_t id = item_id(prng);
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
    nanodbc::execute(stmt);
    txn.commit();

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
    nanodbc::execute(stmt);
  };

  for (uint64_t i = 0; i < num_warmup_; ++i) {
    while (true) {
      utils::BackoffManager backoff;
      try {
        run_txn();
        break;
      } catch (nanodbc::database_error& ex) {
        // Forced abort. We will retry.
        // TODO: Aborts via an exception are not the best idea, according to the
        // discussion in "Opportunities for Optimism in Contended Main-Memory
        // Multicore Transactions (VLDB 2020)."
        backoff.Wait();
      }
    }
  }

  WarmedUpAndReadyToRun();

  while (KeepRunning()) {
    const auto start = std::chrono::steady_clock::now();
    while (true) {
      utils::BackoffManager backoff;
      try {
        run_txn();
        break;
      } catch (nanodbc::database_error& ex) {
        // Forced abort. We will retry.
        // TODO: Aborts via an exception are not the best idea, according to the
        // discussion in "Opportunities for Optimism in Contended Main-Memory
        // Multicore Transactions (VLDB 2020)."
        ++num_aborts_;
        backoff.Wait();
      }
    }
    const auto end = std::chrono::steady_clock::now();
    ++num_txns_;
    AddLatency(end - start);
  }
}

uint64_t MakeSale::GetMaxItemId() const {
  auto result =
      nanodbc::execute(connection_, "SELECT MAX(i_id) FROM inventory_" +
                                        PaddedScaleFactor(scale_factor_) + ";");
  result.next();
  return result.get<uint64_t>(0);
}

uint64_t MakeSale::GenerateSaleId() {
  // To generate unique IDs without clashing with other transactions, we reserve
  // the most significant byte for the client ID.
  const uint64_t id =
      (((static_cast<uint64_t>(client_id_) + 1) & 0xFF) << 56) | next_id_;
  ++next_id_;
  return id;
}

uint64_t MakeSale::GetMaxSaleDatetime() const {
  auto result =
      nanodbc::execute(connection_, "SELECT MAX(s_datetime) FROM sales_" +
                                        PaddedScaleFactor(scale_factor_));
  result.next();
  return result.get<uint64_t>(0);
}
