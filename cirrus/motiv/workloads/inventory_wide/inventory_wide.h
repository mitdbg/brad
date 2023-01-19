#pragma once

#include <nanodbc/nanodbc.h>

#include <cstdint>
#include <memory>
#include <random>
#include <thread>
#include <utility>

#include "../latency_manager.h"
#include "../state.h"
#include "../workload_base.h"
#include "cirrus/cirrus.h"
#include "ycsbr/zipfian_chooser.h"

namespace cirrus {

struct CategoryStockOptions {
  uint32_t client_id;
  uint32_t scale_factor;
  uint64_t num_warmup;
};

// An analytical query.
class CategoryStock : public WorkloadBase {
 public:
  CategoryStock(CategoryStockOptions options, std::shared_ptr<Cirrus> cirrus,
                std::shared_ptr<BenchmarkState> state);
  virtual ~CategoryStock() = default;

  uint64_t NumReportsRun() const;

 private:
  virtual void RunImpl() override;

  CategoryStockOptions options_;

  uint64_t num_reports_run_;

  std::shared_ptr<Cirrus> cirrus_;
  mutable std::mt19937 prng_;
};

struct MakeSaleOptions {
  uint32_t client_id;
  uint32_t scale_factor;
  uint64_t num_warmup;
  uint64_t max_i_id;
  // Configures workload skew.
  double theta;
};

// Runs sales transactions.
class InvMakeSale : public WorkloadBase {
 public:
  // `connection` represents a connection to the write store.
  InvMakeSale(MakeSaleOptions options, nanodbc::connection connection,
              std::shared_ptr<Cirrus> cirrus,
              std::shared_ptr<BenchmarkState> state);
  virtual ~InvMakeSale() = default;

  uint64_t NumTxnsRun() const;
  uint64_t NumAborts() const;

 private:
  virtual void RunImpl() override;

  MakeSaleOptions options_;

  uint64_t num_txns_;
  uint64_t num_aborts_;

  std::shared_ptr<Cirrus> cirrus_;
  mutable nanodbc::connection connection_;
  ycsbr::gen::ScatteredZipfianChooser item_id_chooser_;
};

// Implements an ETL of the inventory table.
// This workload is hardcoded to use AWS S3 for data transfer.
class InvETL : public WorkloadBase {
 public:
  // The ETL will run every `period` milliseconds.
  InvETL(uint32_t scale_factor, std::chrono::milliseconds period,
         nanodbc::connection source, std::shared_ptr<Cirrus> cirrus,
         std::shared_ptr<BenchmarkState> state);
  virtual ~InvETL() = default;

  uint64_t NumRuns() const;

 private:
  std::string GenerateExtractQuery(uint64_t seq, uint64_t max_seq) const;

  virtual void RunImpl() override;

  uint64_t num_runs_;
  uint32_t scale_factor_;
  uint64_t synced_phys_id_;
  uint64_t sequence_number_;
  std::chrono::milliseconds period_;
  std::chrono::steady_clock::time_point run_next_;
  mutable nanodbc::connection source_;
  std::shared_ptr<Cirrus> cirrus_;
};

}  // namespace cirrus
