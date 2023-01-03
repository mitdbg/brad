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

struct SalesReportingOptions {
  uint32_t client_id;
  uint32_t scale_factor;
  uint64_t num_warmup;
};

class SalesReporting : public WorkloadBase {
 public:
  SalesReporting(SalesReportingOptions options, std::shared_ptr<Cirrus> cirrus,
                 std::shared_ptr<BenchmarkState> state);
  virtual ~SalesReporting() = default;

  uint64_t NumReportsRun() const;

 private:
  virtual void RunImpl() override;
  std::pair<uint64_t, uint64_t> GenerateDatetimeRange() const;

  SalesReportingOptions options_;

  uint64_t max_datetime_;
  uint64_t num_reports_run_;

  std::shared_ptr<Cirrus> cirrus_;
  mutable std::mt19937 prng_;
};

struct MakeSaleOptions {
  uint32_t client_id;
  uint32_t scale_factor;
  uint64_t num_warmup;
  uint64_t max_s_datetime;
  uint64_t max_i_id;
  // Configures workload skew.
  double theta;
};

class MakeSale : public WorkloadBase {
 public:
  // `connection` represents a connection to the write store.
  MakeSale(MakeSaleOptions options, nanodbc::connection connection,
           std::shared_ptr<Cirrus> cirrus,
           std::shared_ptr<BenchmarkState> state);
  virtual ~MakeSale() = default;

  uint64_t NumTxnsRun() const;
  uint64_t NumAborts() const;

 private:
  virtual void RunImpl() override;
  uint64_t GenerateSaleId();

  MakeSaleOptions options_;

  uint64_t num_txns_;
  uint64_t num_aborts_;

  uint64_t next_id_;
  uint64_t next_datetime_;

  std::shared_ptr<Cirrus> cirrus_;
  mutable nanodbc::connection connection_;
  ycsbr::gen::ScatteredZipfianChooser item_id_chooser_;
};

struct StockFeatureOptions {
  uint64_t num_warmup;
};

class StockFeature : public WorkloadBase {
 public:
  StockFeature(StockFeatureOptions options, std::shared_ptr<Cirrus> cirrus,
               std::shared_ptr<BenchmarkState> state);
  virtual ~StockFeature() = default;

  uint64_t NumQueries() const;

 private:
  virtual void RunImpl() override;

  uint64_t num_queries_;

  StockFeatureOptions options_;
  std::shared_ptr<Cirrus> cirrus_;
};

}  // namespace cirrus
