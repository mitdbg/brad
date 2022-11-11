#include "tpch.h"

#include <sstream>

#include "../utils/connection.h"
#include "../utils/sf.h"

namespace {

// The query should be terminated with a semicolon.
std::string RepeatQuery(const std::string& query, uint32_t times) {
  std::stringstream builder;
  for (uint32_t i = 0; i < times; ++i) {
    builder << query;
  }
  return builder.str();
}

std::string GetQ5ForScaleFactor(uint32_t sf) {
  std::stringstream builder;
  builder << "SELECT"
             " n_name,"
             " SUM(l_extendedprice * (1 - l_discount)) AS revenue "
             "FROM ";
  builder << "  customer_" << PaddedScaleFactor(sf) << ",";
  builder << "  orders_" << PaddedScaleFactor(sf) << ",";
  builder << "  lineitem_" << PaddedScaleFactor(sf) << ",";
  builder << "  supplier_" << PaddedScaleFactor(sf) << ",";
  builder << "  nation_" << PaddedScaleFactor(sf) << ",";
  builder << "  region_" << PaddedScaleFactor(sf) << " ";
  builder << "WHERE"
             "  c_custkey = o_custkey"
             "  AND l_orderkey = o_orderkey"
             "  AND l_suppkey = s_suppkey"
             "  AND c_nationkey = s_nationkey"
             "  AND s_nationkey = n_nationkey"
             "  AND n_regionkey = r_regionkey"
             "  AND r_name = 'ASIA'"
             "  AND o_orderdate >= date '1994-01-01'"
             "  AND o_orderdate < date '1995-01-01' "
             "GROUP BY"
             " n_name;";
  return builder.str();
}

}  // namespace

RunQ5::RunQ5(uint64_t num_warmup, uint64_t batch_size, uint32_t scale_factor,
             std::shared_ptr<BenchmarkState> state)
    : num_warmup_(num_warmup),
      batch_size_(batch_size),
      scale_factor_(scale_factor),
      num_queries_run_(0),
      state_(std::move(state)),
      connection_(Connection::GetConnectionString()),
      joined_(false) {
  thread_ = std::thread(&RunQ5::Run, this);
}

RunQ5::~RunQ5() { Wait(); }

void RunQ5::Wait() {
  if (joined_) return;
  thread_.join();
  joined_ = true;
}

void RunQ5::Run() {
  const std::string kBatchedQuery =
      RepeatQuery(GetQ5ForScaleFactor(scale_factor_), batch_size_);

  for (uint64_t i = 0; i < num_warmup_; ++i) {
    nanodbc::execute(connection_, kBatchedQuery);
  }

  state_->BumpReady();
  state_->WaitToStart();

  while (state_->KeepRunning()) {
    nanodbc::execute(connection_, kBatchedQuery);
    num_queries_run_ += batch_size_;
  }
}
