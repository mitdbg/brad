#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <functional>
#include <any>
#include <atomic>
#include <mutex>

#include <arrow/flight/sql/server.h>
#include "brad_statement.h"
#include <arrow/result.h>

#include "libcuckoo/cuckoohash_map.hh"

#include <pybind11/pybind11.h>

namespace py = pybind11;
using namespace pybind11::literals;

namespace brad {

class BradFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
 public:
  explicit BradFlightSqlServer();

  ~BradFlightSqlServer() override;

  static std::shared_ptr<BradFlightSqlServer> Create();

  void InitWrapper(const std::string &host,
                   int port,
                   std::function<std::vector<py::tuple>(std::string)>);

  void ServeWrapper();

  void ShutdownWrapper();

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
    GetFlightInfoStatement(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::StatementQuery &command,
      const arrow::flight::FlightDescriptor &descriptor) override;

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
    DoGetStatement(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::StatementQueryTicket &command) override;

 private:
  std::function<std::vector<py::tuple>(std::string)> handle_query_;

  libcuckoo::cuckoohash_map<std::string, std::shared_ptr<BradStatement>> query_data_;

  std::atomic<uint64_t> autoincrement_id_;
};

}  // namespace brad
