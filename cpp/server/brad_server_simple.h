#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <utility>

#include <arrow/flight/sql/server.h>
#include "brad_statement.h"
#include <arrow/result.h>

#include "libcuckoo/cuckoohash_map.hh"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace brad {

// The type of a Python function that will execute the given SQL query (given as
// a string). The function returns the results and a schema object.
//
// NOTE: The GIL must be held when invoking this function.
using PythonRunQueryFn = std::function<std::pair<std::vector<pybind11::tuple>, pybind11::object>(std::string)>;

class BradFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
 public:
  explicit BradFlightSqlServer();

  ~BradFlightSqlServer() override;

  static std::shared_ptr<BradFlightSqlServer> Create();

  void InitWrapper(const std::string &host,
                   int port,
                   PythonRunQueryFn handle_query);

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
  PythonRunQueryFn handle_query_;

  libcuckoo::cuckoohash_map<std::string, std::shared_ptr<BradStatement>> query_data_;

  std::atomic<uint64_t> autoincrement_id_;
};

}  // namespace brad
