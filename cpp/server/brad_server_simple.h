#pragma once

#include <arrow/flight/sql/server.h>
#include <arrow/result.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <mutex>

#include "brad_statement.h"
#include "libcuckoo/cuckoohash_map.hh"

namespace brad {

// The type of a Python function that will execute the given SQL query (given as
// a string). The function returns the results and a schema object.
//
// NOTE: The GIL must be held when invoking this function.
using PythonRunQueryFn =
    std::function<std::pair<std::vector<pybind11::tuple>, pybind11::object>(
        std::string)>;

class BradFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
 public:
  explicit BradFlightSqlServer();

  ~BradFlightSqlServer() override;

  static std::shared_ptr<BradFlightSqlServer> Create();

  void InitWrapper(const std::string& host, int port,
                   PythonRunQueryFn handle_query);

  void ServeWrapper();

  void ShutdownWrapper();

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
  GetFlightInfoStatement(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::StatementQuery& command,
      const arrow::flight::FlightDescriptor& descriptor) override;

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
  DoGetStatement(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::StatementQueryTicket& command) override;

  arrow::Result<arrow::flight::sql::ActionCreatePreparedStatementResult>
  CreatePreparedStatement(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::ActionCreatePreparedStatementRequest& request)
      override;
  arrow::Status ClosePreparedStatement(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::ActionClosePreparedStatementRequest& request)
      override;

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
  GetFlightInfoPreparedStatement(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::PreparedStatementQuery& command,
      const arrow::flight::FlightDescriptor& descriptor) override;

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
  DoGetPreparedStatement(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::PreparedStatementQuery& command) override;

  // Currently unimplemented.

  // Bind params.
  arrow::Status DoPutPreparedStatementQuery(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::PreparedStatementQuery& command,
      arrow::flight::FlightMessageReader* reader,
      arrow::flight::FlightMetadataWriter* writer) override;

  // Update the prepared statement.
  arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::PreparedStatementUpdate& command,
      arrow::flight::FlightMessageReader* reader) override;

 private:
  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoImpl(
      const std::string& query, const std::string& transaction_id,
      const arrow::flight::FlightDescriptor& descriptor);

  struct PreparedStatementContext {
    std::string query;
    std::string transaction_id;
  };

  PythonRunQueryFn handle_query_;

  libcuckoo::cuckoohash_map<std::string, std::shared_ptr<BradStatement>>
      query_data_;
  libcuckoo::cuckoohash_map<std::string, PreparedStatementContext>
      prepared_statements_;

  std::atomic<uint64_t> autoincrement_id_;
  std::mutex mutex_;
};

}  // namespace brad
