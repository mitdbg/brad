#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <functional>
#include <any>
#include <atomic>
#include <mutex>

#include <arrow/flight/sql/server.h>
#include <arrow/result.h>

namespace brad {

class BradFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
 public:
  explicit BradFlightSqlServer();

  ~BradFlightSqlServer() override;

  static std::shared_ptr<BradFlightSqlServer> Create();

  void InitWrapper(const std::string &host,
                   int port,
                   std::function<std::vector<std::tuple<int>>(std::string)>);

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

  // TODO: Create and reuse type for RowList
  std::function<std::vector<std::tuple<int>>(std::string)> _handle_query;

  std::unordered_map<std::string, std::vector<std::tuple<int>>> _query_data;
  std::mutex _query_data_mutex;

  std::atomic<uint64_t> _autoincrement_id;
};

}  // namespace brad
