#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <arrow/flight/sql/server.h>
#include <arrow/result.h>

namespace brad {

class BradFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
 public:
  ~BradFlightSqlServer() override;

  static arrow::Result<std::shared_ptr<BradFlightSqlServer>> Create();

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoStatement(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::StatementQuery& command,
      const arrow::flight::FlightDescriptor& descriptor) override;

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::StatementQueryTicket &command) override;

  class Impl;

  std::shared_ptr<Impl> impl_;

  explicit BradFlightSqlServer(std::shared_ptr<Impl> impl);
};

}  // namespace brad
