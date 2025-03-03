#include "brad_server.h"

#define BOOST_NO_CXX98_FUNCTION_BASE  // ARROW-17805
#include <boost/algorithm/string.hpp>
#include <mutex>
#include <random>
#include <sstream>
#include <unordered_map>
#include <utility>

#include <arrow/array/builder_binary.h>
#include "brad_sql_info.h"
#include "brad_statement.h"
#include "brad_statement_batch_reader.h"
#include "brad_tables_schema_batch_reader.h"
#include <arrow/flight/sql/server.h>
#include <arrow/scalar.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/logging.h>

namespace brad {

using arrow::internal::checked_cast;
using namespace arrow::flight;
using namespace arrow::flight::sql;

namespace {

std::string PrepareQueryForGetTables(const GetTables& command) {
  return "SELECT 1";
}

arrow::Result<std::unique_ptr<FlightDataStream>> DoGetBradQuery(
    const std::string& query, const std::shared_ptr<arrow::Schema>& schema) {
  std::shared_ptr<BradStatement> statement;

  ARROW_ASSIGN_OR_RAISE(statement, BradStatement::Create(query));

  std::shared_ptr<BradStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, BradStatementBatchReader::Create(statement, schema));

  return std::make_unique<RecordBatchStream>(reader);
}

arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoForCommand(
    const FlightDescriptor& descriptor,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::vector<FlightEndpoint> endpoints{
      FlightEndpoint{{descriptor.cmd}, {}, std::nullopt, ""}};
  ARROW_ASSIGN_OR_RAISE(auto result,
                        FlightInfo::Make(*schema, descriptor, endpoints, -1, -1, false))

  return std::make_unique<FlightInfo>(result);
}

}  // namespace

class BradFlightSqlServer::Impl {
 private:
  // Create a Ticket that combines a query and a transaction ID.
  arrow::Result<Ticket> EncodeTransactionQuery(
      const std::string& query,
      const std::string& transaction_id) {
    std::string transaction_query = transaction_id;
    transaction_query += ':';
    transaction_query += query;
    ARROW_ASSIGN_OR_RAISE(auto ticket_string,
                          CreateStatementQueryTicket(transaction_query));
    return Ticket{std::move(ticket_string)};
  }

  arrow::Result<std::pair<std::string, std::string>> DecodeTransactionQuery(
      const std::string& ticket) {
    auto divider = ticket.find(':');
    if (divider == std::string::npos) {
      return arrow::Status::Invalid("Malformed ticket");
    }
    std::string transaction_id = ticket.substr(0, divider);
    std::string query = ticket.substr(divider + 1);
    return std::make_pair(std::move(query), std::move(transaction_id));
  }

 public:
  explicit Impl() {}

  ~Impl() = default;

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
      const ServerCallContext& context, const StatementQuery& command,
      const FlightDescriptor& descriptor) {
    const std::string& query = command.query;
    ARROW_ASSIGN_OR_RAISE(auto statement, BradStatement::Create(query));
    ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema());
    ARROW_ASSIGN_OR_RAISE(auto ticket,
                          EncodeTransactionQuery(query, command.transaction_id));
    std::vector<FlightEndpoint> endpoints{
        FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};
    // TODO: Set true only when "ORDER BY" is used in a main "SELECT"
    // in the given query.
    const bool ordered = false;
    ARROW_ASSIGN_OR_RAISE(
        auto result, FlightInfo::Make(*schema, descriptor, endpoints, -1, -1, ordered));

    return std::make_unique<FlightInfo>(result);
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context, const StatementQueryTicket& command) {
    ARROW_ASSIGN_OR_RAISE(auto pair, DecodeTransactionQuery(command.statement_handle));
    const std::string& sql = pair.first;
    const std::string transaction_id = pair.second;

    std::shared_ptr<BradStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, BradStatement::Create(sql));

    std::shared_ptr<BradStatementBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, BradStatementBatchReader::Create(statement));

    return std::make_unique<RecordBatchStream>(reader);
  }
};

BradFlightSqlServer::BradFlightSqlServer(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

arrow::Result<std::shared_ptr<BradFlightSqlServer>> BradFlightSqlServer::Create() {
  std::shared_ptr<Impl> impl = std::make_shared<Impl>();

  std::shared_ptr<BradFlightSqlServer> result(
      new BradFlightSqlServer(std::move(impl)));
  for (const auto& id_to_result : GetSqlInfoResultMap()) {
    result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
  }

  return result;
}

BradFlightSqlServer::~BradFlightSqlServer() = default;

arrow::Result<std::unique_ptr<FlightInfo>> BradFlightSqlServer::GetFlightInfoStatement(
    const ServerCallContext& context,
    const StatementQuery& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoStatement(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> BradFlightSqlServer::DoGetStatement(
    const ServerCallContext& context,
    const StatementQueryTicket& command) {
  return impl_->DoGetStatement(context, command);
}

}  // namespace brad
