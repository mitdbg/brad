#include "brad_server_simple.h"

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

arrow::Result<Ticket> EncodeTransactionQuery(
  const std::string &query,
  const std::string &transaction_id) {
  std::string transaction_query = transaction_id;
  transaction_query += ':';
  transaction_query += query;
  ARROW_ASSIGN_OR_RAISE(auto ticket_string,
                        CreateStatementQueryTicket(transaction_query));
  return Ticket{std::move(ticket_string)};
}

arrow::Result<std::pair<std::string, std::string>> DecodeTransactionQuery(
  const std::string &ticket) {
  auto divider = ticket.find(':');
  if (divider == std::string::npos) {
    return arrow::Status::Invalid("Malformed ticket");
  }
  std::string transaction_id = ticket.substr(0, divider);
  std::string query = ticket.substr(divider + 1);
  return std::make_pair(std::move(query), std::move(transaction_id));
}

BradFlightSqlServer::BradFlightSqlServer() = default;

BradFlightSqlServer::~BradFlightSqlServer() = default;

std::shared_ptr<BradFlightSqlServer>
  BradFlightSqlServer::Create() {
    // std::shared_ptr<BradFlightSqlServer> result(new BradFlightSqlServer());
    std::shared_ptr<BradFlightSqlServer> result =
      std::make_shared<BradFlightSqlServer>();
    for (const auto &id_to_result : GetSqlInfoResultMap()) {
      result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
    }
    return result;
}

arrow::Result<std::unique_ptr<FlightInfo>>
  BradFlightSqlServer::GetFlightInfoStatement(
    const ServerCallContext &context,
    const StatementQuery &command,
    const FlightDescriptor &descriptor) {
  const std::string &query = command.query;
  ARROW_ASSIGN_OR_RAISE(auto statement, BradStatement::Create(query));
  ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema());
  ARROW_ASSIGN_OR_RAISE(auto ticket,
                        EncodeTransactionQuery(query, command.transaction_id));
  std::vector<FlightEndpoint> endpoints{
    FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};

  const bool ordered = false;
  ARROW_ASSIGN_OR_RAISE(auto result, FlightInfo::Make(*schema,
                                                      descriptor,
                                                      endpoints,
                                                      -1,
                                                      -1,
                                                      ordered));

  return std::make_unique<FlightInfo>(result);
}

arrow::Result<std::unique_ptr<FlightDataStream>>
  BradFlightSqlServer::DoGetStatement(
    const ServerCallContext &context,
    const StatementQueryTicket &command) {
  ARROW_ASSIGN_OR_RAISE(auto pair,
                        DecodeTransactionQuery(command.statement_handle));
  const std::string &sql = pair.first;
  const std::string transaction_id = pair.second;

  std::shared_ptr<BradStatement> statement;
  ARROW_ASSIGN_OR_RAISE(statement, BradStatement::Create(sql));

  std::shared_ptr<BradStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, BradStatementBatchReader::Create(statement));

  return std::make_unique<RecordBatchStream>(reader);
}

}  // namespace brad
