#include "brad_server_simple.h"

#include <mutex>
#include <random>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <iostream>
#include <functional>
#include <any>

#include <arrow/array/builder_binary.h>
#include "brad_sql_info.h"
#include "brad_statement.h"
#include "brad_statement_batch_reader.h"
#include "brad_tables_schema_batch_reader.h"
#include <arrow/flight/sql/server.h>
#include <arrow/scalar.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/logging.h>

#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace brad {

using arrow::internal::checked_cast;
using namespace arrow::flight;
using namespace arrow::flight::sql;

arrow::Result<Ticket> EncodeTransactionQuery(
  const std::string &query_ticket) {
  ARROW_ASSIGN_OR_RAISE(auto ticket_string,
                        CreateStatementQueryTicket(query_ticket));
  return Ticket{std::move(ticket_string)};
}

std::string GetQueryTicket(
  const std::string &query,
  const std::string &transaction_id) {
  return transaction_id + ':' + query;
}

arrow::Result<std::pair<std::string, std::string>> DecodeTransactionQuery(
  const std::string &ticket) {
  auto divider = ticket.find(':');
  if (divider == std::string::npos) {
    return arrow::Status::Invalid("Malformed ticket");
  }
  std::string transaction_id = ticket.substr(0, divider);
  std::string autoincrement_id = ticket.substr(divider + 1);
  return std::make_pair(std::move(autoincrement_id), std::move(transaction_id));
}

std::vector<std::vector<std::any>> TransformQueryResult(
  std::vector<py::tuple> query_result) {
  std::vector<std::vector<std::any>> transformed_query_result;
  for (const auto &tup : query_result) {
    std::vector<std::any> transformed_tup{};
    for (const auto &elt : tup) {
      transformed_tup.push_back(elt);
    }
    transformed_query_result.push_back(transformed_tup);
  }
  return transformed_query_result;  
}

BradFlightSqlServer::BradFlightSqlServer() = default;

BradFlightSqlServer::~BradFlightSqlServer() = default;

std::shared_ptr<BradFlightSqlServer>
  BradFlightSqlServer::Create() {
  std::shared_ptr<BradFlightSqlServer> result =
    std::make_shared<BradFlightSqlServer>();
  for (const auto &id_to_result : GetSqlInfoResultMap()) {
    result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
  }
  return result;
}

void BradFlightSqlServer::InitWrapper(
  const std::string &host,
  int port,
  std::function<std::vector<py::tuple>(std::string)> handle_query) {
  auto location = arrow::flight::Location::ForGrpcTcp(host, port).ValueOrDie();
  arrow::flight::FlightServerOptions options(location);

  handle_query_ = handle_query;

  this->Init(options);
}

void BradFlightSqlServer::ServeWrapper() {
  this->Serve();
}

void BradFlightSqlServer::ShutdownWrapper() {
  this->Shutdown(nullptr);
}

arrow::Result<std::unique_ptr<FlightInfo>>
  BradFlightSqlServer::GetFlightInfoStatement(
    const ServerCallContext &context,
    const StatementQuery &command,
    const FlightDescriptor &descriptor) {
  const std::string &query = command.query;

  const std::string &autoincrement_id = std::to_string(++autoincrement_id_);
  const std::string &query_ticket = GetQueryTicket(autoincrement_id, command.transaction_id);
  ARROW_ASSIGN_OR_RAISE(auto ticket,
                        EncodeTransactionQuery(query_ticket));

  std::vector<std::vector<std::any>> transformed_query_result;

  { 
    py::gil_scoped_acquire guard;
    std::vector<py::tuple> query_result;
    query_result = handle_query_(query);
    transformed_query_result = TransformQueryResult(query_result);
  }

  {
    std::scoped_lock guard(query_data_mutex_);
    query_data_.insert(query_ticket, transformed_query_result);
  }

  ARROW_ASSIGN_OR_RAISE(auto statement, BradStatement::Create(transformed_query_result));
  ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema());

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
  const std::string &autoincrement_id = pair.first;
  const std::string transaction_id = pair.second;

  const std::string &query_ticket = transaction_id + ':' + autoincrement_id;
  const auto query_result = query_data_.find(query_ticket);

  std::shared_ptr<BradStatement> statement;
  ARROW_ASSIGN_OR_RAISE(statement, BradStatement::Create(query_result));

  std::shared_ptr<BradStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, BradStatementBatchReader::Create(statement));

  return std::make_unique<RecordBatchStream>(reader);
}

}  // namespace brad
