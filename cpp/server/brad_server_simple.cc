#include "brad_server_simple.h"

#include <mutex>
#include <random>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <stdexcept>

#include <arrow/array/builder_binary.h>
#include "brad_sql_info.h"
#include "brad_statement.h"
#include "brad_statement_batch_reader.h"
#include "brad_tables_schema_batch_reader.h"
#include "python_utils.h"
#include <arrow/flight/sql/server.h>
#include <arrow/scalar.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/logging.h>

namespace brad {

using arrow::internal::checked_cast;
using namespace arrow::flight;
using namespace arrow::flight::sql;

namespace py = pybind11;

std::string GetQueryTicket(
  const std::string &autoincrement_id,
  const std::string &transaction_id) {
  return transaction_id + ':' + autoincrement_id;
}

arrow::Result<Ticket> EncodeTransactionQuery(
  const std::string &query_ticket) {
  ARROW_ASSIGN_OR_RAISE(auto ticket_string,
                        CreateStatementQueryTicket(query_ticket));
  return Ticket{std::move(ticket_string)};
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
  for (const auto &row : query_result) {
    std::vector<std::any> transformed_row{};
    for (const auto &field : row) {
      if (py::isinstance<py::int_>(field)) {
        transformed_row.push_back(std::make_any<int>(py::cast<int>(field)));
      } else if (py::isinstance<py::float_>(field)) {
        transformed_row.push_back(std::make_any<float>(py::cast<float>(field)));
      } else {
        transformed_row.push_back(std::make_any<std::string>(py::cast<std::string>(field)));
      }
    }
    transformed_query_result.push_back(transformed_row);
  }
  return transformed_query_result;  
}

BradFlightSqlServer::BradFlightSqlServer() : autoincrement_id_(0ULL) {}

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
  PythonRunQueryFn handle_query) {
  auto location = arrow::flight::Location::ForGrpcTcp(host, port).ValueOrDie();
  arrow::flight::FlightServerOptions options(location);

  handle_query_ = handle_query;

  const auto status = this->Init(options);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

void BradFlightSqlServer::ServeWrapper() {
  const auto status = this->Serve();
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

void BradFlightSqlServer::ShutdownWrapper() {
  const auto status = this->Shutdown(nullptr);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
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

  std::shared_ptr<arrow::Schema> result_schema;
  std::vector<std::vector<std::any>> transformed_query_result;

  { 
    py::gil_scoped_acquire guard;
    auto result = handle_query_(query);
    result_schema = ArrowSchemaFromBradSchema(result.second);
    transformed_query_result = TransformQueryResult(result.first);
  }

  ARROW_ASSIGN_OR_RAISE(auto statement, BradStatement::Create(transformed_query_result));
  query_data_.insert(query_ticket, statement);

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

  std::shared_ptr<BradStatement> result;
  const bool found = query_data_.erase_fn(query_ticket, [&result](auto& qr) {
    result = qr;
    return true;
  });

  if (!found) {
    return arrow::Status::Invalid("Invalid ticket.");
  }

  std::shared_ptr<BradStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, BradStatementBatchReader::Create(result));

  return std::make_unique<RecordBatchStream>(reader);
}

}  // namespace brad
