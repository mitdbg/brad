#include "brad_server_simple.h"

#include <mutex>
#include <random>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <stdexcept>

#include <arrow/api.h>
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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ResultToRecordBatch(
  const std::vector<py::tuple> &query_result,
  const std::shared_ptr<arrow::Schema> &schema) {
  const size_t num_rows = query_result.size();

  const size_t num_columns = schema->num_fields();
  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(num_columns);

  for (int field_ix = 0; field_ix < num_columns; ++field_ix) {
    const auto &field_type = schema->field(field_ix)->type();
    if (field_type->Equals(arrow::int64())) {
      arrow::Int64Builder int64builder;
      for (int row_ix = 0; row_ix < num_rows; ++row_ix) {
        const std::optional<int64_t> val =
          py::cast<std::optional<int64_t>>(query_result[row_ix][field_ix]);
        if (val) {
          ARROW_RETURN_NOT_OK(int64builder.Append(*val));
        } else {
          ARROW_RETURN_NOT_OK(int64builder.AppendNull());
        }
      }
      std::shared_ptr<arrow::Array> values;
      ARROW_ASSIGN_OR_RAISE(values, int64builder.Finish());
      columns.push_back(values);

    } else if (field_type->Equals(arrow::float32())) {
      arrow::FloatBuilder floatbuilder;
      for (int row_ix = 0; row_ix < num_rows; ++row_ix) {
        const std::optional<float> val =
          py::cast<std::optional<float>>(query_result[row_ix][field_ix]);
        if (val) {
          ARROW_RETURN_NOT_OK(floatbuilder.Append(*val));
        } else {
          ARROW_RETURN_NOT_OK(floatbuilder.AppendNull());
        }
      }
      std::shared_ptr<arrow::Array> values;
      ARROW_ASSIGN_OR_RAISE(values, floatbuilder.Finish());
      columns.push_back(values);

    } else if (field_type->Equals(arrow::decimal(/*precision=*/10, /*scale=*/2))) {
      arrow::Decimal128Builder decimalbuilder(arrow::decimal(/*precision=*/10, /*scale=*/2));
      for (int row_ix = 0; row_ix < num_rows; ++row_ix) {
        const std::optional<std::string> val =
          py::cast<std::optional<std::string>>(query_result[row_ix][field_ix]);
        if (val) {
          ARROW_RETURN_NOT_OK(
            decimalbuilder.Append(arrow::Decimal128::FromString(*val).ValueOrDie()));
        } else {
          ARROW_RETURN_NOT_OK(decimalbuilder.AppendNull());
        }
      }
      std::shared_ptr<arrow::Array> values;
      ARROW_ASSIGN_OR_RAISE(values, decimalbuilder.Finish());
      columns.push_back(values);  

    } else if (field_type->Equals(arrow::utf8())) {
      arrow::StringBuilder stringbuilder;
      for (int row_ix = 0; row_ix < num_rows; ++row_ix) {
        const std::optional<std::string> str =
          py::cast<std::optional<std::string>>(query_result[row_ix][field_ix]);
        if (str) {
          ARROW_RETURN_NOT_OK(stringbuilder.Append(str->data(), str->size()));
        } else {
          ARROW_RETURN_NOT_OK(stringbuilder.AppendNull());
        }
      }
      std::shared_ptr<arrow::Array> values;
      ARROW_ASSIGN_OR_RAISE(values, stringbuilder.Finish());
      columns.push_back(values);

    } else if (field_type->Equals(arrow::date64())) {
      arrow::Date64Builder datebuilder;
      for (int row_ix = 0; row_ix < num_rows; ++row_ix) {
        const std::optional<int64_t> val =
          py::cast<std::optional<int64_t>>(query_result[row_ix][field_ix]);
        if (val) {
          ARROW_RETURN_NOT_OK(datebuilder.Append(*val));
        } else {
          ARROW_RETURN_NOT_OK(datebuilder.AppendNull());
        }
      }
      std::shared_ptr<arrow::Array> values;
      ARROW_ASSIGN_OR_RAISE(values, datebuilder.Finish());
      columns.push_back(values); 

    } else if (field_type->Equals(arrow::null())) {
      arrow::NullBuilder nullbuilder;
      for (int row_ix = 0; row_ix < num_rows; ++row_ix) {
        ARROW_RETURN_NOT_OK(nullbuilder.AppendNull());
      }
      std::shared_ptr<arrow::Array> values;
      ARROW_ASSIGN_OR_RAISE(values, nullbuilder.Finish());
      columns.push_back(values);
    }
  }

  std::shared_ptr<arrow::RecordBatch> result_record_batch =
    arrow::RecordBatch::Make(schema, num_rows, columns);

  return result_record_batch;
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
  std::shared_ptr<arrow::RecordBatch> result_record_batch;

  { 
    py::gil_scoped_acquire guard;
    auto result = handle_query_(query);
    result_schema = ArrowSchemaFromBradSchema(result.second);
    result_record_batch = ResultToRecordBatch(result.first, result_schema).ValueOrDie();
  }

  ARROW_ASSIGN_OR_RAISE(auto statement, BradStatement::Create(std::move(result_record_batch), result_schema));
  query_data_.insert(query_ticket, statement);

  std::vector<FlightEndpoint> endpoints{
    FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};

  const bool ordered = false;
  ARROW_ASSIGN_OR_RAISE(auto result, FlightInfo::Make(*result_schema,
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
