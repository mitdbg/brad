#include "brad_server_simple.h"

#include <arrow/api.h>
#include <arrow/array/builder_binary.h>
#include <arrow/flight/sql/server.h>
#include <arrow/scalar.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/logging.h>

#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include "brad_sql_info.h"
#include "brad_statement.h"
#include "brad_statement_batch_reader.h"
#include "brad_tables_schema_batch_reader.h"
#include "python_utils.h"

namespace brad {

using arrow::internal::checked_cast;
using namespace arrow::flight;
using namespace arrow::flight::sql;

namespace py = pybind11;

std::string GetQueryTicket(const std::string& autoincrement_id,
                           const std::string& transaction_id) {
  return transaction_id + ':' + autoincrement_id;
}

arrow::Result<Ticket> EncodeTransactionQuery(const std::string& query_ticket) {
  ARROW_ASSIGN_OR_RAISE(auto ticket_string,
                        CreateStatementQueryTicket(query_ticket));
  return Ticket{std::move(ticket_string)};
}

arrow::Result<std::pair<std::string, std::string>> DecodeTransactionQuery(
    const std::string& ticket) {
  auto divider = ticket.find(':');
  if (divider == std::string::npos) {
    return arrow::Status::Invalid("Malformed ticket");
  }
  std::string transaction_id = ticket.substr(0, divider);
  std::string autoincrement_id = ticket.substr(divider + 1);
  return std::make_pair(std::move(autoincrement_id), std::move(transaction_id));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ResultToRecordBatch(
    const std::vector<py::tuple>& query_result,
    const std::shared_ptr<arrow::Schema>& schema) {
  const size_t num_rows = query_result.size();

  const size_t num_columns = schema->num_fields();
  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(num_columns);

  for (int field_ix = 0; field_ix < num_columns; ++field_ix) {
    const auto& field_type = schema->field(field_ix)->type();
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

    } else if (field_type->Equals(
                   arrow::decimal(/*precision=*/10, /*scale=*/2))) {
      arrow::Decimal128Builder decimalbuilder(
          arrow::decimal(/*precision=*/10, /*scale=*/2));
      for (int row_ix = 0; row_ix < num_rows; ++row_ix) {
        const std::optional<std::string> val =
            py::cast<std::optional<std::string>>(
                query_result[row_ix][field_ix]);
        if (val) {
          ARROW_RETURN_NOT_OK(decimalbuilder.Append(
              arrow::Decimal128::FromString(*val).ValueOrDie()));
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
            py::cast<std::optional<std::string>>(
                query_result[row_ix][field_ix]);
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

std::shared_ptr<BradFlightSqlServer> BradFlightSqlServer::Create() {
  std::shared_ptr<BradFlightSqlServer> result =
      std::make_shared<BradFlightSqlServer>();
  for (const auto& id_to_result : GetSqlInfoResultMap()) {
    result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
  }
  return result;
}

void BradFlightSqlServer::InitWrapper(const std::string& host, int port,
                                      PythonRunQueryFn handle_query) {
  auto location = arrow::flight::Location::ForGrpcTcp(host, port).ValueOrDie();
  arrow::flight::FlightServerOptions options(location);

  // NOTE: We bypass authentication for simplicity -- this is not recommended in
  // a production setting.
  options.auth_handler = std::make_shared<arrow::flight::NoOpAuthHandler>();

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
    const ServerCallContext& context, const StatementQuery& command,
    const FlightDescriptor& descriptor) {
  const std::string& query = command.query;
  const std::string& transaction_id = command.transaction_id;
  return GetFlightInfoImpl(query, transaction_id, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>>
BradFlightSqlServer::DoGetStatement(const ServerCallContext& context,
                                    const StatementQueryTicket& command) {
  ARROW_ASSIGN_OR_RAISE(auto pair,
                        DecodeTransactionQuery(command.statement_handle));
  const std::string& autoincrement_id = pair.first;
  const std::string transaction_id = pair.second;

  const std::string& query_ticket = transaction_id + ':' + autoincrement_id;

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

arrow::Result<arrow::flight::sql::ActionCreatePreparedStatementResult>
BradFlightSqlServer::CreatePreparedStatement(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::sql::ActionCreatePreparedStatementRequest& request) {
  const auto id = std::to_string(++autoincrement_id_);
  const PreparedStatementContext statement_context{request.query,
                                                   request.transaction_id};
  prepared_statements_.insert(id, statement_context);
  // std::cerr << "Registered prepared statement " << id << " " << request.query
  //           << std::endl;
  return arrow::flight::sql::ActionCreatePreparedStatementResult{nullptr,
                                                                 nullptr, id};
}

arrow::Status BradFlightSqlServer::ClosePreparedStatement(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::sql::ActionClosePreparedStatementRequest& request) {
  // std::cerr << "ClosePreparedStatement called "
  //           << request.prepared_statement_handle << std::endl;
  const bool erased =
      prepared_statements_.erase(request.prepared_statement_handle);
  if (!erased) {
    return arrow::Status::Invalid("Invalid prepared statement handle.");
  }
  return arrow::Status();
}

arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
BradFlightSqlServer::GetFlightInfoPreparedStatement(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::sql::PreparedStatementQuery& command,
    const arrow::flight::FlightDescriptor& descriptor) {
  // std::cerr << "GetFlightInfoPreparedStatement called "
  //           << command.prepared_statement_handle << std::endl;
  const PreparedStatementContext* statement_ctx = nullptr;
  prepared_statements_.find_fn(
      command.prepared_statement_handle,
      [&statement_ctx](const auto& ps_ctx) { statement_ctx = &ps_ctx; });
  if (statement_ctx == nullptr) {
    return arrow::Status::Invalid("Invalid prepared statement handle.");
  }

  const std::string& query = statement_ctx->query;
  const std::string& transaction_id = statement_ctx->transaction_id;
  return GetFlightInfoImpl(query, transaction_id, descriptor);
}

// Currently unimplemented.

arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
BradFlightSqlServer::DoGetPreparedStatement(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::sql::PreparedStatementQuery& command) {
  std::cerr << "DoGetPreparedStatement called "
            << command.prepared_statement_handle << std::endl;
  return arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>();
}

arrow::Status BradFlightSqlServer::DoPutPreparedStatementQuery(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::sql::PreparedStatementQuery& command,
    arrow::flight::FlightMessageReader* reader,
    arrow::flight::FlightMetadataWriter* writer) {
  std::cerr << "DoPutPreparedStatementQuery called "
            << command.prepared_statement_handle << std::endl;
  return arrow::Status();
}

arrow::Result<int64_t> BradFlightSqlServer::DoPutPreparedStatementUpdate(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::sql::PreparedStatementUpdate& command,
    arrow::flight::FlightMessageReader* reader) {
  std::cerr << "DoPutPreparedStatementUpdate called "
            << command.prepared_statement_handle << std::endl;
  return arrow::Result<int64_t>();
}

arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
BradFlightSqlServer::GetFlightInfoImpl(const std::string& query,
                                       const std::string& transaction_id,
                                       const FlightDescriptor& descriptor) {
  const std::string autoincrement_id = std::to_string(++autoincrement_id_);
  const std::string query_ticket =
      GetQueryTicket(autoincrement_id, transaction_id);
  ARROW_ASSIGN_OR_RAISE(auto ticket, EncodeTransactionQuery(query_ticket));

  std::shared_ptr<arrow::Schema> result_schema;
  std::shared_ptr<arrow::RecordBatch> result_record_batch;

  {
    py::gil_scoped_acquire guard;
    auto result = handle_query_(query);
    result_schema = ArrowSchemaFromBradSchema(result.second);
    result_record_batch =
        ResultToRecordBatch(result.first, result_schema).ValueOrDie();
  }

  ARROW_ASSIGN_OR_RAISE(
      auto statement,
      BradStatement::Create(std::move(result_record_batch), result_schema));
  query_data_.insert(query_ticket, statement);

  std::vector<FlightEndpoint> endpoints{
      FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};

  const bool ordered = false;
  ARROW_ASSIGN_OR_RAISE(
      auto result,
      FlightInfo::Make(*result_schema, descriptor, endpoints, -1, -1, ordered));

  return std::make_unique<FlightInfo>(result);
}

}  // namespace brad
