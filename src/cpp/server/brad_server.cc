// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<BradStatement>> prepared_statements_;
  std::default_random_engine gen_;

  arrow::Result<std::shared_ptr<BradStatement>> GetStatementByHandle(
      const std::string& handle) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto search = prepared_statements_.find(handle);
    if (search == prepared_statements_.end()) {
      return arrow::Status::KeyError("Prepared statement not found");
    }
    return search->second;
  }

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

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCatalogs(
      const ServerCallContext& context,
      const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetCatalogsSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCatalogs(
      const ServerCallContext& context) {
    std::string query = "SELECT 1";

    return DoGetBradQuery(query, SqlSchema::GetCatalogsSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSchemas(
      const ServerCallContext &context,
      const GetDbSchemas &command,
      const FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetDbSchemasSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetDbSchemas(
      const ServerCallContext& context, const GetDbSchemas& command) {
    std::string query = "SELECT 1";

    return DoGetBradQuery(query, SqlSchema::GetDbSchemasSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPreparedStatement(
      const ServerCallContext& context,
      const PreparedStatementQuery& command) {
    const std::string& sql = "SELECT 1";

    std::shared_ptr<BradStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, BradStatement::Create(sql));

    std::shared_ptr<BradStatementBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, BradStatementBatchReader::Create(statement));

    return std::make_unique<RecordBatchStream>(reader);
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTables(
      const ServerCallContext& context,
      const GetTables& command,
      const FlightDescriptor& descriptor) {
    std::vector<FlightEndpoint> endpoints{
        FlightEndpoint{{descriptor.cmd}, {}, std::nullopt, ""}};

    bool include_schema = command.include_schema;
    ARROW_LOG(INFO) << "GetTables include_schema=" << include_schema;

    ARROW_ASSIGN_OR_RAISE(
        auto result,
        FlightInfo::Make(include_schema ? *SqlSchema::GetTablesSchemaWithIncludedSchema()
                                        : *SqlSchema::GetTablesSchema(),
                         descriptor, endpoints, -1, -1, false))

    return std::make_unique<FlightInfo>(std::move(result));
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTables(
      const ServerCallContext& context, const GetTables& command) {
    std::string query = PrepareQueryForGetTables(command);
    ARROW_LOG(INFO) << "GetTables: " << query;

    std::shared_ptr<BradStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, BradStatement::Create(query));

    std::shared_ptr<BradStatementBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, BradStatementBatchReader::Create(
      statement, SqlSchema::GetTablesSchema()));

    if (command.include_schema) {
      std::shared_ptr<BradTablesWithSchemaBatchReader> table_schema_reader =
          std::make_shared<BradTablesWithSchemaBatchReader>(reader, query);
      return std::make_unique<RecordBatchStream>(table_schema_reader);
    } else {
      return std::make_unique<RecordBatchStream>(reader);
    }
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTableTypes(
      const ServerCallContext& context, const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetTableTypesSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTableTypes(
      const ServerCallContext& context) {
    std::string query = "SELECT 1";

    return DoGetBradQuery(query, SqlSchema::GetTableTypesSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoImportedKeys(
      const ServerCallContext &context,
      const GetImportedKeys &command,
      const FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetImportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetImportedKeys(
      const ServerCallContext &context, const GetImportedKeys &command) {
    std::string query = "SELECT 1";

    return DoGetBradQuery(query, SqlSchema::GetImportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoExportedKeys(
      const ServerCallContext &context,
      const GetExportedKeys &command,
      const FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetExportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetExportedKeys(
      const ServerCallContext &context,
      const GetExportedKeys &command) {
      const TableRef &table_ref = command.table_ref;
    std::string query = "SELECT 1";

    return DoGetBradQuery(query, SqlSchema::GetExportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPrimaryKeys(
      const ServerCallContext& context, const GetPrimaryKeys& command,
      const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetPrimaryKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPrimaryKeys(
      const ServerCallContext& context, const GetPrimaryKeys& command) {

    std::string query = "SELECT 1";
    return DoGetBradQuery(query, SqlSchema::GetPrimaryKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCrossReference(
      const ServerCallContext& context,
      const GetCrossReference& command,
      const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetCrossReferenceSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCrossReference(
      const ServerCallContext& context,
      const GetCrossReference& command) {
    std::string query = "SELECT 1";
    return DoGetBradQuery(query, SqlSchema::GetCrossReferenceSchema());
  }

  arrow::Result<ActionBeginTransactionResult> BeginTransaction(
      const ServerCallContext& context,
      const ActionBeginTransactionRequest& request) {
    return arrow::Status::OK();
  }

  arrow::Status EndTransaction(const ServerCallContext& context,
                               const ActionEndTransactionRequest& request) {
    return arrow::Status::OK();
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

arrow::Result<std::unique_ptr<FlightInfo>> BradFlightSqlServer::GetFlightInfoCatalogs(
    const ServerCallContext& context,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoCatalogs(context, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> BradFlightSqlServer::DoGetCatalogs(
    const ServerCallContext& context) {
  return impl_->DoGetCatalogs(context);
}

arrow::Result<std::unique_ptr<FlightInfo>> BradFlightSqlServer::GetFlightInfoSchemas(
    const ServerCallContext& context, const GetDbSchemas& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoSchemas(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> BradFlightSqlServer::DoGetDbSchemas(
    const ServerCallContext& context, const GetDbSchemas& command) {
  return impl_->DoGetDbSchemas(context, command);
}

arrow::Result<std::unique_ptr<FlightDataStream>> BradFlightSqlServer::DoGetPreparedStatement(
    const ServerCallContext& context,
    const PreparedStatementQuery& command) {
  return impl_->DoGetPreparedStatement(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> BradFlightSqlServer::GetFlightInfoTables(
    const ServerCallContext& context,
    const GetTables& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTables(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> BradFlightSqlServer::DoGetTables(
    const ServerCallContext& context,
    const GetTables& command) {
  return impl_->DoGetTables(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> BradFlightSqlServer::GetFlightInfoTableTypes(
    const ServerCallContext& context,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTableTypes(context, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> BradFlightSqlServer::DoGetTableTypes(
    const ServerCallContext& context) {
  return impl_->DoGetTableTypes(context);
}

arrow::Result<std::unique_ptr<FlightInfo>>
BradFlightSqlServer::GetFlightInfoImportedKeys(
    const ServerCallContext& context,
    const GetImportedKeys& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoImportedKeys(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>>
BradFlightSqlServer::DoGetImportedKeys(
    const ServerCallContext& context,
    const GetImportedKeys& command) {
  return impl_->DoGetImportedKeys(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>>
BradFlightSqlServer::GetFlightInfoExportedKeys(
    const ServerCallContext& context,
    const GetExportedKeys& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoExportedKeys(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>>
BradFlightSqlServer::DoGetExportedKeys(
    const ServerCallContext& context,
    const GetExportedKeys& command) {
  return impl_->DoGetExportedKeys(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>>
BradFlightSqlServer::GetFlightInfoCrossReference(
    const ServerCallContext& context,
    const GetCrossReference& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoCrossReference(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>>
BradFlightSqlServer::DoGetCrossReference(
    const ServerCallContext& context,
    const GetCrossReference& command) {
  return impl_->DoGetCrossReference(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>>
BradFlightSqlServer::GetFlightInfoPrimaryKeys(
    const ServerCallContext& context,
    const GetPrimaryKeys& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoPrimaryKeys(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>>
BradFlightSqlServer::DoGetPrimaryKeys(
    const ServerCallContext& context,
    const GetPrimaryKeys& command) {
  return impl_->DoGetPrimaryKeys(context, command);
}

arrow::Result<ActionBeginTransactionResult>
BradFlightSqlServer::BeginTransaction(
    const ServerCallContext& context,
    const ActionBeginTransactionRequest& request) {
  return impl_->BeginTransaction(context, request);
}

arrow::Status BradFlightSqlServer::EndTransaction(
    const ServerCallContext& context,
    const ActionEndTransactionRequest& request) {
  return impl_->EndTransaction(context, request);
}

}  // namespace brad
