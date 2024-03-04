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

  // arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoCatalogs(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::FlightDescriptor& descriptor) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetCatalogs(
  //     const arrow::flight::ServerCallContext &context) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoSchemas(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::GetDbSchemas& command,
  //     const arrow::flight::FlightDescriptor& descriptor) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetDbSchemas(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::GetDbSchemas& command) override;

  // arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedStatement(
  //     const ServerCallContext& context,
  //     const ActionCreatePreparedStatementRequest& request) override;

  // Status ClosePreparedStatement(
  //     const ServerCallContext& context,
  //     const ActionClosePreparedStatementRequest& request) override;

  // arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPreparedStatement(
  //     const ServerCallContext& context,
  //     const PreparedStatementQuery& command,
  //     const FlightDescriptor& descriptor) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetPreparedStatement(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::PreparedStatementQuery& command) override;

  // Status DoPutPreparedStatementQuery(
  //     const ServerCallContext& context,
  //     const PreparedStatementQuery& command,
  //     FlightMessageReader* reader,
  //     FlightMetadataWriter* writer) override;

  // arrow::Result<int64_t> DoPutPreparedStatementUpdate(
  //     const ServerCallContext& context,
  //     const PreparedStatementUpdate& command,
  //     FlightMessageReader* reader) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoTables(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::GetTables& command,
  //     const arrow::flight::FlightDescriptor& descriptor) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetTables(
  //     const arrow::flight::ServerCallContext &context,
  //     const arrow::flight::sql::GetTables &command) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoTableTypes(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::FlightDescriptor& descriptor) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetTableTypes(
  //     const arrow::flight::ServerCallContext &context) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoImportedKeys(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::GetImportedKeys& command,
  //     const arrow::flight::FlightDescriptor& descriptor) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetImportedKeys(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::GetImportedKeys& command) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoExportedKeys(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::GetExportedKeys& command,
  //     const arrow::flight::FlightDescriptor& descriptor) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetExportedKeys(
  //     const arrow::flight::ServerCallContext &context,
  //     const arrow::flight::sql::GetExportedKeys &command) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoCrossReference(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::GetCrossReference& command,
  //     const arrow::flight::FlightDescriptor& descriptor) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetCrossReference(
  //     const arrow::flight::ServerCallContext &context,
  //     const arrow::flight::sql::GetCrossReference &command) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoPrimaryKeys(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::GetPrimaryKeys& command,
  //     const arrow::flight::FlightDescriptor& descriptor) override;

  // arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetPrimaryKeys(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::GetPrimaryKeys& command) override;

  // arrow::Result<arrow::flight::sql::ActionBeginTransactionResult> BeginTransaction(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::ActionBeginTransactionRequest& request) override;

  // arrow::Status EndTransaction(
  //     const arrow::flight::ServerCallContext& context,
  //     const arrow::flight::sql::ActionEndTransactionRequest& request) override;

 // private:
  class Impl;

  std::shared_ptr<Impl> impl_;

  explicit BradFlightSqlServer(std::shared_ptr<Impl> impl);
};

}  // namespace brad
