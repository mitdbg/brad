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

 // private:
  class Impl;

  std::shared_ptr<Impl> impl_;

  explicit BradFlightSqlServer(std::shared_ptr<Impl> impl);
};

class BradFlightSqlServer::Impl {
 public:
  explicit Impl() {}

  ~Impl() = default;
};

}  // namespace brad
