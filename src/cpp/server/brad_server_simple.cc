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

#include "brad_server_simple.h"

#define BOOST_NO_CXX98_FUNCTION_BASE  // ARROW-17805
#include <boost/algorithm/string.hpp>
#include <mutex>
#include <random>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <iostream>

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

BradFlightSqlServer::BradFlightSqlServer(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {
  std::cout << "Constructed instance of BradFlightSqlServer\n";
}

BradFlightSqlServer::~BradFlightSqlServer() = default;

}  // namespace brad
