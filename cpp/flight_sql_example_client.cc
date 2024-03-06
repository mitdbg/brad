// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// A minimal example of executing a SQL query via Flight SQL.
//
// For a corresponding server to test against, this will work with the
// SQLite example included in the test suite, "flight-sql-test-server".
// Building this example will also build the server; then, start the
// server before running this example.

#include <cstdlib>
#include <iostream>
#include <chrono>

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/table.h>
#include <gflags/gflags.h>

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

DEFINE_string(host, "localhost", "The host of the Flight SQL server.");
DEFINE_int32(port, 31337, "The port of the Flight SQL server.");
DEFINE_string(query, "SELECT 1", "The query to execute.");

arrow::Status Main() {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        flight::Location::ForGrpcTcp(FLAGS_host, FLAGS_port));
  std::cout << "Connecting to " << location.ToString() << std::endl;

  // Set up the Flight SQL client
  std::unique_ptr<flight::FlightClient> flight_client;
  ARROW_ASSIGN_OR_RAISE(flight_client, flight::FlightClient::Connect(location));
  std::unique_ptr<flightsql::FlightSqlClient> client(
      new flightsql::FlightSqlClient(std::move(flight_client)));

  flight::FlightCallOptions call_options;

  // Execute the query, getting a FlightInfo describing how to fetch the results
  std::cout << "Executing query: '" << FLAGS_query << "'" << std::endl;

  // Get time data for benchmarking
  const int num_trials = 10000;
  std::chrono::duration<double> total_execution_time;

  for (int i = 0; i < num_trials; ++i) {
    const auto start_time = std::chrono::steady_clock::now();
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<flight::FlightInfo> flight_info,
                          client->Execute(call_options, FLAGS_query));

    // Fetch each partition sequentially (though this can be done in parallel)
    for (const flight::FlightEndpoint& endpoint : flight_info->endpoints()) {
      // Here we assume each partition is on the same server we originally queried, but this
      // isn't true in general: the server may split the query results between multiple
      // other servers, which we would have to connect to.

      // The "ticket" in the endpoint is opaque to the client. The server uses it to
      // identify which part of the query results to return.
      ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(call_options, endpoint.ticket));
      // Read all results into an Arrow Table, though we can iteratively process record
      // batches as they arrive as well
      ARROW_ASSIGN_OR_RAISE(auto table, stream->ToTable());
      #if(!RELEASE)
        std::cout << "Read one chunk:" << std::endl;
        std::cout << table->ToString() << std::endl;
      #endif
    }
    const auto end_time = std::chrono::steady_clock::now();
    const std::chrono::duration<double> time_diff = end_time - start_time;
    total_execution_time += time_diff;
  }

  const auto average_execution_time = total_execution_time.count() / num_trials;
  std::cout << "Average time to execute query is " << average_execution_time << '\n';

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto status = Main();
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
