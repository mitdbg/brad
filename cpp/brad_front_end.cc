#include <cstdlib>
#include <iostream>
#include <chrono>

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/flight/server.h>
// #include "server/brad_server.h"
#include <arrow/table.h>
#include <arrow/util/logging.h>

#include <arrow/io/test_common.h>

#include "brad_front_end.h"

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

namespace brad {

void BradFrontEnd::AddServer(const std::string &host, int port) {
  ServerInfo server_info{};
  server_info.host = host;
  server_info.port = port;
  server_info_objects_.push_back(server_info);
}

arrow::Status BradFrontEnd::ExecuteQuery(const std::string &query) {
  ServerInfo server_info = server_info_objects_[0];

  ARROW_ASSIGN_OR_RAISE(auto location,
                        flight::Location::ForGrpcTcp(server_info.host,
                                                     server_info.port));
  std::cout << "Connecting to " << location.ToString() << std::endl;

  // Set up the Flight SQL client
  std::unique_ptr<flight::FlightClient> flight_client;
  ARROW_ASSIGN_OR_RAISE(flight_client, flight::FlightClient::Connect(location));
  std::unique_ptr<flightsql::FlightSqlClient> client(
      new flightsql::FlightSqlClient(std::move(flight_client)));

  flight::FlightCallOptions call_options;

  // Execute the query, getting a FlightInfo describing how to fetch the results
  std::cout << "Executing query: '" << query << "'" << std::endl;

  // Get time data for benchmarking
  const int num_trials = 10000;
  std::chrono::duration<double> total_execution_time;

  for (int i = 0; i < num_trials; ++i) {
    const auto start_time = std::chrono::steady_clock::now();
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<flight::FlightInfo> flight_info,
                          client->Execute(call_options, query));

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

} // namespace brad

arrow::Status Main() {
  brad::BradFrontEnd front_end_server;
  front_end_server.AddServer("localhost", 31337);

  std::string query = "SELECT 1";
  return front_end_server.ExecuteQuery(query);
}

int main(int argc, char** argv) {
  auto status = Main();
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
