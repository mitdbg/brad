cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(FlightMinimalExample)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(ArrowFlightSql REQUIRED)

find_package(gflags REQUIRED)

add_executable(flight_sql_example flight_sql_example.cc)
target_link_libraries(flight_sql_example
		      		  PRIVATE Arrow::arrow_shared
		      		  PRIVATE ArrowFlight::arrow_flight_shared
		      		  PRIVATE ArrowFlightSql::arrow_flight_sql_shared
		      		  gflags)

add_executable(flight_sql_test_server test_server_cli.cc)
target_link_libraries(flight_sql_test_server
		      		  PRIVATE Arrow::arrow_shared
		      		  PRIVATE ArrowFlight::arrow_flight_shared
		      		  PRIVATE ArrowFlightSql::arrow_flight_sql_shared
		      		  gflags)


