cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(FlightMinimalExample)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(ArrowFlightSql REQUIRED)

find_package(gflags REQUIRED)

add_executable(brad_example main.cc
               brad_server.cc
               brad_server.h
               brad_sql_info.cc
               brad_sql_info.h
               brad_statement.cc
               brad_statement.h
               brad_statement_batch_reader.cc
               brad_statement_batch_reader.h
               brad_tables_schema_batch_reader.cc
               brad_tables_schema_batch_reader.h)

target_link_libraries(brad_example
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      gflags)
