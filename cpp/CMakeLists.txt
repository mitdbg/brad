cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_definitions("RELEASE=$<CONFIG:Release>")

project(FlightMinimalExample)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(ArrowFlightSql REQUIRED)

find_package(gflags REQUIRED)

find_package(SQLite3 REQUIRED)

add_executable(flight_sql_example flight_sql_example.cc)
target_link_libraries(flight_sql_example
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      gflags
                      ${SQLite3_LIBRARIES})

add_executable(flight_sql_test_server test_server_cli.cc
               example/sqlite_server.cc
               example/sqlite_server.h
               example/sqlite_server.cc
               example/sqlite_server.h
               example/sqlite_sql_info.cc
               example/sqlite_sql_info.h
               example/sqlite_statement_batch_reader.cc
               example/sqlite_statement_batch_reader.h
               example/sqlite_statement.cc
               example/sqlite_statement.h
               example/sqlite_tables_schema_batch_reader.cc
               example/sqlite_tables_schema_batch_reader.h
               example/sqlite_type_info.cc
               example/sqlite_type_info.h)
target_link_libraries(flight_sql_test_server
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      gflags
                      ${SQLite3_LIBRARIES})

add_executable(flight_sql_brad_server brad_server_cli.cc
               example/brad_server.cc
               example/brad_server.h
               example/brad_sql_info.cc
               example/brad_sql_info.h
               example/brad_statement_batch_reader.cc
               example/brad_statement_batch_reader.h
               example/brad_statement.cc
               example/brad_statement.h
               example/brad_tables_schema_batch_reader.cc
               example/brad_tables_schema_batch_reader.h)
target_link_libraries(flight_sql_brad_server
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      gflags)


