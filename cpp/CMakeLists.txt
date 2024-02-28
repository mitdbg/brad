cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_definitions("RELEASE=$<CONFIG:Release>")

project(brad)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(ArrowFlightSql REQUIRED)

find_package(gflags REQUIRED)

find_package(SQLite3 REQUIRED)

add_executable(flight_sql_example_client flight_sql_example_client.cc)
target_link_libraries(flight_sql_example_client
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      gflags)

add_executable(flight_sql_example_server flight_sql_example_server.cc
               sqlite_server/sqlite_server.cc
               sqlite_server/sqlite_server.h
               sqlite_server/sqlite_sql_info.cc
               sqlite_server/sqlite_sql_info.h
               sqlite_server/sqlite_statement_batch_reader.cc
               sqlite_server/sqlite_statement_batch_reader.h
               sqlite_server/sqlite_statement.cc
               sqlite_server/sqlite_statement.h
               sqlite_server/sqlite_tables_schema_batch_reader.cc
               sqlite_server/sqlite_tables_schema_batch_reader.h
               sqlite_server/sqlite_type_info.cc
               sqlite_server/sqlite_type_info.h)
target_link_libraries(flight_sql_example_server
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      gflags
                      ${SQLite3_LIBRARIES})

add_executable(flight_sql_brad_server flight_sql_brad_server.cc
               server/brad_server.cc
               server/brad_server.h
               server/brad_sql_info.cc
               server/brad_sql_info.h
               server/brad_statement_batch_reader.cc
               server/brad_statement_batch_reader.h
               server/brad_statement.cc
               server/brad_statement.h
               server/brad_tables_schema_batch_reader.cc
               server/brad_tables_schema_batch_reader.h)
target_link_libraries(flight_sql_brad_server
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      gflags)

add_executable(brad_front_end brad_front_end.cc)
target_link_libraries(brad_front_end
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      gflags)
