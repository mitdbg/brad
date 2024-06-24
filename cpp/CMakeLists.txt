cmake_minimum_required(VERSION 3.16)

option(BRAD_BUILD_EXPERIMENTAL OFF "Set to build the experimental code.")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fPIC")

add_compile_definitions("RELEASE=$<CONFIG:Release>")

project(brad)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(ArrowFlightSql REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(Boost REQUIRED)

add_subdirectory(third_party)

if(BRAD_BUILD_EXPERIMENTAL)
  add_subdirectory(experimental)
endif()

add_library(sqlite_server_lib OBJECT
            sqlite_server/sqlite_server.cc
            sqlite_server/sqlite_sql_info.cc
            sqlite_server/sqlite_statement_batch_reader.cc
            sqlite_server/sqlite_statement.cc
            sqlite_server/sqlite_tables_schema_batch_reader.cc
            sqlite_server/sqlite_type_info.cc)

pybind11_add_module(pybind_brad_server pybind/brad_server.cc
                                       server/brad_server_simple.cc
                                       server/brad_sql_info.cc
                                       server/brad_statement_batch_reader.cc
                                       server/brad_statement.cc
                                       server/brad_tables_schema_batch_reader.cc
                                       server/python_utils.cc)

target_link_libraries(pybind_brad_server
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      PUBLIC libcuckoo)

add_executable(flight_sql_example_client flight_sql_example_client.cc)
target_link_libraries(flight_sql_example_client
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      gflags)

add_executable(flight_sql_example_server flight_sql_example_server.cc)
target_link_libraries(flight_sql_example_server
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      PRIVATE sqlite_server_lib
                      gflags
                      ${SQLite3_LIBRARIES}
                      ${Boost_LIBRARIES})

add_executable(brad_front_end brad_front_end.cc)
target_link_libraries(brad_front_end
                      PRIVATE Arrow::arrow_shared
                      PRIVATE ArrowFlight::arrow_flight_shared
                      PRIVATE ArrowFlightSql::arrow_flight_sql_shared
                      gflags)
