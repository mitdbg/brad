#include <pybind11/pybind11.h>
// #include <arrow/python/pyarrow.h>

#include <iostream>

#include "../server/brad_server_simple.h"

namespace py = pybind11;

PYBIND11_MODULE(pybind_brad_server, m) {
  m.doc() = "BradFlightSqlServer Python bindings";

  py::class_<brad::BradFlightSqlServer> bradServer(m, "BradFlightSqlServer");

  bradServer
    .def(py::init<>())
    .def("create", &brad::BradFlightSqlServer::Create);
    // .def("get_flight_info_statement",
    //      &brad::BradFlightSqlServer::GetFlightInfoStatement)
    // .def("do_get_statement", &brad::BradFlightSqlServer::DoGetStatement);
}
