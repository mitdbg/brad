#include <pybind11/pybind11.h>

#include <iostream>

#include "../server/brad_server_simple.h"

namespace py = pybind11;

PYBIND11_MODULE(pybind_brad_server, m) {
  m.doc() = "BradFlightSqlServer Python bindings";

  py::class_<brad::BradFlightSqlServer> brad_server(m, "BradFlightSqlServer");

  brad_server
    .def(py::init<>())
    .def("create", &brad::BradFlightSqlServer::Create);
}