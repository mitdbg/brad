#include <pybind11/pybind11.h>

#include <iostream>

// #include "../server/brad_server_simple.h"
#include "../server/brad_server.cc"

namespace py = pybind11;

PYBIND11_MODULE(pybind_brad_server, m) {
  m.doc() = "BradFlightSqlServer Python bindings";

  py::class_<brad::BradFlightSqlServer> bradServer(m, "BradFlightSqlServer");

  bradServer
      .def(py::init<std::shared_ptr<brad::BradFlightSqlServer::Impl>>());
      // .def("create", &brad::BradFlightSqlServer::Create);
      // .def("get_flight_info_statement",
      //      &brad::BradFlightSqlServer::GetFlightInfoStatement);
      // .def("do_get_statement", &brad::BradFlightSqlServer::DoGetStatement);

  py::class_<brad::BradFlightSqlServer::Impl,
             std::shared_ptr<brad::BradFlightSqlServer::Impl>> impl(m, "Impl");

  impl.def(py::init<>());
}
