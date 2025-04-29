#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include "client.h"
#include "terminal_ui.h"

namespace py = pybind11;

PYBIND11_MODULE(tinymq_module, m)
{
    py::class_<tinymq::client::Client>(m, "Client")
        .def(py::init<const std::string &, const std::string &, uint16_t>(),
             py::arg("client_id"), py::arg("host") = "localhost", py::arg("port") = 1505)
        .def("connect", &tinymq::client::Client::connect)
        .def("disconnect", &tinymq::client::Client::disconnect)
        //.def("subscribe", &tinymq::client::Client::subscribe)

        // Hacer consulta a la bd para un topico
        .def("get_history", &tinymq::client::Client::get_history,
             py::arg("topic"), py::arg("limit") = 10, py::arg("timeout") = 5)

        .def("subscribe", py::overload_cast<const std::string &, const tinymq::client::MessageCallback &>(
                              &tinymq::client::Client::subscribe))
        .def("subscribe", py::overload_cast<const std::string &>(
                              &tinymq::client::Client::subscribe))

        .def("unsubscribe", &tinymq::client::Client::unsubscribe)
        .def("publish", py::overload_cast<const std::string &, const std::vector<uint8_t> &>(
                            &tinymq::client::Client::publish))
        .def("publish", py::overload_cast<const std::string &, const std::string &>(
                            &tinymq::client::Client::publish))
        .def("is_connected", &tinymq::client::Client::is_connected)
        .def("poll", &tinymq::client::Client::poll);

    py::enum_<tinymq::ui::MessageType>(m, "MessageType")
        .value("INFO", tinymq::ui::MessageType::INFO)
        .value("SUCCESS", tinymq::ui::MessageType::SUCCESS)
        .value("WARNING", tinymq::ui::MessageType::WARNING)
        .value("ERROR", tinymq::ui::MessageType::ERROR)
        .value("INCOMING", tinymq::ui::MessageType::INCOMING)
        .value("OUTGOING", tinymq::ui::MessageType::OUTGOING)
        .value("SYSTEM", tinymq::ui::MessageType::SYSTEM);

    m.def("print_message", &tinymq::ui::print_message,
          py::arg("source"), py::arg("message"), py::arg("type") = tinymq::ui::MessageType::INFO);

    m.def("print_divider", &tinymq::ui::print_divider);

    m.def("print_header", &tinymq::ui::print_header,
          py::arg("app_name"), py::arg("version") = "0.1.0");
}
