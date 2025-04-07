// bindings.cpp
#include <pybind11/pybind11.h>
#include "broker.h" 
#include "session.h"
#include "packet.h"
#include "terminal_ui.h"

namespace py = pybind11;

PYBIND11_MODULE(tinymq_module, m) {
    m.doc() = "Módulo de Python para controlar el broker TinyMQ";
    py::class_<tinymq::Broker>(m, "Broker")
        .def(py::init<uint16_t, size_t>(),
             py::arg("port") = 1505,
             py::arg("thread_pool_size") = 4,
             "Constructor del Broker TinyMQ")
        .def("start", &tinymq::Broker::start, "Inicia el broker")
        .def("stop", &tinymq::Broker::stop, "Detiene el broker")
        .def("register_session", &tinymq::Broker::register_session, "Registra una sesión")
        .def("remove_session", &tinymq::Broker::remove_session, "Elimina una sesión")
        .def("subscribe", &tinymq::Broker::subscribe, "Suscribe una sesión a un tema")
        .def("unsubscribe", &tinymq::Broker::unsubscribe, "Cancela la suscripción de una sesión a un tema")
        .def("publish", &tinymq::Broker::publish, "Publica un mensaje en un tema");

    py::class_<tinymq::Session, std::shared_ptr<tinymq::Session>>(m, "Session")
        .def("start", &tinymq::Session::start, "Inicia la sesión")
        .def("send_packet", &tinymq::Session::send_packet, "Envía un paquete")
        .def("client_id", &tinymq::Session::client_id, "Obtiene el ID del cliente")
        .def("is_authenticated", &tinymq::Session::is_authenticated, "Verifica si está autenticado")
        .def("remote_endpoint", &tinymq::Session::remote_endpoint, "Obtiene el endpoint remoto");

    py::enum_<tinymq::PacketType>(m, "PacketType")
        .value("CONN", tinymq::PacketType::CONN)
        .value("CONNACK", tinymq::PacketType::CONNACK)
        .value("PUB", tinymq::PacketType::PUB)
        .value("PUBACK", tinymq::PacketType::PUBACK)
        .value("SUB", tinymq::PacketType::SUB)
        .value("SUBACK", tinymq::PacketType::SUBACK)
        .value("UNSUB", tinymq::PacketType::UNSUB)
        .value("UNSUBACK", tinymq::PacketType::UNSUBACK)
        .export_values();

    py::class_<tinymq::Packet>(m, "Packet")
        .def(py::init<tinymq::PacketType, uint8_t, const std::vector<uint8_t>&>(),
             py::arg("type"), py::arg("flags"), py::arg("payload"))
        .def(py::init<>())
        .def("serialize", &tinymq::Packet::serialize, "Serializa el paquete")
        .def("deserialize", &tinymq::Packet::deserialize, "Deserializa el paquete")
        .def("type", &tinymq::Packet::type, "Obtiene el tipo del paquete")
        .def("flags", &tinymq::Packet::flags, "Obtiene los flags del paquete")
        .def("payload", &tinymq::Packet::payload, "Obtiene el payload del paquete");

    py::enum_<tinymq::ui::MessageType>(m, "MessageType")
        .value("INFO", tinymq::ui::MessageType::INFO)
        .value("SUCCESS", tinymq::ui::MessageType::SUCCESS)
        .value("WARNING", tinymq::ui::MessageType::WARNING)
        .value("ERROR", tinymq::ui::MessageType::ERROR)
        .value("INCOMING", tinymq::ui::MessageType::INCOMING)
        .value("OUTGOING", tinymq::ui::MessageType::OUTGOING)
        .value("SYSTEM", tinymq::ui::MessageType::SYSTEM)
        .export_values();

    m.def("get_timestamp", &tinymq::ui::get_timestamp, "Obtiene la marca de tiempo actual");

    m.def("print_message", &tinymq::ui::print_message,
          py::arg("source"), py::arg("message"), py::arg("type") = tinymq::ui::MessageType::INFO,
          "Imprime un mensaje formateado en la terminal");

    m.def("print_divider", &tinymq::ui::print_divider, "Imprime un divisor en la terminal");

    m.def("print_header", &tinymq::ui::print_header,
          py::arg("app_name"), py::arg("version") = "0.1.0",
          "Imprime un encabezado en la terminal");
}

