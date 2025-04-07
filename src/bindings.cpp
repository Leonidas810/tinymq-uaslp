// bindings.cpp
#include <pybind11/pybind11.h>
#include "broker.h"  // Asegúrate de que la ruta al header sea la correcta
#include "session.h"
#include "packet.h"

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
}

