#pragma once

#include <boost/asio.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <map>
#include <set>
#include <atomic>
#include <nlohmann/json.hpp>
#include "packet.h"
#include <pqxx/pqxx>

namespace tinymq
{
    class Session;

    class Broker
    {
    public:
        // Modifiqué la firma para que coincida con la implementación en broker.cpp
        Broker(uint16_t port = 1505, size_t thread_pool_size = 4);
        ~Broker();

        void start();
        void stop();
        bool is_running() const { return running_; }

        // Métodos originales - ahora implementados como alias hacia los métodos para Python
        void add_subscriber(const std::string &topic, std::shared_ptr<Session> session)
        {
            subscribe(session, topic);
        }
        void remove_subscriber(std::shared_ptr<Session> session)
        {
            remove_session(session);
        }

        void publish(const std::string &topic, const std::vector<uint8_t> &message);

        // Métodos necesarios para los bindings de Python
        void register_session(std::shared_ptr<Session> session);
        void remove_session(std::shared_ptr<Session> session);
        void subscribe(std::shared_ptr<Session> session, const std::string &topic);
        void unsubscribe(std::shared_ptr<Session> session, const std::string &topic);

    private:
        // Método privado para formatear y reenviar mensajes al storage_service
        void forward_to_storage(const std::string &topic, const std::vector<uint8_t> &message);

        // Método para aceptar nuevas conexiones - cambié el nombre para que coincida
        void accept_connections();

        //Metodo para cargar datos de la DB 
        void loadTopicsFromDatabase();


        // Asio I/O Context
        boost::asio::io_context io_context_;
        boost::asio::ip::tcp::acceptor acceptor_;

        // Hilos y control - actualizado para usar los mismos nombres que en la implementación
        size_t thread_pool_size_;
        std::atomic<bool> running_;
        std::vector<std::thread> threads_;

        // Gestión de tópicos
        std::mutex topics_mutex_;
        std::map<std::string, std::vector<std::shared_ptr<Session>>> topic_subscribers_;
        std::map<std::shared_ptr<Session>, std::set<std::string>> session_topics_;

        // Mutex para la gestión de sesiones
        std::mutex sessions_mutex_;
        std::map<std::string, std::shared_ptr<Session>> sessions_;

        //Db connection
        std::unique_ptr<pqxx::connection> db_conn_;
    };

} // namespace tinymq