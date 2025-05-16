#include "broker.h"
#include "session.h"
#include "terminal_ui.h"
#include <algorithm>
#include <iostream>
#include <nlohmann/json.hpp>
#include <pqxx/pqxx>
using json = nlohmann::json;

namespace tinymq
{

    Broker::Broker(uint16_t port, size_t thread_pool_size)
        : io_context_(),
          acceptor_(io_context_, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
          thread_pool_size_(thread_pool_size),
          running_(false)
    {
    }

    Broker::~Broker()
    {
        stop();
    }

    void Broker::loadTopicsFromDatabase()
    {
        try
        {
            pqxx::work txn(*db_conn_);

            pqxx::result result = txn.exec("SELECT name FROM chat_topics");

            for (const auto &row : result)
            {
                std::string topic_name = row["name"].as<std::string>();
                topic_subscribers_[topic_name] = {};
            }

            txn.commit();

            std::cout << "Topics cargados desde la base de datos: " << topic_subscribers_.size() << std::endl;
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error al cargar topics desde la base de datos: " << e.what() << std::endl;
        }
    }

    void Broker::start()
    {
        if (running_)
        {
            return;
        }

        running_ = true;

        accept_connections();

        threads_.reserve(thread_pool_size_);
        for (size_t i = 0; i < thread_pool_size_; ++i)
        {
            threads_.emplace_back([this]()
                                  {
            try {
                io_context_.run();
            } catch (const std::exception& e) {
                ui::print_message("Thread", "Exception: " + std::string(e.what()), ui::MessageType::ERROR);
            } });
        }

        try
        {
            db_conn_ = std::make_unique<pqxx::connection>("dbname=tinymq user=postgres password=leolopez810 host=192.168.0.144 port=5432");

            if (db_conn_->is_open())
            {
                std::cout << "Conexión exitosa a la base de datos: " << db_conn_->dbname() << std::endl;

                loadTopicsFromDatabase();
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error: " << e.what() << std::endl;
        }

        ui::print_message("Broker", "Started on port " + std::to_string(acceptor_.local_endpoint().port()) + " with " + std::to_string(thread_pool_size_) + " threads",
                          ui::MessageType::SUCCESS);
    }

    void Broker::stop()
    {
        if (!running_)
        {
            return;
        }

        running_ = false;

        acceptor_.close();

        io_context_.stop();

        for (auto &thread : threads_)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }

        {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            sessions_.clear();
        }

        {
            std::lock_guard<std::mutex> lock(topics_mutex_);
            topic_subscribers_.clear();
        }

        threads_.clear();

        ui::print_message("Broker", "Stopped", ui::MessageType::INFO);
    }

    void Broker::accept_connections()
    {
        acceptor_.async_accept(
            [this](boost::system::error_code ec, boost::asio::ip::tcp::socket socket)
            {
                if (!ec)
                {
                    auto session = std::make_shared<Session>(std::move(socket), *this);
                    ui::print_message("Broker", "New connection from " + session->remote_endpoint(), ui::MessageType::INCOMING);
                    session->start();
                }
                else
                {
                    ui::print_message("Broker", "Accept error: " + ec.message(), ui::MessageType::ERROR);
                }

                if (running_)
                {
                    accept_connections();
                }
            });
    }

    void Broker::register_session(std::shared_ptr<Session> session)
    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        const auto &client_id = session->client_id();

        auto it = sessions_.find(client_id);
        if (it != sessions_.end())
        {
            std::shared_ptr<Session> old_session = it->second;

            ui::print_message("Broker", "Client ID already in use, disconnecting old session: " + client_id,
                              ui::MessageType::WARNING);

            {
                std::lock_guard<std::mutex> topics_lock(topics_mutex_);
                for (auto &topic_entry : topic_subscribers_)
                {
                    auto &subscribers = topic_entry.second;
                    subscribers.erase(
                        std::remove_if(
                            subscribers.begin(),
                            subscribers.end(),
                            [&old_session](const std::shared_ptr<Session> &s)
                            {
                                return s == old_session;
                            }),
                        subscribers.end());
                }
            }

            it->second.reset();
        }

        try
        {
            pqxx::work txn(*db_conn_);

            pqxx::result result = txn.exec_params(
                "SELECT id FROM chat_users WHERE username = $1", client_id);

            if (result.empty())
            {
                txn.exec_params(
                    "INSERT INTO chat_users (username) VALUES ($1)", client_id);

                std::cout << "Usuario '" << client_id << "' registrado en la base de datos.\n";
            }
            else
            {
                int user_id = result[0]["id"].as<int>();
                std::cout << "Usuario '" << client_id << "' ya existe con ID: " << user_id << "\n";

                pqxx::result topics_result = txn.exec_params(
                    "SELECT t.name FROM chat_subscriptions s "
                    "JOIN chat_topics t ON s.topic_id = t.id "
                    "WHERE s.user_id = $1 AND s.deleted_at IS NULL",
                    user_id);

                std::cout << "Topics suscritos por el usuario:\n";
                for (const auto &row : topics_result)
                {
                    std::string topic_name = row["name"].as<std::string>();
                    std::cout << "- " << topic_name << "\n";
                    auto &subscribers = topic_subscribers_[topic_name];
                    subscribers.push_back(session);
                }
            }

            txn.commit();
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error al registrar usuario: " << e.what() << std::endl;
        }

        sessions_[client_id] = session;
        ui::print_message("Broker", "Session registered: " + client_id, ui::MessageType::SUCCESS);
    }

    void Broker::remove_session(std::shared_ptr<Session> session)
    {
        const auto &client_id = session->client_id();

        if (client_id.empty())
        {
            return;
        }

        {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            sessions_.erase(client_id);
        }

        {
            std::lock_guard<std::mutex> lock(topics_mutex_);
            for (auto &topic_entry : topic_subscribers_)
            {
                auto &subscribers = topic_entry.second;
                subscribers.erase(
                    std::remove_if(
                        subscribers.begin(),
                        subscribers.end(),
                        [&session](const std::shared_ptr<Session> &s)
                        {
                            return s == session;
                        }),
                    subscribers.end());
            }
        }

        ui::print_message("Broker", "Session removed: " + client_id, ui::MessageType::INFO);
    }

    void Broker::subscribe(std::shared_ptr<Session> session, const std::string &topic)
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);

        auto &subscribers = topic_subscribers_[topic];
        if (std::find(subscribers.begin(), subscribers.end(), session) == subscribers.end())
        {
            try
            {
                pqxx::work txn(*db_conn_);

                // Obtener ID del usuario
                pqxx::result user_result = txn.exec_params(
                    "SELECT id FROM chat_users WHERE username = $1", session->client_id());

                if (user_result.empty())
                {
                    std::cerr << "Error: Usuario no encontrado.\n";
                    return;
                }

                int user_id = user_result[0]["id"].as<int>();

                // Verificar si el topic ya existe
                pqxx::result topic_result = txn.exec_params(
                    "SELECT id FROM chat_topics WHERE name = $1", topic);

                int topic_id;
                if (topic_result.empty())
                {
                    pqxx::result insert_result = txn.exec_params(
                        "INSERT INTO chat_topics (name, created_by) VALUES ($1, $2) RETURNING id",
                        topic, user_id);

                    topic_id = insert_result[0]["id"].as<int>();
                    std::cout << "Topic '" << topic << "' creado por usuario ID: " << user_id << "\n";
                }
                else
                {
                    topic_id = topic_result[0]["id"].as<int>();
                    std::cout << "El topic '" << topic << "' ya existe.\n";
                }

                // Insertar subscripcion
                try
                {
                    txn.exec_params(
                        "INSERT INTO chat_subscriptions (user_id, topic_id) VALUES ($1, $2)",
                        user_id, topic_id);
                    std::cout << "Usuario " << user_id << " suscrito al topic " << topic_id << "\n";
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Nota: El usuario ya estaba suscrito o hubo un error: " << e.what() << "\n";
                }

                txn.commit();
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error al registrar topic o suscripción: " << e.what() << std::endl;
            }

            subscribers.push_back(session);
            ui::print_message("Topic", "Client " + session->client_id() + " subscribed to topic: " + topic, ui::MessageType::INFO);
        }
    }

    void Broker::unsubscribe(std::shared_ptr<Session> session, const std::string &topic)
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);
        const auto &client_id = session->client_id();

        try
        {
            pqxx::work txn(*db_conn_);

            // Obtener el ID del usuario
            pqxx::result user_result = txn.exec_params(
                "SELECT id FROM chat_users WHERE username = $1", client_id);

            if (user_result.empty())
            {
                std::cerr << "Usuario no encontrado: " << client_id << std::endl;
                return;
            }

            int user_id = user_result[0]["id"].as<int>();

            // Obtener el ID del topic
            pqxx::result topic_result = txn.exec_params(
                "SELECT id FROM chat_topics WHERE name = $1", topic);

            if (topic_result.empty())
            {
                std::cerr << "Topic no encontrado: " << topic << std::endl;
                return;
            }

            int topic_id = topic_result[0]["id"].as<int>();

            // Verificar que la suscripción exista y esté activa
            pqxx::result sub_result = txn.exec_params(
                "SELECT 1 FROM chat_subscriptions "
                "WHERE user_id = $1 AND topic_id = $2 AND deleted_at IS NULL",
                user_id, topic_id);

            if (sub_result.empty())
            {
                std::cout << "No hay suscripción activa para desuscribir.\n";
            }
            else
            {
                // Marcar como desuscrita
                txn.exec_params(
                    "UPDATE chat_subscriptions "
                    "SET deleted_at = CURRENT_TIMESTAMP "
                    "WHERE user_id = $1 AND topic_id = $2",
                    user_id, topic_id);

                std::cout << "Usuario " << user_id << " desuscrito del topic " << topic_id << "\n";
            }

            txn.commit();
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error desuscribiendo usuario: " << e.what() << std::endl;
        }

        // Find the topic
        auto it = topic_subscribers_.find(topic);
        if (it != topic_subscribers_.end())
        {
            auto &subscribers = it->second;
            subscribers.erase(
                std::remove(subscribers.begin(), subscribers.end(), session),
                subscribers.end());

            ui::print_message("Topic", "Client " + session->client_id() + " unsubscribed from topic: " + topic, ui::MessageType::INFO);

            if (subscribers.empty())
            {
                topic_subscribers_.erase(it);
            }
        }
    }

    void Broker::publish(const std::string& topic, const std::vector<uint8_t>& message) {
        std::vector<std::shared_ptr<Session>> subscribers;
        
        {
            std::lock_guard<std::mutex> lock(topics_mutex_);
            auto it = topic_subscribers_.find(topic);
            if (it != topic_subscribers_.end()) {
                subscribers = it->second;
            }
        }
        
        if (subscribers.empty()) {
            ui::print_message("Topic", "No subscribers for topic: " + topic, ui::MessageType::INFO);
            return;
        }
        
        ui::print_message("Topic", "Publishing to " + std::to_string(subscribers.size()) + 
                       " subscribers on topic: " + topic, ui::MessageType::OUTGOING);
        
        std::vector<uint8_t> payload;
        
        payload.push_back(static_cast<uint8_t>(topic.size()));
        
        payload.insert(payload.end(), topic.begin(), topic.end());
        
        payload.insert(payload.end(), message.begin(), message.end());
        
        Packet packet(PacketType::PUB, 0, payload);
        
        for (auto& subscriber : subscribers) {
            subscriber->send_packet(packet);
        }
    }
} 