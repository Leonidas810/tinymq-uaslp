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

            db_conn_ = std::make_unique<pqxx::connection>("dbname=tinymq user=postgres password=<password> host=<host> port=5432");

            if (db_conn_->is_open())
            {
                std::cout << "Conexión exitosa a la base de datos: " << db_conn_->dbname() << std::endl;
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
            subscribers.push_back(session);
            ui::print_message("Topic", "Client " + session->client_id() + " subscribed to topic: " + topic, ui::MessageType::INFO);
        }
    }

    void Broker::unsubscribe(std::shared_ptr<Session> session, const std::string &topic)
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);

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

    bool topic_matches(const std::string &sub, const std::string &pub)
    {
        if (sub == pub)
            return true;
        size_t sub_pos = 0, pub_pos = 0;
        while (sub_pos < sub.size() && pub_pos < pub.size())
        {
            if (sub[sub_pos] == '#')
            {
                // '#' al final del sub-topic
                return sub_pos + 1 == sub.size();
            }
            if (sub[sub_pos] == '+')
            {
                // Salta hasta el siguiente separador en pub
                while (pub_pos < pub.size() && pub[pub_pos] != '/')
                    ++pub_pos;
                ++sub_pos;
                if (pub_pos < pub.size())
                    ++pub_pos;
            }
            else if (sub[sub_pos] == pub[pub_pos])
            {
                ++sub_pos;
                ++pub_pos;
            }
            else
            {
                return false;
            }
        }
        // Permitir '#' al final del sub-topic
        if (sub_pos == sub.size() - 1 && sub[sub_pos] == '#')
            return true;
        return sub_pos == sub.size() && pub_pos == pub.size();
    }

    void Broker::publish(const std::string &topic, const std::vector<uint8_t> &message)
    {
        std::vector<std::shared_ptr<Session>> subscribers;

        {
            std::lock_guard<std::mutex> lock(topics_mutex_);
            for (const auto &entry : topic_subscribers_)
            {
                const std::string &sub_topic = entry.first;
                if (topic_matches(sub_topic, topic))
                {
                    subscribers.insert(subscribers.end(), entry.second.begin(), entry.second.end());
                    ui::print_message("Broker", "Wildcard/prefijo: " + sub_topic + " coincide con publicación en: " + topic, ui::MessageType::INFO);
                }
            }
        }

        if (subscribers.empty())
        {
            ui::print_message("Topic", "No subscribers for topic: " + topic, ui::MessageType::INFO);
        }
        else
        {
            ui::print_message("Topic", "Publishing to " + std::to_string(subscribers.size()) + " subscribers on topic: " + topic, ui::MessageType::OUTGOING);
        }

        std::vector<uint8_t> payload;
        payload.push_back(static_cast<uint8_t>(topic.size()));
        payload.insert(payload.end(), topic.begin(), topic.end());
        payload.insert(payload.end(), message.begin(), message.end());

        Packet packet(PacketType::PUB, 0, payload);

        for (auto &subscriber : subscribers)
        {
            subscriber->send_packet(packet);
        }

        // Reenviar al storage_service si no es un mensaje del sistema
        if (!topic.empty() && topic[0] != '_' && topic != "_broker/storage")
        {
            forward_to_storage(topic, message);
        }
    }

    // ===== NUEVO MÉTODO =====
    void Broker::forward_to_storage(const std::string &topic, const std::vector<uint8_t> &message)
    {
        try
        {
            // Convertir el mensaje a string
            std::string message_str(message.begin(), message.end());

            // Crear el JSON para storage_service
            json storage_payload = {
                {"topic", topic},
                {"message", message_str},
                {"timestamp", std::time(nullptr)}};

            // Convertir a string y luego a bytes
            std::string json_str = storage_payload.dump();
            std::vector<uint8_t> storage_message(json_str.begin(), json_str.end());

            // Publicar directamente al tópico de storage
            std::string storage_topic = "_broker/storage";

            // No usar publish() para evitar recursión infinita
            std::vector<std::shared_ptr<Session>> storage_subscribers;

            {
                std::lock_guard<std::mutex> lock(topics_mutex_);
                auto it = topic_subscribers_.find(storage_topic);
                if (it != topic_subscribers_.end())
                {
                    storage_subscribers = it->second;
                }
            }

            if (!storage_subscribers.empty())
            {
                // Preparar payload para storage_service
                std::vector<uint8_t> storage_payload;
                storage_payload.push_back(static_cast<uint8_t>(storage_topic.size()));
                storage_payload.insert(storage_payload.end(), storage_topic.begin(), storage_topic.end());
                storage_payload.insert(storage_payload.end(), storage_message.begin(), storage_message.end());

                Packet storage_packet(PacketType::PUB, 0, storage_payload);

                // Enviar a todos los suscriptores de storage
                for (auto &subscriber : storage_subscribers)
                {
                    subscriber->send_packet(storage_packet);
                }

                ui::print_message("Storage", "Message forwarded to storage service", ui::MessageType::INFO);
            }
            else
            {
                ui::print_message("Storage", "Storage service not available", ui::MessageType::WARNING);
            }
        }
        catch (const std::exception &e)
        {
            ui::print_message("Storage", "Error forwarding to storage: " + std::string(e.what()),
                              ui::MessageType::ERROR);
        }
    }
}