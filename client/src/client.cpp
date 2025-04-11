#include "client.h"
#include "terminal_ui.h"
#include <chrono>
#include <iostream>

namespace tinymq
{
    namespace client
    {

        Client::Client(const std::string &client_id, const std::string &host, uint16_t port)
            : client_id_(client_id),
              host_(host),
              port_(port),
              read_buffer_(1024)
        {
        }

        Client::~Client()
        {
            disconnect();
        }

        bool Client::connect()
        {
            if (connected_)
            {
                ui::print_message("Client", "Already connected", ui::MessageType::INFO);
                return true;
            }

            try
            {
                ui::print_message("Client", "Connecting to " + host_ + ":" + std::to_string(port_) + " as '" + client_id_ + "'", ui::MessageType::INFO);

                socket_ = std::make_unique<boost::asio::ip::tcp::socket>(io_context_);

                boost::asio::ip::tcp::resolver resolver(io_context_);
                auto endpoints = resolver.resolve(host_, std::to_string(port_));

                boost::asio::connect(*socket_, endpoints);

                std::vector<uint8_t> payload(client_id_.begin(), client_id_.end());
                Packet connect_packet(PacketType::CONN, 0, payload);

                if (!send_packet(connect_packet))
                {
                    ui::print_message("Client", "Failed to send CONNECT packet", ui::MessageType::ERROR);
                    socket_->close();
                    return false;
                }

                io_thread_ = std::thread([this]()
                                         {
            try {
                start_read();
                io_context_.run();
            } catch (const std::exception& e) {
                ui::print_message("Client", "IO thread exception: " + std::string(e.what()), 
                                ui::MessageType::ERROR);
            } });

                std::this_thread::sleep_for(std::chrono::milliseconds(100));

                return connected_;
            }
            catch (const std::exception &e)
            {
                ui::print_message("Client", "Connection error: " + std::string(e.what()), ui::MessageType::ERROR);
                return false;
            }
        }

        void Client::disconnect()
        {
            if (!connected_)
            {
                return;
            }

            std::lock_guard<std::mutex> lock(mutex_);

            connected_ = false;

            ui::print_message("Client", "Disconnecting...", ui::MessageType::INFO);

            if (socket_ && socket_->is_open())
            {
                boost::system::error_code ec;
                socket_->close(ec);
            }

            io_context_.stop();

            if (io_thread_.joinable())
            {
                io_thread_.join();
            }

            topic_handlers_.clear();

            ui::print_message("Client", "Disconnected", ui::MessageType::SUCCESS);
        }

        bool Client::subscribe(const std::string &topic, const MessageCallback &callback)
        {
            if (!connected_)
            {
                ui::print_message("Client", "Not connected", ui::MessageType::ERROR);
                return false;
            }

            ui::print_message("Client", "Subscribing to topic: " + topic, ui::MessageType::INFO);

            std::vector<uint8_t> payload(topic.begin(), topic.end());
            Packet sub_packet(PacketType::SUB, 0, payload);

            {
                std::lock_guard<std::mutex> lock(mutex_);
                topic_handlers_[topic] = callback;
            }

            if (!send_packet(sub_packet))
            {
                ui::print_message("Client", "Failed to send SUB packet for topic: " + topic, ui::MessageType::ERROR);
                return false;
            }

            return true;
        }

        bool Client::subscribe(const std::string &topic)
        {
            return subscribe(topic, nullptr);
        }

        bool Client::unsubscribe(const std::string &topic)
        {
            if (!connected_)
            {
                ui::print_message("Client", "Not connected", ui::MessageType::ERROR);
                return false;
            }

            ui::print_message("Client", "Unsubscribing from topic: " + topic, ui::MessageType::INFO);

            std::vector<uint8_t> payload(topic.begin(), topic.end());
            Packet unsub_packet(PacketType::UNSUB, 0, payload);

            {
                std::lock_guard<std::mutex> lock(mutex_);
                topic_handlers_.erase(topic);
            }

            if (!send_packet(unsub_packet))
            {
                ui::print_message("Client", "Failed to send UNSUB packet for topic: " + topic, ui::MessageType::ERROR);
                return false;
            }

            return true;
        }

        bool Client::publish(const std::string &topic, const std::vector<uint8_t> &message)
        {
            if (!connected_)
            {
                ui::print_message("Client", "Not connected", ui::MessageType::ERROR);
                return false;
            }

            std::string msg_preview;
            for (size_t i = 0; i < std::min(message.size(), size_t(20)); ++i)
            {
                char c = static_cast<char>(message[i]);
                if (isprint(c))
                {
                    msg_preview += c;
                }
                else
                {
                    msg_preview += '?';
                }
            }
            if (message.size() > 20)
            {
                msg_preview += "...";
            }

            ui::print_message("Client", "Publishing to topic '" + topic + "': " + msg_preview, ui::MessageType::OUTGOING);

            std::vector<uint8_t> payload;

            payload.push_back(static_cast<uint8_t>(topic.size()));

            payload.insert(payload.end(), topic.begin(), topic.end());

            payload.insert(payload.end(), message.begin(), message.end());

            Packet pub_packet(PacketType::PUB, 0, payload);

            if (!send_packet(pub_packet))
            {
                ui::print_message("Client", "Failed to publish to topic: " + topic, ui::MessageType::ERROR);
                return false;
            }

            return true;
        }

        bool Client::publish(const std::string &topic, const std::string &message)
        {
            std::vector<uint8_t> message_bytes(message.begin(), message.end());
            return publish(topic, message_bytes);
        }

        void Client::poll()
        {
            io_context_.poll();
        }

        void Client::start_read()
        {
            read_header();
        }

        void Client::read_header()
        {
            if (!socket_ || !socket_->is_open())
            {
                return;
            }

            boost::asio::async_read(
                *socket_,
                boost::asio::buffer(read_buffer_.data(), header_length),
                [this](boost::system::error_code ec, std::size_t length)
                {
                    if (!ec && length == header_length)
                    {
                        PacketHeader header;
                        header.type = static_cast<PacketType>(read_buffer_[0]);
                        header.flags = read_buffer_[1];
                        header.payload_length = (static_cast<uint16_t>(read_buffer_[2]) << 8) | read_buffer_[3];

                        if (header.payload_length > 0)
                        {
                            read_payload(header);
                        }
                        else
                        {
                            Packet packet(header.type, header.flags, {});
                            process_packet(packet);
                        }
                    }
                    else
                    {
                        if (ec != boost::asio::error::eof && ec != boost::asio::error::operation_aborted)
                        {
                            ui::print_message("Client", "Read header error: " + ec.message(), ui::MessageType::ERROR);
                        }
                        disconnect();
                    }
                });
        }

        void Client::read_payload(PacketHeader header)
        {
            if (!socket_ || !socket_->is_open())
            {
                return;
            }

            if (read_buffer_.size() < header.payload_length)
            {
                read_buffer_.resize(header.payload_length);
            }

            boost::asio::async_read(
                *socket_,
                boost::asio::buffer(read_buffer_.data(), header.payload_length),
                [this, header](boost::system::error_code ec, std::size_t length)
                {
                    if (!ec && length == header.payload_length)
                    {
                        std::vector<uint8_t> payload(read_buffer_.begin(), read_buffer_.begin() + length);
                        Packet packet(header.type, header.flags, payload);

                        process_packet(packet);
                    }
                    else
                    {
                        if (ec != boost::asio::error::eof && ec != boost::asio::error::operation_aborted)
                        {
                            ui::print_message("Client", "Read payload error: " + ec.message(), ui::MessageType::ERROR);
                        }
                        disconnect();
                    }
                });
        }

        void Client::process_packet(const Packet &packet)
        {
            switch (packet.type())
            {
            case PacketType::CONNACK:
                handle_connack(packet);
                break;

            case PacketType::PUBACK:
                handle_puback(packet);
                break;

            case PacketType::SUBACK:
                handle_suback(packet);
                break;

            case PacketType::UNSUBACK:
                handle_unsuback(packet);
                break;

            case PacketType::PUB:
                handle_publish(packet);
                break;

            default:
                ui::print_message("Client", "Received unsupported packet type: " + std::to_string(static_cast<int>(packet.type())), ui::MessageType::WARNING);
                break;
            }

            read_header();
        }

        void Client::handle_connack(const Packet &packet)
        {
            ui::print_message("Client", "Connection acknowledged", ui::MessageType::SUCCESS);
            connected_ = true;
        }

        void Client::handle_puback(const Packet &packet)
        {
            ui::print_message("Client", "Publish acknowledged", ui::MessageType::SUCCESS);
        }

        void Client::handle_suback(const Packet &packet)
        {
            ui::print_message("Client", "Subscribe acknowledged", ui::MessageType::SUCCESS);
        }

        void Client::handle_unsuback(const Packet &packet)
        {
            ui::print_message("Client", "Unsubscribe acknowledged", ui::MessageType::SUCCESS);
        }

        void Client::handle_publish(const Packet &packet)
        {
            const auto &payload = packet.payload();

            if (payload.size() > 1)
            {
                uint8_t topic_length = payload[0];

                if (payload.size() > topic_length + 1)
                {
                    std::string topic(payload.begin() + 1, payload.begin() + 1 + topic_length);
                    std::vector<uint8_t> message(payload.begin() + 1 + topic_length, payload.end());

                    std::string msg_preview;
                    for (size_t i = 0; i < std::min(message.size(), size_t(20)); ++i)
                    {
                        char c = static_cast<char>(message[i]);
                        if (isprint(c))
                        {
                            msg_preview += c;
                        }
                        else
                        {
                            msg_preview += '?';
                        }
                    }
                    if (message.size() > 20)
                    {
                        msg_preview += "...";
                    }

                    ui::print_message("Client", "Received message on topic '" + topic + "': " + msg_preview,
                                      ui::MessageType::INCOMING);

                    MessageCallback callback = nullptr;
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        auto it = topic_handlers_.find(topic);
                        if (it != topic_handlers_.end())
                        {
                            callback = it->second;
                        }
                    }

                    // Call the handler if registered
                    if (callback)
                    {
                        callback(topic, message);
                    }
                }
            }
        }

        bool Client::send_packet(const Packet &packet)
        {
            if (!socket_ || !socket_->is_open())
            {
                return false;
            }

            try
            {
                auto serialized = packet.serialize();
                boost::asio::write(*socket_, boost::asio::buffer(serialized));
                return true;
            }
            catch (const std::exception &e)
            {
                ui::print_message("Client", "Send error: " + std::string(e.what()), ui::MessageType::ERROR);
                return false;
            }
        }

    } // namespace client
} // namespace tinymq