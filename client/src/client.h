#pragma once

#include <boost/asio.hpp>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "packet.h"

namespace tinymq
{
    namespace client
    {

        using MessageCallback = std::function<void(const std::string &, const std::vector<uint8_t> &)>;

        class Client
        {
        public:
            Client(const std::string &client_id,
                   const std::string &host = "localhost",
                   uint16_t port = 1505);

            ~Client();

            bool connect();
            void disconnect();

            bool subscribe(const std::string &topic, const MessageCallback &callback);

            static bool topic_matches(const std::string &sub, const std::string &pub);
            std::vector<std::string> get_history(const std::string &topic, int limit = 10, int timeout = 5);

            // Sobscribe to a topic without a callback
            bool subscribe(const std::string &topic);

            bool unsubscribe(const std::string &topic);
            bool publish(const std::string &topic, const std::vector<uint8_t> &message);
            bool publish(const std::string &topic, const std::string &message);

            bool is_connected() const { return connected_; }

            void poll();

        private:
            void start_read();
            void read_header();
            void read_payload(tinymq::PacketHeader header);
            void process_packet(const tinymq::Packet &packet);

            void handle_connack(const tinymq::Packet &packet);
            void handle_puback(const tinymq::Packet &packet);
            void handle_suback(const tinymq::Packet &packet);
            void handle_unsuback(const tinymq::Packet &packet);
            void handle_publish(const tinymq::Packet &packet);

            bool send_packet(const tinymq::Packet &packet);

        private:
            std::string client_id_;
            std::string host_;
            uint16_t port_;
            bool connected_{false};

            boost::asio::io_context io_context_;
            std::unique_ptr<boost::asio::ip::tcp::socket> socket_;

            std::thread io_thread_;
            std::mutex mutex_;

            std::vector<uint8_t> read_buffer_;
            static constexpr size_t header_length = 4;

            std::unordered_map<std::string, MessageCallback> topic_handlers_;
        };

    } // namespace client
} // namespace tinymq