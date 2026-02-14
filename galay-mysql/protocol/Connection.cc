#include "Connection.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <netdb.h>
#include <cerrno>
#include <cstring>

namespace galay::mysql::protocol
{

Connection::Connection()
    : m_socket_fd(-1)
    , m_connected(false)
    , m_recv_buffer(BUFFER_SIZE)
    , m_recv_pos(0)
    , m_recv_len(0)
{
}

Connection::~Connection()
{
    disconnect();
}

Connection::Connection(Connection&& other) noexcept
    : m_socket_fd(other.m_socket_fd)
    , m_connected(other.m_connected)
    , m_recv_buffer(std::move(other.m_recv_buffer))
    , m_recv_pos(other.m_recv_pos)
    , m_recv_len(other.m_recv_len)
{
    other.m_socket_fd = -1;
    other.m_connected = false;
    other.m_recv_pos = 0;
    other.m_recv_len = 0;
}

Connection& Connection::operator=(Connection&& other) noexcept
{
    if (this != &other) {
        disconnect();
        m_socket_fd = other.m_socket_fd;
        m_connected = other.m_connected;
        m_recv_buffer = std::move(other.m_recv_buffer);
        m_recv_pos = other.m_recv_pos;
        m_recv_len = other.m_recv_len;
        other.m_socket_fd = -1;
        other.m_connected = false;
        other.m_recv_pos = 0;
        other.m_recv_len = 0;
    }
    return *this;
}

std::expected<void, MysqlError> Connection::connect(const std::string& host, uint16_t port, uint32_t timeout_ms)
{
    if (m_connected) {
        disconnect();
    }

    m_socket_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (m_socket_fd < 0) {
        return std::unexpected(MysqlError(MYSQL_ERROR_CONNECTION, "Failed to create socket: " + std::string(strerror(errno))));
    }

    // 设置非阻塞用于超时连接
    int flags = fcntl(m_socket_fd, F_GETFL, 0);
    fcntl(m_socket_fd, F_SETFL, flags | O_NONBLOCK);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        // 尝试DNS解析
        struct addrinfo hints{}, *result = nullptr;
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        int ret = getaddrinfo(host.c_str(), nullptr, &hints, &result);
        if (ret != 0 || !result) {
            ::close(m_socket_fd);
            m_socket_fd = -1;
            return std::unexpected(MysqlError(MYSQL_ERROR_CONNECTION, "Failed to resolve host: " + host));
        }
        addr.sin_addr = reinterpret_cast<struct sockaddr_in*>(result->ai_addr)->sin_addr;
        freeaddrinfo(result);
    }

    int ret = ::connect(m_socket_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (ret < 0 && errno != EINPROGRESS) {
        ::close(m_socket_fd);
        m_socket_fd = -1;
        return std::unexpected(MysqlError(MYSQL_ERROR_CONNECTION, "Connect failed: " + std::string(strerror(errno))));
    }

    if (ret < 0) {
        // 等待连接完成
        struct pollfd pfd{};
        pfd.fd = m_socket_fd;
        pfd.events = POLLOUT;
        int poll_ret = ::poll(&pfd, 1, static_cast<int>(timeout_ms));
        if (poll_ret <= 0) {
            ::close(m_socket_fd);
            m_socket_fd = -1;
            return std::unexpected(MysqlError(MYSQL_ERROR_TIMEOUT, "Connection timed out"));
        }

        int error = 0;
        socklen_t len = sizeof(error);
        getsockopt(m_socket_fd, SOL_SOCKET, SO_ERROR, &error, &len);
        if (error != 0) {
            ::close(m_socket_fd);
            m_socket_fd = -1;
            return std::unexpected(MysqlError(MYSQL_ERROR_CONNECTION, "Connect failed: " + std::string(strerror(error))));
        }
    }

    // 恢复阻塞模式
    fcntl(m_socket_fd, F_SETFL, flags);

    m_connected = true;
    m_recv_pos = 0;
    m_recv_len = 0;
    return {};
}

void Connection::disconnect()
{
    if (m_socket_fd >= 0) {
        ::close(m_socket_fd);
        m_socket_fd = -1;
    }
    m_connected = false;
    m_recv_pos = 0;
    m_recv_len = 0;
}

std::expected<void, MysqlError> Connection::send(const std::string& data)
{
    if (!m_connected) {
        return std::unexpected(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Not connected"));
    }

    size_t total_sent = 0;
    while (total_sent < data.size()) {
        ssize_t n = ::send(m_socket_fd, data.data() + total_sent, data.size() - total_sent, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            m_connected = false;
            return std::unexpected(MysqlError(MYSQL_ERROR_SEND, "Send failed: " + std::string(strerror(errno))));
        }
        if (n == 0) {
            m_connected = false;
            return std::unexpected(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Connection closed during send"));
        }
        total_sent += n;
    }
    return {};
}

std::expected<void, MysqlError> Connection::ensureData(size_t n)
{
    while (m_recv_len - m_recv_pos < n) {
        // 如果缓冲区空间不够，移动数据到开头
        if (m_recv_pos > 0) {
            size_t remaining = m_recv_len - m_recv_pos;
            if (remaining > 0) {
                memmove(m_recv_buffer.data(), m_recv_buffer.data() + m_recv_pos, remaining);
            }
            m_recv_len = remaining;
            m_recv_pos = 0;
        }

        // 如果缓冲区太小，扩容
        if (m_recv_buffer.size() < n) {
            m_recv_buffer.resize(n * 2);
        }

        ssize_t received = ::recv(m_socket_fd, m_recv_buffer.data() + m_recv_len,
                                   m_recv_buffer.size() - m_recv_len, 0);
        if (received < 0) {
            if (errno == EINTR) continue;
            m_connected = false;
            return std::unexpected(MysqlError(MYSQL_ERROR_RECV, "Recv failed: " + std::string(strerror(errno))));
        }
        if (received == 0) {
            m_connected = false;
            return std::unexpected(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Connection closed during recv"));
        }
        m_recv_len += received;
    }
    return {};
}

std::expected<std::string, MysqlError> Connection::recv(size_t expected_len)
{
    auto result = ensureData(expected_len);
    if (!result) return std::unexpected(result.error());

    std::string data(m_recv_buffer.data() + m_recv_pos, expected_len);
    m_recv_pos += expected_len;
    return data;
}

std::expected<std::pair<uint8_t, std::string>, MysqlError> Connection::recvPacket()
{
    // 先读取4字节包头
    auto header_result = ensureData(MYSQL_PACKET_HEADER_SIZE);
    if (!header_result) return std::unexpected(header_result.error());

    const char* header_data = m_recv_buffer.data() + m_recv_pos;
    uint32_t payload_len = readUint24(header_data);
    uint8_t seq_id = static_cast<uint8_t>(header_data[3]);
    m_recv_pos += MYSQL_PACKET_HEADER_SIZE;

    // 读取payload
    auto payload_result = ensureData(payload_len);
    if (!payload_result) return std::unexpected(payload_result.error());

    std::string payload(m_recv_buffer.data() + m_recv_pos, payload_len);
    m_recv_pos += payload_len;

    return std::make_pair(seq_id, std::move(payload));
}

} // namespace galay::mysql::protocol
