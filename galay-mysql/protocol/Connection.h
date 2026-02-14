#ifndef GALAY_MYSQL_PROTOCOL_CONNECTION_H
#define GALAY_MYSQL_PROTOCOL_CONNECTION_H

#include "MysqlProtocol.h"
#include "galay-mysql/base/MysqlError.h"
#include "galay-mysql/base/MysqlConfig.h"
#include <string>
#include <vector>
#include <expected>
#include <cstdint>

namespace galay::mysql::protocol
{

/**
 * @brief 同步TCP连接封装，用于同步MySQL操作
 */
class Connection
{
public:
    Connection();
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&& other) noexcept;
    Connection& operator=(Connection&& other) noexcept;

    /**
     * @brief 连接到服务器
     */
    std::expected<void, MysqlError> connect(const std::string& host, uint16_t port, uint32_t timeout_ms = 5000);

    /**
     * @brief 断开连接
     */
    void disconnect();

    /**
     * @brief 检查连接状态
     */
    bool isConnected() const { return m_connected; }

    /**
     * @brief 发送数据
     */
    std::expected<void, MysqlError> send(const std::string& data);

    /**
     * @brief 接收指定长度的数据
     */
    std::expected<std::string, MysqlError> recv(size_t expected_len);

    /**
     * @brief 接收一个完整的MySQL包
     * @return pair<sequence_id, payload>
     */
    std::expected<std::pair<uint8_t, std::string>, MysqlError> recvPacket();

    /**
     * @brief 获取socket fd
     */
    int fd() const { return m_socket_fd; }

private:
    /**
     * @brief 接收至少n字节到内部缓冲区
     */
    std::expected<void, MysqlError> ensureData(size_t n);

    int m_socket_fd;
    bool m_connected;
    std::vector<char> m_recv_buffer;
    size_t m_recv_pos;
    size_t m_recv_len;
    static constexpr size_t BUFFER_SIZE = 16384;
};

} // namespace galay::mysql::protocol

#endif // GALAY_MYSQL_PROTOCOL_CONNECTION_H
