#ifndef GALAY_MYSQL_ASYNC_CONFIG_H
#define GALAY_MYSQL_ASYNC_CONFIG_H

#include <chrono>
#include <cstddef>

namespace galay::mysql
{

/**
 * @brief 异步MySQL超时配置
 */
struct AsyncMysqlConfig
{
    std::chrono::milliseconds send_timeout = std::chrono::milliseconds(-1);
    std::chrono::milliseconds recv_timeout = std::chrono::milliseconds(-1);
    size_t buffer_size = 16384;
    // 结果集行预分配提示（0表示不预分配）
    size_t result_row_reserve_hint = 0;

    bool isSendTimeoutEnabled() const
    {
        return send_timeout >= std::chrono::milliseconds(0);
    }

    bool isRecvTimeoutEnabled() const
    {
        return recv_timeout >= std::chrono::milliseconds(0);
    }

    static AsyncMysqlConfig withTimeout(std::chrono::milliseconds send,
                                        std::chrono::milliseconds recv)
    {
        AsyncMysqlConfig cfg;
        cfg.send_timeout = send;
        cfg.recv_timeout = recv;
        return cfg;
    }

    static AsyncMysqlConfig withRecvTimeout(std::chrono::milliseconds recv)
    {
        AsyncMysqlConfig cfg;
        cfg.recv_timeout = recv;
        return cfg;
    }

    static AsyncMysqlConfig withSendTimeout(std::chrono::milliseconds send)
    {
        AsyncMysqlConfig cfg;
        cfg.send_timeout = send;
        return cfg;
    }

    static AsyncMysqlConfig noTimeout()
    {
        return {};
    }
};

} // namespace galay::mysql

#endif // GALAY_MYSQL_ASYNC_CONFIG_H
