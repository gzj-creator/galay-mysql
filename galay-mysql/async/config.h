/**
 * @file config.h
 * @brief 异步MySQL客户端配置
 * @author galay-mysql
 * @version 1.0.0
 *
 * @details 定义了异步MySQL客户端的超时、缓冲区大小等配置参数。
 */

#ifndef GALAY_MYSQL_ASYNC_CONFIG_H
#define GALAY_MYSQL_ASYNC_CONFIG_H

#include <chrono>
#include <cstddef>

namespace galay::mysql
{

/**
 * @brief 异步MySQL超时配置
 * @details 包含发送/接收超时、缓冲区大小以及结果集行预分配提示等参数。
 */
struct AsyncMysqlConfig
{
    std::chrono::milliseconds send_timeout = std::chrono::milliseconds(-1); ///< 发送超时（毫秒），-1表示不超时
    std::chrono::milliseconds recv_timeout = std::chrono::milliseconds(-1); ///< 接收超时（毫秒），-1表示不超时
    size_t buffer_size = 16384; ///< 缓冲区大小（字节）
    size_t result_row_reserve_hint = 0; ///< 结果集行预分配提示（0表示不预分配）

    /**
     * @brief 判断是否启用了发送超时
     * @return 启用时返回true
     */
    bool isSendTimeoutEnabled() const
    {
        return send_timeout >= std::chrono::milliseconds(0);
    }

    /**
     * @brief 判断是否启用了接收超时
     * @return 启用时返回true
     */
    bool isRecvTimeoutEnabled() const
    {
        return recv_timeout >= std::chrono::milliseconds(0);
    }

    /**
     * @brief 创建同时具有发送和接收超时的配置
     * @param send 发送超时时长
     * @param recv 接收超时时长
     * @return 配置对象
     */
    static AsyncMysqlConfig withTimeout(std::chrono::milliseconds send,
                                        std::chrono::milliseconds recv)
    {
        AsyncMysqlConfig cfg;
        cfg.send_timeout = send;
        cfg.recv_timeout = recv;
        return cfg;
    }

    /**
     * @brief 创建仅具有接收超时的配置
     * @param recv 接收超时时长
     * @return 配置对象
     */
    static AsyncMysqlConfig withRecvTimeout(std::chrono::milliseconds recv)
    {
        AsyncMysqlConfig cfg;
        cfg.recv_timeout = recv;
        return cfg;
    }

    /**
     * @brief 创建仅具有发送超时的配置
     * @param send 发送超时时长
     * @return 配置对象
     */
    static AsyncMysqlConfig withSendTimeout(std::chrono::milliseconds send)
    {
        AsyncMysqlConfig cfg;
        cfg.send_timeout = send;
        return cfg;
    }

    /**
     * @brief 创建无超时限制的默认配置
     * @return 配置对象
     */
    static AsyncMysqlConfig noTimeout()
    {
        return {};
    }
};

} // namespace galay::mysql

#endif // GALAY_MYSQL_ASYNC_CONFIG_H
