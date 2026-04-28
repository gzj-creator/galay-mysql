#ifndef GALAY_MYSQL_CONFIG_H
#define GALAY_MYSQL_CONFIG_H

#include <string>
#include <cstdint>

namespace galay::mysql
{

/**
 * @brief MySQL连接配置
 */
struct MysqlConfig
{
    std::string host = "127.0.0.1";
    uint16_t port = 3306;
    std::string username;
    std::string password;
    std::string database;
    std::string charset = "utf8mb4";
    uint32_t connect_timeout_ms = 5000;

    /**
     * @brief 创建默认配置
     */
    static MysqlConfig defaultConfig()
    {
        return {};
    }

    /**
     * @brief 创建指定连接参数的配置
     */
    static MysqlConfig create(const std::string& host, uint16_t port,
                              const std::string& user, const std::string& password,
                              const std::string& database = "")
    {
        MysqlConfig config;
        config.host = host;
        config.port = port;
        config.username = user;
        config.password = password;
        config.database = database;
        return config;
    }
};

} // namespace galay::mysql

#endif // GALAY_MYSQL_CONFIG_H
