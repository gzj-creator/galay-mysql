#ifndef GALAY_MYSQL_AUTH_H
#define GALAY_MYSQL_AUTH_H

#include <string>

namespace galay::mysql::protocol
{

/**
 * @brief MySQL认证插件
 */
class AuthPlugin
{
public:
    /**
     * @brief mysql_native_password 认证
     * @details SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
     * @param password 明文密码
     * @param salt 服务器发送的salt（20字节）
     * @return 认证数据（20字节）
     */
    static std::string nativePasswordAuth(const std::string& password, const std::string& salt);

    /**
     * @brief caching_sha2_password 认证
     * @details XOR(SHA256(password), SHA256(SHA256(SHA256(password)) + salt))
     * @param password 明文密码
     * @param salt 服务器发送的salt（20字节）
     * @return 认证数据（32字节）
     */
    static std::string cachingSha2Auth(const std::string& password, const std::string& salt);

    /**
     * @brief SHA1哈希
     */
    static std::string sha1(const std::string& data);

    /**
     * @brief SHA256哈希
     */
    static std::string sha256(const std::string& data);

    /**
     * @brief XOR两个等长字符串
     */
    static std::string xorStrings(const std::string& a, const std::string& b);
};

} // namespace galay::mysql::protocol

#endif // GALAY_MYSQL_AUTH_H
