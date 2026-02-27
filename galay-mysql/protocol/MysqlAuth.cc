#include "MysqlAuth.h"
#include <openssl/sha.h>
#include <cstring>
#include <algorithm>

namespace galay::mysql::protocol
{

std::string AuthPlugin::sha1(const std::string& data)
{
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA1(reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash);
    return std::string(reinterpret_cast<char*>(hash), SHA_DIGEST_LENGTH);
}

std::string AuthPlugin::sha256(const std::string& data)
{
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash);
    return std::string(reinterpret_cast<char*>(hash), SHA256_DIGEST_LENGTH);
}

std::string AuthPlugin::xorStrings(const std::string& a, const std::string& b)
{
    size_t min_len = std::min(a.size(), b.size());
    std::string result(min_len, '\0');
    for (size_t i = 0; i < min_len; ++i) {
        result[i] = a[i] ^ b[i];
    }
    return result;
}

std::string AuthPlugin::nativePasswordAuth(const std::string& password, const std::string& salt)
{
    if (password.empty()) {
        return "";
    }

    // SHA1(password)
    std::string hash1 = sha1(password);
    // SHA1(SHA1(password))
    std::string hash2 = sha1(hash1);
    // SHA1(salt + SHA1(SHA1(password)))
    std::string combined = salt + hash2;
    std::string hash3 = sha1(combined);
    // SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
    return xorStrings(hash1, hash3);
}

std::string AuthPlugin::cachingSha2Auth(const std::string& password, const std::string& salt)
{
    if (password.empty()) {
        return "";
    }

    // SHA256(password)
    std::string hash1 = sha256(password);
    // SHA256(SHA256(password))
    std::string hash2 = sha256(hash1);
    // SHA256(SHA256(SHA256(password)) + salt)
    std::string combined = hash2 + salt;
    std::string hash3 = sha256(combined);
    // XOR(SHA256(password), SHA256(SHA256(SHA256(password)) + salt))
    return xorStrings(hash1, hash3);
}

} // namespace galay::mysql::protocol
