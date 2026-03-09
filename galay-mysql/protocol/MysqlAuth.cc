#include "MysqlAuth.h"
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/sha.h>
#include <cstring>
#include <algorithm>

namespace galay::mysql::protocol
{

namespace {

std::string getOpenSslError()
{
    const unsigned long error = ERR_get_error();
    if (error == 0) {
        return "unknown OpenSSL error";
    }
    char buffer[256];
    ERR_error_string_n(error, buffer, sizeof(buffer));
    return std::string(buffer);
}

} // namespace

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

std::expected<std::string, std::string> AuthPlugin::cachingSha2FullAuth(const std::string& password,
                                                                        const std::string& salt,
                                                                        std::string_view pem_public_key)
{
    std::string public_key(pem_public_key);
    if (!public_key.empty() && public_key.back() == '\0') {
        public_key.pop_back();
    }
    if (public_key.empty()) {
        return std::unexpected("empty RSA public key");
    }

    std::string payload = password;
    payload.push_back('\0');

    if (!salt.empty()) {
        for (size_t i = 0; i < payload.size(); ++i) {
            payload[i] ^= salt[i % salt.size()];
        }
    }

    BIO* bio = BIO_new_mem_buf(public_key.data(), static_cast<int>(public_key.size()));
    if (!bio) {
        return std::unexpected("BIO_new_mem_buf failed: " + getOpenSslError());
    }

    EVP_PKEY* pkey = PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (!pkey) {
        return std::unexpected("PEM_read_bio_PUBKEY failed: " + getOpenSslError());
    }

    EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new(pkey, nullptr);
    if (!ctx) {
        EVP_PKEY_free(pkey);
        return std::unexpected("EVP_PKEY_CTX_new failed: " + getOpenSslError());
    }

    if (EVP_PKEY_encrypt_init(ctx) <= 0) {
        EVP_PKEY_CTX_free(ctx);
        EVP_PKEY_free(pkey);
        return std::unexpected("EVP_PKEY_encrypt_init failed: " + getOpenSslError());
    }

    if (EVP_PKEY_CTX_set_rsa_padding(ctx, RSA_PKCS1_OAEP_PADDING) <= 0) {
        EVP_PKEY_CTX_free(ctx);
        EVP_PKEY_free(pkey);
        return std::unexpected("EVP_PKEY_CTX_set_rsa_padding failed: " + getOpenSslError());
    }

    size_t encrypted_size = 0;
    if (EVP_PKEY_encrypt(ctx,
                         nullptr,
                         &encrypted_size,
                         reinterpret_cast<const unsigned char*>(payload.data()),
                         payload.size()) <= 0) {
        EVP_PKEY_CTX_free(ctx);
        EVP_PKEY_free(pkey);
        return std::unexpected("EVP_PKEY_encrypt(size) failed: " + getOpenSslError());
    }

    std::string encrypted(encrypted_size, '\0');
    if (EVP_PKEY_encrypt(ctx,
                         reinterpret_cast<unsigned char*>(encrypted.data()),
                         &encrypted_size,
                         reinterpret_cast<const unsigned char*>(payload.data()),
                         payload.size()) <= 0) {
        EVP_PKEY_CTX_free(ctx);
        EVP_PKEY_free(pkey);
        return std::unexpected("EVP_PKEY_encrypt(data) failed: " + getOpenSslError());
    }

    encrypted.resize(encrypted_size);
    EVP_PKEY_CTX_free(ctx);
    EVP_PKEY_free(pkey);
    return encrypted;
}

} // namespace galay::mysql::protocol
