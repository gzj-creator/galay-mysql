#include <iostream>
#include <cassert>
#include <iomanip>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include "galay-mysql/protocol/MysqlAuth.h"

using namespace galay::mysql::protocol;

void printHex(const std::string& data, const std::string& label)
{
    std::cout << "  " << label << " (" << data.size() << " bytes): ";
    for (unsigned char c : data) {
        std::cout << std::hex << std::setw(2) << std::setfill('0') << (int)c;
    }
    std::cout << std::dec << std::endl;
}

void testSha1()
{
    std::cout << "Testing SHA1..." << std::endl;
    auto hash = AuthPlugin::sha1("hello");
    assert(hash.size() == 20);
    printHex(hash, "SHA1('hello')");
    std::cout << "  PASSED" << std::endl;
}

void testSha256()
{
    std::cout << "Testing SHA256..." << std::endl;
    auto hash = AuthPlugin::sha256("hello");
    assert(hash.size() == 32);
    printHex(hash, "SHA256('hello')");
    std::cout << "  PASSED" << std::endl;
}

void testXorStrings()
{
    std::cout << "Testing XOR strings..." << std::endl;
    std::string a = "\x01\x02\x03\x04";
    std::string b = "\x05\x06\x07\x08";
    auto result = AuthPlugin::xorStrings(a, b);
    assert(result.size() == 4);
    assert(result[0] == '\x04');
    assert(result[1] == '\x04');
    assert(result[2] == '\x04');
    assert(result[3] == '\x0c');
    std::cout << "  PASSED" << std::endl;
}

void testNativePasswordAuth()
{
    std::cout << "Testing mysql_native_password auth..." << std::endl;
    std::string salt = "12345678901234567890";
    auto result = AuthPlugin::nativePasswordAuth("password", salt);
    assert(result.size() == 20);
    printHex(result, "native_password_auth");

    // 空密码应返回空字符串
    auto empty = AuthPlugin::nativePasswordAuth("", salt);
    assert(empty.empty());

    std::cout << "  PASSED" << std::endl;
}

void testCachingSha2Auth()
{
    std::cout << "Testing caching_sha2_password auth..." << std::endl;
    std::string salt = "12345678901234567890";
    auto result = AuthPlugin::cachingSha2Auth("password", salt);
    assert(result.size() == 32);
    printHex(result, "caching_sha2_auth");

    auto empty = AuthPlugin::cachingSha2Auth("", salt);
    assert(empty.empty());

    std::cout << "  PASSED" << std::endl;
}

void testCachingSha2FullAuth()
{
    std::cout << "Testing caching_sha2_password full auth..." << std::endl;

    EVP_PKEY_CTX* keygen_ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
    assert(keygen_ctx != nullptr);
    assert(EVP_PKEY_keygen_init(keygen_ctx) > 0);
    assert(EVP_PKEY_CTX_set_rsa_keygen_bits(keygen_ctx, 2048) > 0);

    EVP_PKEY* private_key = nullptr;
    assert(EVP_PKEY_keygen(keygen_ctx, &private_key) > 0);
    EVP_PKEY_CTX_free(keygen_ctx);
    assert(private_key != nullptr);

    BIO* pub_bio = BIO_new(BIO_s_mem());
    assert(pub_bio != nullptr);
    assert(PEM_write_bio_PUBKEY(pub_bio, private_key) == 1);

    BUF_MEM* pub_mem = nullptr;
    BIO_get_mem_ptr(pub_bio, &pub_mem);
    assert(pub_mem != nullptr);
    std::string public_key_pem(pub_mem->data, pub_mem->length);
    BIO_free(pub_bio);

    const std::string password = "GalayPass_123!";
    const std::string salt = "12345678901234567890";

    auto encrypted = AuthPlugin::cachingSha2FullAuth(password, salt, public_key_pem);
    assert(encrypted.has_value());
    assert(!encrypted->empty());

    EVP_PKEY_CTX* decrypt_ctx = EVP_PKEY_CTX_new(private_key, nullptr);
    assert(decrypt_ctx != nullptr);
    assert(EVP_PKEY_decrypt_init(decrypt_ctx) > 0);
    assert(EVP_PKEY_CTX_set_rsa_padding(decrypt_ctx, RSA_PKCS1_OAEP_PADDING) > 0);

    size_t decrypted_len = 0;
    assert(EVP_PKEY_decrypt(decrypt_ctx,
                            nullptr,
                            &decrypted_len,
                            reinterpret_cast<const unsigned char*>(encrypted->data()),
                            encrypted->size()) > 0);

    std::string decrypted(decrypted_len, '\0');
    assert(EVP_PKEY_decrypt(decrypt_ctx,
                            reinterpret_cast<unsigned char*>(decrypted.data()),
                            &decrypted_len,
                            reinterpret_cast<const unsigned char*>(encrypted->data()),
                            encrypted->size()) > 0);
    decrypted.resize(decrypted_len);

    EVP_PKEY_CTX_free(decrypt_ctx);
    EVP_PKEY_free(private_key);

    std::string expected = password;
    expected.push_back('\0');
    for (size_t index = 0; index < expected.size(); ++index) {
        expected[index] ^= salt[index % salt.size()];
    }

    assert(decrypted == expected);
    std::cout << "  PASSED" << std::endl;
}

int main()
{
    std::cout << "=== T2: MySQL Auth Tests ===" << std::endl;

    testSha1();
    testSha256();
    testXorStrings();
    testNativePasswordAuth();
    testCachingSha2Auth();
    testCachingSha2FullAuth();

    std::cout << "\nAll auth tests PASSED!" << std::endl;
    return 0;
}
