#include <iostream>
#include <cassert>
#include <iomanip>
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

int main()
{
    std::cout << "=== T2: MySQL Auth Tests ===" << std::endl;

    testSha1();
    testSha256();
    testXorStrings();
    testNativePasswordAuth();
    testCachingSha2Auth();

    std::cout << "\nAll auth tests PASSED!" << std::endl;
    return 0;
}
