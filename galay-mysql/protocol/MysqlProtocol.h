#ifndef GALAY_MYSQL_PROTOCOL_H
#define GALAY_MYSQL_PROTOCOL_H

#include "MysqlPacket.h"
#include "MysqlAuth.h"
#include "galay-mysql/base/MysqlConfig.h"
#include <string>
#include <string_view>
#include <span>
#include <vector>
#include <expected>
#include <cstdint>

namespace galay::mysql::protocol
{

// ======================== 辅助函数 ========================

/**
 * @brief 读取length-encoded integer
 * @param data 数据指针
 * @param len 可用长度
 * @param consumed 输出：消耗的字节数
 * @return 解析的整数值
 */
std::expected<uint64_t, ParseError> readLenEncInt(const char* data, size_t len, size_t& consumed);

/**
 * @brief 读取length-encoded string
 * @param data 数据指针
 * @param len 可用长度
 * @param consumed 输出：消耗的字节数
 * @return 解析的字符串
 */
std::expected<std::string, ParseError> readLenEncString(const char* data, size_t len, size_t& consumed);

/**
 * @brief 读取null-terminated string
 */
std::expected<std::string, ParseError> readNullTermString(const char* data, size_t len, size_t& consumed);

/**
 * @brief 读取固定长度整数（小端序）
 */
uint16_t readUint16(const char* data);
uint32_t readUint24(const char* data);
uint32_t readUint32(const char* data);
uint64_t readUint64(const char* data);

/**
 * @brief 写入固定长度整数（小端序）
 */
void writeUint16(std::string& buf, uint16_t val);
void writeUint24(std::string& buf, uint32_t val);
void writeUint32(std::string& buf, uint32_t val);
void writeUint64(std::string& buf, uint64_t val);

/**
 * @brief 写入length-encoded integer
 */
void writeLenEncInt(std::string& buf, uint64_t val);

/**
 * @brief 写入length-encoded string
 */
void writeLenEncString(std::string& buf, std::string_view str);

// ======================== 解析器 ========================

class MysqlParser
{
public:
    MysqlParser() = default;

    /**
     * @brief 解析包头
     * @param data 原始数据（至少4字节）
     * @param len 数据长度
     * @return PacketHeader 或 ParseError
     */
    std::expected<PacketHeader, ParseError> parseHeader(const char* data, size_t len);

    /**
     * @brief 解析握手包
     * @param data payload数据（不含包头）
     * @param len payload长度
     */
    std::expected<HandshakeV10, ParseError> parseHandshake(const char* data, size_t len);

    /**
     * @brief 判断响应类型
     * @param first_byte payload的第一个字节
     * @param payload_len payload长度
     */
    ResponseType identifyResponse(uint8_t first_byte, uint32_t payload_len);

    /**
     * @brief 解析OK包
     * @param data payload数据（不含包头，含0x00标识字节）
     * @param len payload长度
     * @param capabilities 客户端能力标志
     */
    std::expected<OkPacket, ParseError> parseOk(const char* data, size_t len, uint32_t capabilities);

    /**
     * @brief 解析ERR包
     * @param data payload数据（不含包头，含0xFF标识字节）
     * @param len payload长度
     * @param capabilities 客户端能力标志
     */
    std::expected<ErrPacket, ParseError> parseErr(const char* data, size_t len, uint32_t capabilities);

    /**
     * @brief 解析EOF包
     * @param data payload数据（不含包头，含0xFE标识字节）
     * @param len payload长度
     */
    std::expected<EofPacket, ParseError> parseEof(const char* data, size_t len);

    /**
     * @brief 解析列定义包
     * @param data payload数据（不含包头）
     * @param len payload长度
     */
    std::expected<ColumnDefinitionPacket, ParseError> parseColumnDefinition(const char* data, size_t len);

    /**
     * @brief 解析文本协议行数据
     * @param data payload数据（不含包头）
     * @param len payload长度
     * @param column_count 列数
     * @return 一行数据（每列为optional<string>，NULL用nullopt表示）
     */
    std::expected<std::vector<std::optional<std::string>>, ParseError>
    parseTextRow(const char* data, size_t len, size_t column_count);

    /**
     * @brief 解析COM_STMT_PREPARE响应的OK部分
     * @param data payload数据（不含包头）
     * @param len payload长度
     */
    std::expected<StmtPrepareOkPacket, ParseError> parseStmtPrepareOk(const char* data, size_t len);

    /**
     * @brief 从完整的缓冲区中解析一个完整的MySQL包
     * @param data 缓冲区数据
     * @param len 缓冲区长度
     * @param consumed 输出：消耗的总字节数（包头+payload）
     * @return payload数据的起始位置和长度
     */
    struct PacketView {
        const char* payload;
        uint32_t payload_len;
        uint8_t sequence_id;
    };
    std::expected<PacketView, ParseError> extractPacket(const char* data, size_t len, size_t& consumed);
};

// ======================== 编码器 ========================

class MysqlEncoder
{
public:
    MysqlEncoder() = default;

    /**
     * @brief 编码认证响应包
     * @param resp 认证响应数据
     * @param sequence_id 序列号
     * @return 完整的MySQL包（包头+payload）
     */
    std::string encodeHandshakeResponse(const HandshakeResponse41& resp, uint8_t sequence_id);

    /**
     * @brief 编码COM_QUERY命令
     * @param sql SQL语句
     * @param sequence_id 序列号（通常为0）
     * @return 完整的MySQL包
     */
    std::string encodeQuery(std::string_view sql, uint8_t sequence_id = 0);

    /**
     * @brief 编码COM_STMT_PREPARE命令
     * @param sql SQL语句
     * @param sequence_id 序列号
     * @return 完整的MySQL包
     */
    std::string encodeStmtPrepare(std::string_view sql, uint8_t sequence_id = 0);

    /**
     * @brief 编码COM_STMT_EXECUTE命令
     * @param stmt_id 语句ID
     * @param params 参数值（字符串形式）
     * @param param_types 参数类型
     * @param sequence_id 序列号
     * @return 完整的MySQL包
     */
    std::string encodeStmtExecute(uint32_t stmt_id,
                                   std::span<const std::optional<std::string>> params,
                                   std::span<const uint8_t> param_types,
                                   uint8_t sequence_id = 0);
    std::string encodeStmtExecute(uint32_t stmt_id,
                                   std::span<const std::optional<std::string_view>> params,
                                   std::span<const uint8_t> param_types,
                                   uint8_t sequence_id = 0);

    /**
     * @brief 编码COM_STMT_CLOSE命令
     * @param stmt_id 语句ID
     * @param sequence_id 序列号
     * @return 完整的MySQL包
     */
    std::string encodeStmtClose(uint32_t stmt_id, uint8_t sequence_id = 0);

    /**
     * @brief 编码COM_QUIT命令
     * @param sequence_id 序列号
     * @return 完整的MySQL包
     */
    std::string encodeQuit(uint8_t sequence_id = 0);

    /**
     * @brief 编码COM_PING命令
     * @param sequence_id 序列号
     * @return 完整的MySQL包
     */
    std::string encodePing(uint8_t sequence_id = 0);

    /**
     * @brief 编码COM_INIT_DB命令
     * @param database 数据库名
     * @param sequence_id 序列号
     * @return 完整的MySQL包
     */
    std::string encodeInitDb(std::string_view database, uint8_t sequence_id = 0);

    /**
     * @brief 编码COM_RESET_CONNECTION命令
     */
    std::string encodeResetConnection(uint8_t sequence_id = 0);

private:
    /**
     * @brief 给payload添加包头
     * @param payload payload数据
     * @param sequence_id 序列号
     * @return 完整的MySQL包
     */
    std::string wrapPacket(std::string_view payload, uint8_t sequence_id);

    /**
     * @brief 编码简单命令（1字节命令 + 可选payload）
     */
    std::string encodeSimpleCommand(CommandType cmd, std::string_view payload, uint8_t sequence_id);
};

} // namespace galay::mysql::protocol

#endif // GALAY_MYSQL_PROTOCOL_H
