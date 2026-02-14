#ifndef GALAY_MYSQL_PACKET_H
#define GALAY_MYSQL_PACKET_H

#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include "galay-mysql/base/MysqlValue.h"

namespace galay::mysql::protocol
{

// ======================== MySQL协议常量 ========================

constexpr uint32_t MYSQL_PACKET_HEADER_SIZE = 4;
constexpr uint32_t MYSQL_MAX_PACKET_SIZE = 0xFFFFFF; // 16MB - 1

// MySQL命令类型
enum class CommandType : uint8_t
{
    COM_SLEEP           = 0x00,
    COM_QUIT            = 0x01,
    COM_INIT_DB         = 0x02,
    COM_QUERY           = 0x03,
    COM_FIELD_LIST      = 0x04,
    COM_CREATE_DB       = 0x05,
    COM_DROP_DB         = 0x06,
    COM_REFRESH         = 0x07,
    COM_SHUTDOWN        = 0x08,
    COM_STATISTICS      = 0x09,
    COM_PROCESS_INFO    = 0x0a,
    COM_CONNECT         = 0x0b,
    COM_PROCESS_KILL    = 0x0c,
    COM_DEBUG           = 0x0d,
    COM_PING            = 0x0e,
    COM_CHANGE_USER     = 0x11,
    COM_RESET_CONNECTION= 0x1f,
    COM_STMT_PREPARE    = 0x16,
    COM_STMT_EXECUTE    = 0x17,
    COM_STMT_SEND_LONG_DATA = 0x18,
    COM_STMT_CLOSE      = 0x19,
    COM_STMT_RESET      = 0x1a,
};

// 能力标志
enum CapabilityFlags : uint32_t
{
    CLIENT_LONG_PASSWORD                  = 0x00000001,
    CLIENT_FOUND_ROWS                     = 0x00000002,
    CLIENT_LONG_FLAG                      = 0x00000004,
    CLIENT_CONNECT_WITH_DB                = 0x00000008,
    CLIENT_NO_SCHEMA                      = 0x00000010,
    CLIENT_COMPRESS                       = 0x00000020,
    CLIENT_ODBC                           = 0x00000040,
    CLIENT_LOCAL_FILES                    = 0x00000080,
    CLIENT_IGNORE_SPACE                   = 0x00000100,
    CLIENT_PROTOCOL_41                    = 0x00000200,
    CLIENT_INTERACTIVE                    = 0x00000400,
    CLIENT_SSL                            = 0x00000800,
    CLIENT_IGNORE_SIGPIPE                 = 0x00001000,
    CLIENT_TRANSACTIONS                   = 0x00002000,
    CLIENT_RESERVED                       = 0x00004000,
    CLIENT_SECURE_CONNECTION              = 0x00008000,
    CLIENT_MULTI_STATEMENTS               = 0x00010000,
    CLIENT_MULTI_RESULTS                  = 0x00020000,
    CLIENT_PS_MULTI_RESULTS               = 0x00040000,
    CLIENT_PLUGIN_AUTH                    = 0x00080000,
    CLIENT_CONNECT_ATTRS                  = 0x00100000,
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000,
    CLIENT_DEPRECATE_EOF                  = 0x01000000,
};

// 服务器状态标志
enum ServerStatusFlags : uint16_t
{
    SERVER_STATUS_IN_TRANS             = 0x0001,
    SERVER_STATUS_AUTOCOMMIT           = 0x0002,
    SERVER_MORE_RESULTS_EXISTS         = 0x0008,
    SERVER_STATUS_NO_GOOD_INDEX_USED   = 0x0010,
    SERVER_STATUS_NO_INDEX_USED        = 0x0020,
    SERVER_STATUS_CURSOR_EXISTS        = 0x0040,
    SERVER_STATUS_LAST_ROW_SENT        = 0x0080,
    SERVER_STATUS_DB_DROPPED           = 0x0100,
    SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200,
    SERVER_STATUS_METADATA_CHANGED     = 0x0400,
    SERVER_QUERY_WAS_SLOW              = 0x0800,
    SERVER_PS_OUT_PARAMS               = 0x1000,
    SERVER_STATUS_IN_TRANS_READONLY    = 0x2000,
    SERVER_SESSION_STATE_CHANGED       = 0x4000,
};

// 字符集
enum CharacterSet : uint8_t
{
    CHARSET_UTF8_GENERAL_CI = 33,
    CHARSET_UTF8MB4_GENERAL_CI = 45,
    CHARSET_UTF8MB4_0900_AI_CI = 255,
    CHARSET_BINARY = 63,
};

// ======================== 数据包结构 ========================

/**
 * @brief MySQL包头
 */
struct PacketHeader
{
    uint32_t length = 0;       // 3字节，payload长度
    uint8_t sequence_id = 0;   // 序列号
};

/**
 * @brief 握手包 (HandshakeV10)
 */
struct HandshakeV10
{
    uint8_t protocol_version = 0;
    std::string server_version;
    uint32_t connection_id = 0;
    std::string auth_plugin_data;   // salt (20字节)
    uint32_t capability_flags = 0;
    uint8_t character_set = 0;
    uint16_t status_flags = 0;
    std::string auth_plugin_name;
};

/**
 * @brief 认证响应包 (HandshakeResponse41)
 */
struct HandshakeResponse41
{
    uint32_t capability_flags = 0;
    uint32_t max_packet_size = MYSQL_MAX_PACKET_SIZE;
    uint8_t character_set = CHARSET_UTF8MB4_GENERAL_CI;
    std::string username;
    std::string auth_response;
    std::string database;
    std::string auth_plugin_name;
};

/**
 * @brief OK包
 */
struct OkPacket
{
    uint64_t affected_rows = 0;
    uint64_t last_insert_id = 0;
    uint16_t status_flags = 0;
    uint16_t warnings = 0;
    std::string info;
};

/**
 * @brief ERR包
 */
struct ErrPacket
{
    uint16_t error_code = 0;
    std::string sql_state;      // 5字节
    std::string error_message;
};

/**
 * @brief EOF包
 */
struct EofPacket
{
    uint16_t warnings = 0;
    uint16_t status_flags = 0;
};

/**
 * @brief 列定义包
 */
struct ColumnDefinitionPacket
{
    std::string catalog;
    std::string schema;
    std::string table;
    std::string org_table;
    std::string name;
    std::string org_name;
    uint16_t character_set = 0;
    uint32_t column_length = 0;
    uint8_t column_type = 0;
    uint16_t flags = 0;
    uint8_t decimals = 0;
};

/**
 * @brief 结果集包（解析后的完整结果）
 */
struct ResultSetPacket
{
    uint64_t column_count = 0;
    std::vector<ColumnDefinitionPacket> columns;
    std::vector<std::vector<std::optional<std::string>>> rows;
    // OK包信息（最后的OK/EOF包）
    uint16_t status_flags = 0;
    uint16_t warnings = 0;
};

/**
 * @brief COM_STMT_PREPARE响应
 */
struct StmtPrepareOkPacket
{
    uint32_t statement_id = 0;
    uint16_t num_columns = 0;
    uint16_t num_params = 0;
    uint16_t warning_count = 0;
    std::vector<ColumnDefinitionPacket> param_defs;
    std::vector<ColumnDefinitionPacket> column_defs;
};

/**
 * @brief 响应包类型标识
 */
enum class ResponseType : uint8_t
{
    OK      = 0x00,
    ERR     = 0xFF,
    EOF_PKT = 0xFE,
    LOCAL_INFILE = 0xFB,
};

// ======================== 解析错误 ========================

enum class ParseError
{
    Success,
    Incomplete,
    InvalidFormat,
    InvalidType,
    InvalidLength,
    BufferOverflow,
};

} // namespace galay::mysql::protocol

#endif // GALAY_MYSQL_PACKET_H
