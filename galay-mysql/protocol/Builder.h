#ifndef GALAY_MYSQL_PROTOCOL_BUILDER_H
#define GALAY_MYSQL_PROTOCOL_BUILDER_H

#include "MysqlProtocol.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace galay::mysql::protocol
{

enum class MysqlCommandKind : uint8_t
{
    Raw = 0,
    Query,
    StmtPrepare,
    InitDb,
    Ping,
    Quit,
    ResetConnection
};

struct MysqlCommandView
{
    std::string_view encoded;
    MysqlCommandKind kind = MysqlCommandKind::Raw;
    uint8_t sequence_id = 0;
};

struct MysqlEncodedBatch
{
    std::string encoded;
    size_t expected_responses = 0;
};

class MysqlCommandBuilder
{
public:
    MysqlCommandBuilder() = default;

    void clear() noexcept;
    void reserve(size_t command_count, size_t encoded_bytes);

    MysqlCommandBuilder& appendQuery(std::string_view sql, uint8_t sequence_id = 0);
    MysqlCommandBuilder& appendStmtPrepare(std::string_view sql, uint8_t sequence_id = 0);
    MysqlCommandBuilder& appendInitDb(std::string_view database, uint8_t sequence_id = 0);
    MysqlCommandBuilder& appendPing(uint8_t sequence_id = 0);
    MysqlCommandBuilder& appendQuit(uint8_t sequence_id = 0);
    MysqlCommandBuilder& appendResetConnection(uint8_t sequence_id = 0);
    MysqlCommandBuilder& appendSimple(CommandType cmd,
                                      std::string_view payload = {},
                                      uint8_t sequence_id = 0,
                                      MysqlCommandKind kind = MysqlCommandKind::Raw);

    // Fast-path: caller guarantees enough reserved capacity.
    MysqlCommandBuilder& appendFast(CommandType cmd,
                                    std::string_view payload,
                                    uint8_t sequence_id = 0,
                                    MysqlCommandKind kind = MysqlCommandKind::Raw);

    [[nodiscard]] std::span<const MysqlCommandView> commands() const;
    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] bool empty() const noexcept;
    [[nodiscard]] const std::string& encoded() const noexcept;

    [[nodiscard]] MysqlEncodedBatch build() const;
    [[nodiscard]] MysqlEncodedBatch release();

private:
    struct Slice
    {
        size_t offset = 0;
        size_t length = 0;
    };

    struct CommandMeta
    {
        Slice encoded;
        MysqlCommandKind kind = MysqlCommandKind::Raw;
        uint8_t sequence_id = 0;
    };

    static void appendPacketHeaderFast(std::string& out, uint32_t payload_len, uint8_t sequence_id);
    static size_t estimateSimplePacketBytes(size_t payload_size) noexcept;
    void appendSimpleFast(CommandType cmd,
                          std::string_view payload,
                          uint8_t sequence_id,
                          MysqlCommandKind kind);
    void rebuildViewsIfNeeded() const;

    std::string m_encoded;
    std::vector<CommandMeta> m_commands;
    mutable std::vector<MysqlCommandView> m_command_views;
    mutable bool m_views_dirty = true;
};

} // namespace galay::mysql::protocol

#endif // GALAY_MYSQL_PROTOCOL_BUILDER_H
