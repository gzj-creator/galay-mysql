#ifndef GALAY_MYSQL_SYNC_CLIENT_H
#define GALAY_MYSQL_SYNC_CLIENT_H

#include "galay-mysql/base/MysqlConfig.h"
#include "galay-mysql/base/MysqlError.h"
#include "galay-mysql/base/MysqlValue.h"
#include "galay-mysql/protocol/Builder.h"
#include "galay-mysql/protocol/MysqlAuth.h"
#include "galay-mysql/protocol/MysqlProtocol.h"

#include <galay-kernel/common/Buffer.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <sys/uio.h>
#include <utility>
#include <vector>

namespace galay::mysql
{

using MysqlResult = std::expected<MysqlResultSet, MysqlError>;
using MysqlVoidResult = std::expected<void, MysqlError>;
using MysqlBatchResult = std::expected<std::vector<MysqlResultSet>, MysqlError>;

/**
 * @brief 同步MySQL客户端（readv/writev + ring buffer）
 */
class MysqlClient
{
public:
    MysqlClient();
    ~MysqlClient();

    MysqlClient(const MysqlClient&) = delete;
    MysqlClient& operator=(const MysqlClient&) = delete;
    MysqlClient(MysqlClient&& other) noexcept;
    MysqlClient& operator=(MysqlClient&& other) noexcept;

    // ======================== 连接 ========================

    MysqlVoidResult connect(const MysqlConfig& config);
    MysqlVoidResult connect(const std::string& host, uint16_t port,
                            const std::string& user, const std::string& password,
                            const std::string& database = "");

    // ======================== 查询 ========================

    MysqlResult query(const std::string& sql);
    MysqlBatchResult batch(std::span<const protocol::MysqlCommandView> commands);
    MysqlBatchResult pipeline(std::span<const std::string_view> sqls);

    // ======================== 预处理语句 ========================

    struct PrepareResult {
        uint32_t statement_id;
        uint16_t num_columns;
        uint16_t num_params;
    };

    std::expected<PrepareResult, MysqlError> prepare(const std::string& sql);
    MysqlResult stmtExecute(uint32_t stmt_id,
                            const std::vector<std::optional<std::string>>& params,
                            const std::vector<uint8_t>& param_types = {});
    MysqlVoidResult stmtClose(uint32_t stmt_id);

    // ======================== 事务 ========================

    MysqlVoidResult beginTransaction();
    MysqlVoidResult commit();
    MysqlVoidResult rollback();

    // ======================== 工具 ========================

    MysqlVoidResult ping();
    MysqlVoidResult useDatabase(const std::string& database);

    // ======================== 连接管理 ========================

    void close();
    bool isConnected() const { return m_connected; }

private:
    using Packet = std::pair<uint8_t, std::string>;

    static constexpr size_t kRecvBufferCapacity = 256 * 1024;

    MysqlVoidResult connectSocket(const std::string& host, uint16_t port, uint32_t timeout_ms);
    void closeSocket() noexcept;

    MysqlVoidResult sendAll(std::string_view data);
    MysqlVoidResult sendAllv(std::span<const struct iovec> iovecs);

    MysqlVoidResult recvIntoRingBuffer();
    std::expected<std::optional<Packet>, MysqlError> tryExtractPacket();
    std::expected<Packet, MysqlError> recvPacket();

    MysqlResult receiveResultSet();
    MysqlVoidResult executeSimple(const std::string& sql);

    int m_socket_fd;
    bool m_connected;
    galay::kernel::RingBuffer m_recv_ring_buffer;
    std::string m_parse_scratch;

    protocol::MysqlParser m_parser;
    protocol::MysqlEncoder m_encoder;
    uint32_t m_server_capabilities = 0;
};

} // namespace galay::mysql

#endif // GALAY_MYSQL_SYNC_CLIENT_H
