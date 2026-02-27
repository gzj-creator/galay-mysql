#ifndef GALAY_MYSQL_ASYNC_CLIENT_H
#define GALAY_MYSQL_ASYNC_CLIENT_H

#include <galay-kernel/async/TcpSocket.h>
#include <galay-kernel/kernel/IOScheduler.hpp>
#include <galay-kernel/kernel/Coroutine.h>
#include <galay-kernel/kernel/Timeout.hpp>
#include <galay-kernel/common/Host.hpp>
#include <galay-kernel/common/Buffer.h>
#include <galay-kernel/common/Error.h>
#include <memory>
#include <string>
#include <string_view>
#include <span>
#include <expected>
#include <optional>
#include <vector>
#include <coroutine>
#include "galay-mysql/base/MysqlError.h"
#include "galay-mysql/base/MysqlLog.h"
#include "galay-mysql/base/MysqlValue.h"
#include "galay-mysql/base/MysqlConfig.h"
#include "galay-mysql/protocol/MysqlProtocol.h"
#include "galay-mysql/protocol/MysqlAuth.h"
#include "AsyncMysqlConfig.h"

namespace galay::mysql
{

using galay::async::TcpSocket;
using galay::kernel::IOScheduler;
using galay::kernel::Coroutine;
using galay::kernel::Host;
using galay::kernel::IOError;
using galay::kernel::IPType;
using galay::kernel::RingBuffer;
using galay::kernel::SendAwaitable;
using galay::kernel::ReadvAwaitable;
using galay::kernel::ReadvIOContext;
using galay::kernel::ConnectAwaitable;
using galay::kernel::ConnectIOContext;
using galay::kernel::CustomAwaitable;

// 类型别名
using MysqlResult = std::expected<MysqlResultSet, MysqlError>;
using MysqlVoidResult = std::expected<void, MysqlError>;

// 前向声明
class AsyncMysqlClient;

// ======================== MysqlConnectAwaitable ========================

/**
 * @brief MySQL连接等待体
 * @details 基于CustomAwaitable链式执行 CONNECT -> READV -> SEND -> READV
 */
class MysqlConnectAwaitable : public CustomAwaitable
{
public:
    class ProtocolConnectAwaitable : public ConnectIOContext
    {
    public:
        explicit ProtocolConnectAwaitable(MysqlConnectAwaitable* owner);

#ifdef USE_IOURING
        bool handleComplete(struct io_uring_cqe* cqe, GHandle handle) override;
#else
        bool handleComplete(GHandle handle) override;
#endif

    private:
        MysqlConnectAwaitable* m_owner;
    };

    class ProtocolHandshakeRecvAwaitable : public ReadvIOContext
    {
    public:
        explicit ProtocolHandshakeRecvAwaitable(MysqlConnectAwaitable* owner);

#ifdef USE_IOURING
        bool handleComplete(struct io_uring_cqe* cqe, GHandle handle) override;
#else
        bool handleComplete(GHandle handle) override;
#endif

    private:
        MysqlConnectAwaitable* m_owner;
    };

    class ProtocolAuthSendAwaitable : public SendAwaitable
    {
    public:
        explicit ProtocolAuthSendAwaitable(MysqlConnectAwaitable* owner);

#ifdef USE_IOURING
        bool handleComplete(struct io_uring_cqe* cqe, GHandle handle) override;
#else
        bool handleComplete(GHandle handle) override;
#endif

    private:
        MysqlConnectAwaitable* m_owner;
    };

    class ProtocolAuthResultRecvAwaitable : public ReadvIOContext
    {
    public:
        explicit ProtocolAuthResultRecvAwaitable(MysqlConnectAwaitable* owner);

#ifdef USE_IOURING
        bool handleComplete(struct io_uring_cqe* cqe, GHandle handle) override;
#else
        bool handleComplete(GHandle handle) override;
#endif

    private:
        MysqlConnectAwaitable* m_owner;
    };

    MysqlConnectAwaitable(AsyncMysqlClient& client, MysqlConfig config);

    bool await_ready() const noexcept { return false; }
    using CustomAwaitable::await_suspend;
    std::expected<std::optional<bool>, MysqlError> await_resume();

    bool isInvalid() const { return m_lifecycle == Lifecycle::Invalid; }

private:
    enum class Lifecycle {
        Invalid,
        Running,
        Done
    };

    void reset() noexcept;
    void setError(MysqlError error) noexcept;
    void setConnectError(const IOError& io_error) noexcept;
    void setSendError(const IOError& io_error) noexcept;
    void setRecvError(const std::string& phase, const IOError& io_error) noexcept;
    std::expected<bool, MysqlError> parseHandshakeFromRingBuffer();
    std::expected<bool, MysqlError> parseAuthResultFromRingBuffer();

    AsyncMysqlClient& m_client;
    MysqlConfig m_config;
    Lifecycle m_lifecycle;

    // 握手数据
    protocol::HandshakeV10 m_handshake;
    std::string m_auth_packet;
    size_t m_sent;
    bool m_connected = false;

    ProtocolConnectAwaitable m_connect_awaitable;
    ProtocolHandshakeRecvAwaitable m_handshake_recv_awaitable;
    ProtocolAuthSendAwaitable m_auth_send_awaitable;
    ProtocolAuthResultRecvAwaitable m_auth_result_recv_awaitable;
    std::optional<MysqlError> m_chain_error;
    std::string m_parse_scratch;
};

// ============= MysqlQueryAwaitable ========================

/**
 * @brief MySQL查询等待体
 * @details 基于CustomAwaitable链式执行 SEND -> READV，
 *          在“查询包发送完毕”和“结果集解析完毕”两个语义点唤醒。
 */
class MysqlQueryAwaitable : public CustomAwaitable, public galay::kernel::TimeoutSupport<MysqlQueryAwaitable>
{
public:
    class ProtocolSendAwaitable : public SendAwaitable
    {
    public:
        explicit ProtocolSendAwaitable(MysqlQueryAwaitable* owner);

#ifdef USE_IOURING
        bool handleComplete(struct io_uring_cqe* cqe, GHandle handle) override;
#else
        bool handleComplete(GHandle handle) override;
#endif

    private:
        void syncSendWindow();
        bool handleSendResult();

        MysqlQueryAwaitable* m_owner;
    };

    class ProtocolRecvAwaitable : public ReadvIOContext
    {
    public:
        explicit ProtocolRecvAwaitable(MysqlQueryAwaitable* owner);

#ifdef USE_IOURING
        bool handleComplete(struct io_uring_cqe* cqe, GHandle handle) override;
#else
        bool handleComplete(GHandle handle) override;
#endif

    private:
        bool prepareReadIovecs();
        bool tryParseAndCheckDone();
        bool handleReadResult();

        MysqlQueryAwaitable* m_owner;
    };

    MysqlQueryAwaitable(AsyncMysqlClient& client, std::string_view sql);

    bool await_ready() const noexcept { return false; }
    using CustomAwaitable::await_suspend;
    std::expected<std::optional<MysqlResultSet>, MysqlError> await_resume();

    bool isInvalid() const { return m_lifecycle == Lifecycle::Invalid; }

private:
    enum class Lifecycle {
        Invalid,
        Running,
        Done
    };

    enum class State {
        ReceivingHeader,
        ReceivingColumns,
        ReceivingColumnEof,
        ReceivingRows,
    };

    void reset() noexcept;
    void setError(MysqlError error) noexcept;
    void setSendError(const IOError& io_error) noexcept;
    void setRecvError(const IOError& io_error) noexcept;
    std::expected<bool, MysqlError> tryParseFromRingBuffer();

    AsyncMysqlClient& m_client;
    std::string m_encoded_cmd;
    Lifecycle m_lifecycle;
    State m_state;
    size_t m_sent;

    // 结果集构建
    MysqlResultSet m_result_set;
    uint64_t m_column_count;
    size_t m_columns_received;

    ProtocolSendAwaitable m_send_awaitable;
    ProtocolRecvAwaitable m_recv_awaitable;
    std::optional<MysqlError> m_chain_error;
    std::string m_parse_scratch;

public:
    // TimeoutSupport需要访问此成员
    std::expected<std::optional<MysqlResultSet>, galay::kernel::IOError> m_result;
};

// ======================== MysqlPrepareAwaitable ========================

/**
 * @brief MySQL预处理语句准备等待体
 * @details 发送COM_STMT_PREPARE并接收响应
 */
class MysqlPrepareAwaitable : public CustomAwaitable, public galay::kernel::TimeoutSupport<MysqlPrepareAwaitable>
{
public:
    /**
     * @brief 预处理语句结果
     */
    struct PrepareResult {
        uint32_t statement_id;
        uint16_t num_columns;
        uint16_t num_params;
        std::vector<MysqlField> param_fields;
        std::vector<MysqlField> column_fields;
    };

    class ProtocolSendAwaitable : public SendAwaitable
    {
    public:
        explicit ProtocolSendAwaitable(MysqlPrepareAwaitable* owner);

#ifdef USE_IOURING
        bool handleComplete(struct io_uring_cqe* cqe, GHandle handle) override;
#else
        bool handleComplete(GHandle handle) override;
#endif

    private:
        void syncSendWindow();
        bool handleSendResult();

        MysqlPrepareAwaitable* m_owner;
    };

    class ProtocolRecvAwaitable : public ReadvIOContext
    {
    public:
        explicit ProtocolRecvAwaitable(MysqlPrepareAwaitable* owner);

#ifdef USE_IOURING
        bool handleComplete(struct io_uring_cqe* cqe, GHandle handle) override;
#else
        bool handleComplete(GHandle handle) override;
#endif

    private:
        bool prepareReadIovecs();
        bool tryParseAndCheckDone();
        bool handleReadResult();

        MysqlPrepareAwaitable* m_owner;
    };

    MysqlPrepareAwaitable(AsyncMysqlClient& client, std::string_view sql);

    bool await_ready() const noexcept { return false; }
    using CustomAwaitable::await_suspend;
    std::expected<std::optional<PrepareResult>, MysqlError> await_resume();

    bool isInvalid() const { return m_lifecycle == Lifecycle::Invalid; }

private:
    enum class Lifecycle {
        Invalid,
        Running,
        Done
    };

    enum class State {
        ReceivingPrepareOk,
        ReceivingParamDefs,
        ReceivingParamEof,
        ReceivingColumnDefs,
        ReceivingColumnEof,
    };

    void reset() noexcept;
    void setError(MysqlError error) noexcept;
    void setSendError(const IOError& io_error) noexcept;
    void setRecvError(const IOError& io_error) noexcept;
    std::expected<bool, MysqlError> tryParseFromRingBuffer();

    AsyncMysqlClient& m_client;
    std::string m_encoded_cmd;
    Lifecycle m_lifecycle;
    State m_state;
    size_t m_sent;

    PrepareResult m_prepare_result;
    size_t m_params_received;
    size_t m_columns_received;

    ProtocolSendAwaitable m_send_awaitable;
    ProtocolRecvAwaitable m_recv_awaitable;
    std::optional<MysqlError> m_chain_error;
    std::string m_parse_scratch;

public:
    std::expected<std::optional<PrepareResult>, galay::kernel::IOError> m_result;
};

// ======================== MysqlStmtExecuteAwaitable ========================

/**
 * @brief MySQL预处理语句执行等待体
 * @details 发送COM_STMT_EXECUTE并接收结果集
 */
class MysqlStmtExecuteAwaitable : public CustomAwaitable, public galay::kernel::TimeoutSupport<MysqlStmtExecuteAwaitable>
{
public:
    class ProtocolSendAwaitable : public SendAwaitable
    {
    public:
        explicit ProtocolSendAwaitable(MysqlStmtExecuteAwaitable* owner);

#ifdef USE_IOURING
        bool handleComplete(struct io_uring_cqe* cqe, GHandle handle) override;
#else
        bool handleComplete(GHandle handle) override;
#endif

    private:
        void syncSendWindow();
        bool handleSendResult();

        MysqlStmtExecuteAwaitable* m_owner;
    };

    class ProtocolRecvAwaitable : public ReadvIOContext
    {
    public:
        explicit ProtocolRecvAwaitable(MysqlStmtExecuteAwaitable* owner);

#ifdef USE_IOURING
        bool handleComplete(struct io_uring_cqe* cqe, GHandle handle) override;
#else
        bool handleComplete(GHandle handle) override;
#endif

    private:
        bool prepareReadIovecs();
        bool tryParseAndCheckDone();
        bool handleReadResult();

        MysqlStmtExecuteAwaitable* m_owner;
    };

    MysqlStmtExecuteAwaitable(AsyncMysqlClient& client, std::string encoded_cmd);

    bool await_ready() const noexcept { return false; }
    using CustomAwaitable::await_suspend;
    std::expected<std::optional<MysqlResultSet>, MysqlError> await_resume();

    bool isInvalid() const { return m_lifecycle == Lifecycle::Invalid; }

private:
    enum class Lifecycle {
        Invalid,
        Running,
        Done
    };

    enum class State {
        ReceivingHeader,
        ReceivingColumns,
        ReceivingColumnEof,
        ReceivingRows,
    };

    void reset() noexcept;
    void setError(MysqlError error) noexcept;
    void setSendError(const IOError& io_error) noexcept;
    void setRecvError(const IOError& io_error) noexcept;
    std::expected<bool, MysqlError> tryParseFromRingBuffer();

    AsyncMysqlClient& m_client;
    std::string m_encoded_cmd;
    Lifecycle m_lifecycle;
    State m_state;
    size_t m_sent;

    MysqlResultSet m_result_set;
    uint64_t m_column_count;
    size_t m_columns_received;

    ProtocolSendAwaitable m_send_awaitable;
    ProtocolRecvAwaitable m_recv_awaitable;
    std::optional<MysqlError> m_chain_error;
    std::string m_parse_scratch;

public:
    std::expected<std::optional<MysqlResultSet>, galay::kernel::IOError> m_result;
};

// ======================== AsyncMysqlClient ========================

/**
 * @brief 异步MySQL客户端
 * @details 所有异步接口返回自定义Awaitable值对象（而非Coroutine）
 *
 * @code
 * Coroutine testMysql(IOScheduler* scheduler) {
 *     AsyncMysqlClient client(scheduler);
 *     auto config = MysqlConfig::create("127.0.0.1", 3306, "root", "password", "test_db");
 *     auto connect_result = co_await client.connect(config);
 *     if (!connect_result) { co_return; }
 *
 *     auto result = co_await client.query("SELECT * FROM users");
 *     // 处理结果...
 *
 *     co_await client.close();
 * }
 * @endcode
 */
class AsyncMysqlClient
{
public:
    AsyncMysqlClient(IOScheduler* scheduler, AsyncMysqlConfig config = AsyncMysqlConfig::noTimeout());

    AsyncMysqlClient(AsyncMysqlClient&& other) noexcept;
    AsyncMysqlClient& operator=(AsyncMysqlClient&& other) noexcept;

    AsyncMysqlClient(const AsyncMysqlClient&) = delete;
    AsyncMysqlClient& operator=(const AsyncMysqlClient&) = delete;

    ~AsyncMysqlClient() = default;

    // ======================== 连接 ========================

    MysqlConnectAwaitable connect(MysqlConfig config);
    MysqlConnectAwaitable connect(std::string_view host, uint16_t port,
                                  std::string_view user, std::string_view password,
                                  std::string_view database = "");

    // ======================== 查询 ========================

    MysqlQueryAwaitable query(std::string_view sql);

    // ======================== 预处理语句 ========================

    MysqlPrepareAwaitable prepare(std::string_view sql);
    MysqlStmtExecuteAwaitable stmtExecute(uint32_t stmt_id,
                                          std::span<const std::optional<std::string>> params,
                                          std::span<const uint8_t> param_types = {});
    MysqlStmtExecuteAwaitable stmtExecute(uint32_t stmt_id,
                                          std::span<const std::optional<std::string_view>> params,
                                          std::span<const uint8_t> param_types = {});

    // ======================== 事务 ========================

    MysqlQueryAwaitable beginTransaction();
    MysqlQueryAwaitable commit();
    MysqlQueryAwaitable rollback();

    // ======================== 工具命令 ========================

    MysqlQueryAwaitable ping();
    MysqlQueryAwaitable useDatabase(std::string_view database);

    // ======================== 连接管理 ========================

    auto close() { m_is_closed = true; return m_socket.close(); }
    bool isClosed() const { return m_is_closed; }

    // ======================== 内部访问 ========================

    TcpSocket& socket() { return m_socket; }
    RingBuffer& ringBuffer() { return m_ring_buffer; }
    protocol::MysqlParser& parser() { return m_parser; }
    protocol::MysqlEncoder& encoder() { return m_encoder; }
    uint32_t serverCapabilities() const { return m_server_capabilities; }
    void setServerCapabilities(uint32_t caps) { m_server_capabilities = caps; }
    MysqlLoggerPtr& logger() { return m_logger; }
    void setLogger(MysqlLoggerPtr logger) { m_logger = std::move(logger); }

private:
    friend class MysqlConnectAwaitable;
    friend class MysqlQueryAwaitable;
    friend class MysqlPrepareAwaitable;
    friend class MysqlStmtExecuteAwaitable;

    bool m_is_closed = false;
    TcpSocket m_socket;
    IOScheduler* m_scheduler;
    protocol::MysqlParser m_parser;
    protocol::MysqlEncoder m_encoder;
    AsyncMysqlConfig m_config;
    RingBuffer m_ring_buffer;
    uint32_t m_server_capabilities = 0;

    MysqlLoggerPtr m_logger;
};

} // namespace galay::mysql

#endif // GALAY_MYSQL_ASYNC_CLIENT_H
