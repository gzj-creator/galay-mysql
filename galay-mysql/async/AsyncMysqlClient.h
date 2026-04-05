#ifndef GALAY_MYSQL_ASYNC_CLIENT_H
#define GALAY_MYSQL_ASYNC_CLIENT_H

#include <galay-kernel/async/TcpSocket.h>
#include <galay-kernel/kernel/Awaitable.h>
#include <galay-kernel/kernel/IOScheduler.hpp>
#include <galay-kernel/kernel/Task.h>
#include <galay-kernel/kernel/Timeout.hpp>
#include <galay-kernel/common/Host.hpp>
#include <galay-kernel/common/Error.h>
#include <memory>
#include <string>
#include <string_view>
#include <span>
#include <array>
#include <expected>
#include <optional>
#include <vector>
#include <coroutine>
#include <utility>
#include <sys/uio.h>
#include "galay-mysql/base/MysqlError.h"
#include "galay-mysql/base/MysqlLog.h"
#include "galay-mysql/base/MysqlValue.h"
#include "galay-mysql/base/MysqlConfig.h"
#include "galay-mysql/protocol/MysqlProtocol.h"
#include "galay-mysql/protocol/MysqlAuth.h"
#include "galay-mysql/protocol/Builder.h"
#include "AsyncMysqlConfig.h"
#include "MysqlBufferProvider.h"

namespace galay::mysql
{

using galay::async::TcpSocket;
using galay::kernel::IOScheduler;
using galay::kernel::Host;
using galay::kernel::IOError;
using galay::kernel::IPType;
using galay::kernel::Task;

using Coroutine = Task<void>;

// 类型别名
using MysqlResult = std::expected<MysqlResultSet, MysqlError>;
using MysqlVoidResult = std::expected<void, MysqlError>;

// 前向声明
class AsyncMysqlClient;

class AsyncMysqlClientBuilder
{
public:
    AsyncMysqlClientBuilder& scheduler(IOScheduler* scheduler)
    {
        m_scheduler = scheduler;
        return *this;
    }

    AsyncMysqlClientBuilder& config(AsyncMysqlConfig config)
    {
        m_config = std::move(config);
        return *this;
    }

    AsyncMysqlClientBuilder& sendTimeout(std::chrono::milliseconds timeout)
    {
        m_config.send_timeout = timeout;
        return *this;
    }

    AsyncMysqlClientBuilder& recvTimeout(std::chrono::milliseconds timeout)
    {
        m_config.recv_timeout = timeout;
        return *this;
    }

    AsyncMysqlClientBuilder& bufferSize(size_t size)
    {
        m_config.buffer_size = size;
        return *this;
    }

    AsyncMysqlClientBuilder& bufferProvider(std::shared_ptr<MysqlBufferProvider> provider)
    {
        m_buffer_provider = std::move(provider);
        return *this;
    }

    AsyncMysqlClientBuilder& resultRowReserveHint(size_t hint)
    {
        m_config.result_row_reserve_hint = hint;
        return *this;
    }

    AsyncMysqlClient build() const;

    AsyncMysqlConfig buildConfig() const
    {
        return m_config;
    }

private:
    IOScheduler* m_scheduler = nullptr;
    AsyncMysqlConfig m_config = AsyncMysqlConfig::noTimeout();
    std::shared_ptr<MysqlBufferProvider> m_buffer_provider;
};

// ======================== MysqlConnectAwaitable ========================

/**
 * @brief MySQL连接等待体
 * @details 通过最新 state-machine awaitable 内核执行 CONNECT -> READV -> SEND -> READV
 */
class MysqlConnectAwaitable
{
public:
    using Result = std::expected<std::optional<bool>, MysqlError>;

    MysqlConnectAwaitable(AsyncMysqlClient& client, MysqlConfig config);
    MysqlConnectAwaitable(MysqlConnectAwaitable&&) noexcept = default;
    MysqlConnectAwaitable& operator=(MysqlConnectAwaitable&&) noexcept = default;
    MysqlConnectAwaitable(const MysqlConnectAwaitable&) = delete;
    MysqlConnectAwaitable& operator=(const MysqlConnectAwaitable&) = delete;

    bool await_ready() { return m_inner.await_ready(); }
    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> handle)
    {
        return m_inner.await_suspend(handle);
    }
    Result await_resume() { return m_inner.await_resume(); }

    bool isInvalid() const;

private:
    enum class AuthStage {
        InitialResponse,
        AwaitFastAuthResult,
        AwaitPublicKey,
        AwaitFinalResult
    };

    enum class Phase {
        Invalid,
        Connect,
        HandshakeRead,
        AuthWrite,
        AuthResultRead,
        Done
    };

    struct SharedState {
        explicit SharedState(AsyncMysqlClient& client, MysqlConfig config);

        AsyncMysqlClient* client = nullptr;
        MysqlConfig config;
        galay::kernel::Host host;
        Phase phase = Phase::Connect;
        protocol::HandshakeV10 handshake;
        AuthStage auth_stage = AuthStage::InitialResponse;
        std::string auth_packet;
        size_t sent = 0;
        bool connected = false;
        std::string parse_scratch;
        std::array<struct iovec, 2> read_iovecs{};
        size_t read_iov_count = 0;
        std::optional<Result> result;
    };

    struct Machine {
        using result_type = Result;
        static constexpr galay::kernel::SequenceOwnerDomain kSequenceOwnerDomain =
            galay::kernel::SequenceOwnerDomain::ReadWrite;

        explicit Machine(std::shared_ptr<SharedState> state);

        galay::kernel::MachineAction<result_type> advance();
        void onConnect(std::expected<void, IOError> result);
        void onRead(std::expected<size_t, IOError> result);
        void onWrite(std::expected<size_t, IOError> result);

    private:
        bool prepareReadWindow();
        std::expected<bool, MysqlError> parseHandshakeFromRingBuffer();
        std::expected<bool, MysqlError> parseAuthResultFromRingBuffer();
        void setError(MysqlError error) noexcept;
        void setConnectError(const IOError& io_error) noexcept;
        void setSendError(const IOError& io_error) noexcept;
        void setRecvError(const std::string& phase, const IOError& io_error) noexcept;
        void completeSuccess() noexcept;

        std::shared_ptr<SharedState> m_state;
    };

    using InnerAwaitable = galay::kernel::StateMachineAwaitable<Machine>;

    std::shared_ptr<SharedState> m_state;
    InnerAwaitable m_inner;
};

// ============= MysqlQueryAwaitable ========================

/**
 * @brief MySQL查询等待体
 * @details 基于 sequence awaitable 链式执行 SEND -> READV，
 *          在“查询包发送完毕”和“结果集解析完毕”两个语义点唤醒。
 */
class MysqlQueryAwaitable
    : public galay::kernel::TimeoutSupport<MysqlQueryAwaitable>
{
public:
    using Result = std::expected<std::optional<MysqlResultSet>, MysqlError>;

    MysqlQueryAwaitable(AsyncMysqlClient& client, std::string_view sql);
    MysqlQueryAwaitable(MysqlQueryAwaitable&&) noexcept = default;
    MysqlQueryAwaitable& operator=(MysqlQueryAwaitable&&) noexcept = default;
    MysqlQueryAwaitable(const MysqlQueryAwaitable&) = delete;
    MysqlQueryAwaitable& operator=(const MysqlQueryAwaitable&) = delete;

    bool await_ready() { return m_inner.await_ready(); }
    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> handle)
    {
        return m_inner.await_suspend(handle);
    }
    Result await_resume() { return m_inner.await_resume(); }
    void markTimeout() { m_inner.markTimeout(); }

    bool isInvalid() const;

private:
    enum class Phase {
        Invalid,
        SendCommand,
        ReceivingHeader,
        ReceivingColumns,
        ReceivingColumnEof,
        ReceivingRows,
        Done
    };

    struct SharedState {
        SharedState(AsyncMysqlClient& client, std::string_view sql);

        AsyncMysqlClient* client = nullptr;
        std::string encoded_cmd;
        Phase phase = Phase::SendCommand;
        size_t sent = 0;
        MysqlResultSet result_set;
        uint64_t column_count = 0;
        size_t columns_received = 0;
        std::string parse_scratch;
        std::array<struct iovec, 2> read_iovecs{};
        size_t read_iov_count = 0;
        std::optional<Result> result;
    };

    struct Machine {
        using result_type = Result;
        static constexpr galay::kernel::SequenceOwnerDomain kSequenceOwnerDomain =
            galay::kernel::SequenceOwnerDomain::ReadWrite;

        explicit Machine(std::shared_ptr<SharedState> state);

        galay::kernel::MachineAction<result_type> advance();
        void onRead(std::expected<size_t, IOError> result);
        void onWrite(std::expected<size_t, IOError> result);

    private:
        bool prepareReadWindow();
        std::expected<bool, MysqlError> tryParseFromRingBuffer();
        void setError(MysqlError error) noexcept;
        void setSendError(const IOError& io_error) noexcept;
        void setRecvError(const IOError& io_error) noexcept;

        std::shared_ptr<SharedState> m_state;
    };

    using InnerAwaitable = galay::kernel::StateMachineAwaitable<Machine>;

    std::shared_ptr<SharedState> m_state;
    InnerAwaitable m_inner;
};

// ======================== MysqlPrepareAwaitable ========================

/**
 * @brief MySQL预处理语句准备等待体
 * @details 发送COM_STMT_PREPARE并接收响应
 */
class MysqlPrepareAwaitable
    : public galay::kernel::TimeoutSupport<MysqlPrepareAwaitable>
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
    using Result = std::expected<std::optional<PrepareResult>, MysqlError>;

    MysqlPrepareAwaitable(AsyncMysqlClient& client, std::string_view sql);
    MysqlPrepareAwaitable(MysqlPrepareAwaitable&&) noexcept = default;
    MysqlPrepareAwaitable& operator=(MysqlPrepareAwaitable&&) noexcept = default;
    MysqlPrepareAwaitable(const MysqlPrepareAwaitable&) = delete;
    MysqlPrepareAwaitable& operator=(const MysqlPrepareAwaitable&) = delete;

    bool await_ready() { return m_inner.await_ready(); }
    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> handle)
    {
        return m_inner.await_suspend(handle);
    }
    Result await_resume() { return m_inner.await_resume(); }
    void markTimeout() { m_inner.markTimeout(); }

    bool isInvalid() const;

private:
    enum class Phase {
        Invalid,
        SendCommand,
        ReceivingPrepareOk,
        ReceivingParamDefs,
        ReceivingParamEof,
        ReceivingColumnDefs,
        ReceivingColumnEof,
        Done
    };

    struct SharedState {
        SharedState(AsyncMysqlClient& client, std::string_view sql);

        AsyncMysqlClient* client = nullptr;
        std::string encoded_cmd;
        Phase phase = Phase::SendCommand;
        size_t sent = 0;
        PrepareResult prepare_result;
        size_t params_received = 0;
        size_t columns_received = 0;
        std::string parse_scratch;
        std::array<struct iovec, 2> read_iovecs{};
        size_t read_iov_count = 0;
        std::optional<Result> result;
    };

    struct Machine {
        using result_type = Result;
        static constexpr galay::kernel::SequenceOwnerDomain kSequenceOwnerDomain =
            galay::kernel::SequenceOwnerDomain::ReadWrite;

        explicit Machine(std::shared_ptr<SharedState> state);

        galay::kernel::MachineAction<result_type> advance();
        void onRead(std::expected<size_t, IOError> result);
        void onWrite(std::expected<size_t, IOError> result);

    private:
        bool prepareReadWindow();
        std::expected<bool, MysqlError> tryParseFromRingBuffer();
        void setError(MysqlError error) noexcept;
        void setSendError(const IOError& io_error) noexcept;
        void setRecvError(const IOError& io_error) noexcept;

        std::shared_ptr<SharedState> m_state;
    };

    using InnerAwaitable = galay::kernel::StateMachineAwaitable<Machine>;

    std::shared_ptr<SharedState> m_state;
    InnerAwaitable m_inner;
};

// ======================== MysqlStmtExecuteAwaitable ========================

/**
 * @brief MySQL预处理语句执行等待体
 * @details 发送COM_STMT_EXECUTE并接收结果集
 */
class MysqlStmtExecuteAwaitable
    : public galay::kernel::TimeoutSupport<MysqlStmtExecuteAwaitable>
{
public:
    using Result = std::expected<std::optional<MysqlResultSet>, MysqlError>;

    MysqlStmtExecuteAwaitable(AsyncMysqlClient& client, std::string encoded_cmd);
    MysqlStmtExecuteAwaitable(MysqlStmtExecuteAwaitable&&) noexcept = default;
    MysqlStmtExecuteAwaitable& operator=(MysqlStmtExecuteAwaitable&&) noexcept = default;
    MysqlStmtExecuteAwaitable(const MysqlStmtExecuteAwaitable&) = delete;
    MysqlStmtExecuteAwaitable& operator=(const MysqlStmtExecuteAwaitable&) = delete;

    bool await_ready() { return m_inner.await_ready(); }
    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> handle)
    {
        return m_inner.await_suspend(handle);
    }
    Result await_resume() { return m_inner.await_resume(); }
    void markTimeout() { m_inner.markTimeout(); }

    bool isInvalid() const;

private:
    enum class Phase {
        Invalid,
        SendCommand,
        ReceivingHeader,
        ReceivingColumns,
        ReceivingColumnEof,
        ReceivingRows,
        Done
    };

    struct SharedState {
        SharedState(AsyncMysqlClient& client, std::string encoded_cmd);

        AsyncMysqlClient* client = nullptr;
        std::string encoded_cmd;
        Phase phase = Phase::SendCommand;
        size_t sent = 0;
        MysqlResultSet result_set;
        uint64_t column_count = 0;
        size_t columns_received = 0;
        std::string parse_scratch;
        std::array<struct iovec, 2> read_iovecs{};
        size_t read_iov_count = 0;
        std::optional<Result> result;
    };

    struct Machine {
        using result_type = Result;
        static constexpr galay::kernel::SequenceOwnerDomain kSequenceOwnerDomain =
            galay::kernel::SequenceOwnerDomain::ReadWrite;

        explicit Machine(std::shared_ptr<SharedState> state);

        galay::kernel::MachineAction<result_type> advance();
        void onRead(std::expected<size_t, IOError> result);
        void onWrite(std::expected<size_t, IOError> result);

    private:
        bool prepareReadWindow();
        std::expected<bool, MysqlError> tryParseFromRingBuffer();
        void setError(MysqlError error) noexcept;
        void setSendError(const IOError& io_error) noexcept;
        void setRecvError(const IOError& io_error) noexcept;

        std::shared_ptr<SharedState> m_state;
    };

    using InnerAwaitable = galay::kernel::StateMachineAwaitable<Machine>;

    std::shared_ptr<SharedState> m_state;
    InnerAwaitable m_inner;
};

// ======================== MysqlPipelineAwaitable ========================

/**
 * @brief MySQL Pipeline等待体
 * @details 批量发送编码后的COM_QUERY包并统一接收/解析响应
 */
class MysqlPipelineAwaitable
    : public galay::kernel::TimeoutSupport<MysqlPipelineAwaitable>
{
public:
    using Result = std::expected<std::optional<std::vector<MysqlResultSet>>, MysqlError>;

    MysqlPipelineAwaitable(AsyncMysqlClient& client,
                           std::span<const protocol::MysqlCommandView> commands);
    MysqlPipelineAwaitable(MysqlPipelineAwaitable&&) noexcept = default;
    MysqlPipelineAwaitable& operator=(MysqlPipelineAwaitable&&) noexcept = default;
    MysqlPipelineAwaitable(const MysqlPipelineAwaitable&) = delete;
    MysqlPipelineAwaitable& operator=(const MysqlPipelineAwaitable&) = delete;

    bool await_ready() { return m_inner.await_ready(); }
    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> handle)
    {
        return m_inner.await_suspend(handle);
    }
    Result await_resume() { return m_inner.await_resume(); }
    void markTimeout() { m_inner.markTimeout(); }

    bool isInvalid() const;

private:
    enum class Phase {
        Invalid,
        SendCommands,
        ReceivingHeader,
        ReceivingColumns,
        ReceivingColumnEof,
        ReceivingRows,
        Done
    };

    struct EncodedSlice {
        size_t offset = 0;
        size_t length = 0;
    };

    struct SharedState {
        SharedState(AsyncMysqlClient& client,
                    std::span<const protocol::MysqlCommandView> commands);

        AsyncMysqlClient* client = nullptr;
        size_t expected_results = 0;
        std::string encoded_buffer;
        std::vector<EncodedSlice> encoded_slices;
        std::vector<struct iovec> write_iovecs;
        size_t write_iov_cursor = 0;
        size_t next_command_index = 0;
        Phase phase = Phase::SendCommands;
        std::vector<MysqlResultSet> results;
        MysqlResultSet current_result;
        uint64_t column_count = 0;
        size_t columns_received = 0;
        std::string parse_scratch;
        std::array<struct iovec, 2> read_iovecs{};
        size_t read_iov_count = 0;
        std::optional<Result> result;
    };

    struct Machine {
        using result_type = Result;
        static constexpr galay::kernel::SequenceOwnerDomain kSequenceOwnerDomain =
            galay::kernel::SequenceOwnerDomain::ReadWrite;

        explicit Machine(std::shared_ptr<SharedState> state);

        galay::kernel::MachineAction<result_type> advance();
        void onRead(std::expected<size_t, IOError> result);
        void onWrite(std::expected<size_t, IOError> result);

    private:
        bool prepareReadWindow();
        size_t pendingWriteIovCount();
        bool advanceAfterWrite(size_t sent_bytes);
        void refillWriteIovWindow();
        void resetCurrentResult();
        void finalizeCurrentResult();
        std::expected<bool, MysqlError> tryParseFromRingBuffer();
        void setError(MysqlError error) noexcept;
        void setSendError(const IOError& io_error) noexcept;
        void setRecvError(const IOError& io_error) noexcept;

        std::shared_ptr<SharedState> m_state;
    };

    using InnerAwaitable = galay::kernel::StateMachineAwaitable<Machine>;

    std::shared_ptr<SharedState> m_state;
    InnerAwaitable m_inner;
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
    AsyncMysqlClient(IOScheduler* scheduler,
                     AsyncMysqlConfig config = AsyncMysqlConfig::noTimeout(),
                     std::shared_ptr<MysqlBufferProvider> buffer_provider = nullptr);

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
    MysqlPipelineAwaitable batch(std::span<const protocol::MysqlCommandView> commands);
    MysqlPipelineAwaitable pipeline(std::span<const std::string_view> sqls);

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
    MysqlBufferHandle& ringBuffer() { return m_ring_buffer; }
    MysqlBufferProvider& bufferProvider() { return m_ring_buffer.provider(); }
    const MysqlBufferProvider& bufferProvider() const { return m_ring_buffer.provider(); }
    protocol::MysqlParser& parser() { return m_parser; }
    protocol::MysqlEncoder& encoder() { return m_encoder; }
    const AsyncMysqlConfig& asyncConfig() const { return m_config; }
    uint32_t serverCapabilities() const { return m_server_capabilities; }
    void setServerCapabilities(uint32_t caps) { m_server_capabilities = caps; }
    MysqlLoggerPtr& logger() { return m_logger; }
    void setLogger(MysqlLoggerPtr logger) { m_logger = std::move(logger); }

private:
    friend class MysqlConnectAwaitable;
    friend class MysqlQueryAwaitable;
    friend class MysqlPrepareAwaitable;
    friend class MysqlStmtExecuteAwaitable;
    friend class MysqlPipelineAwaitable;

    bool m_is_closed = false;
    TcpSocket m_socket;
    IOScheduler* m_scheduler;
    protocol::MysqlParser m_parser;
    protocol::MysqlEncoder m_encoder;
    AsyncMysqlConfig m_config;
    MysqlBufferHandle m_ring_buffer;
    uint32_t m_server_capabilities = 0;

    MysqlLoggerPtr m_logger;
};

inline galay::mysql::AsyncMysqlClient galay::mysql::AsyncMysqlClientBuilder::build() const
{
    return AsyncMysqlClient(m_scheduler, m_config, m_buffer_provider);
}

} // namespace galay::mysql

#endif // GALAY_MYSQL_ASYNC_CLIENT_H
