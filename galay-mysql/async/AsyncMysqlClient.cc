#include "AsyncMysqlClient.h"
#include "galay-mysql/base/MysqlLog.h"
#include "galay-mysql/protocol/Builder.h"
#include <concepts>
#include <sys/uio.h>
#include <utility>

namespace galay::mysql
{

namespace detail
{

template<typename Fn>
concept IoErrorCallback = std::invocable<Fn, const IOError&>;

template<typename Fn>
concept VoidCallback = std::invocable<Fn>;

template<typename Fn>
concept ParseErrorCallback = std::invocable<Fn, MysqlError>;

template<typename Fn>
concept ParseFn = requires(Fn&& fn) {
    { std::forward<Fn>(fn)() } -> std::same_as<std::expected<bool, MysqlError>>;
};

inline void syncSendWindow(const std::string& payload, size_t sent, const char*& buffer, size_t& length)
{
    if (sent >= payload.size()) {
        buffer = nullptr;
        length = 0;
        return;
    }
    buffer = payload.data() + sent;
    length = payload.size() - sent;
}

template<typename OnIoError, typename OnZeroSend, typename OnDone>
requires IoErrorCallback<OnIoError> &&
         VoidCallback<OnZeroSend> &&
         VoidCallback<OnDone>
bool handleSendResult(std::expected<size_t, IOError>& io_result,
                      size_t& sent,
                      size_t total,
                      OnIoError&& on_io_error,
                      OnZeroSend&& on_zero_send,
                      OnDone&& on_done)
{
    if (!io_result.has_value()) {
        on_io_error(io_result.error());
        return true;
    }

    const size_t sent_once = io_result.value();
    if (sent_once == 0) {
        on_zero_send();
        return true;
    }

    sent += sent_once;
    if (sent >= total) {
        on_done();
        return true;
    }
    return false;
}

bool prepareRecvWindow(MysqlBufferHandle& ring_buffer, std::vector<struct iovec>& iovecs)
{
    struct iovec raw_iovecs[2];
    const size_t count = ring_buffer.getWriteIovecs(raw_iovecs, 2);
    iovecs.assign(raw_iovecs, raw_iovecs + count);
    return !iovecs.empty();
}

template<typename ParseFnType, typename OnParseError>
requires ParseFn<ParseFnType> &&
         ParseErrorCallback<OnParseError>
bool parseOrSetError(ParseFnType&& parse_fn, OnParseError&& on_parse_error)
{
    auto parsed = parse_fn();
    if (!parsed.has_value()) {
        on_parse_error(std::move(parsed.error()));
        return true;
    }
    return parsed.value();
}

template<typename BufferLike, typename OnIoError, typename OnClosed, typename ParseFnType, typename OnParseError>
requires IoErrorCallback<OnIoError> &&
         VoidCallback<OnClosed> &&
         ParseFn<ParseFnType> &&
         ParseErrorCallback<OnParseError>
bool handleReadResult(std::expected<size_t, IOError>& io_result,
                      BufferLike& ring_buffer,
                      OnIoError&& on_io_error,
                      OnClosed&& on_closed,
                      ParseFnType&& parse_fn,
                      OnParseError&& on_parse_error)
{
    if (!io_result.has_value()) {
        on_io_error(io_result.error());
        return true;
    }

    const size_t n = io_result.value();
    if (n == 0) {
        on_closed();
        return true;
    }

    ring_buffer.produce(n);
    return detail::parseOrSetError(std::forward<ParseFnType>(parse_fn),
                                   std::forward<OnParseError>(on_parse_error));
}

MysqlError toTimeoutOrInternalError(const IOError& io_error)
{
    if (IOError::contains(io_error.code(), galay::kernel::kTimeout)) {
        return MysqlError(MYSQL_ERROR_TIMEOUT, io_error.message());
    }
    return MysqlError(MYSQL_ERROR_INTERNAL, io_error.message());
}

inline std::string_view linearizeReadIovecs(std::span<const struct iovec> iovecs, std::string& scratch)
{
    if (iovecs.size() == 1) {
        return std::string_view(static_cast<const char*>(iovecs[0].iov_base),
                                iovecs[0].iov_len);
    }
    scratch.clear();
    for (const auto& iov : iovecs) {
        scratch.append(static_cast<const char*>(iov.iov_base), iov.iov_len);
    }
    return std::string_view(scratch);
}

inline std::string buildSingleCommandPacket(protocol::CommandType cmd,
                                            std::string_view payload,
                                            protocol::MysqlCommandKind kind)
{
    protocol::MysqlCommandBuilder builder;
    builder.reserve(1, protocol::MYSQL_PACKET_HEADER_SIZE + 1 + payload.size());
    builder.appendFast(cmd, payload, 0, kind);
    return std::move(builder.release().encoded);
}

#ifdef IOV_MAX
constexpr int kPipelineWritevMaxIov = IOV_MAX > 0 ? IOV_MAX : 1024;
#else
constexpr int kPipelineWritevMaxIov = 1024;
#endif

}

// ======================== MysqlConnectAwaitable ========================

MysqlConnectAwaitable::ProtocolConnectAwaitable::ProtocolConnectAwaitable(MysqlConnectAwaitable* owner)
    : ConnectIOContext(Host(IPType::IPV4, owner->m_config.host, owner->m_config.port))
    , m_owner(owner)
{
}

#ifdef USE_IOURING
bool MysqlConnectAwaitable::ProtocolConnectAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!ConnectIOContext::handleComplete(cqe, handle)) {
        return false;
    }

    if (!m_result.has_value()) {
        m_owner->setConnectError(m_result.error());
        return true;
    }

    m_owner->m_client.m_socket.option().handleNonBlock();
    return true;
}
#else
bool MysqlConnectAwaitable::ProtocolConnectAwaitable::handleComplete(GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    if (!ConnectIOContext::handleComplete(handle)) {
        return false;
    }

    if (!m_result.has_value()) {
        m_owner->setConnectError(m_result.error());
        return true;
    }

    m_owner->m_client.m_socket.option().handleNonBlock();
    return true;
}
#endif

MysqlConnectAwaitable::ProtocolHandshakeRecvAwaitable::ProtocolHandshakeRecvAwaitable(MysqlConnectAwaitable* owner)
    : ReadvIOContext({})
    , m_owner(owner)
{
    m_iovecs.reserve(2);
}

#ifdef USE_IOURING
bool MysqlConnectAwaitable::ProtocolHandshakeRecvAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    if (detail::parseOrSetError([&]() { return m_owner->parseHandshakeFromRingBuffer(); },
                                [&](MysqlError err) { m_owner->setError(std::move(err)); })) {
        return true;
    }

    if (!detail::prepareRecvWindow(m_owner->m_client.m_ring_buffer, m_iovecs)) {
        m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space while reading handshake"));
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!ReadvIOContext::handleComplete(cqe, handle)) {
        return false;
    }

    return detail::handleReadResult(
        m_result,
        m_owner->m_client.m_ring_buffer,
        [&](const IOError& io_error) { m_owner->setRecvError("handshake", io_error); },
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Connection closed during handshake")); },
        [&]() { return m_owner->parseHandshakeFromRingBuffer(); },
        [&](MysqlError err) { m_owner->setError(std::move(err)); }
    );
}
#else
bool MysqlConnectAwaitable::ProtocolHandshakeRecvAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        if (detail::parseOrSetError([&]() { return m_owner->parseHandshakeFromRingBuffer(); },
                                    [&](MysqlError err) { m_owner->setError(std::move(err)); })) {
            return true;
        }

        if (!detail::prepareRecvWindow(m_owner->m_client.m_ring_buffer, m_iovecs)) {
            m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space while reading handshake"));
            return true;
        }

        if (!ReadvIOContext::handleComplete(handle)) {
            return false;
        }

        if (detail::handleReadResult(
                m_result,
                m_owner->m_client.m_ring_buffer,
                [&](const IOError& io_error) { m_owner->setRecvError("handshake", io_error); },
                [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Connection closed during handshake")); },
                [&]() { return m_owner->parseHandshakeFromRingBuffer(); },
                [&](MysqlError err) { m_owner->setError(std::move(err)); })) {
            return true;
        }
    }
    return true;
}
#endif

MysqlConnectAwaitable::ProtocolAuthSendAwaitable::ProtocolAuthSendAwaitable(MysqlConnectAwaitable* owner)
    : WritevIOContext({})
    , m_owner(owner)
{
    m_iovecs.reserve(1);
}

void MysqlConnectAwaitable::ProtocolAuthSendAwaitable::syncSendIovecs()
{
    detail::syncSendWindow(m_owner->m_auth_packet, m_owner->m_sent, m_buffer, m_length);
    m_iovecs.clear();
    if (m_length == 0 || m_buffer == nullptr) {
        return;
    }
    m_iovecs.push_back(iovec{const_cast<char*>(m_buffer), m_length});
}

#ifdef USE_IOURING
bool MysqlConnectAwaitable::ProtocolAuthSendAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    syncSendIovecs();
    if (m_iovecs.empty()) {
        m_owner->m_client.m_ring_buffer.clear();
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!WritevIOContext::handleComplete(cqe, handle)) {
        return false;
    }

    return detail::handleSendResult(
        m_result,
        m_owner->m_sent,
        m_owner->m_auth_packet.size(),
        [&](const IOError& io_error) { m_owner->setSendError(io_error); },
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_SEND, "Send returned 0 bytes")); },
        [&]() { m_owner->m_client.m_ring_buffer.clear(); }
    );
}
#else
bool MysqlConnectAwaitable::ProtocolAuthSendAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        syncSendIovecs();
        if (m_iovecs.empty()) {
            m_owner->m_client.m_ring_buffer.clear();
            return true;
        }

        if (!WritevIOContext::handleComplete(handle)) {
            return false;
        }

        if (detail::handleSendResult(
                m_result,
                m_owner->m_sent,
                m_owner->m_auth_packet.size(),
                [&](const IOError& io_error) { m_owner->setSendError(io_error); },
                [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_SEND, "Send returned 0 bytes")); },
                [&]() { m_owner->m_client.m_ring_buffer.clear(); })) {
            return true;
        }
    }
    return true;
}
#endif

MysqlConnectAwaitable::ProtocolAuthResultRecvAwaitable::ProtocolAuthResultRecvAwaitable(MysqlConnectAwaitable* owner)
    : ReadvIOContext({})
    , m_owner(owner)
{
    m_iovecs.reserve(2);
}

#ifdef USE_IOURING
bool MysqlConnectAwaitable::ProtocolAuthResultRecvAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    if (detail::parseOrSetError([&]() { return m_owner->parseAuthResultFromRingBuffer(); },
                                [&](MysqlError err) { m_owner->setError(std::move(err)); })) {
        return true;
    }

    if (!detail::prepareRecvWindow(m_owner->m_client.m_ring_buffer, m_iovecs)) {
        m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space while reading auth result"));
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!ReadvIOContext::handleComplete(cqe, handle)) {
        return false;
    }

    return detail::handleReadResult(
        m_result,
        m_owner->m_client.m_ring_buffer,
        [&](const IOError& io_error) { m_owner->setRecvError("auth", io_error); },
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Connection closed during auth")); },
        [&]() { return m_owner->parseAuthResultFromRingBuffer(); },
        [&](MysqlError err) { m_owner->setError(std::move(err)); }
    );
}
#else
bool MysqlConnectAwaitable::ProtocolAuthResultRecvAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        if (detail::parseOrSetError([&]() { return m_owner->parseAuthResultFromRingBuffer(); },
                                    [&](MysqlError err) { m_owner->setError(std::move(err)); })) {
            return true;
        }

        if (!detail::prepareRecvWindow(m_owner->m_client.m_ring_buffer, m_iovecs)) {
            m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space while reading auth result"));
            return true;
        }

        if (!ReadvIOContext::handleComplete(handle)) {
            return false;
        }

        if (detail::handleReadResult(
                m_result,
                m_owner->m_client.m_ring_buffer,
                [&](const IOError& io_error) { m_owner->setRecvError("auth", io_error); },
                [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Connection closed during auth")); },
                [&]() { return m_owner->parseAuthResultFromRingBuffer(); },
                [&](MysqlError err) { m_owner->setError(std::move(err)); })) {
            return true;
        }
    }
    return true;
}
#endif

MysqlConnectAwaitable::MysqlConnectAwaitable(AsyncMysqlClient& client, MysqlConfig config)
    : CustomAwaitable(client.m_socket.controller())
    , m_client(client)
    , m_config(std::move(config))
    , m_lifecycle(Lifecycle::Running)
    , m_handshake()
    , m_auth_packet()
    , m_sent(0)
    , m_connected(false)
    , m_connect_awaitable(this)
    , m_handshake_recv_awaitable(this)
    , m_auth_send_awaitable(this)
    , m_auth_result_recv_awaitable(this)
    , m_chain_error(std::nullopt)
{
    addTask(IOEventType::CONNECT, &m_connect_awaitable);
    addTask(IOEventType::READV, &m_handshake_recv_awaitable);
    addTask(IOEventType::SEND, &m_auth_send_awaitable);
    addTask(IOEventType::READV, &m_auth_result_recv_awaitable);
}

void MysqlConnectAwaitable::reset() noexcept
{
    m_lifecycle = Lifecycle::Invalid;
    m_handshake = protocol::HandshakeV10{};
    m_auth_packet.clear();
    m_sent = 0;
    m_connected = false;
    m_chain_error.reset();
}

void MysqlConnectAwaitable::setError(MysqlError error) noexcept
{
    m_chain_error = std::move(error);
    m_lifecycle = Lifecycle::Invalid;
}

void MysqlConnectAwaitable::setConnectError(const IOError& io_error) noexcept
{
    setError(MysqlError(MYSQL_ERROR_CONNECTION, io_error.message()));
}

void MysqlConnectAwaitable::setSendError(const IOError& io_error) noexcept
{
    setError(MysqlError(MYSQL_ERROR_SEND, io_error.message()));
}

void MysqlConnectAwaitable::setRecvError(const std::string& phase, const IOError& io_error) noexcept
{
    setError(MysqlError(MYSQL_ERROR_RECV, io_error.message() + " during " + phase));
}

std::expected<bool, MysqlError> MysqlConnectAwaitable::parseHandshakeFromRingBuffer()
{
    struct iovec read_iovecs[2];
    const size_t read_iovecs_count = m_client.m_ring_buffer.getReadIovecs(read_iovecs, 2);
    if (read_iovecs_count == 0) {
        return false;
    }

    auto linear = detail::linearizeReadIovecs(
        std::span<const struct iovec>(read_iovecs, read_iovecs_count),
        m_parse_scratch);
    const char* data = linear.data();
    size_t len = linear.size();

    size_t consumed = 0;
    auto pkt = m_client.m_parser.extractPacket(data, len, consumed);
    if (!pkt) {
        if (pkt.error() == protocol::ParseError::Incomplete) {
            return false;
        }
        return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse handshake packet"));
    }

    if (static_cast<uint8_t>(pkt->payload[0]) == 0xFF) {
        auto err = m_client.m_parser.parseErr(pkt->payload, pkt->payload_len, protocol::CLIENT_PROTOCOL_41);
        m_client.m_ring_buffer.consume(consumed);
        if (err) {
            return std::unexpected(MysqlError(MYSQL_ERROR_SERVER, err->error_code, err->error_message));
        }
        return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse handshake ERR packet"));
    }

    auto hs = m_client.m_parser.parseHandshake(pkt->payload, pkt->payload_len);
    m_client.m_ring_buffer.consume(consumed);
    if (!hs) {
        return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse handshake packet body"));
    }
    m_handshake = std::move(hs.value());

    protocol::HandshakeResponse41 resp;
    resp.capability_flags = protocol::CLIENT_PROTOCOL_41
        | protocol::CLIENT_SECURE_CONNECTION
        | protocol::CLIENT_PLUGIN_AUTH
        | protocol::CLIENT_TRANSACTIONS
        | protocol::CLIENT_MULTI_STATEMENTS
        | protocol::CLIENT_MULTI_RESULTS
        | protocol::CLIENT_PS_MULTI_RESULTS
        | protocol::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    if (!m_config.database.empty()) {
        resp.capability_flags |= protocol::CLIENT_CONNECT_WITH_DB;
    }
    resp.capability_flags &= m_handshake.capability_flags;
    m_client.m_server_capabilities = resp.capability_flags;
    resp.character_set = protocol::CHARSET_UTF8MB4_GENERAL_CI;
    resp.username = m_config.username;
    resp.database = m_config.database;
    resp.auth_plugin_name = m_handshake.auth_plugin_name;

    if (m_handshake.auth_plugin_name == "mysql_native_password") {
        resp.auth_response = protocol::AuthPlugin::nativePasswordAuth(m_config.password, m_handshake.auth_plugin_data);
    } else if (m_handshake.auth_plugin_name == "caching_sha2_password") {
        resp.auth_response = protocol::AuthPlugin::cachingSha2Auth(m_config.password, m_handshake.auth_plugin_data);
    } else {
        resp.auth_response = protocol::AuthPlugin::nativePasswordAuth(m_config.password, m_handshake.auth_plugin_data);
        resp.auth_plugin_name = "mysql_native_password";
    }

    m_auth_packet = m_client.m_encoder.encodeHandshakeResponse(resp, pkt->sequence_id + 1);
    m_sent = 0;
    return true;
}

std::expected<bool, MysqlError> MysqlConnectAwaitable::parseAuthResultFromRingBuffer()
{
    while (true) {
        struct iovec read_iovecs[2];
        const size_t read_iovecs_count = m_client.m_ring_buffer.getReadIovecs(read_iovecs, 2);
        if (read_iovecs_count == 0) {
            return false;
        }

        auto linear = detail::linearizeReadIovecs(
            std::span<const struct iovec>(read_iovecs, read_iovecs_count),
            m_parse_scratch);
        const char* data = linear.data();
        size_t len = linear.size();

        size_t consumed = 0;
        auto pkt = m_client.m_parser.extractPacket(data, len, consumed);
        if (!pkt) {
            if (pkt.error() == protocol::ParseError::Incomplete) {
                return false;
            }
            return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse auth response packet"));
        }

        const uint8_t first_byte = static_cast<uint8_t>(pkt->payload[0]);
        m_client.m_ring_buffer.consume(consumed);

        if (first_byte == 0x00) {
            m_connected = true;
            m_lifecycle = Lifecycle::Done;
            MysqlLogInfo(m_client.m_logger, "MySQL connected successfully to {}:{}", m_config.host, m_config.port);
            return true;
        }

        if (first_byte == 0xFF) {
            auto err = m_client.m_parser.parseErr(pkt->payload, pkt->payload_len, m_client.m_server_capabilities);
            if (err) {
                return std::unexpected(MysqlError(MYSQL_ERROR_AUTH, err->error_code, err->error_message));
            }
            return std::unexpected(MysqlError(MYSQL_ERROR_AUTH, "Authentication failed"));
        }

        if (first_byte == 0x01) {
            if (pkt->payload_len == 2 && static_cast<uint8_t>(pkt->payload[1]) == 0x03) {
                continue;
            }
            return std::unexpected(MysqlError(MYSQL_ERROR_AUTH,
                                              "Full authentication not supported, use mysql_native_password"));
        }

        if (first_byte == 0xFE) {
            return std::unexpected(MysqlError(MYSQL_ERROR_AUTH, "Auth switch is not supported"));
        }

        return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Unexpected auth response packet"));
    }
}

std::expected<std::optional<bool>, MysqlError> MysqlConnectAwaitable::await_resume()
{
    onCompleted();

    if (m_chain_error.has_value()) {
        auto err = std::move(*m_chain_error);
        reset();
        return std::unexpected(std::move(err));
    }

    if (m_lifecycle != Lifecycle::Done || !m_connected) {
        reset();
        return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Connect awaitable did not reach done state"));
    }

    reset();
    return std::optional<bool>(true);
}

// ======================== MysqlQueryAwaitable ========================

MysqlQueryAwaitable::ProtocolSendAwaitable::ProtocolSendAwaitable(MysqlQueryAwaitable* owner)
    : WritevIOContext({})
    , m_owner(owner)
{
    m_iovecs.reserve(1);
}

void MysqlQueryAwaitable::ProtocolSendAwaitable::syncSendIovecs()
{
    detail::syncSendWindow(m_owner->m_encoded_cmd, m_owner->m_sent, m_buffer, m_length);
    m_iovecs.clear();
    if (m_length == 0 || m_buffer == nullptr) {
        return;
    }
    m_iovecs.push_back(iovec{const_cast<char*>(m_buffer), m_length});
}

bool MysqlQueryAwaitable::ProtocolSendAwaitable::handleSendResult()
{
    return detail::handleSendResult(
        m_result,
        m_owner->m_sent,
        m_owner->m_encoded_cmd.size(),
        [&](const IOError& io_error) { m_owner->setSendError(io_error); },
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_SEND, "Send returned 0 bytes")); },
        [&]() { m_owner->m_client.m_ring_buffer.clear(); }
    );
}

#ifdef USE_IOURING
bool MysqlQueryAwaitable::ProtocolSendAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    syncSendIovecs();
    if (m_iovecs.empty()) {
        m_owner->m_client.m_ring_buffer.clear();
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!WritevIOContext::handleComplete(cqe, handle)) {
        return false;
    }
    return handleSendResult();
}
#else
bool MysqlQueryAwaitable::ProtocolSendAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        syncSendIovecs();
        if (m_iovecs.empty()) {
            m_owner->m_client.m_ring_buffer.clear();
            return true;
        }

        if (!WritevIOContext::handleComplete(handle)) {
            return false;
        }
        if (handleSendResult()) {
            return true;
        }
    }
    return true;
}
#endif

MysqlQueryAwaitable::ProtocolRecvAwaitable::ProtocolRecvAwaitable(MysqlQueryAwaitable* owner)
    : ReadvIOContext({})
    , m_owner(owner)
{
    m_iovecs.reserve(2);
}

bool MysqlQueryAwaitable::ProtocolRecvAwaitable::prepareRecvWindow()
{
    if (!detail::prepareRecvWindow(m_owner->m_client.m_ring_buffer, m_iovecs)) {
        m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space"));
        return false;
    }
    return true;
}

bool MysqlQueryAwaitable::ProtocolRecvAwaitable::tryParseAndCheckDone()
{
    return detail::parseOrSetError(
        [&]() { return m_owner->tryParseFromRingBuffer(); },
        [&](MysqlError err) { m_owner->setError(std::move(err)); }
    );
}

bool MysqlQueryAwaitable::ProtocolRecvAwaitable::handleReadResult()
{
    return detail::handleReadResult(
        m_result,
        m_owner->m_client.m_ring_buffer,
        [&](const IOError& io_error) { m_owner->setRecvError(io_error); },
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Connection closed")); },
        [&]() { return m_owner->tryParseFromRingBuffer(); },
        [&](MysqlError err) { m_owner->setError(std::move(err)); }
    );
}

#ifdef USE_IOURING
bool MysqlQueryAwaitable::ProtocolRecvAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    if (tryParseAndCheckDone()) {
        return true;
    }

    if (!prepareRecvWindow()) {
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!ReadvIOContext::handleComplete(cqe, handle)) {
        return false;
    }
    return handleReadResult();
}
#else
bool MysqlQueryAwaitable::ProtocolRecvAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        if (tryParseAndCheckDone()) {
            return true;
        }

        if (!prepareRecvWindow()) {
            return true;
        }

        if (!ReadvIOContext::handleComplete(handle)) {
            return false;
        }

        if (handleReadResult()) {
            return true;
        }
    }
    return true;
}
#endif

MysqlQueryAwaitable::MysqlQueryAwaitable(AsyncMysqlClient& client, std::string_view sql)
    : CustomAwaitable(client.m_socket.controller())
    , m_client(client)
    , m_encoded_cmd(detail::buildSingleCommandPacket(protocol::CommandType::COM_QUERY,
                                                     sql,
                                                     protocol::MysqlCommandKind::Query))
    , m_lifecycle(Lifecycle::Running)
    , m_state(State::ReceivingHeader)
    , m_sent(0)
    , m_column_count(0)
    , m_columns_received(0)
    , m_send_awaitable(this)
    , m_recv_awaitable(this)
    , m_chain_error(std::nullopt)
    , m_result(std::nullopt)
{
    if (m_client.m_config.result_row_reserve_hint > 0) {
        m_result_set.reserveRows(m_client.m_config.result_row_reserve_hint);
    }
    addTask(IOEventType::SEND, &m_send_awaitable);
    addTask(IOEventType::READV, &m_recv_awaitable);
}

void MysqlQueryAwaitable::reset() noexcept
{
    m_lifecycle = Lifecycle::Invalid;
    m_state = State::ReceivingHeader;
    m_result_set = MysqlResultSet{};
    if (m_client.m_config.result_row_reserve_hint > 0) {
        m_result_set.reserveRows(m_client.m_config.result_row_reserve_hint);
    }
    m_sent = 0;
    m_column_count = 0;
    m_columns_received = 0;
    m_chain_error.reset();
    m_result = std::nullopt;
}

void MysqlQueryAwaitable::setError(MysqlError error) noexcept
{
    m_chain_error = std::move(error);
    m_lifecycle = Lifecycle::Invalid;
}

void MysqlQueryAwaitable::setSendError(const IOError& io_error) noexcept
{
    MysqlLogDebug(m_client.m_logger, "send query failed: {}", io_error.message());
    setError(MysqlError(MYSQL_ERROR_SEND, io_error.message()));
}

void MysqlQueryAwaitable::setRecvError(const IOError& io_error) noexcept
{
    MysqlLogDebug(m_client.m_logger, "recv query failed: {}", io_error.message());
    setError(MysqlError(MYSQL_ERROR_RECV, io_error.message()));
}

std::expected<bool, MysqlError> MysqlQueryAwaitable::tryParseFromRingBuffer()
{
    while (true) {
        struct iovec read_iovecs[2];
        const size_t read_iovecs_count = m_client.m_ring_buffer.getReadIovecs(read_iovecs, 2);
        if (read_iovecs_count == 0) {
            return false;
        }

        auto linear = detail::linearizeReadIovecs(
            std::span<const struct iovec>(read_iovecs, read_iovecs_count),
            m_parse_scratch);
        const char* data = linear.data();
        size_t len = linear.size();

        size_t consumed = 0;
        auto pkt = m_client.m_parser.extractPacket(data, len, consumed);
        if (!pkt) {
            if (pkt.error() == protocol::ParseError::Incomplete) {
                return false;
            }
            return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Parse query packet failed"));
        }

        const uint8_t first_byte = static_cast<uint8_t>(pkt->payload[0]);
        const uint32_t caps = m_client.m_server_capabilities;

        if (m_state == State::ReceivingHeader) {
            if (first_byte == 0xFF) {
                auto err = m_client.m_parser.parseErr(pkt->payload, pkt->payload_len, caps);
                m_client.m_ring_buffer.consume(consumed);
                if (err) {
                    return std::unexpected(MysqlError(MYSQL_ERROR_SERVER, err->error_code, err->error_message));
                }
                return std::unexpected(MysqlError(MYSQL_ERROR_QUERY, "Query failed"));
            }

            if (first_byte == 0x00) {
                auto ok = m_client.m_parser.parseOk(pkt->payload, pkt->payload_len, caps);
                m_client.m_ring_buffer.consume(consumed);
                if (!ok) {
                    return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse OK packet"));
                }
                m_result_set.setAffectedRows(ok->affected_rows);
                m_result_set.setLastInsertId(ok->last_insert_id);
                m_result_set.setWarnings(ok->warnings);
                m_result_set.setStatusFlags(ok->status_flags);
                m_result_set.setInfo(ok->info);
                m_lifecycle = Lifecycle::Done;
                return true;
            }

            size_t int_consumed = 0;
            auto col_count = protocol::readLenEncInt(pkt->payload, pkt->payload_len, int_consumed);
            if (!col_count) {
                m_client.m_ring_buffer.consume(consumed);
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse column count"));
            }

            m_column_count = col_count.value();
            m_columns_received = 0;
            m_result_set.reserveFields(static_cast<size_t>(m_column_count));
            m_client.m_ring_buffer.consume(consumed);
            m_state = State::ReceivingColumns;
            continue;
        }

        if (m_state == State::ReceivingColumns) {
            auto col = m_client.m_parser.parseColumnDefinition(pkt->payload, pkt->payload_len);
            m_client.m_ring_buffer.consume(consumed);
            if (!col) {
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse column definition"));
            }

            MysqlField field(col->name,
                             static_cast<MysqlFieldType>(col->column_type),
                             col->flags,
                             col->column_length,
                             col->decimals);
            field.setCatalog(col->catalog);
            field.setSchema(col->schema);
            field.setTable(col->table);
            field.setOrgTable(col->org_table);
            field.setOrgName(col->org_name);
            field.setCharacterSet(col->character_set);
            m_result_set.addField(std::move(field));

            ++m_columns_received;
            if (m_columns_received >= m_column_count) {
                m_state = (caps & protocol::CLIENT_DEPRECATE_EOF)
                    ? State::ReceivingRows
                    : State::ReceivingColumnEof;
            }
            continue;
        }

        if (m_state == State::ReceivingColumnEof) {
            m_client.m_ring_buffer.consume(consumed);
            m_state = State::ReceivingRows;
            continue;
        }

        if (m_state == State::ReceivingRows) {
            if (first_byte == 0xFE && pkt->payload_len < 0xFFFFFF) {
                if (caps & protocol::CLIENT_DEPRECATE_EOF) {
                    auto ok = m_client.m_parser.parseOk(pkt->payload, pkt->payload_len, caps);
                    if (ok) {
                        m_result_set.setWarnings(ok->warnings);
                        m_result_set.setStatusFlags(ok->status_flags);
                    }
                } else {
                    auto eof = m_client.m_parser.parseEof(pkt->payload, pkt->payload_len);
                    if (eof) {
                        m_result_set.setWarnings(eof->warnings);
                        m_result_set.setStatusFlags(eof->status_flags);
                    }
                }
                m_client.m_ring_buffer.consume(consumed);
                m_lifecycle = Lifecycle::Done;
                return true;
            }

            if (first_byte == 0xFF) {
                auto err = m_client.m_parser.parseErr(pkt->payload, pkt->payload_len, caps);
                m_client.m_ring_buffer.consume(consumed);
                if (err) {
                    return std::unexpected(MysqlError(MYSQL_ERROR_SERVER, err->error_code, err->error_message));
                }
                return std::unexpected(MysqlError(MYSQL_ERROR_QUERY, "Error during row fetch"));
            }

            auto row = m_client.m_parser.parseTextRow(pkt->payload, pkt->payload_len, m_column_count);
            m_client.m_ring_buffer.consume(consumed);
            if (!row) {
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse text row"));
            }
            m_result_set.addRow(MysqlRow(std::move(row.value())));
            continue;
        }

        return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Invalid query parser state"));
    }
}

std::expected<std::optional<MysqlResultSet>, MysqlError> MysqlQueryAwaitable::await_resume()
{
    onCompleted();

    if (!m_result.has_value()) {
        auto err = detail::toTimeoutOrInternalError(m_result.error());
        reset();
        return std::unexpected(std::move(err));
    }

    if (m_chain_error.has_value()) {
        auto err = std::move(*m_chain_error);
        reset();
        return std::unexpected(std::move(err));
    }

    if (m_lifecycle != Lifecycle::Done) {
        reset();
        return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Query awaitable did not reach done state"));
    }

    auto result = std::move(m_result_set);
    reset();
    return std::optional<MysqlResultSet>(std::move(result));
}

// ======================== MysqlPrepareAwaitable ========================

MysqlPrepareAwaitable::ProtocolSendAwaitable::ProtocolSendAwaitable(MysqlPrepareAwaitable* owner)
    : WritevIOContext({})
    , m_owner(owner)
{
    m_iovecs.reserve(1);
}

void MysqlPrepareAwaitable::ProtocolSendAwaitable::syncSendIovecs()
{
    detail::syncSendWindow(m_owner->m_encoded_cmd, m_owner->m_sent, m_buffer, m_length);
    m_iovecs.clear();
    if (m_length == 0 || m_buffer == nullptr) {
        return;
    }
    m_iovecs.push_back(iovec{const_cast<char*>(m_buffer), m_length});
}

bool MysqlPrepareAwaitable::ProtocolSendAwaitable::handleSendResult()
{
    return detail::handleSendResult(
        m_result,
        m_owner->m_sent,
        m_owner->m_encoded_cmd.size(),
        [&](const IOError& io_error) { m_owner->setSendError(io_error); },
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_SEND, "Send returned 0 bytes")); },
        [&]() { m_owner->m_client.m_ring_buffer.clear(); }
    );
}

#ifdef USE_IOURING
bool MysqlPrepareAwaitable::ProtocolSendAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    syncSendIovecs();
    if (m_iovecs.empty()) {
        m_owner->m_client.m_ring_buffer.clear();
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!WritevIOContext::handleComplete(cqe, handle)) {
        return false;
    }
    return handleSendResult();
}
#else
bool MysqlPrepareAwaitable::ProtocolSendAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        syncSendIovecs();
        if (m_iovecs.empty()) {
            m_owner->m_client.m_ring_buffer.clear();
            return true;
        }

        if (!WritevIOContext::handleComplete(handle)) {
            return false;
        }

        if (handleSendResult()) {
            return true;
        }
    }
    return true;
}
#endif

MysqlPrepareAwaitable::ProtocolRecvAwaitable::ProtocolRecvAwaitable(MysqlPrepareAwaitable* owner)
    : ReadvIOContext({})
    , m_owner(owner)
{
    m_iovecs.reserve(2);
}

bool MysqlPrepareAwaitable::ProtocolRecvAwaitable::prepareRecvWindow()
{
    if (!detail::prepareRecvWindow(m_owner->m_client.m_ring_buffer, m_iovecs)) {
        m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space"));
        return false;
    }
    return true;
}

bool MysqlPrepareAwaitable::ProtocolRecvAwaitable::tryParseAndCheckDone()
{
    return detail::parseOrSetError(
        [&]() { return m_owner->tryParseFromRingBuffer(); },
        [&](MysqlError err) { m_owner->setError(std::move(err)); }
    );
}

bool MysqlPrepareAwaitable::ProtocolRecvAwaitable::handleReadResult()
{
    return detail::handleReadResult(
        m_result,
        m_owner->m_client.m_ring_buffer,
        [&](const IOError& io_error) { m_owner->setRecvError(io_error); },
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Connection closed")); },
        [&]() { return m_owner->tryParseFromRingBuffer(); },
        [&](MysqlError err) { m_owner->setError(std::move(err)); }
    );
}

#ifdef USE_IOURING
bool MysqlPrepareAwaitable::ProtocolRecvAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    if (tryParseAndCheckDone()) {
        return true;
    }

    if (!prepareRecvWindow()) {
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!ReadvIOContext::handleComplete(cqe, handle)) {
        return false;
    }
    return handleReadResult();
}
#else
bool MysqlPrepareAwaitable::ProtocolRecvAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        if (tryParseAndCheckDone()) {
            return true;
        }

        if (!prepareRecvWindow()) {
            return true;
        }

        if (!ReadvIOContext::handleComplete(handle)) {
            return false;
        }

        if (handleReadResult()) {
            return true;
        }
    }
    return true;
}
#endif

MysqlPrepareAwaitable::MysqlPrepareAwaitable(AsyncMysqlClient& client, std::string_view sql)
    : CustomAwaitable(client.m_socket.controller())
    , m_client(client)
    , m_encoded_cmd(detail::buildSingleCommandPacket(protocol::CommandType::COM_STMT_PREPARE,
                                                     sql,
                                                     protocol::MysqlCommandKind::StmtPrepare))
    , m_lifecycle(Lifecycle::Running)
    , m_state(State::ReceivingPrepareOk)
    , m_sent(0)
    , m_prepare_result()
    , m_params_received(0)
    , m_columns_received(0)
    , m_send_awaitable(this)
    , m_recv_awaitable(this)
    , m_chain_error(std::nullopt)
    , m_result(std::nullopt)
{
    addTask(IOEventType::SEND, &m_send_awaitable);
    addTask(IOEventType::READV, &m_recv_awaitable);
}

void MysqlPrepareAwaitable::reset() noexcept
{
    m_lifecycle = Lifecycle::Invalid;
    m_state = State::ReceivingPrepareOk;
    m_sent = 0;
    m_prepare_result = PrepareResult{};
    m_params_received = 0;
    m_columns_received = 0;
    m_chain_error.reset();
    m_result = std::nullopt;
}

void MysqlPrepareAwaitable::setError(MysqlError error) noexcept
{
    m_chain_error = std::move(error);
    m_lifecycle = Lifecycle::Invalid;
}

void MysqlPrepareAwaitable::setSendError(const IOError& io_error) noexcept
{
    setError(MysqlError(MYSQL_ERROR_SEND, io_error.message()));
}

void MysqlPrepareAwaitable::setRecvError(const IOError& io_error) noexcept
{
    setError(MysqlError(MYSQL_ERROR_RECV, io_error.message()));
}

std::expected<bool, MysqlError> MysqlPrepareAwaitable::tryParseFromRingBuffer()
{
    while (true) {
        struct iovec read_iovecs[2];
        const size_t read_iovecs_count = m_client.m_ring_buffer.getReadIovecs(read_iovecs, 2);
        if (read_iovecs_count == 0) {
            return false;
        }

        auto linear = detail::linearizeReadIovecs(
            std::span<const struct iovec>(read_iovecs, read_iovecs_count),
            m_parse_scratch);
        const char* data = linear.data();
        size_t len = linear.size();

        size_t consumed = 0;
        auto pkt = m_client.m_parser.extractPacket(data, len, consumed);
        if (!pkt) {
            if (pkt.error() == protocol::ParseError::Incomplete) {
                return false;
            }
            return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Parse prepare packet failed"));
        }

        const uint8_t first_byte = static_cast<uint8_t>(pkt->payload[0]);
        const uint32_t caps = m_client.m_server_capabilities;

        if (m_state == State::ReceivingPrepareOk) {
            if (first_byte == 0xFF) {
                auto err = m_client.m_parser.parseErr(pkt->payload, pkt->payload_len, caps);
                m_client.m_ring_buffer.consume(consumed);
                if (err) {
                    return std::unexpected(MysqlError(MYSQL_ERROR_PREPARED_STMT, err->error_code, err->error_message));
                }
                return std::unexpected(MysqlError(MYSQL_ERROR_PREPARED_STMT, "Prepare failed"));
            }

            auto ok = m_client.m_parser.parseStmtPrepareOk(pkt->payload, pkt->payload_len);
            m_client.m_ring_buffer.consume(consumed);
            if (!ok) {
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse COM_STMT_PREPARE OK"));
            }

            m_prepare_result.statement_id = ok->statement_id;
            m_prepare_result.num_params = ok->num_params;
            m_prepare_result.num_columns = ok->num_columns;
            m_prepare_result.param_fields.reserve(m_prepare_result.num_params);
            m_prepare_result.column_fields.reserve(m_prepare_result.num_columns);
            m_params_received = 0;
            m_columns_received = 0;

            if (ok->num_params > 0) {
                m_state = State::ReceivingParamDefs;
                continue;
            }
            if (ok->num_columns > 0) {
                m_state = State::ReceivingColumnDefs;
                continue;
            }
            m_lifecycle = Lifecycle::Done;
            return true;
        }

        if (m_state == State::ReceivingParamDefs) {
            auto col = m_client.m_parser.parseColumnDefinition(pkt->payload, pkt->payload_len);
            m_client.m_ring_buffer.consume(consumed);
            if (!col) {
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Parse parameter definition failed"));
            }

            MysqlField field(col->name,
                             static_cast<MysqlFieldType>(col->column_type),
                             col->flags,
                             col->column_length,
                             col->decimals);
            m_prepare_result.param_fields.push_back(std::move(field));
            ++m_params_received;
            if (m_params_received >= m_prepare_result.num_params) {
                m_state = State::ReceivingParamEof;
            }
            continue;
        }

        if (m_state == State::ReceivingParamEof) {
            m_client.m_ring_buffer.consume(consumed);
            if (m_prepare_result.num_columns > 0) {
                m_state = State::ReceivingColumnDefs;
                continue;
            }
            m_lifecycle = Lifecycle::Done;
            return true;
        }

        if (m_state == State::ReceivingColumnDefs) {
            auto col = m_client.m_parser.parseColumnDefinition(pkt->payload, pkt->payload_len);
            m_client.m_ring_buffer.consume(consumed);
            if (!col) {
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Parse column definition failed"));
            }

            MysqlField field(col->name,
                             static_cast<MysqlFieldType>(col->column_type),
                             col->flags,
                             col->column_length,
                             col->decimals);
            m_prepare_result.column_fields.push_back(std::move(field));
            ++m_columns_received;
            if (m_columns_received >= m_prepare_result.num_columns) {
                m_state = State::ReceivingColumnEof;
            }
            continue;
        }

        if (m_state == State::ReceivingColumnEof) {
            m_client.m_ring_buffer.consume(consumed);
            m_lifecycle = Lifecycle::Done;
            return true;
        }

        return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Invalid prepare parser state"));
    }
}

std::expected<std::optional<MysqlPrepareAwaitable::PrepareResult>, MysqlError>
MysqlPrepareAwaitable::await_resume()
{
    onCompleted();

    if (!m_result.has_value()) {
        auto err = detail::toTimeoutOrInternalError(m_result.error());
        reset();
        return std::unexpected(std::move(err));
    }

    if (m_chain_error.has_value()) {
        auto err = std::move(*m_chain_error);
        reset();
        return std::unexpected(std::move(err));
    }

    if (m_lifecycle != Lifecycle::Done) {
        reset();
        return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Prepare awaitable did not reach done state"));
    }

    auto result = std::move(m_prepare_result);
    reset();
    return std::optional<PrepareResult>(std::move(result));
}

// ======================== MysqlStmtExecuteAwaitable ========================

MysqlStmtExecuteAwaitable::ProtocolSendAwaitable::ProtocolSendAwaitable(MysqlStmtExecuteAwaitable* owner)
    : WritevIOContext({})
    , m_owner(owner)
{
    m_iovecs.reserve(1);
}

void MysqlStmtExecuteAwaitable::ProtocolSendAwaitable::syncSendIovecs()
{
    detail::syncSendWindow(m_owner->m_encoded_cmd, m_owner->m_sent, m_buffer, m_length);
    m_iovecs.clear();
    if (m_length == 0 || m_buffer == nullptr) {
        return;
    }
    m_iovecs.push_back(iovec{const_cast<char*>(m_buffer), m_length});
}

bool MysqlStmtExecuteAwaitable::ProtocolSendAwaitable::handleSendResult()
{
    return detail::handleSendResult(
        m_result,
        m_owner->m_sent,
        m_owner->m_encoded_cmd.size(),
        [&](const IOError& io_error) { m_owner->setSendError(io_error); },
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_SEND, "Send returned 0 bytes")); },
        [&]() { m_owner->m_client.m_ring_buffer.clear(); }
    );
}

#ifdef USE_IOURING
bool MysqlStmtExecuteAwaitable::ProtocolSendAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    syncSendIovecs();
    if (m_iovecs.empty()) {
        m_owner->m_client.m_ring_buffer.clear();
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!WritevIOContext::handleComplete(cqe, handle)) {
        return false;
    }
    return handleSendResult();
}
#else
bool MysqlStmtExecuteAwaitable::ProtocolSendAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        syncSendIovecs();
        if (m_iovecs.empty()) {
            m_owner->m_client.m_ring_buffer.clear();
            return true;
        }

        if (!WritevIOContext::handleComplete(handle)) {
            return false;
        }

        if (handleSendResult()) {
            return true;
        }
    }
    return true;
}
#endif

MysqlStmtExecuteAwaitable::ProtocolRecvAwaitable::ProtocolRecvAwaitable(MysqlStmtExecuteAwaitable* owner)
    : ReadvIOContext({})
    , m_owner(owner)
{
    m_iovecs.reserve(2);
}

bool MysqlStmtExecuteAwaitable::ProtocolRecvAwaitable::prepareRecvWindow()
{
    if (!detail::prepareRecvWindow(m_owner->m_client.m_ring_buffer, m_iovecs)) {
        m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space"));
        return false;
    }
    return true;
}

bool MysqlStmtExecuteAwaitable::ProtocolRecvAwaitable::tryParseAndCheckDone()
{
    return detail::parseOrSetError(
        [&]() { return m_owner->tryParseFromRingBuffer(); },
        [&](MysqlError err) { m_owner->setError(std::move(err)); }
    );
}

bool MysqlStmtExecuteAwaitable::ProtocolRecvAwaitable::handleReadResult()
{
    return detail::handleReadResult(
        m_result,
        m_owner->m_client.m_ring_buffer,
        [&](const IOError& io_error) { m_owner->setRecvError(io_error); },
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Connection closed")); },
        [&]() { return m_owner->tryParseFromRingBuffer(); },
        [&](MysqlError err) { m_owner->setError(std::move(err)); }
    );
}

#ifdef USE_IOURING
bool MysqlStmtExecuteAwaitable::ProtocolRecvAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    if (tryParseAndCheckDone()) {
        return true;
    }

    if (!prepareRecvWindow()) {
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!ReadvIOContext::handleComplete(cqe, handle)) {
        return false;
    }

    return handleReadResult();
}
#else
bool MysqlStmtExecuteAwaitable::ProtocolRecvAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        if (tryParseAndCheckDone()) {
            return true;
        }

        if (!prepareRecvWindow()) {
            return true;
        }

        if (!ReadvIOContext::handleComplete(handle)) {
            return false;
        }

        if (handleReadResult()) {
            return true;
        }
    }
    return true;
}
#endif

MysqlStmtExecuteAwaitable::MysqlStmtExecuteAwaitable(AsyncMysqlClient& client, std::string encoded_cmd)
    : CustomAwaitable(client.m_socket.controller())
    , m_client(client)
    , m_encoded_cmd(std::move(encoded_cmd))
    , m_lifecycle(Lifecycle::Running)
    , m_state(State::ReceivingHeader)
    , m_sent(0)
    , m_result_set()
    , m_column_count(0)
    , m_columns_received(0)
    , m_send_awaitable(this)
    , m_recv_awaitable(this)
    , m_chain_error(std::nullopt)
    , m_result(std::nullopt)
{
    if (m_client.m_config.result_row_reserve_hint > 0) {
        m_result_set.reserveRows(m_client.m_config.result_row_reserve_hint);
    }
    addTask(IOEventType::SEND, &m_send_awaitable);
    addTask(IOEventType::READV, &m_recv_awaitable);
}

void MysqlStmtExecuteAwaitable::reset() noexcept
{
    m_lifecycle = Lifecycle::Invalid;
    m_state = State::ReceivingHeader;
    m_result_set = MysqlResultSet{};
    if (m_client.m_config.result_row_reserve_hint > 0) {
        m_result_set.reserveRows(m_client.m_config.result_row_reserve_hint);
    }
    m_sent = 0;
    m_column_count = 0;
    m_columns_received = 0;
    m_chain_error.reset();
    m_result = std::nullopt;
}

void MysqlStmtExecuteAwaitable::setError(MysqlError error) noexcept
{
    m_chain_error = std::move(error);
    m_lifecycle = Lifecycle::Invalid;
}

void MysqlStmtExecuteAwaitable::setSendError(const IOError& io_error) noexcept
{
    setError(MysqlError(MYSQL_ERROR_SEND, io_error.message()));
}

void MysqlStmtExecuteAwaitable::setRecvError(const IOError& io_error) noexcept
{
    setError(MysqlError(MYSQL_ERROR_RECV, io_error.message()));
}

std::expected<bool, MysqlError> MysqlStmtExecuteAwaitable::tryParseFromRingBuffer()
{
    while (true) {
        struct iovec read_iovecs[2];
        const size_t read_iovecs_count = m_client.m_ring_buffer.getReadIovecs(read_iovecs, 2);
        if (read_iovecs_count == 0) {
            return false;
        }

        auto linear = detail::linearizeReadIovecs(
            std::span<const struct iovec>(read_iovecs, read_iovecs_count),
            m_parse_scratch);
        const char* data = linear.data();
        size_t len = linear.size();

        size_t consumed = 0;
        auto pkt = m_client.m_parser.extractPacket(data, len, consumed);
        if (!pkt) {
            if (pkt.error() == protocol::ParseError::Incomplete) {
                return false;
            }
            return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Parse stmt-execute packet failed"));
        }

        const uint8_t first_byte = static_cast<uint8_t>(pkt->payload[0]);
        const uint32_t caps = m_client.m_server_capabilities;

        if (m_state == State::ReceivingHeader) {
            if (first_byte == 0xFF) {
                auto err = m_client.m_parser.parseErr(pkt->payload, pkt->payload_len, caps);
                m_client.m_ring_buffer.consume(consumed);
                if (err) {
                    return std::unexpected(MysqlError(MYSQL_ERROR_SERVER, err->error_code, err->error_message));
                }
                return std::unexpected(MysqlError(MYSQL_ERROR_QUERY, "Execute failed"));
            }

            if (first_byte == 0x00) {
                auto ok = m_client.m_parser.parseOk(pkt->payload, pkt->payload_len, caps);
                m_client.m_ring_buffer.consume(consumed);
                if (!ok) {
                    return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse OK packet"));
                }
                m_result_set.setAffectedRows(ok->affected_rows);
                m_result_set.setLastInsertId(ok->last_insert_id);
                m_result_set.setWarnings(ok->warnings);
                m_result_set.setStatusFlags(ok->status_flags);
                m_lifecycle = Lifecycle::Done;
                return true;
            }

            size_t int_consumed = 0;
            auto col_count = protocol::readLenEncInt(pkt->payload, pkt->payload_len, int_consumed);
            if (!col_count) {
                m_client.m_ring_buffer.consume(consumed);
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse column count"));
            }
            m_column_count = col_count.value();
            m_columns_received = 0;
            m_result_set.reserveFields(static_cast<size_t>(m_column_count));
            m_client.m_ring_buffer.consume(consumed);
            m_state = State::ReceivingColumns;
            continue;
        }

        if (m_state == State::ReceivingColumns) {
            auto col = m_client.m_parser.parseColumnDefinition(pkt->payload, pkt->payload_len);
            m_client.m_ring_buffer.consume(consumed);
            if (!col) {
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Parse column definition failed"));
            }

            MysqlField field(col->name,
                             static_cast<MysqlFieldType>(col->column_type),
                             col->flags,
                             col->column_length,
                             col->decimals);
            field.setCatalog(col->catalog);
            field.setSchema(col->schema);
            field.setTable(col->table);
            field.setOrgTable(col->org_table);
            field.setOrgName(col->org_name);
            field.setCharacterSet(col->character_set);
            m_result_set.addField(std::move(field));

            ++m_columns_received;
            if (m_columns_received >= m_column_count) {
                m_state = (caps & protocol::CLIENT_DEPRECATE_EOF)
                    ? State::ReceivingRows
                    : State::ReceivingColumnEof;
            }
            continue;
        }

        if (m_state == State::ReceivingColumnEof) {
            m_client.m_ring_buffer.consume(consumed);
            m_state = State::ReceivingRows;
            continue;
        }

        if (m_state == State::ReceivingRows) {
            if (first_byte == 0xFE && pkt->payload_len < 0xFFFFFF) {
                m_client.m_ring_buffer.consume(consumed);
                m_lifecycle = Lifecycle::Done;
                return true;
            }

            if (first_byte == 0xFF) {
                auto err = m_client.m_parser.parseErr(pkt->payload, pkt->payload_len, caps);
                m_client.m_ring_buffer.consume(consumed);
                if (err) {
                    return std::unexpected(MysqlError(MYSQL_ERROR_SERVER, err->error_code, err->error_message));
                }
                return std::unexpected(MysqlError(MYSQL_ERROR_QUERY, "Error during row fetch"));
            }

            auto row = m_client.m_parser.parseTextRow(pkt->payload, pkt->payload_len, m_column_count);
            m_client.m_ring_buffer.consume(consumed);
            if (!row) {
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Parse row failed"));
            }
            m_result_set.addRow(MysqlRow(std::move(row.value())));
            continue;
        }

        return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Invalid stmt-execute parser state"));
    }
}

std::expected<std::optional<MysqlResultSet>, MysqlError> MysqlStmtExecuteAwaitable::await_resume()
{
    onCompleted();

    if (!m_result.has_value()) {
        auto err = detail::toTimeoutOrInternalError(m_result.error());
        reset();
        return std::unexpected(std::move(err));
    }

    if (m_chain_error.has_value()) {
        auto err = std::move(*m_chain_error);
        reset();
        return std::unexpected(std::move(err));
    }

    if (m_lifecycle != Lifecycle::Done) {
        reset();
        return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "StmtExecute awaitable did not reach done state"));
    }

    auto result = std::move(m_result_set);
    reset();
    return std::optional<MysqlResultSet>(std::move(result));
}

// ======================== MysqlPipelineAwaitable ========================

MysqlPipelineAwaitable::ProtocolSendAwaitable::ProtocolSendAwaitable(MysqlPipelineAwaitable* owner)
    : WritevIOContext({})
    , m_owner(owner)
{
    rebind(owner);
}

void MysqlPipelineAwaitable::ProtocolSendAwaitable::rebind(MysqlPipelineAwaitable* owner)
{
    m_owner = owner;
    m_iov_cursor = 0;
    m_next_command_index = 0;
    m_iovecs.clear();
    if (!m_owner) {
        return;
    }

    const size_t reserve_hint = m_owner->m_encoded_slices.size() <
                                        static_cast<size_t>(detail::kPipelineWritevMaxIov)
                                    ? m_owner->m_encoded_slices.size()
                                    : static_cast<size_t>(detail::kPipelineWritevMaxIov);
    m_iovecs.reserve(reserve_hint);
    refillIovWindow();
}

void MysqlPipelineAwaitable::ProtocolSendAwaitable::refillIovWindow()
{
    if (!m_owner) {
        m_iovecs.clear();
        m_iov_cursor = 0;
        return;
    }

    if (m_iov_cursor > 0) {
        m_iovecs.erase(
            m_iovecs.begin(),
            m_iovecs.begin() + static_cast<std::vector<struct iovec>::difference_type>(m_iov_cursor));
        m_iov_cursor = 0;
    }

    while (m_iovecs.size() < static_cast<size_t>(detail::kPipelineWritevMaxIov) &&
           m_next_command_index < m_owner->m_encoded_slices.size()) {
        const auto encoded_slice = m_owner->m_encoded_slices[m_next_command_index++];
        if (encoded_slice.length == 0) {
            continue;
        }

        struct iovec iov{};
        iov.iov_base = const_cast<char*>(m_owner->m_encoded_buffer.data() + encoded_slice.offset);
        iov.iov_len = encoded_slice.length;
        m_iovecs.push_back(iov);
    }
}

int MysqlPipelineAwaitable::ProtocolSendAwaitable::pendingIovCount()
{
    while (m_iov_cursor < m_iovecs.size() && m_iovecs[m_iov_cursor].iov_len == 0) {
        ++m_iov_cursor;
    }

    if (m_iov_cursor >= m_iovecs.size()) {
        refillIovWindow();
        while (m_iov_cursor < m_iovecs.size() && m_iovecs[m_iov_cursor].iov_len == 0) {
            ++m_iov_cursor;
        }
    }

    if (m_iov_cursor >= m_iovecs.size()) {
        return 0;
    }

    return static_cast<int>(m_iovecs.size() - m_iov_cursor);
}

bool MysqlPipelineAwaitable::ProtocolSendAwaitable::advanceAfterWrite(size_t sent_bytes)
{
    size_t remaining = sent_bytes;
    while (remaining > 0 && m_iov_cursor < m_iovecs.size()) {
        auto& iov = m_iovecs[m_iov_cursor];
        if (iov.iov_len == 0) {
            ++m_iov_cursor;
            continue;
        }

        if (remaining < iov.iov_len) {
            iov.iov_base = static_cast<char*>(iov.iov_base) + remaining;
            iov.iov_len -= remaining;
            return true;
        }

        remaining -= iov.iov_len;
        iov.iov_len = 0;
        ++m_iov_cursor;
    }

    if (remaining != 0) {
        return false;
    }

    if (m_iov_cursor >= m_iovecs.size()) {
        refillIovWindow();
    }
    return true;
}

#ifdef USE_IOURING
bool MysqlPipelineAwaitable::ProtocolSendAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    if (pendingIovCount() == 0) {
        return true;
    }
    if (cqe == nullptr) {
        return false;
    }

    auto result = galay::kernel::io::handleWritev(cqe);
    if (!result && IOError::contains(result.error().code(), galay::kernel::kNotReady)) {
        return false;
    }
    if (!result) {
        m_owner->setSendError(result.error());
        return true;
    }

    const size_t sent = result.value();
    if (sent == 0) {
        m_owner->setError(MysqlError(MYSQL_ERROR_SEND, "Send returned 0 bytes in pipeline"));
        return true;
    }

    if (!advanceAfterWrite(sent)) {
        m_owner->setSendError(IOError(galay::kernel::kSendFailed, 0));
        return true;
    }
    return pendingIovCount() == 0;
}
#else
bool MysqlPipelineAwaitable::ProtocolSendAwaitable::handleComplete(GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    while (true) {
        const int iov_count = pendingIovCount();
        if (iov_count == 0) {
            return true;
        }

        auto result = galay::kernel::io::handleWritev(handle,
                                                      m_iovecs.data() + m_iov_cursor,
                                                      iov_count);
        if (!result && IOError::contains(result.error().code(), galay::kernel::kNotReady)) {
            return false;
        }
        if (!result) {
            m_owner->setSendError(result.error());
            return true;
        }

        const size_t sent = result.value();
        if (sent == 0) {
            m_owner->setError(MysqlError(MYSQL_ERROR_SEND, "Send returned 0 bytes in pipeline"));
            return true;
        }

        if (!advanceAfterWrite(sent)) {
            m_owner->setSendError(IOError(galay::kernel::kSendFailed, 0));
            return true;
        }
    }
}
#endif

MysqlPipelineAwaitable::ProtocolRecvAwaitable::ProtocolRecvAwaitable(MysqlPipelineAwaitable* owner)
    : ReadvIOContext({})
    , m_owner(owner)
{
    m_iovecs.reserve(2);
    rebind(owner);
}

void MysqlPipelineAwaitable::ProtocolRecvAwaitable::rebind(MysqlPipelineAwaitable* owner)
{
    m_owner = owner;
    m_iovecs.clear();
}

bool MysqlPipelineAwaitable::ProtocolRecvAwaitable::prepareRecvWindow()
{
    if (!detail::prepareRecvWindow(m_owner->m_client.m_ring_buffer, m_iovecs)) {
        m_owner->setError(MysqlError(MYSQL_ERROR_RECV,
                                     "No writable ring buffer space while receiving pipeline response"));
        return false;
    }
    return true;
}

bool MysqlPipelineAwaitable::ProtocolRecvAwaitable::tryParseAndCheckDone()
{
    return detail::parseOrSetError(
        [&]() { return m_owner->tryParseFromRingBuffer(); },
        [&](MysqlError err) { m_owner->setError(std::move(err)); }
    );
}

bool MysqlPipelineAwaitable::ProtocolRecvAwaitable::handleReadResult()
{
    return detail::handleReadResult(
        m_result,
        m_owner->m_client.m_ring_buffer,
        [&](const IOError& io_error) { m_owner->setRecvError(io_error); },
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, "Connection closed")); },
        [&]() { return m_owner->tryParseFromRingBuffer(); },
        [&](MysqlError err) { m_owner->setError(std::move(err)); }
    );
}

#ifdef USE_IOURING
bool MysqlPipelineAwaitable::ProtocolRecvAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    if (tryParseAndCheckDone()) {
        return true;
    }

    if (!prepareRecvWindow()) {
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!ReadvIOContext::handleComplete(cqe, handle)) {
        return false;
    }
    return handleReadResult();
}
#else
bool MysqlPipelineAwaitable::ProtocolRecvAwaitable::handleComplete(GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    while (true) {
        if (tryParseAndCheckDone()) {
            return true;
        }

        if (!prepareRecvWindow()) {
            return true;
        }

        if (!ReadvIOContext::handleComplete(handle)) {
            return false;
        }

        if (handleReadResult()) {
            return true;
        }
    }
}
#endif

MysqlPipelineAwaitable::MysqlPipelineAwaitable(AsyncMysqlClient& client,
                                               std::span<const protocol::MysqlCommandView> commands)
    : CustomAwaitable(client.m_socket.controller())
    , m_client(client)
    , m_expected_results(commands.size())
    , m_lifecycle(commands.empty() ? Lifecycle::Done : Lifecycle::Running)
    , m_state(State::ReceivingHeader)
    , m_results()
    , m_current_result()
    , m_column_count(0)
    , m_columns_received(0)
    , m_send_awaitable(this)
    , m_recv_awaitable(this)
    , m_chain_error(std::nullopt)
    , m_result(std::nullopt)
{
    m_results.reserve(m_expected_results);
    if (m_client.m_config.result_row_reserve_hint > 0) {
        m_current_result.reserveRows(m_client.m_config.result_row_reserve_hint);
    }

    size_t encoded_bytes = 0;
    for (const auto& cmd : commands) {
        if (cmd.encoded.empty()) {
            setError(MysqlError(MYSQL_ERROR_PROTOCOL, "Pipeline command encoded payload is empty"));
            return;
        }
        encoded_bytes += cmd.encoded.size();
    }

    m_encoded_buffer.reserve(encoded_bytes);
    m_encoded_slices.reserve(commands.size());
    for (const auto& cmd : commands) {
        const size_t offset = m_encoded_buffer.size();
        m_encoded_buffer.append(cmd.encoded.data(), cmd.encoded.size());
        m_encoded_slices.push_back(EncodedSlice{offset, cmd.encoded.size()});
    }

    if (m_lifecycle == Lifecycle::Running) {
        initTaskQueue();
    }
}

void MysqlPipelineAwaitable::initTaskQueue()
{
    m_tasks.clear();
    m_cursor = 0;
    addTask(IOEventType::SEND, &m_send_awaitable);
    addTask(IOEventType::READV, &m_recv_awaitable);
}

void MysqlPipelineAwaitable::resetCurrentResult()
{
    m_state = State::ReceivingHeader;
    m_current_result = MysqlResultSet{};
    if (m_client.m_config.result_row_reserve_hint > 0) {
        m_current_result.reserveRows(m_client.m_config.result_row_reserve_hint);
    }
    m_column_count = 0;
    m_columns_received = 0;
}

void MysqlPipelineAwaitable::finalizeCurrentResult()
{
    m_results.push_back(std::move(m_current_result));
    if (m_results.size() >= m_expected_results) {
        m_lifecycle = Lifecycle::Done;
        return;
    }
    resetCurrentResult();
}

void MysqlPipelineAwaitable::reset() noexcept
{
    m_lifecycle = Lifecycle::Invalid;
    m_state = State::ReceivingHeader;
    m_expected_results = 0;
    m_encoded_buffer.clear();
    m_encoded_slices.clear();
    m_results.clear();
    m_current_result = MysqlResultSet{};
    m_column_count = 0;
    m_columns_received = 0;
    m_chain_error.reset();
    m_parse_scratch.clear();
    m_tasks.clear();
    m_cursor = 0;
    m_result = std::nullopt;
    m_send_awaitable.rebind(this);
    m_recv_awaitable.rebind(this);
}

void MysqlPipelineAwaitable::setError(MysqlError error) noexcept
{
    m_chain_error = std::move(error);
    m_lifecycle = Lifecycle::Invalid;
}

void MysqlPipelineAwaitable::setSendError(const IOError& io_error) noexcept
{
    if (IOError::contains(io_error.code(), galay::kernel::kDisconnectError)) {
        setError(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, io_error.message()));
        return;
    }
    setError(MysqlError(MYSQL_ERROR_SEND, io_error.message()));
}

void MysqlPipelineAwaitable::setRecvError(const IOError& io_error) noexcept
{
    if (IOError::contains(io_error.code(), galay::kernel::kDisconnectError)) {
        setError(MysqlError(MYSQL_ERROR_CONNECTION_CLOSED, io_error.message()));
        return;
    }
    setError(MysqlError(MYSQL_ERROR_RECV, io_error.message()));
}

std::expected<bool, MysqlError> MysqlPipelineAwaitable::tryParseFromRingBuffer()
{
    while (m_results.size() < m_expected_results) {
        struct iovec read_iovecs[2];
        const size_t read_iovecs_count = m_client.m_ring_buffer.getReadIovecs(read_iovecs, 2);
        if (read_iovecs_count == 0) {
            return false;
        }

        auto linear = detail::linearizeReadIovecs(
            std::span<const struct iovec>(read_iovecs, read_iovecs_count),
            m_parse_scratch);
        const char* data = linear.data();
        size_t len = linear.size();

        size_t consumed = 0;
        auto pkt = m_client.m_parser.extractPacket(data, len, consumed);
        if (!pkt) {
            if (pkt.error() == protocol::ParseError::Incomplete) {
                return false;
            }
            return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Parse pipeline packet failed"));
        }

        const uint8_t first_byte = static_cast<uint8_t>(pkt->payload[0]);
        const uint32_t caps = m_client.m_server_capabilities;

        if (m_state == State::ReceivingHeader) {
            if (first_byte == 0xFF) {
                auto err = m_client.m_parser.parseErr(pkt->payload, pkt->payload_len, caps);
                m_client.m_ring_buffer.consume(consumed);
                if (err) {
                    return std::unexpected(MysqlError(MYSQL_ERROR_SERVER, err->error_code, err->error_message));
                }
                return std::unexpected(MysqlError(MYSQL_ERROR_QUERY, "Pipeline query failed"));
            }

            if (first_byte == 0x00) {
                auto ok = m_client.m_parser.parseOk(pkt->payload, pkt->payload_len, caps);
                m_client.m_ring_buffer.consume(consumed);
                if (!ok) {
                    return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse OK packet"));
                }

                m_current_result.setAffectedRows(ok->affected_rows);
                m_current_result.setLastInsertId(ok->last_insert_id);
                m_current_result.setWarnings(ok->warnings);
                m_current_result.setStatusFlags(ok->status_flags);
                m_current_result.setInfo(ok->info);
                finalizeCurrentResult();
                continue;
            }

            size_t int_consumed = 0;
            auto col_count = protocol::readLenEncInt(pkt->payload, pkt->payload_len, int_consumed);
            if (!col_count) {
                m_client.m_ring_buffer.consume(consumed);
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse column count"));
            }

            m_column_count = col_count.value();
            m_columns_received = 0;
            m_current_result.reserveFields(static_cast<size_t>(m_column_count));
            m_client.m_ring_buffer.consume(consumed);
            m_state = State::ReceivingColumns;
            continue;
        }

        if (m_state == State::ReceivingColumns) {
            auto col = m_client.m_parser.parseColumnDefinition(pkt->payload, pkt->payload_len);
            m_client.m_ring_buffer.consume(consumed);
            if (!col) {
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse column definition"));
            }

            MysqlField field(col->name,
                             static_cast<MysqlFieldType>(col->column_type),
                             col->flags,
                             col->column_length,
                             col->decimals);
            field.setCatalog(col->catalog);
            field.setSchema(col->schema);
            field.setTable(col->table);
            field.setOrgTable(col->org_table);
            field.setOrgName(col->org_name);
            field.setCharacterSet(col->character_set);
            m_current_result.addField(std::move(field));

            ++m_columns_received;
            if (m_columns_received >= m_column_count) {
                m_state = (caps & protocol::CLIENT_DEPRECATE_EOF)
                    ? State::ReceivingRows
                    : State::ReceivingColumnEof;
            }
            continue;
        }

        if (m_state == State::ReceivingColumnEof) {
            m_client.m_ring_buffer.consume(consumed);
            m_state = State::ReceivingRows;
            continue;
        }

        if (m_state == State::ReceivingRows) {
            if (first_byte == 0xFE && pkt->payload_len < 0xFFFFFF) {
                if (caps & protocol::CLIENT_DEPRECATE_EOF) {
                    auto ok = m_client.m_parser.parseOk(pkt->payload, pkt->payload_len, caps);
                    if (ok) {
                        m_current_result.setWarnings(ok->warnings);
                        m_current_result.setStatusFlags(ok->status_flags);
                    }
                } else {
                    auto eof = m_client.m_parser.parseEof(pkt->payload, pkt->payload_len);
                    if (eof) {
                        m_current_result.setWarnings(eof->warnings);
                        m_current_result.setStatusFlags(eof->status_flags);
                    }
                }

                m_client.m_ring_buffer.consume(consumed);
                finalizeCurrentResult();
                continue;
            }

            if (first_byte == 0xFF) {
                auto err = m_client.m_parser.parseErr(pkt->payload, pkt->payload_len, caps);
                m_client.m_ring_buffer.consume(consumed);
                if (err) {
                    return std::unexpected(MysqlError(MYSQL_ERROR_SERVER, err->error_code, err->error_message));
                }
                return std::unexpected(MysqlError(MYSQL_ERROR_QUERY, "Pipeline row fetch failed"));
            }

            auto row = m_client.m_parser.parseTextRow(pkt->payload, pkt->payload_len, m_column_count);
            m_client.m_ring_buffer.consume(consumed);
            if (!row) {
                return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse text row"));
            }
            m_current_result.addRow(MysqlRow(std::move(row.value())));
            continue;
        }

        return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Invalid pipeline parser state"));
    }

    m_lifecycle = Lifecycle::Done;
    return true;
}

std::expected<std::optional<std::vector<MysqlResultSet>>, MysqlError> MysqlPipelineAwaitable::await_resume()
{
    onCompleted();

    if (!m_result.has_value()) {
        auto err = detail::toTimeoutOrInternalError(m_result.error());
        reset();
        return std::unexpected(std::move(err));
    }

    if (m_chain_error.has_value()) {
        auto err = std::move(*m_chain_error);
        reset();
        return std::unexpected(std::move(err));
    }

    if (m_lifecycle != Lifecycle::Done) {
        reset();
        return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Pipeline awaitable did not reach done state"));
    }

    auto results = std::move(m_results);
    reset();
    return std::optional<std::vector<MysqlResultSet>>(std::move(results));
}

// ======================== AsyncMysqlClient 实现 ========================

AsyncMysqlClient::AsyncMysqlClient(IOScheduler* scheduler,
                                   AsyncMysqlConfig config,
                                   std::shared_ptr<MysqlBufferProvider> buffer_provider)
    : m_scheduler(scheduler)
    , m_config(std::move(config))
    , m_ring_buffer(m_config.buffer_size, std::move(buffer_provider))
{
    m_logger = MysqlLog::getInstance()->getLogger();
}

AsyncMysqlClient::AsyncMysqlClient(AsyncMysqlClient&& other) noexcept
    : m_is_closed(other.m_is_closed)
    , m_socket(std::move(other.m_socket))
    , m_scheduler(other.m_scheduler)
    , m_parser(std::move(other.m_parser))
    , m_encoder(std::move(other.m_encoder))
    , m_config(std::move(other.m_config))
    , m_ring_buffer(std::move(other.m_ring_buffer))
    , m_server_capabilities(other.m_server_capabilities)
    , m_logger(std::move(other.m_logger))
{
    other.m_is_closed = true;
}

AsyncMysqlClient& AsyncMysqlClient::operator=(AsyncMysqlClient&& other) noexcept
{
    if (this != &other) {
        m_is_closed = other.m_is_closed;
        m_socket = std::move(other.m_socket);
        m_scheduler = other.m_scheduler;
        m_parser = std::move(other.m_parser);
        m_encoder = std::move(other.m_encoder);
        m_config = std::move(other.m_config);
        m_ring_buffer = std::move(other.m_ring_buffer);
        m_server_capabilities = other.m_server_capabilities;
        m_logger = std::move(other.m_logger);
        other.m_is_closed = true;
    }
    return *this;
}

MysqlConnectAwaitable AsyncMysqlClient::connect(MysqlConfig config)
{
    return MysqlConnectAwaitable(*this, std::move(config));
}

MysqlConnectAwaitable AsyncMysqlClient::connect(std::string_view host, uint16_t port,
                                                std::string_view user, std::string_view password,
                                                std::string_view database)
{
    MysqlConfig config;
    config.host.assign(host.data(), host.size());
    config.port = port;
    config.username.assign(user.data(), user.size());
    config.password.assign(password.data(), password.size());
    config.database.assign(database.data(), database.size());
    return connect(std::move(config));
}

MysqlQueryAwaitable AsyncMysqlClient::query(std::string_view sql)
{
    return MysqlQueryAwaitable(*this, sql);
}

MysqlPipelineAwaitable AsyncMysqlClient::batch(std::span<const protocol::MysqlCommandView> commands)
{
    return MysqlPipelineAwaitable(*this, commands);
}

MysqlPipelineAwaitable AsyncMysqlClient::pipeline(std::span<const std::string_view> sqls)
{
    size_t reserve_bytes = 0;
    for (const auto sql : sqls) {
        reserve_bytes += protocol::MYSQL_PACKET_HEADER_SIZE + 1 + sql.size();
    }

    protocol::MysqlCommandBuilder builder;
    builder.reserve(sqls.size(), reserve_bytes);
    for (const auto sql : sqls) {
        builder.appendQuery(sql);
    }

    return batch(builder.commands());
}

MysqlPrepareAwaitable AsyncMysqlClient::prepare(std::string_view sql)
{
    return MysqlPrepareAwaitable(*this, sql);
}

MysqlStmtExecuteAwaitable AsyncMysqlClient::stmtExecute(uint32_t stmt_id,
                                                        std::span<const std::optional<std::string>> params,
                                                        std::span<const uint8_t> param_types)
{
    return MysqlStmtExecuteAwaitable(*this, m_encoder.encodeStmtExecute(stmt_id, params, param_types, 0));
}

MysqlStmtExecuteAwaitable AsyncMysqlClient::stmtExecute(uint32_t stmt_id,
                                                        std::span<const std::optional<std::string_view>> params,
                                                        std::span<const uint8_t> param_types)
{
    return MysqlStmtExecuteAwaitable(*this, m_encoder.encodeStmtExecute(stmt_id, params, param_types, 0));
}

MysqlQueryAwaitable AsyncMysqlClient::beginTransaction()
{
    return query("BEGIN");
}

MysqlQueryAwaitable AsyncMysqlClient::commit()
{
    return query("COMMIT");
}

MysqlQueryAwaitable AsyncMysqlClient::rollback()
{
    return query("ROLLBACK");
}

MysqlQueryAwaitable AsyncMysqlClient::ping()
{
    return query("SELECT 1");
}

MysqlQueryAwaitable AsyncMysqlClient::useDatabase(std::string_view database)
{
    std::string sql;
    sql.reserve(4 + database.size());
    sql.append("USE ");
    sql.append(database.data(), database.size());
    return query(sql);
}

} // namespace galay::mysql
