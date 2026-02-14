#include "MysqlClient.h"
#include "galay-mysql/base/MysqlLog.h"

namespace galay::mysql
{

namespace detail
{

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

template<typename OnNoSpace>
bool prepareReadIovecs(RingBuffer& ring_buffer, std::vector<struct iovec>& iovecs, OnNoSpace&& on_no_space)
{
    iovecs = ring_buffer.getWriteIovecs();
    if (iovecs.empty() || iovecs.front().iov_len == 0) {
        on_no_space();
        return false;
    }
    return true;
}

template<typename ParseFn, typename OnParseError>
bool parseOrSetError(ParseFn&& parse_fn, OnParseError&& on_parse_error)
{
    auto parsed = parse_fn();
    if (!parsed.has_value()) {
        on_parse_error(std::move(parsed.error()));
        return true;
    }
    return parsed.value();
}

template<typename OnIoError, typename OnClosed, typename ParseFn, typename OnParseError>
bool handleReadResult(std::expected<size_t, IOError>& io_result,
                      RingBuffer& ring_buffer,
                      OnIoError&& on_io_error,
                      OnClosed&& on_closed,
                      ParseFn&& parse_fn,
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
    return detail::parseOrSetError(std::forward<ParseFn>(parse_fn),
                                   std::forward<OnParseError>(on_parse_error));
}

MysqlError toTimeoutOrInternalError(const IOError& io_error)
{
    if (IOError::contains(io_error.code(), galay::kernel::kTimeout)) {
        return MysqlError(MYSQL_ERROR_TIMEOUT, io_error.message());
    }
    return MysqlError(MYSQL_ERROR_INTERNAL, io_error.message());
}

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

    if (!detail::prepareReadIovecs(m_owner->m_client.m_ring_buffer, m_iovecs, [&]() {
        m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space while reading handshake"));
    })) {
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

        if (!detail::prepareReadIovecs(m_owner->m_client.m_ring_buffer, m_iovecs, [&]() {
            m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space while reading handshake"));
        })) {
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
    : SendAwaitable(owner->m_client.m_socket.controller(),
                    owner->m_auth_packet.data(),
                    owner->m_auth_packet.size())
    , m_owner(owner)
{
}

#ifdef USE_IOURING
bool MysqlConnectAwaitable::ProtocolAuthSendAwaitable::handleComplete(struct io_uring_cqe* cqe, GHandle handle)
{
    if (m_owner->m_lifecycle != Lifecycle::Running) {
        return true;
    }

    detail::syncSendWindow(m_owner->m_auth_packet, m_owner->m_sent, m_buffer, m_length);
    if (m_length == 0) {
        m_owner->m_client.m_ring_buffer.clear();
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!SendIOContext::handleComplete(cqe, handle)) {
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
        detail::syncSendWindow(m_owner->m_auth_packet, m_owner->m_sent, m_buffer, m_length);
        if (m_length == 0) {
            m_owner->m_client.m_ring_buffer.clear();
            return true;
        }

        if (!SendIOContext::handleComplete(handle)) {
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

    if (!detail::prepareReadIovecs(m_owner->m_client.m_ring_buffer, m_iovecs, [&]() {
        m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space while reading auth result"));
    })) {
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

        if (!detail::prepareReadIovecs(m_owner->m_client.m_ring_buffer, m_iovecs, [&]() {
            m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space while reading auth result"));
        })) {
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

MysqlConnectAwaitable::MysqlConnectAwaitable(MysqlClient& client, MysqlConfig config)
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
    auto read_iovecs = m_client.m_ring_buffer.getReadIovecs();
    if (read_iovecs.empty()) {
        return false;
    }

    const char* data = static_cast<const char*>(read_iovecs[0].iov_base);
    size_t len = read_iovecs[0].iov_len;
    if (read_iovecs.size() > 1) {
        len += read_iovecs[1].iov_len;
    }

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
        auto read_iovecs = m_client.m_ring_buffer.getReadIovecs();
        if (read_iovecs.empty()) {
            return false;
        }

        const char* data = static_cast<const char*>(read_iovecs[0].iov_base);
        size_t len = read_iovecs[0].iov_len;
        if (read_iovecs.size() > 1) {
            len += read_iovecs[1].iov_len;
        }

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
    : SendAwaitable(owner->m_client.m_socket.controller(),
                    owner->m_encoded_cmd.data(),
                    owner->m_encoded_cmd.size())
    , m_owner(owner)
{
}

void MysqlQueryAwaitable::ProtocolSendAwaitable::syncSendWindow()
{
    detail::syncSendWindow(m_owner->m_encoded_cmd, m_owner->m_sent, m_buffer, m_length);
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

    syncSendWindow();
    if (m_length == 0) {
        m_owner->m_client.m_ring_buffer.clear();
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!SendIOContext::handleComplete(cqe, handle)) {
        return false;
    }
    return handleSendResult();
}
#else
bool MysqlQueryAwaitable::ProtocolSendAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        syncSendWindow();
        if (m_length == 0) {
            m_owner->m_client.m_ring_buffer.clear();
            return true;
        }

        if (!SendIOContext::handleComplete(handle)) {
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
}

bool MysqlQueryAwaitable::ProtocolRecvAwaitable::prepareReadIovecs()
{
    return detail::prepareReadIovecs(
        m_owner->m_client.m_ring_buffer,
        m_iovecs,
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space")); }
    );
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

    if (!prepareReadIovecs()) {
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

        if (!prepareReadIovecs()) {
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

MysqlQueryAwaitable::MysqlQueryAwaitable(MysqlClient& client, std::string_view sql)
    : CustomAwaitable(client.m_socket.controller())
    , m_client(client)
    , m_encoded_cmd(m_client.m_encoder.encodeQuery(sql, 0))
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
        auto read_iovecs = m_client.m_ring_buffer.getReadIovecs();
        if (read_iovecs.empty()) {
            return false;
        }

        const char* data = static_cast<const char*>(read_iovecs[0].iov_base);
        size_t len = read_iovecs[0].iov_len;
        if (read_iovecs.size() > 1) len += read_iovecs[1].iov_len;

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
    : SendAwaitable(owner->m_client.m_socket.controller(),
                    owner->m_encoded_cmd.data(),
                    owner->m_encoded_cmd.size())
    , m_owner(owner)
{
}

void MysqlPrepareAwaitable::ProtocolSendAwaitable::syncSendWindow()
{
    detail::syncSendWindow(m_owner->m_encoded_cmd, m_owner->m_sent, m_buffer, m_length);
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

    syncSendWindow();
    if (m_length == 0) {
        m_owner->m_client.m_ring_buffer.clear();
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!SendIOContext::handleComplete(cqe, handle)) {
        return false;
    }
    return handleSendResult();
}
#else
bool MysqlPrepareAwaitable::ProtocolSendAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        syncSendWindow();
        if (m_length == 0) {
            m_owner->m_client.m_ring_buffer.clear();
            return true;
        }

        if (!SendIOContext::handleComplete(handle)) {
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
}

bool MysqlPrepareAwaitable::ProtocolRecvAwaitable::prepareReadIovecs()
{
    return detail::prepareReadIovecs(
        m_owner->m_client.m_ring_buffer,
        m_iovecs,
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space")); }
    );
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

    if (!prepareReadIovecs()) {
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

        if (!prepareReadIovecs()) {
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

MysqlPrepareAwaitable::MysqlPrepareAwaitable(MysqlClient& client, std::string_view sql)
    : CustomAwaitable(client.m_socket.controller())
    , m_client(client)
    , m_encoded_cmd(m_client.m_encoder.encodeStmtPrepare(sql, 0))
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
        auto read_iovecs = m_client.m_ring_buffer.getReadIovecs();
        if (read_iovecs.empty()) {
            return false;
        }

        const char* data = static_cast<const char*>(read_iovecs[0].iov_base);
        size_t len = read_iovecs[0].iov_len;
        if (read_iovecs.size() > 1) len += read_iovecs[1].iov_len;

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
    : SendAwaitable(owner->m_client.m_socket.controller(),
                    owner->m_encoded_cmd.data(),
                    owner->m_encoded_cmd.size())
    , m_owner(owner)
{
}

void MysqlStmtExecuteAwaitable::ProtocolSendAwaitable::syncSendWindow()
{
    detail::syncSendWindow(m_owner->m_encoded_cmd, m_owner->m_sent, m_buffer, m_length);
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

    syncSendWindow();
    if (m_length == 0) {
        m_owner->m_client.m_ring_buffer.clear();
        return true;
    }

    if (cqe == nullptr) {
        return false;
    }

    if (!SendIOContext::handleComplete(cqe, handle)) {
        return false;
    }
    return handleSendResult();
}
#else
bool MysqlStmtExecuteAwaitable::ProtocolSendAwaitable::handleComplete(GHandle handle)
{
    while (m_owner->m_lifecycle == Lifecycle::Running) {
        syncSendWindow();
        if (m_length == 0) {
            m_owner->m_client.m_ring_buffer.clear();
            return true;
        }

        if (!SendIOContext::handleComplete(handle)) {
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
}

bool MysqlStmtExecuteAwaitable::ProtocolRecvAwaitable::prepareReadIovecs()
{
    return detail::prepareReadIovecs(
        m_owner->m_client.m_ring_buffer,
        m_iovecs,
        [&]() { m_owner->setError(MysqlError(MYSQL_ERROR_RECV, "No writable ring buffer space")); }
    );
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

    if (!prepareReadIovecs()) {
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

        if (!prepareReadIovecs()) {
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

MysqlStmtExecuteAwaitable::MysqlStmtExecuteAwaitable(MysqlClient& client, std::string encoded_cmd)
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
        auto read_iovecs = m_client.m_ring_buffer.getReadIovecs();
        if (read_iovecs.empty()) {
            return false;
        }

        const char* data = static_cast<const char*>(read_iovecs[0].iov_base);
        size_t len = read_iovecs[0].iov_len;
        if (read_iovecs.size() > 1) len += read_iovecs[1].iov_len;

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

// ======================== MysqlClient 实现 ========================

MysqlClient::MysqlClient(IOScheduler* scheduler, AsyncMysqlConfig config)
    : m_scheduler(scheduler)
    , m_config(std::move(config))
    , m_ring_buffer(m_config.buffer_size)
{
    try {
        m_logger = spdlog::get("MysqlClientLogger");
        if (!m_logger) {
            m_logger = spdlog::stdout_color_mt("MysqlClientLogger");
        }
    } catch (const spdlog::spdlog_ex&) {
        m_logger = spdlog::get("MysqlClientLogger");
    }
}

MysqlClient::MysqlClient(MysqlClient&& other) noexcept
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

MysqlClient& MysqlClient::operator=(MysqlClient&& other) noexcept
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
        m_connect_awaitable.reset();
        m_query_awaitable.reset();
        m_prepare_awaitable.reset();
        m_stmt_execute_awaitable.reset();
        m_logger = std::move(other.m_logger);
        other.m_is_closed = true;
    }
    return *this;
}

MysqlConnectAwaitable& MysqlClient::connect(MysqlConfig config)
{
    if (!m_connect_awaitable.has_value() || m_connect_awaitable->isInvalid()) {
        m_connect_awaitable.emplace(*this, std::move(config));
    }
    return *m_connect_awaitable;
}

MysqlConnectAwaitable& MysqlClient::connect(std::string_view host, uint16_t port,
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

MysqlQueryAwaitable& MysqlClient::query(std::string_view sql)
{
    if (!m_query_awaitable.has_value() || m_query_awaitable->isInvalid()) {
        m_query_awaitable.emplace(*this, sql);
    }
    return *m_query_awaitable;
}

MysqlPrepareAwaitable& MysqlClient::prepare(std::string_view sql)
{
    if (!m_prepare_awaitable.has_value() || m_prepare_awaitable->isInvalid()) {
        m_prepare_awaitable.emplace(*this, sql);
    }
    return *m_prepare_awaitable;
}

MysqlStmtExecuteAwaitable& MysqlClient::stmtExecute(uint32_t stmt_id,
                                                       std::span<const std::optional<std::string>> params,
                                                       std::span<const uint8_t> param_types)
{
    if (!m_stmt_execute_awaitable.has_value() || m_stmt_execute_awaitable->isInvalid()) {
        m_stmt_execute_awaitable.emplace(*this, m_encoder.encodeStmtExecute(stmt_id, params, param_types, 0));
    }
    return *m_stmt_execute_awaitable;
}

MysqlStmtExecuteAwaitable& MysqlClient::stmtExecute(uint32_t stmt_id,
                                                       std::span<const std::optional<std::string_view>> params,
                                                       std::span<const uint8_t> param_types)
{
    if (!m_stmt_execute_awaitable.has_value() || m_stmt_execute_awaitable->isInvalid()) {
        m_stmt_execute_awaitable.emplace(*this, m_encoder.encodeStmtExecute(stmt_id, params, param_types, 0));
    }
    return *m_stmt_execute_awaitable;
}

MysqlQueryAwaitable& MysqlClient::beginTransaction()
{
    return query("BEGIN");
}

MysqlQueryAwaitable& MysqlClient::commit()
{
    return query("COMMIT");
}

MysqlQueryAwaitable& MysqlClient::rollback()
{
    return query("ROLLBACK");
}

MysqlQueryAwaitable& MysqlClient::ping()
{
    return query("SELECT 1");
}

MysqlQueryAwaitable& MysqlClient::useDatabase(std::string_view database)
{
    std::string sql;
    sql.reserve(4 + database.size());
    sql.append("USE ");
    sql.append(database.data(), database.size());
    return query(sql);
}

} // namespace galay::mysql
