#include "MysqlSession.h"
#include <cstring>

namespace galay::mysql
{

MysqlSession::MysqlSession() = default;
MysqlSession::~MysqlSession() { close(); }

MysqlSession::MysqlSession(MysqlSession&& other) noexcept
    : m_connection(std::move(other.m_connection))
    , m_parser(std::move(other.m_parser))
    , m_encoder(std::move(other.m_encoder))
    , m_server_capabilities(other.m_server_capabilities)
{
    other.m_server_capabilities = 0;
}

MysqlSession& MysqlSession::operator=(MysqlSession&& other) noexcept
{
    if (this != &other) {
        close();
        m_connection = std::move(other.m_connection);
        m_parser = std::move(other.m_parser);
        m_encoder = std::move(other.m_encoder);
        m_server_capabilities = other.m_server_capabilities;
        other.m_server_capabilities = 0;
    }
    return *this;
}

MysqlVoidResult MysqlSession::connect(const MysqlConfig& config)
{
    // TCP连接
    auto conn_result = m_connection.connect(config.host, config.port, config.connect_timeout_ms);
    if (!conn_result) return std::unexpected(conn_result.error());

    // 读取握手包
    auto pkt_result = m_connection.recvPacket();
    if (!pkt_result) return std::unexpected(pkt_result.error());
    auto& [seq_id, payload] = pkt_result.value();

    if (static_cast<uint8_t>(payload[0]) == 0xFF) {
        auto err = m_parser.parseErr(payload.data(), payload.size(), protocol::CLIENT_PROTOCOL_41);
        if (err) return std::unexpected(MysqlError(MYSQL_ERROR_SERVER, err->error_code, err->error_message));
        return std::unexpected(MysqlError(MYSQL_ERROR_CONNECTION, "Server sent error during handshake"));
    }

    auto hs = m_parser.parseHandshake(payload.data(), payload.size());
    if (!hs) return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse handshake"));
    m_server_capabilities = hs->capability_flags;

    // 构建认证响应
    protocol::HandshakeResponse41 resp;
    resp.capability_flags = protocol::CLIENT_PROTOCOL_41
        | protocol::CLIENT_SECURE_CONNECTION
        | protocol::CLIENT_PLUGIN_AUTH
        | protocol::CLIENT_TRANSACTIONS
        | protocol::CLIENT_MULTI_STATEMENTS
        | protocol::CLIENT_MULTI_RESULTS
        | protocol::CLIENT_PS_MULTI_RESULTS
        | protocol::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    if (!config.database.empty()) {
        resp.capability_flags |= protocol::CLIENT_CONNECT_WITH_DB;
    }
    resp.capability_flags &= hs->capability_flags;
    m_server_capabilities = resp.capability_flags;  // 使用协商后的能力标志
    resp.character_set = protocol::CHARSET_UTF8MB4_GENERAL_CI;
    resp.username = config.username;
    resp.database = config.database;
    resp.auth_plugin_name = hs->auth_plugin_name;

    if (hs->auth_plugin_name == "mysql_native_password") {
        resp.auth_response = protocol::AuthPlugin::nativePasswordAuth(config.password, hs->auth_plugin_data);
    } else if (hs->auth_plugin_name == "caching_sha2_password") {
        resp.auth_response = protocol::AuthPlugin::cachingSha2Auth(config.password, hs->auth_plugin_data);
    } else {
        resp.auth_response = protocol::AuthPlugin::nativePasswordAuth(config.password, hs->auth_plugin_data);
        resp.auth_plugin_name = "mysql_native_password";
    }

    auto auth_packet = m_encoder.encodeHandshakeResponse(resp, seq_id + 1);
    auto send_result = m_connection.send(auth_packet);
    if (!send_result) return std::unexpected(send_result.error());

    // 读取认证结果
    auto auth_result = m_connection.recvPacket();
    if (!auth_result) return std::unexpected(auth_result.error());
    auto& [auth_seq, auth_payload] = auth_result.value();

    uint8_t first_byte = static_cast<uint8_t>(auth_payload[0]);
    if (first_byte == 0x00) {
        return {};  // OK - auth success
    } else if (first_byte == 0xFF) {
        auto err = m_parser.parseErr(auth_payload.data(), auth_payload.size(), m_server_capabilities);
        if (err) return std::unexpected(MysqlError(MYSQL_ERROR_AUTH, err->error_code, err->error_message));
        return std::unexpected(MysqlError(MYSQL_ERROR_AUTH, "Authentication failed"));
    } else if (first_byte == 0x01) {
        // caching_sha2_password fast auth
        if (auth_payload.size() == 2 && static_cast<uint8_t>(auth_payload[1]) == 0x03) {
            // Fast auth success - read the actual OK/ERR packet
            auto ok_result = m_connection.recvPacket();
            if (!ok_result) return std::unexpected(ok_result.error());
            auto& [ok_seq, ok_payload] = ok_result.value();
            uint8_t ok_first = static_cast<uint8_t>(ok_payload[0]);
            if (ok_first == 0x00) {
                return {};  // OK
            } else if (ok_first == 0xFF) {
                auto err = m_parser.parseErr(ok_payload.data(), ok_payload.size(), m_server_capabilities);
                if (err) return std::unexpected(MysqlError(MYSQL_ERROR_AUTH, err->error_code, err->error_message));
                return std::unexpected(MysqlError(MYSQL_ERROR_AUTH, "Authentication failed"));
            }
            return {};
        }
        return std::unexpected(MysqlError(MYSQL_ERROR_AUTH, "Full auth not supported"));
    }
    return std::unexpected(MysqlError(MYSQL_ERROR_AUTH, "Unexpected auth response"));
}

MysqlVoidResult MysqlSession::connect(const std::string& host, uint16_t port,
                                       const std::string& user, const std::string& password,
                                       const std::string& database)
{
    return connect(MysqlConfig::create(host, port, user, password, database));
}

MysqlResult MysqlSession::query(const std::string& sql)
{
    auto cmd = m_encoder.encodeQuery(sql, 0);
    auto send_result = m_connection.send(cmd);
    if (!send_result) return std::unexpected(MysqlError(MYSQL_ERROR_SEND, send_result.error().message()));
    return receiveResultSet();
}

MysqlResult MysqlSession::receiveResultSet()
{
    auto pkt_result = m_connection.recvPacket();
    if (!pkt_result) return std::unexpected(pkt_result.error());
    auto& [seq_id, payload] = pkt_result.value();

    uint8_t first_byte = static_cast<uint8_t>(payload[0]);

    // ERR packet
    if (first_byte == 0xFF) {
        auto err = m_parser.parseErr(payload.data(), payload.size(), m_server_capabilities);
        if (err) return std::unexpected(MysqlError(MYSQL_ERROR_SERVER, err->error_code, err->error_message));
        return std::unexpected(MysqlError(MYSQL_ERROR_QUERY, "Query error"));
    }

    // OK packet (INSERT/UPDATE/DELETE)
    if (first_byte == 0x00) {
        auto ok = m_parser.parseOk(payload.data(), payload.size(), m_server_capabilities);
        if (!ok) return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse OK"));
        MysqlResultSet rs;
        rs.setAffectedRows(ok->affected_rows);
        rs.setLastInsertId(ok->last_insert_id);
        rs.setWarnings(ok->warnings);
        rs.setStatusFlags(ok->status_flags);
        rs.setInfo(ok->info);
        return rs;
    }

    // Result set - parse column count
    size_t int_consumed = 0;
    auto col_count_result = protocol::readLenEncInt(payload.data(), payload.size(), int_consumed);
    if (!col_count_result) return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse column count"));
    uint64_t col_count = col_count_result.value();

    MysqlResultSet rs;

    // Read column definitions
    for (uint64_t i = 0; i < col_count; ++i) {
        auto col_pkt = m_connection.recvPacket();
        if (!col_pkt) return std::unexpected(col_pkt.error());
        auto& [cseq, cpayload] = col_pkt.value();
        auto col = m_parser.parseColumnDefinition(cpayload.data(), cpayload.size());
        if (!col) return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse column definition"));
        MysqlField field(col->name, static_cast<MysqlFieldType>(col->column_type),
                        col->flags, col->column_length, col->decimals);
        field.setCatalog(col->catalog);
        field.setSchema(col->schema);
        field.setTable(col->table);
        field.setOrgTable(col->org_table);
        field.setOrgName(col->org_name);
        field.setCharacterSet(col->character_set);
        rs.addField(std::move(field));
    }

    // Read EOF after columns (if not CLIENT_DEPRECATE_EOF)
    if (!(m_server_capabilities & protocol::CLIENT_DEPRECATE_EOF)) {
        auto eof_pkt = m_connection.recvPacket();
        if (!eof_pkt) return std::unexpected(eof_pkt.error());
    }

    // Read rows until EOF/OK
    while (true) {
        auto row_pkt = m_connection.recvPacket();
        if (!row_pkt) return std::unexpected(row_pkt.error());
        auto& [rseq, rpayload] = row_pkt.value();

        uint8_t fb = static_cast<uint8_t>(rpayload[0]);
        if (fb == 0xFE && rpayload.size() < 0xFFFFFF) {
            // EOF or OK (end of rows)
            if (m_server_capabilities & protocol::CLIENT_DEPRECATE_EOF) {
                auto ok = m_parser.parseOk(rpayload.data(), rpayload.size(), m_server_capabilities);
                if (ok) { rs.setWarnings(ok->warnings); rs.setStatusFlags(ok->status_flags); }
            } else {
                auto eof = m_parser.parseEof(rpayload.data(), rpayload.size());
                if (eof) { rs.setWarnings(eof->warnings); rs.setStatusFlags(eof->status_flags); }
            }
            break;
        }
        if (fb == 0xFF) {
            auto err = m_parser.parseErr(rpayload.data(), rpayload.size(), m_server_capabilities);
            if (err) return std::unexpected(MysqlError(MYSQL_ERROR_SERVER, err->error_code, err->error_message));
            return std::unexpected(MysqlError(MYSQL_ERROR_QUERY, "Error during row fetch"));
        }

        auto row = m_parser.parseTextRow(rpayload.data(), rpayload.size(), col_count);
        if (!row) return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse row"));
        rs.addRow(MysqlRow(std::move(row.value())));
    }

    return rs;
}

std::expected<MysqlSession::PrepareResult, MysqlError> MysqlSession::prepare(const std::string& sql)
{
    auto cmd = m_encoder.encodeStmtPrepare(sql, 0);
    auto send_result = m_connection.send(cmd);
    if (!send_result) return std::unexpected(MysqlError(MYSQL_ERROR_SEND, send_result.error().message()));

    auto pkt_result = m_connection.recvPacket();
    if (!pkt_result) return std::unexpected(pkt_result.error());
    auto& [seq_id, payload] = pkt_result.value();

    if (static_cast<uint8_t>(payload[0]) == 0xFF) {
        auto err = m_parser.parseErr(payload.data(), payload.size(), m_server_capabilities);
        if (err) return std::unexpected(MysqlError(MYSQL_ERROR_PREPARED_STMT, err->error_code, err->error_message));
        return std::unexpected(MysqlError(MYSQL_ERROR_PREPARED_STMT, "Prepare failed"));
    }

    auto ok = m_parser.parseStmtPrepareOk(payload.data(), payload.size());
    if (!ok) return std::unexpected(MysqlError(MYSQL_ERROR_PROTOCOL, "Failed to parse prepare ok"));

    // Skip param definitions + EOF
    for (uint16_t i = 0; i < ok->num_params; ++i) {
        auto p = m_connection.recvPacket();
        if (!p) return std::unexpected(p.error());
    }
    if (ok->num_params > 0 && !(m_server_capabilities & protocol::CLIENT_DEPRECATE_EOF)) {
        auto eof = m_connection.recvPacket();
        if (!eof) return std::unexpected(eof.error());
    }

    // Skip column definitions + EOF
    for (uint16_t i = 0; i < ok->num_columns; ++i) {
        auto c = m_connection.recvPacket();
        if (!c) return std::unexpected(c.error());
    }
    if (ok->num_columns > 0 && !(m_server_capabilities & protocol::CLIENT_DEPRECATE_EOF)) {
        auto eof = m_connection.recvPacket();
        if (!eof) return std::unexpected(eof.error());
    }

    return PrepareResult{ok->statement_id, ok->num_columns, ok->num_params};
}

MysqlResult MysqlSession::stmtExecute(uint32_t stmt_id,
                                       const std::vector<std::optional<std::string>>& params,
                                       const std::vector<uint8_t>& param_types)
{
    auto cmd = m_encoder.encodeStmtExecute(stmt_id, params, param_types, 0);
    auto send_result = m_connection.send(cmd);
    if (!send_result) return std::unexpected(MysqlError(MYSQL_ERROR_SEND, send_result.error().message()));
    return receiveResultSet();
}

MysqlVoidResult MysqlSession::stmtClose(uint32_t stmt_id)
{
    auto cmd = m_encoder.encodeStmtClose(stmt_id, 0);
    auto send_result = m_connection.send(cmd);
    if (!send_result) return std::unexpected(MysqlError(MYSQL_ERROR_SEND, send_result.error().message()));
    return {};
}

MysqlVoidResult MysqlSession::executeSimple(const std::string& sql)
{
    auto result = query(sql);
    if (!result) return std::unexpected(result.error());
    return {};
}

MysqlVoidResult MysqlSession::beginTransaction() { return executeSimple("BEGIN"); }
MysqlVoidResult MysqlSession::commit() { return executeSimple("COMMIT"); }
MysqlVoidResult MysqlSession::rollback() { return executeSimple("ROLLBACK"); }
MysqlVoidResult MysqlSession::ping() { return executeSimple("SELECT 1"); }
MysqlVoidResult MysqlSession::useDatabase(const std::string& database) { return executeSimple("USE " + database); }

void MysqlSession::close()
{
    if (m_connection.isConnected()) {
        auto quit = m_encoder.encodeQuit(0);
        m_connection.send(quit);  // best effort
        m_connection.disconnect();
    }
}

} // namespace galay::mysql
