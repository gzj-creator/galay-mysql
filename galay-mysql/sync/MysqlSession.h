#ifndef GALAY_MYSQL_SESSION_H
#define GALAY_MYSQL_SESSION_H

#include "galay-mysql/base/MysqlError.h"
#include "galay-mysql/base/MysqlValue.h"
#include "galay-mysql/base/MysqlConfig.h"
#include "galay-mysql/protocol/MysqlProtocol.h"
#include "galay-mysql/protocol/MysqlAuth.h"
#include "galay-mysql/protocol/Connection.h"
#include <string>
#include <expected>
#include <vector>
#include <optional>
#include <cstdint>

namespace galay::mysql
{

using MysqlResult = std::expected<MysqlResultSet, MysqlError>;
using MysqlVoidResult = std::expected<void, MysqlError>;

/**
 * @brief 同步MySQL会话
 * @details 使用阻塞socket的同步MySQL客户端
 *
 * @code
 * MysqlSession session;
 * auto config = MysqlConfig::create("127.0.0.1", 3306, "root", "password", "test_db");
 * auto result = session.connect(config);
 * if (!result) { return; }
 * auto query_result = session.query("SELECT * FROM users");
 * session.close();
 * @endcode
 */
class MysqlSession
{
public:
    MysqlSession();
    ~MysqlSession();

    MysqlSession(const MysqlSession&) = delete;
    MysqlSession& operator=(const MysqlSession&) = delete;
    MysqlSession(MysqlSession&& other) noexcept;
    MysqlSession& operator=(MysqlSession&& other) noexcept;

    // ======================== 连接 ========================

    MysqlVoidResult connect(const MysqlConfig& config);
    MysqlVoidResult connect(const std::string& host, uint16_t port,
                            const std::string& user, const std::string& password,
                            const std::string& database = "");

    // ======================== 查询 ========================

    MysqlResult query(const std::string& sql);

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
    bool isConnected() const { return m_connection.isConnected(); }

private:
    /**
     * @brief 接收并解析完整的查询结果
     */
    MysqlResult receiveResultSet();

    /**
     * @brief 执行简单命令并检查OK/ERR
     */
    MysqlVoidResult executeSimple(const std::string& sql);

    protocol::Connection m_connection;
    protocol::MysqlParser m_parser;
    protocol::MysqlEncoder m_encoder;
    uint32_t m_server_capabilities = 0;
};

} // namespace galay::mysql

#endif // GALAY_MYSQL_SESSION_H
