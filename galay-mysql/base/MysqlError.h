#ifndef GALAY_MYSQL_ERROR_H
#define GALAY_MYSQL_ERROR_H

#include <string>
#include <cstdint>

namespace galay::mysql
{

enum MysqlErrorType
{
    MYSQL_ERROR_SUCCESS,
    MYSQL_ERROR_CONNECTION,
    MYSQL_ERROR_AUTH,
    MYSQL_ERROR_QUERY,
    MYSQL_ERROR_PROTOCOL,
    MYSQL_ERROR_TIMEOUT,
    MYSQL_ERROR_SEND,
    MYSQL_ERROR_RECV,
    MYSQL_ERROR_CONNECTION_CLOSED,
    MYSQL_ERROR_PREPARED_STMT,
    MYSQL_ERROR_TRANSACTION,
    MYSQL_ERROR_SERVER,
    MYSQL_ERROR_INTERNAL,
    MYSQL_ERROR_BUFFER_OVERFLOW,
    MYSQL_ERROR_INVALID_PARAM,
};

class MysqlError
{
public:
    MysqlError(MysqlErrorType type);
    MysqlError(MysqlErrorType type, std::string extra_msg);
    MysqlError(MysqlErrorType type, uint16_t server_errno, std::string server_msg);

    MysqlErrorType type() const;
    std::string message() const;
    uint16_t serverErrno() const;

private:
    MysqlErrorType m_type;
    uint16_t m_server_errno = 0;
    std::string m_extra_msg;
};

} // namespace galay::mysql

#endif // GALAY_MYSQL_ERROR_H
