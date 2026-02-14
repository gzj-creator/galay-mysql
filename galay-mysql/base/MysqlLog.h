#ifndef GALAY_MYSQL_LOG_H
#define GALAY_MYSQL_LOG_H

#include <spdlog/spdlog.h>

#define MysqlLogTrace(logger, ...) \
    do { if (logger) SPDLOG_LOGGER_TRACE(logger, __VA_ARGS__); } while(0)

#define MysqlLogDebug(logger, ...) \
    do { if (logger) SPDLOG_LOGGER_DEBUG(logger, __VA_ARGS__); } while(0)

#define MysqlLogInfo(logger, ...) \
    do { if (logger) SPDLOG_LOGGER_INFO(logger, __VA_ARGS__); } while(0)

#define MysqlLogWarn(logger, ...) \
    do { if (logger) SPDLOG_LOGGER_WARN(logger, __VA_ARGS__); } while(0)

#define MysqlLogError(logger, ...) \
    do { if (logger) SPDLOG_LOGGER_ERROR(logger, __VA_ARGS__); } while(0)

#endif // GALAY_MYSQL_LOG_H
