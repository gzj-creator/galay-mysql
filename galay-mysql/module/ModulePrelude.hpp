#pragma once
// Auto prelude for transitional C++23 module builds on Clang/GCC/MSVC.
// Keep third-party/system/dependency headers in global module fragment.

#if __has_include(<algorithm>)
#include <algorithm>
#endif
#if __has_include(<arpa/inet.h>)
#include <arpa/inet.h>
#endif
#if __has_include(<atomic>)
#include <atomic>
#endif
#if __has_include(<cerrno>)
#include <cerrno>
#endif
#if __has_include(<chrono>)
#include <chrono>
#endif
#if __has_include(<concepts>)
#include <concepts>
#endif
#if __has_include(<coroutine>)
#include <coroutine>
#endif
#if __has_include(<cstddef>)
#include <cstddef>
#endif
#if __has_include(<cstdint>)
#include <cstdint>
#endif
#if __has_include(<cstring>)
#include <cstring>
#endif
#if __has_include(<expected>)
#include <expected>
#endif
#if __has_include(<fcntl.h>)
#include <fcntl.h>
#endif
#if __has_include(<galay-kernel/async/TcpSocket.h>)
#include <galay-kernel/async/TcpSocket.h>
#endif
#if __has_include(<galay-kernel/common/Buffer.h>)
#include <galay-kernel/common/Buffer.h>
#endif
#if __has_include(<galay-kernel/common/Error.h>)
#include <galay-kernel/common/Error.h>
#endif
#if __has_include(<galay-kernel/common/Host.hpp>)
#include <galay-kernel/common/Host.hpp>
#endif
#if __has_include(<galay-kernel/concurrency/AsyncWaiter.h>)
#include <galay-kernel/concurrency/AsyncWaiter.h>
#endif
#if __has_include(<galay-kernel/kernel/Coroutine.h>)
#include <galay-kernel/kernel/Coroutine.h>
#endif
#if __has_include(<galay-kernel/kernel/IOScheduler.hpp>)
#include <galay-kernel/kernel/IOScheduler.hpp>
#endif
#if __has_include(<galay-kernel/kernel/Timeout.hpp>)
#include <galay-kernel/kernel/Timeout.hpp>
#endif
#if __has_include(<memory>)
#include <memory>
#endif
#if __has_include(<mutex>)
#include <mutex>
#endif
#if __has_include(<netdb.h>)
#include <netdb.h>
#endif
#if __has_include(<netinet/in.h>)
#include <netinet/in.h>
#endif
#if __has_include(<openssl/sha.h>)
#include <openssl/sha.h>
#endif
#if __has_include(<optional>)
#include <optional>
#endif
#if __has_include(<poll.h>)
#include <poll.h>
#endif
#if __has_include(<queue>)
#include <queue>
#endif
#if __has_include(<span>)
#include <span>
#endif
#if __has_include(<spdlog/sinks/stdout_color_sinks.h>)
#include <spdlog/sinks/stdout_color_sinks.h>
#endif
#if __has_include(<spdlog/spdlog.h>)
#include <spdlog/spdlog.h>
#endif
#if __has_include(<stdexcept>)
#include <stdexcept>
#endif
#if __has_include(<string>)
#include <string>
#endif
#if __has_include(<string_view>)
#include <string_view>
#endif
#if __has_include(<sys/socket.h>)
#include <sys/socket.h>
#endif
#if __has_include(<unistd.h>)
#include <unistd.h>
#endif
#if __has_include(<unordered_map>)
#include <unordered_map>
#endif
#if __has_include(<utility>)
#include <utility>
#endif
#if __has_include(<vector>)
#include <vector>
#endif
#if __has_include("galay-mysql/async/AsyncMysqlConfig.h")
#include "galay-mysql/async/AsyncMysqlConfig.h"
#endif
#if __has_include("galay-mysql/async/AsyncMysqlClient.h")
#include "galay-mysql/async/AsyncMysqlClient.h"
#endif
#if __has_include("galay-mysql/async/MysqlConnectionPool.h")
#include "galay-mysql/async/MysqlConnectionPool.h"
#endif
#if __has_include("galay-mysql/base/MysqlConfig.h")
#include "galay-mysql/base/MysqlConfig.h"
#endif
#if __has_include("galay-mysql/base/MysqlError.h")
#include "galay-mysql/base/MysqlError.h"
#endif
#if __has_include("galay-mysql/base/MysqlLog.h")
#include "galay-mysql/base/MysqlLog.h"
#endif
#if __has_include("galay-mysql/base/MysqlValue.h")
#include "galay-mysql/base/MysqlValue.h"
#endif
#if __has_include("galay-mysql/module/ModulePrelude.hpp")
#include "galay-mysql/module/ModulePrelude.hpp"
#endif
#if __has_include("galay-mysql/protocol/Connection.h")
#include "galay-mysql/protocol/Connection.h"
#endif
#if __has_include("galay-mysql/protocol/MysqlAuth.h")
#include "galay-mysql/protocol/MysqlAuth.h"
#endif
#if __has_include("galay-mysql/protocol/MysqlProtocol.h")
#include "galay-mysql/protocol/MysqlProtocol.h"
#endif
#if __has_include("galay-mysql/sync/MysqlClient.h")
#include "galay-mysql/sync/MysqlClient.h"
#endif
