# Galay-MySQL

基于 [galay-kernel](https://github.com/gzj-creator/galay) 的 C++23 高性能 MySQL 客户端库，提供异步与同步两套 API。

## 特性

- C++23 + 协程异步 API（`CustomAwaitable` 链式收发）
- 同步阻塞 API，便于脚本化和快速接入
- 支持文本协议、预处理语句、事务、认证流程
- `std::expected` 错误模型，避免异常路径开销
- 结果集行预分配提示与环形缓冲区

## 依赖

- 支持 C++23 的编译器（推荐 GCC 13+、Clang 16+）
- CMake 3.20+
- OpenSSL
- spdlog
- [galay-kernel](https://github.com/gzj-creator/galay)
- MySQL 5.7+ / 8.0+（推荐 8.0）

## 构建

```bash
git clone https://github.com/gzj-creator/galay-mysql.git
cd galay-mysql
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

## 异步快速示例

```cpp
#include <atomic>
#include <chrono>
#include <expected>
#include <iostream>
#include <optional>
#include <thread>
#include <galay-kernel/kernel/Runtime.h>
#include "galay-mysql/async/MysqlClient.h"

using namespace galay::kernel;
using namespace galay::mysql;

struct RunState {
    std::atomic<bool> done{false};
};

Coroutine run(IOScheduler* scheduler, RunState* state) {
    MysqlClient client(scheduler);

    auto& conn_aw = client.connect("127.0.0.1", 3306, "root", "password", "test");
    std::expected<std::optional<bool>, MysqlError> conn_result;
    do {
        conn_result = co_await conn_aw;
        if (!conn_result) {
            std::cerr << "connect failed: " << conn_result.error().message() << '\n';
            state->done.store(true, std::memory_order_release);
            co_return;
        }
    } while (!conn_result->has_value());

    auto& query_aw = client.query("SELECT 1");
    std::expected<std::optional<MysqlResultSet>, MysqlError> query_result;
    do {
        query_result = co_await query_aw;
        if (!query_result) {
            std::cerr << "query failed: " << query_result.error().message() << '\n';
            co_await client.close();
            state->done.store(true, std::memory_order_release);
            co_return;
        }
    } while (!query_result->has_value());

    const MysqlResultSet& rs = query_result->value();
    if (rs.rowCount() > 0) {
        std::cout << "SELECT 1 => " << rs.row(0).getString(0) << '\n';
    }

    co_await client.close();
    state->done.store(true, std::memory_order_release);
}

int main() {
    Runtime runtime;
    runtime.start();

    auto* scheduler = runtime.getNextIOScheduler();
    if (!scheduler) return 1;

    RunState state;
    scheduler->spawn(run(scheduler, &state));

    using namespace std::chrono_literals;
    while (!state.done.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(50ms);
    }

    runtime.stop();
    return 0;
}
```

## 同步快速示例

```cpp
#include <iostream>
#include "galay-mysql/sync/MysqlSession.h"

using namespace galay::mysql;

int main() {
    MysqlSession session;

    MysqlConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 3306;
    cfg.username = "root";
    cfg.password = "password";
    cfg.database = "test";
    cfg.connect_timeout_ms = 5000;

    auto conn = session.connect(cfg);
    if (!conn) {
        std::cerr << "connect failed: " << conn.error().message() << '\n';
        return 1;
    }

    auto res = session.query("SELECT id, name FROM users LIMIT 5");
    if (!res) {
        std::cerr << "query failed: " << res.error().message() << '\n';
        session.close();
        return 1;
    }

    for (size_t i = 0; i < res->rowCount(); ++i) {
        const auto& row = res->row(i);
        std::cout << row.getString(0) << " " << row.getString(1) << '\n';
    }

    session.close();
    return 0;
}
```

## 测试

可通过环境变量配置测试库连接：

- `GALAY_MYSQL_HOST`
- `GALAY_MYSQL_PORT`
- `GALAY_MYSQL_USER`
- `GALAY_MYSQL_PASSWORD`
- `GALAY_MYSQL_DB`

运行示例：

```bash
GALAY_MYSQL_HOST=127.0.0.1 \
GALAY_MYSQL_PORT=3306 \
GALAY_MYSQL_USER=root \
GALAY_MYSQL_PASSWORD=password \
GALAY_MYSQL_DB=test \
./build/test/T3-AsyncMysqlClient
```

## 文档

- [快速开始](docs/01-快速开始.md)
- [架构设计](docs/02-架构设计.md)
- [API 文档](docs/03-API文档.md)
- [示例代码](docs/04-示例代码.md)

## 相关项目

- [galay-kernel](https://github.com/gzj-creator/galay)
