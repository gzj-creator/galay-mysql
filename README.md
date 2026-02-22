# Galay-MySQL

基于 [galay-kernel](https://github.com/gzj-creator/galay-kernel) 的 C++23 高性能 MySQL 客户端库，提供异步与同步两套 API。

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
- Galay 内部依赖（统一联调推荐）：
  - [galay-kernel](https://github.com/gzj-creator/galay-kernel)（构建必需）
  - [galay-utils](https://github.com/gzj-creator/galay-utils)（推荐）
  - [galay-http](https://github.com/gzj-creator/galay-http)（推荐）
- MySQL 5.7+ / 8.0+（推荐 8.0）

## 依赖安装（macOS / Homebrew）

```bash
brew install cmake spdlog openssl mysql-client
```

## 依赖安装（Ubuntu / Debian）

```bash
sudo apt-get update
sudo apt-get install -y cmake g++ libspdlog-dev libssl-dev default-libmysqlclient-dev
```

## 构建

```bash
git clone https://github.com/gzj-creator/galay-kernel.git
git clone https://github.com/gzj-creator/galay-utils.git
git clone https://github.com/gzj-creator/galay-http.git
git clone https://github.com/gzj-creator/galay-mysql.git
cd galay-mysql
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

仅单独构建 `galay-mysql` 时，最小内部依赖为 `galay-kernel`。

常用开关：

- `-DGALAY_MYSQL_BUILD_TESTS=ON/OFF`
- `-DGALAY_MYSQL_BUILD_EXAMPLES=ON/OFF`
- `-DGALAY_MYSQL_BUILD_SHARED_LIBS=ON/OFF`
- `-DGALAY_MYSQL_ENABLE_IMPORT_COMPILATION=ON/OFF`
- `-DGALAY_MYSQL_BUILD_MODULE_EXAMPLES=ON/OFF`

> import 编译当前仅在 `Linux + CMake >= 3.28 + Ninja/Visual Studio + GCC >= 14` 自动开启；不满足条件会自动降级为关闭。
> 兼容旧开关 `GALAY_MYSQL_BUILD_IMPORT_EXAMPLES`，但已废弃，建议切换为 `GALAY_MYSQL_BUILD_MODULE_EXAMPLES`。
> Clang 模块链路会自动检测 `clang-scan-deps`；满足条件时可启用 import 编译路径，不满足条件会自动降级关闭。

## Include / Import

传统包含方式：

```cpp
#include "galay-mysql/async/AsyncMysqlClient.h"
#include "galay-mysql/sync/MysqlClient.h"
```

支持模块的编译器可使用：

```cpp
import galay.mysql;
```

模块接口文件约定：

- `galay-mysql/module/galay.mysql.cppm`
- 统一使用 `.cppm` 后缀

### 模块支持更新（2026-02）

本次模块接口统一为：

- `module;`
- `#include "galay-mysql/module/ModulePrelude.hpp"`
- `export module galay.mysql;`
- `export { #include ... }`

新增预导入头文件：`galay-mysql/module/ModulePrelude.hpp`。  
推荐使用 CMake `>= 3.28` + `Ninja`/`Visual Studio` + 可用 `clang-scan-deps`（Clang）。

示例（Clang 20）：

```bash
cmake -S . -B build-mod -G Ninja \
  -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@20/bin/clang++
cmake --build build-mod --target galay-mysql --parallel
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

#if defined(__cpp_modules) && __cpp_modules >= 201907L
import galay.mysql;
#else
#include "galay-mysql/async/AsyncMysqlClient.h"
#endif

using namespace galay::kernel;
using namespace galay::mysql;

struct RunState {
    std::atomic<bool> done{false};
};

Coroutine run(IOScheduler* scheduler, RunState* state) {
    AsyncMysqlClient client(scheduler);

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

#if defined(__cpp_modules) && __cpp_modules >= 201907L
import galay.mysql;
#else
#include "galay-mysql/sync/MysqlClient.h"
#endif

using namespace galay::mysql;

int main() {
    MysqlClient session;

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

## 示例目录

项目新增 `example/`，每个功能都提供 include/import 两套示例：

- `E1` 异步查询
- `E2` 同步查询
- `E3` 异步连接池
- `E4` 同步预处理 + 事务

构建并运行 include 版本：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DGALAY_MYSQL_BUILD_EXAMPLES=ON
cmake --build build --parallel
./build/example/E1-AsyncQuery-Include
./build/example/E2-SyncQuery-Include
```

尝试构建 import 版本（支持时）：

```bash
cmake -S . -B build-import \
  -DCMAKE_BUILD_TYPE=Release \
  -DGALAY_MYSQL_BUILD_EXAMPLES=ON \
  -DGALAY_MYSQL_ENABLE_IMPORT_COMPILATION=ON \
  -DGALAY_MYSQL_BUILD_MODULE_EXAMPLES=ON
cmake --build build-import --parallel
```

## 文档

- [快速开始](docs/01-快速开始.md)
- [架构设计](docs/02-架构设计.md)
- [API 文档](docs/03-API文档.md)
- [示例代码](docs/04-示例代码.md)

## 相关项目

- [galay-kernel](https://github.com/gzj-creator/galay-kernel)
