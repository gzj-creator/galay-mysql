# Galay-MySQL

`galay-mysql` 是基于 `galay-kernel` 的 C++ MySQL 客户端库，提供：

- `AsyncMysqlClient`：异步单连接客户端
- `MysqlClient`：同步阻塞客户端
- `MysqlConnectionPool`：异步连接池
- `examples/`、`test/`、`benchmark/`：与源码一一对应的示例、验证与压测入口

## 文档导航

- [文档索引](docs/README.md)
- [00-快速开始](docs/00-快速开始.md)
- [01-架构设计](docs/01-架构设计.md)
- [02-API参考](docs/02-API参考.md)
- [03-使用指南](docs/03-使用指南.md)
- [04-示例代码](docs/04-示例代码.md)
- [05-性能测试](docs/05-性能测试.md)
- [06-高级主题](docs/06-高级主题.md)
- [07-常见问题](docs/07-常见问题.md)

## 仓库真相来源

当 README、docs、examples 与源码冲突时，按以下顺序取真：

1. `galay-mysql/*.h` 中的公开头与导出 target
2. `galay-mysql/*.cc` 的实际行为
3. `examples/`
4. `test/`
5. `benchmark/`
6. Markdown 文档

## 构建与兼容性

| 场景 | 当前仓库真实要求 |
| --- | --- |
| include 头文件路径 | CMake `>= 3.20`，可用的 C++23 / `std::expected` 工具链，依赖 `galay-kernel (>= 3.4.4)`、OpenSSL、spdlog |
| import / module 路径 | CMake `>= 3.28`，`Ninja` 或 `Visual Studio` 生成器，并开启 `GALAY_MYSQL_ENABLE_IMPORT_COMPILATION=ON` |
| GCC import 稳定路径 | `Linux + GCC >= 14` |
| Clang import 路径 | 非 `AppleClang`，且能找到 `clang-scan-deps` |

当前 CMake 选项来自 `cmake/option.cmake`：

- `BUILD_TESTING`（标准 CTest 开关）
- `GALAY_MYSQL_BUILD_TESTS`（兼容别名，已废弃）
- `GALAY_MYSQL_BUILD_EXAMPLES`
- `GALAY_MYSQL_BUILD_BENCHMARKS`
- `GALAY_MYSQL_BUILD_SHARED_LIBS`
- `GALAY_MYSQL_INSTALL_MODULE_INTERFACE`
- `GALAY_MYSQL_ENABLE_IMPORT_COMPILATION`
- `GALAY_MYSQL_BUILD_MODULE_EXAMPLES`
- `GALAY_MYSQL_CXX_STANDARD`（缓存字符串，默认 `23`）

## 构建

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON \
  -DGALAY_MYSQL_BUILD_EXAMPLES=ON \
  -DGALAY_MYSQL_BUILD_BENCHMARKS=ON
cmake --build build --parallel
```

已知外部依赖：

- `galay-kernel`（必需）
- OpenSSL（必需）
- spdlog（必需）
- MySQL 服务端（运行 examples/tests/benchmarks 时必需）

默认会优先尝试以下 sibling 前缀（若存在）：

- `../.galay-prefix/latest`
- `../galay-kernel/install-local`
- `../galay-kernel/_install-smoke-344`
- `../galay-kernel/_install-smoke`

## 安装与外部 CMake 消费

安装仓库产物：

```bash
cmake --install build --prefix /tmp/galay-mysql-install
```
外部项目消费方式与当前安装导出保持一致：

```cmake
find_package(GalayMysql REQUIRED CONFIG)

add_executable(app main.cc)
target_link_libraries(app PRIVATE galay-mysql::galay-mysql)
```

安装目录中的主配置文件是 `GalayMysqlConfig.cmake`；同时会安装 `galay-mysqlConfig.cmake` 兼容入口，但文档统一以 `GalayMysql` 作为包名。

头文件入口：

```cpp
#include "galay-mysql/async/AsyncMysqlClient.h"
#include "galay-mysql/async/MysqlConnectionPool.h"
#include "galay-mysql/sync/MysqlClient.h"
```

模块入口文件为 `galay-mysql/module/galay.mysql.cppm`；只有当 import 编译路径被 CMake 判定为可用时，仓库才会额外生成 `galay-mysql-modules` 门面 target（与主库 target 分离）并启用 import 示例与模块 file set。
模块入口文件为 `galay-mysql/module/galay.mysql.cppm`；只有当 import 编译路径被 CMake 判定为可用时，仓库才会额外生成 `galay-mysql-modules` 门面 target（与主库 target 分离）并启用 import 示例与模块 file set。

## 异步 API 语义

- 异步接口返回 **Awaitable 值对象**，不是协程句柄，也不是需要长期持有的引用。
- README、`examples/` 与 `test/` 中的真实使用方式都是：创建 awaitable 局部值，然后对它执行 **一次 `co_await`**。
- `await_resume()` 的返回类型是 `std::expected<std::optional<T>, MysqlError>`；成功路径通常应同时满足 `expected` 成功且内层 `optional` 有值。
- `examples/` 与 `test/` 仍会显式检查 `has_value()`，因为当前实现把“成功但空 optional”视为内部异常状态，而不是正常轮询流程。
- 不要写 `auto& aw = client.query(...)` 这类引用绑定；公开头文件中的接口按值返回 awaitable。

## 异步最小示例

```cpp
#include <chrono>
#include <iostream>
#include <galay-kernel/kernel/Runtime.h>
#include "galay-mysql/async/AsyncMysqlClient.h"

using namespace std::chrono_literals;
using namespace galay::kernel;
using namespace galay::mysql;

Coroutine run(IOScheduler* scheduler)
{
    auto client = AsyncMysqlClientBuilder()
        .scheduler(scheduler)
        .config(AsyncMysqlConfig::withRecvTimeout(5s))
        .build();

    auto connect_result = co_await client.connect("127.0.0.1", 3306, "root", "password", "test");
    if (!connect_result || !connect_result->has_value()) {
        co_return;
    }

    auto query_result = co_await client.query("SELECT 1");
    if (!query_result || !query_result->has_value()) {
        co_await client.close();
        co_return;
    }

    std::cout << query_result->value().row(0).getString(0) << '\n';
    co_await client.close();
}
```

## 同步最小示例

```cpp
#include <iostream>
#include "galay-mysql/sync/MysqlClient.h"

using namespace galay::mysql;

int main()
{
    MysqlConfig cfg = MysqlConfig::create("127.0.0.1", 3306, "root", "password", "test");
    cfg.connect_timeout_ms = 5000;

    MysqlClient client;
    auto connect_result = client.connect(cfg);
    if (!connect_result) {
        std::cerr << connect_result.error().message() << '\n';
        return 1;
    }

    auto query_result = client.query("SELECT 1");
    if (!query_result) {
        std::cerr << query_result.error().message() << '\n';
        client.close();
        return 1;
    }

    std::cout << query_result->row(0).getString(0) << '\n';
    client.close();
    return 0;
}
```

## 运行测试、示例与 benchmark

运行以下二进制前，统一使用这些环境变量配置 MySQL：

- `GALAY_MYSQL_HOST`
- `GALAY_MYSQL_PORT`
- `GALAY_MYSQL_USER`
- `GALAY_MYSQL_PASSWORD`
- `GALAY_MYSQL_DB`

`T0-ConfigContract`、`T1-MysqlProtocol`、`T2-MysqlAuth`、`T8-MysqlAwaitableSurface` 与 `T9-ConfigEnvSurface` 是纯单元测试，不需要 MySQL；`T3`-`T7` 是集成测试，缺少上述环境变量时会以 exit code `125` 退出，`ctest` 会把它们标记为 skipped。

`ctest` 测试项：

- `T0-ConfigContract`
- `T1-MysqlProtocol`
- `T2-MysqlAuth`
- `T3-AsyncMysqlClient`
- `T4-SyncMysqlClient`
- `T5-ConnectionPool`
- `T6-Transaction`
- `T7-PreparedStatement`
- `T8-MysqlAwaitableSurface`
- `T9-ConfigEnvSurface`
- `PackageConfig.ConsumerSmoke`（安装当前构建产物后，用外部 consumer 工程验证 `find_package(GalayMysql)`）

include 示例 target：

- `E1-async_query-Include`
- `E2-sync_query-Include`
- `E3-async_pool-Include`
- `E4-sync_prepared_tx-Include`
- `E5-async_pipeline-Include`

import 示例 target（仅在模块路径实际启用时生成）：

- `E1-async_query-Import`
- `E2-sync_query-Import`
- `E3-async_pool-Import`
- `E4-sync_prepared_tx-Import`
- `E5-async_pipeline-Import`

benchmark target：

- `B1-SyncPressure`
- `B2-AsyncPressure`

常用命令：

```bash
ctest --test-dir build -N
ctest --test-dir build -L unit --output-on-failure
ctest --test-dir build -R '^PackageConfig.ConsumerSmoke$' --output-on-failure

cmake --build build --target T3-AsyncMysqlClient T4-SyncMysqlClient T5-ConnectionPool --parallel
GALAY_MYSQL_HOST=127.0.0.1 \
GALAY_MYSQL_PORT=3306 \
GALAY_MYSQL_USER=root \
GALAY_MYSQL_PASSWORD=password \
GALAY_MYSQL_DB=test \
ctest --test-dir build -L integration --output-on-failure

cmake --build build --target \
  E1-async_query-Include \
  E2-sync_query-Include \
  E3-async_pool-Include \
  E4-sync_prepared_tx-Include \
  E5-async_pipeline-Include --parallel
./build/examples/E1-async_query-Include
./build/examples/E2-sync_query-Include
./build/examples/E3-async_pool-Include
./build/examples/E4-sync_prepared_tx-Include
./build/examples/E5-async_pipeline-Include

cmake --build build --target B1-SyncPressure B2-AsyncPressure --parallel

GALAY_MYSQL_HOST=127.0.0.1 \
GALAY_MYSQL_PORT=3306 \
GALAY_MYSQL_USER=root \
GALAY_MYSQL_PASSWORD=password \
GALAY_MYSQL_DB=test \
./scripts/S2-Bench-Rust-Compare.sh ./build
```

## 已知限制

- import / module 示例是否生成完全由 `GALAY_MYSQL_BUILD_MODULE_EXAMPLES_EFFECTIVE` 决定，不满足工具链条件时会自动关闭。
- 当前文档不把传输层 TLS 当作已验证能力来承诺；OpenSSL 在源码中用于认证相关加密辅助。
- 当前仓库只有 `B1-SyncPressure` 和 `B2-AsyncPressure` 两个 benchmark target；性能数值请以可复现命令和原始输出为准，见 [05-性能测试](docs/05-性能测试.md)。
- Rust 对照 benchmark 位于 `benchmark/compare/rust/`，可通过 `scripts/S2-Bench-Rust-Compare.sh` 与 C++ benchmark 做同场景对比；发布任何 benchmark 数字前必须运行同一套脚本，它会在摘要中打印 `start_time`/`end_time`（UTC）、吞吐和 p50/p95/p99 延迟，并说明资源指标尚需人工采集。
- 请把这个脚本的原始 stdout 用带时间戳的文件名（例如 `benchmark-results/2026-04-12-S2-Bench-Rust-Compare.txt`）保存下来，文件内天然包含命令、环境变量和时间信息。资源占用目前还无法自动化，后续需要人工采集附带的监控或 profiling 结果。
- benchmark CLI 当前没有专门的 `--help` 开关；参数列表应以 `benchmark/common/BenchmarkConfig.h` 与 [05-性能测试](docs/05-性能测试.md) 为准。
- 建议把一个 `AsyncMysqlClient` 当作单条请求/响应流顺序使用，不要在同一连接上叠加并发操作。
