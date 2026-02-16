# API 文档

本文中的示例代码默认可配合模块化导入使用：`import galay.mysql;`（不支持模块时按需包含对应头文件，如 `galay-mysql/async/AsyncMysqlClient.h`）。

## Base 模块

### MysqlConfig

定义位置：`galay-mysql/base/MysqlConfig.h`

```cpp
struct MysqlConfig {
    std::string host = "127.0.0.1";
    uint16_t port = 3306;
    std::string username;
    std::string password;
    std::string database;
    std::string charset = "utf8mb4";
    uint32_t connect_timeout_ms = 5000;

    static MysqlConfig defaultConfig();
    static MysqlConfig create(const std::string& host, uint16_t port,
                              const std::string& user, const std::string& password,
                              const std::string& database = "");
};
```

### MysqlError / MysqlErrorType

定义位置：`galay-mysql/base/MysqlError.h`

```cpp
enum MysqlErrorType {
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

class MysqlError {
public:
    MysqlErrorType type() const;
    std::string message() const;
    uint16_t serverErrno() const;
};
```

### MysqlValue 数据结构

定义位置：`galay-mysql/base/MysqlValue.h`

#### MysqlField

```cpp
class MysqlField {
public:
    const std::string& name() const;
    MysqlFieldType type() const;
    uint16_t flags() const;
    uint32_t columnLength() const;
    uint8_t decimals() const;

    bool isNotNull() const;
    bool isPrimaryKey() const;
    bool isAutoIncrement() const;
    bool isUnsigned() const;
};
```

#### MysqlRow

```cpp
class MysqlRow {
public:
    size_t size() const;
    bool empty() const;
    bool isNull(size_t index) const;

    std::string getString(size_t index, const std::string& default_val = "") const;
    int64_t getInt64(size_t index, int64_t default_val = 0) const;
    uint64_t getUint64(size_t index, uint64_t default_val = 0) const;
    double getDouble(size_t index, double default_val = 0.0) const;
};
```

#### MysqlResultSet

```cpp
class MysqlResultSet {
public:
    size_t fieldCount() const;
    const MysqlField& field(size_t index) const;
    const std::vector<MysqlField>& fields() const;

    size_t rowCount() const;
    const MysqlRow& row(size_t index) const;
    const std::vector<MysqlRow>& rows() const;

    int findField(const std::string& name) const;

    uint64_t affectedRows() const;
    uint64_t lastInsertId() const;
    uint16_t warnings() const;
    uint16_t statusFlags() const;
    const std::string& info() const;
    bool hasResultSet() const;
};
```

## Async 模块

### AsyncMysqlConfig

定义位置：`galay-mysql/async/AsyncMysqlConfig.h`

```cpp
struct AsyncMysqlConfig {
    std::chrono::milliseconds send_timeout = std::chrono::milliseconds(-1);
    std::chrono::milliseconds recv_timeout = std::chrono::milliseconds(-1);
    size_t buffer_size = 16384;
    size_t result_row_reserve_hint = 0;

    bool isSendTimeoutEnabled() const;
    bool isRecvTimeoutEnabled() const;

    static AsyncMysqlConfig withTimeout(std::chrono::milliseconds send,
                                        std::chrono::milliseconds recv);
    static AsyncMysqlConfig withRecvTimeout(std::chrono::milliseconds recv);
    static AsyncMysqlConfig withSendTimeout(std::chrono::milliseconds send);
    static AsyncMysqlConfig noTimeout();
};
```

### AsyncMysqlClient

定义位置：`galay-mysql/async/AsyncMysqlClient.h`

```cpp
class AsyncMysqlClient {
public:
    AsyncMysqlClient(galay::kernel::IOScheduler* scheduler,
                AsyncMysqlConfig config = AsyncMysqlConfig::noTimeout());

    MysqlConnectAwaitable& connect(MysqlConfig config);
    MysqlConnectAwaitable& connect(std::string_view host, uint16_t port,
                                   std::string_view user, std::string_view password,
                                   std::string_view database = "");

    MysqlQueryAwaitable& query(std::string_view sql);
    MysqlPrepareAwaitable& prepare(std::string_view sql);

    MysqlStmtExecuteAwaitable& stmtExecute(
        uint32_t stmt_id,
        std::span<const std::optional<std::string>> params,
        std::span<const uint8_t> param_types = {});

    MysqlStmtExecuteAwaitable& stmtExecute(
        uint32_t stmt_id,
        std::span<const std::optional<std::string_view>> params,
        std::span<const uint8_t> param_types = {});

    MysqlQueryAwaitable& beginTransaction();
    MysqlQueryAwaitable& commit();
    MysqlQueryAwaitable& rollback();

    MysqlQueryAwaitable& ping();
    MysqlQueryAwaitable& useDatabase(std::string_view database);

    auto close();
    bool isClosed() const;
};
```

### Await 返回语义

异步接口普遍采用：

```cpp
std::expected<std::optional<T>, MysqlError>
```

判定方式：

- `!res`：失败，读取 `res.error()`
- `res && !res->has_value()`：未完成，继续 `co_await`
- `res && res->has_value()`：完成，读取 `res->value()`

典型用法：

```cpp
auto& aw = client.query("SELECT * FROM users");
std::expected<std::optional<MysqlResultSet>, MysqlError> res;

do {
    res = co_await aw;
    if (!res) {
        std::cerr << res.error().message() << '\n';
        co_return;
    }
} while (!res->has_value());

const MysqlResultSet& rs = res->value();
for (size_t i = 0; i < rs.rowCount(); ++i) {
    std::cout << rs.row(i).getString(0) << '\n';
}
```

### PrepareResult

`MysqlPrepareAwaitable::PrepareResult`：

```cpp
struct PrepareResult {
    uint32_t statement_id;
    uint16_t num_columns;
    uint16_t num_params;
    std::vector<MysqlField> param_fields;
    std::vector<MysqlField> column_fields;
};
```

### 高性能参数层（`string_view + span`）

`stmtExecute` 支持 `std::span<const std::optional<std::string_view>>`，可减少上层参数容器重组和字符串复制。

```cpp
std::array<std::optional<std::string_view>, 2> params = {
    std::string_view("Alice"),
    std::string_view("25"),
};

auto& exec_aw = client.stmtExecute(stmt_id, std::span<const std::optional<std::string_view>>(params));
```

## 连接池

定义位置：`galay-mysql/async/MysqlConnectionPool.h`

```cpp
class MysqlConnectionPool {
public:
    MysqlConnectionPool(galay::kernel::IOScheduler* scheduler,
                        const MysqlConfig& config,
                        const AsyncMysqlConfig& async_config = AsyncMysqlConfig::noTimeout(),
                        size_t min_connections = 2,
                        size_t max_connections = 10);

    class AcquireAwaitable {
    public:
        std::expected<std::optional<AsyncMysqlClient*>, MysqlError> await_resume();
    };

    AcquireAwaitable& acquire();
    void release(AsyncMysqlClient* client);

    size_t size() const;
    size_t idleCount() const;
};
```

获取连接示例：

```cpp
auto& acq_aw = pool.acquire();
std::expected<std::optional<AsyncMysqlClient*>, MysqlError> acq;

do {
    acq = co_await acq_aw;
    if (!acq) {
        std::cerr << acq.error().message() << '\n';
        co_return;
    }
} while (!acq->has_value());

AsyncMysqlClient* client = acq->value();
```

## Sync 模块

### MysqlClient

定义位置：`galay-mysql/sync/MysqlClient.h`

```cpp
class MysqlClient {
public:
    MysqlVoidResult connect(const MysqlConfig& config);
    MysqlVoidResult connect(const std::string& host, uint16_t port,
                            const std::string& user, const std::string& password,
                            const std::string& database = "");

    MysqlResult query(const std::string& sql);

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

    MysqlVoidResult beginTransaction();
    MysqlVoidResult commit();
    MysqlVoidResult rollback();

    MysqlVoidResult ping();
    MysqlVoidResult useDatabase(const std::string& database);

    void close();
    bool isConnected() const;
};
```

## 错误处理示例

```cpp
auto res = session.query("SELECT * FROM missing_table");
if (!res) {
    const MysqlError& err = res.error();
    std::cerr << "type=" << static_cast<int>(err.type())
              << ", msg=" << err.message() << '\n';
    if (err.type() == MYSQL_ERROR_SERVER) {
        std::cerr << "server errno=" << err.serverErrno() << '\n';
    }
}
```
