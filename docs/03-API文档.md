# 03-API文档

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

#### MysqlFieldType

定义位置：`galay-mysql/base/MysqlValue.h`

```cpp
enum class MysqlFieldType : uint8_t {
    DECIMAL     = 0x00,
    TINY        = 0x01,
    SHORT       = 0x02,
    LONG        = 0x03,
    FLOAT       = 0x04,
    DOUBLE      = 0x05,
    NULL_TYPE   = 0x06,
    TIMESTAMP   = 0x07,
    LONGLONG    = 0x08,
    INT24       = 0x09,
    DATE        = 0x0a,
    TIME        = 0x0b,
    DATETIME    = 0x0c,
    YEAR        = 0x0d,
    NEWDATE     = 0x0e,
    VARCHAR     = 0x0f,
    BIT         = 0x10,
    JSON        = 0xf5,
    NEWDECIMAL  = 0xf6,
    ENUM        = 0xf7,
    SET         = 0xf8,
    TINY_BLOB   = 0xf9,
    MEDIUM_BLOB = 0xfa,
    LONG_BLOB   = 0xfb,
    BLOB        = 0xfc,
    VAR_STRING  = 0xfd,
    STRING      = 0xfe,
    GEOMETRY    = 0xff,
};
```

### MysqlFieldFlags

定义位置：`galay-mysql/base/MysqlValue.h`

```cpp
enum MysqlFieldFlags : uint16_t {
    NOT_NULL_FLAG       = 0x0001,
    PRI_KEY_FLAG        = 0x0002,
    UNIQUE_KEY_FLAG     = 0x0004,
    MULTIPLE_KEY_FLAG   = 0x0008,
    BLOB_FLAG           = 0x0010,
    UNSIGNED_FLAG       = 0x0020,
    ZEROFILL_FLAG       = 0x0040,
    BINARY_FLAG         = 0x0080,
    ENUM_FLAG           = 0x0100,
    AUTO_INCREMENT_FLAG = 0x0200,
    TIMESTAMP_FLAG      = 0x0400,
    SET_FLAG            = 0x0800,
    NUM_FLAG            = 0x8000,
};
```

### MysqlField

```cpp
class MysqlField {
public:
    const std::string& name() const;
    MysqlFieldType type() const;
    uint16_t flags() const;
    uint32_t columnLength() const;
    uint8_t decimals() const;

    // 扩展元数据
    const std::string& catalog() const;
    const std::string& schema() const;
    const std::string& table() const;
    const std::string& orgTable() const;
    const std::string& orgName() const;
    uint16_t characterSet() const;

    // 标志位快捷方法
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

    // 访问方法
    const std::optional<std::string>& operator[](size_t index) const;
    const std::optional<std::string>& at(size_t index) const;

    // 类型转换方法
    std::string getString(size_t index, const std::string& default_val = "") const;
    int64_t getInt64(size_t index, int64_t default_val = 0) const;
    uint64_t getUint64(size_t index, uint64_t default_val = 0) const;
    double getDouble(size_t index, double default_val = 0.0) const;

    const std::vector<std::optional<std::string>>& values() const;
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

    // 支持移动语义
    AsyncMysqlClient(AsyncMysqlClient&& other) noexcept;
    AsyncMysqlClient& operator=(AsyncMysqlClient&& other) noexcept;

    // 禁止拷贝
    AsyncMysqlClient(const AsyncMysqlClient&) = delete;
    AsyncMysqlClient& operator=(const AsyncMysqlClient&) = delete;

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

## 同步客户端预处理语句关闭

```cpp
MysqlVoidResult stmtClose(uint32_t stmt_id);
```

使用示例：

```cpp
auto prep = session.prepare("INSERT INTO users(name) VALUES(?)");
if (prep) {
    // 使用 statement_id...
    session.stmtClose(prep->statement_id);
}
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

## 使用注意事项

### 异步客户端

1. **串行使用**：同一个 `AsyncMysqlClient` 实例应串行执行请求，不要并发发起多个异步操作
2. **Awaitable 生命周期**：返回的 Awaitable 引用在下次调用同类方法前有效
3. **移动语义**：支持移动构造和移动赋值，可以在容器中存储
4. **超时设置**：生产环境建议配置 `AsyncMysqlConfig` 的超时参数

### 同步客户端

1. **阻塞调用**：所有方法都是阻塞的，适合简单场景或测试
2. **连接状态**：使用 `isConnected()` 检查连接状态
3. **预处理语句**：使用完毕后应调用 `stmtClose()` 释放服务端资源

### 连接池

1. **显式归还**：从池中获取的连接必须通过 `release()` 归还
2. **异常安全**：建议使用 RAII 封装或确保异常路径也能归还连接
3. **池满等待**：当池满时 `acquire()` 会挂起等待，直到有连接归还

### 性能优化

1. **预留空间**：对于大结果集，可设置 `AsyncMysqlConfig::result_row_reserve_hint`
2. **零拷贝参数**：`stmtExecute` 使用 `string_view` + `span` 版本可减少内存拷贝
3. **连接复用**：使用连接池避免频繁建立连接的开销
4. **批量操作**：使用事务包裹多个写操作以提升性能

## 常见问题

### Q: 异步客户端可以并发执行多个查询吗？

A: 不可以。同一个 `AsyncMysqlClient` 实例应串行使用。如需并发，请使用连接池获取多个客户端实例。

### Q: 如何处理超时？

A: 异步客户端通过 `AsyncMysqlConfig` 设置超时：

```cpp
AsyncMysqlConfig cfg = AsyncMysqlConfig::withTimeout(
    std::chrono::milliseconds(2000),  // 发送超时
    std::chrono::milliseconds(5000)   // 接收超时
);
AsyncMysqlClient client(scheduler, cfg);
```

### Q: 预处理语句的参数类型如何指定？

A: 通过 `param_types` 参数指定，每个元素对应一个参数的 MySQL 类型。如果不指定，默认按字符串处理。

### Q: 如何判断查询是否返回结果集？

A: 使用 `MysqlResultSet::hasResultSet()` 判断：

```cpp
if (rs.hasResultSet()) {
    // 有列定义，可以访问 rows()
} else {
    // 仅 OK 包，查看 affectedRows() 等
}
```

### Q: 连接池的最小和最大连接数如何设置？

A: 构造时指定：

```cpp
MysqlConnectionPool pool(
    scheduler,
    config,
    async_config,
    2,   // min_connections
    16   // max_connections
);
```

池会预先创建 `min_connections` 个连接，最多扩展到 `max_connections`。
