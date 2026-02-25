#ifndef GALAY_MYSQL_CONNECTION_POOL_H
#define GALAY_MYSQL_CONNECTION_POOL_H

#include "AsyncMysqlClient.h"
#include "galay-mysql/base/MysqlConfig.h"
#include <galay-kernel/kernel/IOScheduler.hpp>
#include <galay-kernel/kernel/Coroutine.h>
#include <galay-kernel/concurrency/AsyncWaiter.h>
#include <memory>
#include <vector>
#include <queue>
#include <mutex>
#include <atomic>
#include <optional>
#include <coroutine>

namespace galay::mysql
{

struct MysqlConnectionPoolConfig
{
    MysqlConfig mysql_config = MysqlConfig::defaultConfig();
    AsyncMysqlConfig async_config = AsyncMysqlConfig::noTimeout();
    size_t min_connections = 2;
    size_t max_connections = 10;
};

/**
 * @brief 异步MySQL连接池
 * @details 管理多个AsyncMysqlClient连接，支持异步获取和归还
 */
class MysqlConnectionPool
{
public:
    MysqlConnectionPool(galay::kernel::IOScheduler* scheduler,
                        MysqlConnectionPoolConfig config = {});

    ~MysqlConnectionPool();

    MysqlConnectionPool(const MysqlConnectionPool&) = delete;
    MysqlConnectionPool& operator=(const MysqlConnectionPool&) = delete;

    /**
     * @brief 获取连接的Awaitable
     * @details 如果池中有空闲连接则立即返回，否则创建新连接或等待
     */
    class AcquireAwaitable
    {
    public:
        AcquireAwaitable(MysqlConnectionPool& pool);

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> handle);
        std::expected<std::optional<AsyncMysqlClient*>, MysqlError> await_resume();

    private:
        enum class State {
            Invalid,
            Ready,       // 有空闲连接
            Waiting,     // 等待连接释放
            Creating,    // 正在创建新连接
        };

        MysqlConnectionPool& m_pool;
        State m_state;
        AsyncMysqlClient* m_client = nullptr;
        std::optional<MysqlConnectAwaitable> m_connect_awaitable;
    };

    /**
     * @brief 获取一个连接
     */
    AcquireAwaitable acquire();

    /**
     * @brief 归还连接到池中
     */
    void release(AsyncMysqlClient* client);

    /**
     * @brief 获取当前池中连接数
     */
    size_t size() const { return m_total_connections.load(std::memory_order_relaxed); }

    /**
     * @brief 获取空闲连接数
     */
    size_t idleCount() const;

private:
    friend class AcquireAwaitable;

    AsyncMysqlClient* tryAcquire();
    AsyncMysqlClient* createClient();

    galay::kernel::IOScheduler* m_scheduler;
    MysqlConfig m_mysql_config;
    AsyncMysqlConfig m_async_config;
    size_t m_min_connections;
    size_t m_max_connections;

    std::mutex m_mutex;
    std::queue<AsyncMysqlClient*> m_idle_clients;
    std::vector<std::unique_ptr<AsyncMysqlClient>> m_all_clients;
    std::queue<std::coroutine_handle<>> m_waiters;
    std::atomic<size_t> m_total_connections{0};

};

} // namespace galay::mysql

#endif // GALAY_MYSQL_CONNECTION_POOL_H
