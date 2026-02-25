#include "MysqlConnectionPool.h"

namespace galay::mysql
{

// ======================== MysqlConnectionPool ========================

MysqlConnectionPool::MysqlConnectionPool(galay::kernel::IOScheduler* scheduler,
                                         const MysqlConfig& config,
                                         const AsyncMysqlConfig& async_config,
                                         size_t min_connections,
                                         size_t max_connections)
    : m_scheduler(scheduler)
    , m_config(config)
    , m_async_config(async_config)
    , m_min_connections(min_connections)
    , m_max_connections(max_connections)
{
}

MysqlConnectionPool::~MysqlConnectionPool()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    while (!m_idle_clients.empty()) {
        m_idle_clients.pop();
    }
    m_all_clients.clear();
}

AsyncMysqlClient* MysqlConnectionPool::tryAcquire()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_idle_clients.empty()) {
        auto* client = m_idle_clients.front();
        m_idle_clients.pop();
        return client;
    }
    return nullptr;
}

AsyncMysqlClient* MysqlConnectionPool::createClient()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_total_connections.load(std::memory_order_relaxed) >= m_max_connections) {
        return nullptr;
    }
    auto client = std::make_unique<AsyncMysqlClient>(m_scheduler, m_async_config);
    auto* ptr = client.get();
    m_all_clients.push_back(std::move(client));
    m_total_connections.fetch_add(1, std::memory_order_relaxed);
    return ptr;
}

void MysqlConnectionPool::release(AsyncMysqlClient* client)
{
    if (!client) return;

    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_waiters.empty()) {
        auto waiter = m_waiters.front();
        m_waiters.pop();
        m_idle_clients.push(client);
        waiter.resume();
    } else {
        m_idle_clients.push(client);
    }
}

size_t MysqlConnectionPool::idleCount() const
{
    // 注意：这不是线程安全的精确值，仅供参考
    return m_idle_clients.size();
}

MysqlConnectionPool::AcquireAwaitable MysqlConnectionPool::acquire() { return AcquireAwaitable(*this); }

// ======================== AcquireAwaitable ========================

MysqlConnectionPool::AcquireAwaitable::AcquireAwaitable(MysqlConnectionPool& pool)
    : m_pool(pool)
    , m_state(State::Invalid)
{
}

bool MysqlConnectionPool::AcquireAwaitable::await_ready() const noexcept
{
    return false;
}

bool MysqlConnectionPool::AcquireAwaitable::await_suspend(std::coroutine_handle<> handle)
{
    if (m_state != State::Invalid) {
        return false;
    }

    // 尝试获取空闲连接
    m_client = m_pool.tryAcquire();
    if (m_client) {
        m_state = State::Ready;
        m_connect_awaitable.reset();
        return false; // 不挂起，立即返回
    }

    // 尝试创建新连接
    m_client = m_pool.createClient();
    if (m_client) {
        m_state = State::Creating;
        m_connect_awaitable.emplace(*m_client, m_pool.m_config);
        return m_connect_awaitable->await_suspend(handle);
    }

    // 池已满，等待连接释放
    m_state = State::Waiting;
    m_connect_awaitable.reset();
    std::lock_guard<std::mutex> lock(m_pool.m_mutex);
    m_pool.m_waiters.push(handle);
    return true;
}

std::expected<std::optional<AsyncMysqlClient*>, MysqlError>
MysqlConnectionPool::AcquireAwaitable::await_resume()
{
    if (m_state == State::Ready) {
        m_state = State::Invalid;
        m_connect_awaitable.reset();
        return m_client;
    }
    else if (m_state == State::Creating) {
        if (!m_connect_awaitable.has_value()) {
            m_state = State::Invalid;
            m_client = nullptr;
            return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Missing connect awaitable in creating state"));
        }

        auto result = m_connect_awaitable.value().await_resume();
        m_connect_awaitable.reset();

        if (!result) {
            m_state = State::Invalid;
            m_client = nullptr;
            return std::unexpected(result.error());
        }
        if (!result->has_value()) {
            m_state = State::Invalid;
            m_client = nullptr;
            return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Connect awaitable resumed without value"));
        }
        m_state = State::Invalid;
        return m_client;
    }
    else if (m_state == State::Waiting) {
        // 被唤醒后，从池中获取连接
        m_client = m_pool.tryAcquire();
        m_state = State::Invalid;
        m_connect_awaitable.reset();
        if (m_client) {
            return m_client;
        }
        return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Failed to acquire connection after wakeup"));
    }

    m_state = State::Invalid;
    m_connect_awaitable.reset();
    m_client = nullptr;
    return std::unexpected(MysqlError(MYSQL_ERROR_INTERNAL, "Invalid acquire state"));
}

} // namespace galay::mysql
