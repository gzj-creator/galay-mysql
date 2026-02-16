#include <atomic>
#include <chrono>
#include <expected>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <galay-kernel/kernel/Runtime.h>
#include "example/common/ExampleConfig.h"
#include "galay-mysql/async/MysqlConnectionPool.h"

using namespace galay::kernel;
using namespace galay::mysql;

namespace
{

struct AsyncState {
    std::atomic<bool> done{false};
    std::atomic<bool> ok{true};
    std::string error;
};

Coroutine run(IOScheduler* scheduler, AsyncState* state, const mysql_example::MysqlExampleConfig& env_cfg)
{
    MysqlConfig cfg;
    cfg.host = env_cfg.host;
    cfg.port = env_cfg.port;
    cfg.username = env_cfg.user;
    cfg.password = env_cfg.password;
    cfg.database = env_cfg.database;

    MysqlConnectionPool pool(
        scheduler,
        cfg,
        AsyncMysqlConfig::withTimeout(std::chrono::milliseconds(3000), std::chrono::milliseconds(5000)),
        1,
        8
    );

    auto& acq_aw = pool.acquire();
    std::expected<std::optional<AsyncMysqlClient*>, MysqlError> acq;
    do {
        acq = co_await acq_aw;
        if (!acq) {
            state->error = "acquire failed: " + acq.error().message();
            state->ok.store(false, std::memory_order_relaxed);
            state->done.store(true, std::memory_order_release);
            co_return;
        }
    } while (!acq->has_value());

    AsyncMysqlClient* client = acq->value();

    auto& query_aw = client->query("SELECT CONNECTION_ID()");
    std::expected<std::optional<MysqlResultSet>, MysqlError> query_res;
    do {
        query_res = co_await query_aw;
        if (!query_res) {
            state->error = "query failed: " + query_res.error().message();
            state->ok.store(false, std::memory_order_relaxed);
            pool.release(client);
            state->done.store(true, std::memory_order_release);
            co_return;
        }
    } while (!query_res->has_value());

    const auto& rs = query_res->value();
    if (rs.rowCount() > 0) {
        std::cout << "[E3] CONNECTION_ID() => " << rs.row(0).getString(0) << std::endl;
    }

    pool.release(client);
    state->done.store(true, std::memory_order_release);
}

} // namespace

int main()
{
    const auto cfg = mysql_example::loadMysqlExampleConfig();
    mysql_example::printMysqlExampleConfig(cfg);

    Runtime runtime;
    runtime.start();

    auto* scheduler = runtime.getNextIOScheduler();
    if (!scheduler) {
        std::cerr << "no IO scheduler" << std::endl;
        runtime.stop();
        return 1;
    }

    AsyncState state;
    scheduler->spawn(run(scheduler, &state, cfg));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (!state.done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    runtime.stop();

    if (!state.done.load(std::memory_order_acquire)) {
        std::cerr << "timeout after 20s" << std::endl;
        return 1;
    }
    if (!state.ok.load(std::memory_order_relaxed)) {
        std::cerr << state.error << std::endl;
        return 1;
    }
    return 0;
}
