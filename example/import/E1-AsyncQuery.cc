import galay.mysql;

#include <atomic>
#include <chrono>
#include <expected>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <galay-kernel/kernel/Runtime.h>
#include "example/common/ExampleConfig.h"

using namespace galay::kernel;
using namespace galay::mysql;

namespace
{

struct AsyncState {
    std::atomic<bool> done{false};
    std::atomic<bool> ok{true};
    std::string error;
};

Coroutine run(IOScheduler* scheduler, AsyncState* state, const mysql_example::MysqlExampleConfig& cfg)
{
    AsyncMysqlClient client(scheduler);

    auto& conn_aw = client.connect(cfg.host, cfg.port, cfg.user, cfg.password, cfg.database);
    std::expected<std::optional<bool>, MysqlError> conn_result;
    do {
        conn_result = co_await conn_aw;
        if (!conn_result) {
            state->error = "connect failed: " + conn_result.error().message();
            state->ok.store(false, std::memory_order_relaxed);
            state->done.store(true, std::memory_order_release);
            co_return;
        }
    } while (!conn_result->has_value());

    auto& query_aw = client.query("SELECT 1");
    std::expected<std::optional<MysqlResultSet>, MysqlError> query_result;
    do {
        query_result = co_await query_aw;
        if (!query_result) {
            state->error = "query failed: " + query_result.error().message();
            state->ok.store(false, std::memory_order_relaxed);
            co_await client.close();
            state->done.store(true, std::memory_order_release);
            co_return;
        }
    } while (!query_result->has_value());

    const MysqlResultSet& rs = query_result->value();
    if (rs.rowCount() > 0) {
        std::cout << "[E1-import] SELECT 1 => " << rs.row(0).getString(0) << std::endl;
    } else {
        std::cout << "[E1-import] empty result" << std::endl;
    }

    co_await client.close();
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
