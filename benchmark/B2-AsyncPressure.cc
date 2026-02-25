#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <galay-kernel/kernel/Runtime.h>
#include "benchmark/common/BenchmarkConfig.h"
#include "galay-mysql/async/AsyncMysqlClient.h"

using namespace galay::kernel;
using namespace galay::mysql;

namespace
{

struct BenchmarkState {
    std::atomic<size_t> finished_clients{0};
    std::atomic<uint64_t> success{0};
    std::atomic<uint64_t> failed{0};
    std::atomic<uint64_t> latency_ns{0};
    std::mutex error_mutex;
    std::string first_error;

    void recordError(std::string message)
    {
        std::lock_guard<std::mutex> lock(error_mutex);
        if (first_error.empty()) {
            first_error = std::move(message);
        }
    }
};

Coroutine runWorker(IOScheduler* scheduler,
                    BenchmarkState* state,
                    mysql_benchmark::MysqlBenchmarkConfig cfg)
{
    AsyncMysqlClient client(scheduler);

    auto connect_result = co_await client.connect(cfg.host, cfg.port, cfg.user, cfg.password, cfg.database);
    if (!connect_result || !connect_result->has_value()) {
        state->failed.fetch_add(static_cast<uint64_t>(cfg.queries_per_client), std::memory_order_relaxed);
        if (!connect_result) {
            state->recordError("connect failed: " + connect_result.error().message());
        } else {
            state->recordError("connect failed: awaitable resumed without value");
        }
        state->finished_clients.fetch_add(1, std::memory_order_release);
        co_return;
    }

    for (size_t i = 0; i < cfg.warmup_queries; ++i) {
        auto _ = co_await client.query(cfg.sql);
        (void)_;
    }

    for (size_t i = 0; i < cfg.queries_per_client; ++i) {
        const auto started = std::chrono::steady_clock::now();
        auto query_result = co_await client.query(cfg.sql);
        const auto finished = std::chrono::steady_clock::now();
        const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count();

        state->latency_ns.fetch_add(static_cast<uint64_t>(elapsed_ns), std::memory_order_relaxed);
        if (!query_result || !query_result->has_value()) {
            state->failed.fetch_add(1, std::memory_order_relaxed);
            if (!query_result) {
                state->recordError("query failed: " + query_result.error().message());
            } else {
                state->recordError("query failed: awaitable resumed without value");
            }
        } else {
            state->success.fetch_add(1, std::memory_order_relaxed);
        }
    }

    auto _ = co_await client.close();
    (void)_;
    state->finished_clients.fetch_add(1, std::memory_order_release);
}

void printSummary(const mysql_benchmark::MysqlBenchmarkConfig& cfg,
                  const BenchmarkState& state,
                  std::chrono::steady_clock::time_point started,
                  std::chrono::steady_clock::time_point finished)
{
    const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count();
    const auto elapsed_sec = static_cast<double>(elapsed_ns) / 1e9;

    const uint64_t success = state.success.load(std::memory_order_relaxed);
    const uint64_t failed = state.failed.load(std::memory_order_relaxed);
    const uint64_t total = success + failed;
    const uint64_t latency_ns = state.latency_ns.load(std::memory_order_relaxed);

    const double qps = elapsed_sec > 0.0 ? static_cast<double>(success) / elapsed_sec : 0.0;
    const double avg_latency_ms = total > 0 ? (static_cast<double>(latency_ns) / static_cast<double>(total)) / 1e6 : 0.0;

    std::cout << "\n=== B2 Async Pressure Summary ===\n"
              << "clients: " << cfg.clients << '\n'
              << "queries_per_client: " << cfg.queries_per_client << '\n'
              << "total_queries: " << total << '\n'
              << "success: " << success << '\n'
              << "failed: " << failed << '\n'
              << "elapsed_sec: " << elapsed_sec << '\n'
              << "qps: " << qps << '\n'
              << "avg_latency_ms: " << avg_latency_ms << std::endl;

    if (!state.first_error.empty()) {
        std::cout << "first_error: " << state.first_error << std::endl;
    }
}

} // namespace

int main(int argc, char* argv[])
{
    auto cfg = mysql_benchmark::loadMysqlBenchmarkConfig();
    if (!mysql_benchmark::parseArgs(cfg, argc, argv, std::cerr)) {
        mysql_benchmark::printUsage(argv[0]);
        return 2;
    }

    mysql_benchmark::printConfig(cfg);
    std::cout << "Running async pressure benchmark..." << std::endl;

    Runtime runtime;
    runtime.start();

    BenchmarkState state;
    for (size_t i = 0; i < cfg.clients; ++i) {
        auto* scheduler = runtime.getNextIOScheduler();
        if (scheduler == nullptr) {
            runtime.stop();
            std::cerr << "failed to get IO scheduler" << std::endl;
            return 1;
        }
        scheduler->spawn(runWorker(scheduler, &state, cfg));
    }

    const auto started = std::chrono::steady_clock::now();
    const auto deadline = started + std::chrono::seconds(cfg.timeout_seconds);
    while (state.finished_clients.load(std::memory_order_acquire) < cfg.clients &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    const auto finished = std::chrono::steady_clock::now();

    runtime.stop();

    if (state.finished_clients.load(std::memory_order_acquire) < cfg.clients) {
        std::cerr << "benchmark timeout after " << cfg.timeout_seconds << " seconds" << std::endl;
        return 1;
    }

    printSummary(cfg, state, started, finished);
    return state.failed.load(std::memory_order_relaxed) == 0 ? 0 : 1;
}

