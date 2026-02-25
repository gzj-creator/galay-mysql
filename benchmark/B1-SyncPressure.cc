#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "benchmark/common/BenchmarkConfig.h"
#include "galay-mysql/sync/MysqlClient.h"

using namespace galay::mysql;

namespace
{

struct BenchmarkState {
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

void runWorker(const mysql_benchmark::MysqlBenchmarkConfig& cfg, BenchmarkState* state)
{
    MysqlClient client;
    auto connect_result = client.connect(cfg.host, cfg.port, cfg.user, cfg.password, cfg.database);
    if (!connect_result) {
        state->failed.fetch_add(static_cast<uint64_t>(cfg.queries_per_client), std::memory_order_relaxed);
        state->recordError("connect failed: " + connect_result.error().message());
        return;
    }

    for (size_t i = 0; i < cfg.warmup_queries; ++i) {
        auto _ = client.query(cfg.sql);
        (void)_;
    }

    for (size_t i = 0; i < cfg.queries_per_client; ++i) {
        const auto started = std::chrono::steady_clock::now();
        auto query_result = client.query(cfg.sql);
        const auto finished = std::chrono::steady_clock::now();
        const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count();

        state->latency_ns.fetch_add(static_cast<uint64_t>(elapsed_ns), std::memory_order_relaxed);
        if (query_result) {
            state->success.fetch_add(1, std::memory_order_relaxed);
        } else {
            state->failed.fetch_add(1, std::memory_order_relaxed);
            state->recordError("query failed: " + query_result.error().message());
        }
    }
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

    std::cout << "\n=== B1 Sync Pressure Summary ===\n"
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
    std::cout << "Running sync pressure benchmark..." << std::endl;

    BenchmarkState state;
    std::vector<std::thread> workers;
    workers.reserve(cfg.clients);

    const auto started = std::chrono::steady_clock::now();
    for (size_t i = 0; i < cfg.clients; ++i) {
        workers.emplace_back(runWorker, std::cref(cfg), &state);
    }

    for (auto& worker : workers) {
        worker.join();
    }
    const auto finished = std::chrono::steady_clock::now();

    printSummary(cfg, state, started, finished);
    return state.failed.load(std::memory_order_relaxed) == 0 ? 0 : 1;
}

