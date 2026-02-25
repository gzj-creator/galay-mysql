#ifndef GALAY_MYSQL_BENCHMARK_CONFIG_H
#define GALAY_MYSQL_BENCHMARK_CONFIG_H

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>

namespace mysql_benchmark
{

struct MysqlBenchmarkConfig {
    std::string host = "127.0.0.1";
    uint16_t port = 3306;
    std::string user = "root";
    std::string password = "password";
    std::string database = "test";

    size_t clients = 16;
    size_t queries_per_client = 1000;
    size_t warmup_queries = 10;
    size_t timeout_seconds = 180;
    std::string sql = "SELECT 1";
};

inline const char* getEnvNonEmpty(const char* key)
{
    const char* value = std::getenv(key);
    if (value == nullptr || value[0] == '\0') {
        return nullptr;
    }
    return value;
}

inline std::string getEnvOrDefault(const char* key1, const char* key2, const std::string& default_value)
{
    if (const char* value = getEnvNonEmpty(key1)) return value;
    if (const char* value = getEnvNonEmpty(key2)) return value;
    return default_value;
}

inline uint16_t parsePortOrDefault(const char* value, uint16_t default_value)
{
    if (value == nullptr || value[0] == '\0') {
        return default_value;
    }

    errno = 0;
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0 || parsed > 65535UL) {
        return default_value;
    }
    return static_cast<uint16_t>(parsed);
}

inline size_t parseSizeOrDefault(const char* value, size_t default_value)
{
    if (value == nullptr || value[0] == '\0') {
        return default_value;
    }

    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0ULL) {
        return default_value;
    }
    return static_cast<size_t>(parsed);
}

inline size_t getEnvSizeOrDefault(const char* key1, const char* key2, size_t default_value)
{
    if (const char* value = getEnvNonEmpty(key1)) return parseSizeOrDefault(value, default_value);
    if (const char* value = getEnvNonEmpty(key2)) return parseSizeOrDefault(value, default_value);
    return default_value;
}

inline uint16_t getEnvPortOrDefault(const char* key1, const char* key2, uint16_t default_value)
{
    if (const char* value = getEnvNonEmpty(key1)) return parsePortOrDefault(value, default_value);
    if (const char* value = getEnvNonEmpty(key2)) return parsePortOrDefault(value, default_value);
    return default_value;
}

inline MysqlBenchmarkConfig loadMysqlBenchmarkConfig()
{
    MysqlBenchmarkConfig cfg;

    cfg.host = getEnvOrDefault("GALAY_MYSQL_HOST", "MYSQL_HOST", cfg.host);
    cfg.port = getEnvPortOrDefault("GALAY_MYSQL_PORT", "MYSQL_PORT", cfg.port);
    cfg.user = getEnvOrDefault("GALAY_MYSQL_USER", "MYSQL_USER", cfg.user);
    cfg.password = getEnvOrDefault("GALAY_MYSQL_PASSWORD", "MYSQL_PASSWORD", cfg.password);
    cfg.database = getEnvOrDefault("GALAY_MYSQL_DB", "MYSQL_DATABASE", cfg.database);

    cfg.clients = getEnvSizeOrDefault("GALAY_MYSQL_BENCH_CLIENTS", "MYSQL_BENCH_CLIENTS", cfg.clients);
    cfg.queries_per_client = getEnvSizeOrDefault("GALAY_MYSQL_BENCH_QUERIES", "MYSQL_BENCH_QUERIES", cfg.queries_per_client);
    cfg.warmup_queries = getEnvSizeOrDefault("GALAY_MYSQL_BENCH_WARMUP", "MYSQL_BENCH_WARMUP", cfg.warmup_queries);
    cfg.timeout_seconds = getEnvSizeOrDefault("GALAY_MYSQL_BENCH_TIMEOUT", "MYSQL_BENCH_TIMEOUT", cfg.timeout_seconds);
    cfg.sql = getEnvOrDefault("GALAY_MYSQL_BENCH_SQL", "MYSQL_BENCH_SQL", cfg.sql);

    return cfg;
}

inline bool parsePositiveSizeArg(int argc, char* argv[], int& i, size_t& out)
{
    if (i + 1 >= argc) {
        return false;
    }

    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(argv[i + 1], &end, 10);
    if (errno != 0 || end == argv[i + 1] || *end != '\0' || parsed == 0ULL) {
        return false;
    }

    out = static_cast<size_t>(parsed);
    ++i;
    return true;
}

inline bool parseArgs(MysqlBenchmarkConfig& cfg, int argc, char* argv[], std::ostream& err)
{
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);

        if (arg == "--clients") {
            if (!parsePositiveSizeArg(argc, argv, i, cfg.clients)) {
                err << "invalid --clients value" << std::endl;
                return false;
            }
            continue;
        }

        if (arg == "--queries") {
            if (!parsePositiveSizeArg(argc, argv, i, cfg.queries_per_client)) {
                err << "invalid --queries value" << std::endl;
                return false;
            }
            continue;
        }

        if (arg == "--warmup") {
            if (!parsePositiveSizeArg(argc, argv, i, cfg.warmup_queries)) {
                err << "invalid --warmup value" << std::endl;
                return false;
            }
            continue;
        }

        if (arg == "--timeout-sec") {
            if (!parsePositiveSizeArg(argc, argv, i, cfg.timeout_seconds)) {
                err << "invalid --timeout-sec value" << std::endl;
                return false;
            }
            continue;
        }

        if (arg == "--sql") {
            if (i + 1 >= argc) {
                err << "missing --sql value" << std::endl;
                return false;
            }
            cfg.sql = argv[++i];
            if (cfg.sql.empty()) {
                err << "empty --sql value" << std::endl;
                return false;
            }
            continue;
        }

        err << "unknown argument: " << arg << std::endl;
        return false;
    }

    return true;
}

inline void printUsage(const char* prog)
{
    std::cout
        << "Usage: " << prog << " [--clients N] [--queries N] [--warmup N] [--timeout-sec N] [--sql \"SELECT 1\"]\n"
        << "Environment overrides:\n"
        << "  GALAY_MYSQL_HOST / GALAY_MYSQL_PORT / GALAY_MYSQL_USER / GALAY_MYSQL_PASSWORD / GALAY_MYSQL_DB\n"
        << "  GALAY_MYSQL_BENCH_CLIENTS / GALAY_MYSQL_BENCH_QUERIES / GALAY_MYSQL_BENCH_WARMUP\n"
        << "  GALAY_MYSQL_BENCH_TIMEOUT / GALAY_MYSQL_BENCH_SQL\n";
}

inline void printConfig(const MysqlBenchmarkConfig& cfg)
{
    std::cout
        << "MySQL config: host=" << cfg.host
        << ", port=" << cfg.port
        << ", user=" << cfg.user
        << ", db=" << cfg.database << '\n'
        << "Benchmark config: clients=" << cfg.clients
        << ", queries_per_client=" << cfg.queries_per_client
        << ", warmup=" << cfg.warmup_queries
        << ", timeout_sec=" << cfg.timeout_seconds << '\n'
        << "SQL: " << cfg.sql << std::endl;
}

} // namespace mysql_benchmark

#endif // GALAY_MYSQL_BENCHMARK_CONFIG_H

