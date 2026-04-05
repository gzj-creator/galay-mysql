#include <expected>
#include <optional>
#include <string>

#include "galay-mysql/async/AsyncMysqlClient.h"
#include "galay-mysql/sync/MysqlClient.h"

int main()
{
    galay::mysql::MysqlConfig cfg =
        galay::mysql::MysqlConfig::create("127.0.0.1", 3306, "user", "password", "db");
    cfg.connect_timeout_ms = 1;
    galay::mysql::MysqlClient sync_client;
    (void)sync_client;

    galay::mysql::AsyncMysqlClientBuilder builder;
    (void)builder;
    return 0;
}
