# Release Note

按时间顺序追加版本记录，避免覆盖历史发布说明。

## v1.2.4 - 2026-04-20

- 版本级别：小版本（patch）
- Git 提交消息：`chore: 发布 v1.2.4`
- Git Tag：`v1.2.4`
- 自述摘要：
  - 对齐包配置导出与兼容消费入口，补上 `GalayMysql`/`galay-mysql` 配置兼容、consumer smoke 和 config contract 校验链路。
  - 新增 `scripts/verify_docs.py`，同步 README、快速开始、API、使用指南与性能文档到当前测试入口、包契约与 benchmark 发布流程。
  - 扩展 Rust 对照 benchmark 与 `scripts/S2-Bench-Rust-Compare.sh` 输出，统一记录 UTC 起止时间、吞吐与 p50/p95/p99 延迟，便于保留可追溯发布证据。

## v1.2.5 - 2026-04-21

- 版本级别：小版本（patch）
- Git 提交消息：`chore: 发布 v1.2.5`
- Git Tag：`v1.2.5`
- 自述摘要：
  - 锁定源码构建入口里的 `galay-kernel 3.4.4` 依赖版本，确保 `galay-mysql` 在最新基础库前缀下稳定解析依赖。
  - 对齐源码构建与导出包配置的依赖版本基线，减少 package consumer、example 与 benchmark 命中旧内核包的风险。

## v1.2.6 - 2026-04-23

- 版本级别：小版本（patch）
- Git 提交消息：`chore: 发布 v1.2.6`
- Git Tag：`v1.2.6`
- 自述摘要：
  - 将源码仓库中的主包配置模板统一重命名为小写 kebab-case `galay-mysql-config.cmake.in`，与其他 `galay-*` 项目保持一致。
  - 将兼容模板独立命名为 `galay-mysql-compat-config.cmake.in`，明确其职责只是生成旧版 `galay-mysqlConfig.cmake` 兼容入口。
  - 同步修正 `configure_package_config_file(...)` 与 `configure_file(...)` 的模板路径，同时保持安装导出的 `GalayMysqlConfig.cmake`、兼容入口与版本文件名不变。
