# CHANGELOG

维护说明：
- 未打 tag 的改动先写入 `

## [Unreleased]

## [v2.0.1] - 2026-05-11

### Chore
- 移除 `benchmark/compare` 目录，避免误提交对比基准测试代码与构建产物。

## [v2.0.0] - 2026-04-29

### Changed
- 统一源码、头文件、测试、示例与 benchmark 文件命名为 `lower_snake_case`，编号前缀同步使用 `t<number>_`、`e<number>_` 与 `b<number>_` 风格。
- 同步更新构建脚本、模块入口、示例、测试、文档与脚本中的文件路径引用。
- 将项目内头文件包含调整为基于公开 include 根或模块根的非相对路径。

### Release
- 按大版本发布要求提升版本到 `v2.0.0`。

## [v1.2.6] - 2026-04-23

### Changed
- 将源码仓库中的主包配置模板重命名为统一的小写 kebab-case `galay-mysql-config.cmake.in`，与其他 `galay-*` 项目保持一致。
- 将兼容模板单独命名为 `galay-mysql-compat-config.cmake.in`，明确其仅用于生成旧版 `galay-mysqlConfig.cmake` 入口。
- 同步更新 `configure_package_config_file(...)` 与 `configure_file(...)` 的模板路径，安装导出的 `GalayMysqlConfig.cmake`、`galay-mysqlConfig.cmake` 与版本文件继续保持兼容。

## [v1.2.5] - 2026-04-21

### Changed
- 锁定源码构建入口中的 `galay-kernel 3.4.4` 依赖版本，避免在多前缀环境下误命中旧基础库。
- 对齐源码构建与安装导出配置的内部依赖约束，使 package consumer、example 与 benchmark 使用同一版本基线。

## [v1.2.4] - 2026-04-20

### Added
- 新增 `scripts/verify_docs.py`，用于校验文档锚点、入口与当前仓库真源保持一致。
- 新增 `t0_config` 与 package consumer smoke 校验输入模板，补齐安装包契约验证路径。

### Changed
- 对齐 `README.md`、`docs/00-快速开始.md`、`docs/02-API参考.md`、`docs/03-使用指南.md`、`docs/05-性能测试.md` 与当前包配置、测试入口和 benchmark 发布要求。
- 更新 `scripts/S2-Bench-Rust-Compare.sh` 与 Rust 对照 benchmark，实现同场景 C++/Rust 对比，并在摘要里输出 `start_time` / `end_time`、吞吐与 p50/p95/p99 延迟。
- 调整测试与 package 配置，使 `BUILD_TESTING`、`PackageConfig.ConsumerSmoke`、兼容 `galay-mysqlConfig.cmake` 入口和文档说明保持一致。

### Fixed
- 修正安装包消费路径与兼容配置入口，保证 `find_package(GalayMysql)` 与兼容配置文件协同工作。
- 修正集成测试配置读取与 skip 语义，使缺省环境下的测试契约与文档描述一致。
