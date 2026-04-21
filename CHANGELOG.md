# CHANGELOG

维护说明：
- 未打 tag 的改动先写入 `## [Unreleased]`。
- 需要发版时，从 `Unreleased` 或“自上次 tag 以来”的累计变更整理出新的版本节。
- 版本号遵循 `major/minor/patch` 规则：大改动升主版本，新功能升次版本，修复与非破坏性维护升修订版本。
- 推荐标题格式为 `## [vX.Y.Z] - YYYY-MM-DD`，正文按 `Added` / `Changed` / `Fixed` / `Docs` / `Chore` 归纳。

## [Unreleased]

## [v1.2.5] - 2026-04-21

### Changed
- 锁定源码构建入口中的 `galay-kernel 3.4.4` 依赖版本，避免在多前缀环境下误命中旧基础库。
- 对齐源码构建与安装导出配置的内部依赖约束，使 package consumer、example 与 benchmark 使用同一版本基线。

## [v1.2.4] - 2026-04-20

### Added
- 新增 `scripts/verify_docs.py`，用于校验文档锚点、入口与当前仓库真源保持一致。
- 新增 `T0-ConfigContract` 与 package consumer smoke 校验输入模板，补齐安装包契约验证路径。

### Changed
- 对齐 `README.md`、`docs/00-快速开始.md`、`docs/02-API参考.md`、`docs/03-使用指南.md`、`docs/05-性能测试.md` 与当前包配置、测试入口和 benchmark 发布要求。
- 更新 `scripts/S2-Bench-Rust-Compare.sh` 与 Rust 对照 benchmark，实现同场景 C++/Rust 对比，并在摘要里输出 `start_time` / `end_time`、吞吐与 p50/p95/p99 延迟。
- 调整测试与 package 配置，使 `BUILD_TESTING`、`PackageConfig.ConsumerSmoke`、兼容 `galay-mysqlConfig.cmake` 入口和文档说明保持一致。

### Fixed
- 修正安装包消费路径与兼容配置入口，保证 `find_package(GalayMysql)` 与兼容配置文件协同工作。
- 修正集成测试配置读取与 skip 语义，使缺省环境下的测试契约与文档描述一致。
