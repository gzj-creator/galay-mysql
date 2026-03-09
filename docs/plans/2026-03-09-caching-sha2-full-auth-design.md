# Caching SHA2 Full Auth Design

**Goal**

为 `galay-mysql` 补全无 `TLS` 场景下的 `caching_sha2_password` full auth，使同步与异步客户端都能连接 MySQL 8 默认认证用户，并支撑 `blog` 服务链路与远端镜像重建。

**Context**

- 当前 `sync` / `async` 都只支持 `caching_sha2_password` 的 fast auth。
- 服务端返回 `0x01 0x04` 时，客户端会报 “Full auth not supported” 或进入未定义行为。
- `blog` 的 `db` 服务依赖 `galay-mysql`，所以这条认证链不通时，`auth` / `gateway` 全链路都无法稳定联调。

**Recommended Approach**

采用最小行为补全，而不是大规模重构连接状态机：

1. 在协议层增加 `caching_sha2_password` full auth 所需的 RSA 公钥加密 helper。
2. 在 `sync MysqlClient` 里补全：
   - 处理服务端 `0x01 0x04`
   - 发送 `0x02` 请求公钥
   - 解析 PEM 公钥
   - 生成 `password + '\0'` 与 salt 异或后的密文
   - 发送加密包并等待最终 `OK/ERR`
3. 在 `async MysqlConnectAwaitable` 里复用现有 `SEND/READV` 链，扩展为：
   - 初始 auth 包
   - full auth 公钥请求包
   - 加密密码包
   - 最终响应解析

**Alternatives Considered**

- **强制 TLS 再发送明文密码**
  - 优点：协议更简单
  - 缺点：不符合当前目标；会扩大依赖面和镜像配置面
- **要求 MySQL 改回 `mysql_native_password`**
  - 优点：改动最少
  - 缺点：把库缺陷转移给部署环境，不适合远端镜像默认行为
- **推荐方案：补全 non-TLS full auth**
  - 优点：覆盖 MySQL 8 默认用户；对现有 API 透明；能直接服务镜像重建
  - 缺点：需要小心同步和异步的状态机一致性

**Data Flow**

1. 握手包解析出 `auth_plugin_name = caching_sha2_password`
2. 客户端发送 fast auth 响应
3. 服务端：
   - `0x00`：成功
   - `0x01 0x03`：fast auth 成功，继续等 `OK`
   - `0x01 0x04`：进入 full auth
4. full auth：
   - 客户端发单字节 `0x02` 请求公钥
   - 服务端返回 `0x01 + PEM public key`
   - 客户端构造 `password\0`，与 handshake salt 循环异或
   - 使用 RSA OAEP 公钥加密
   - 发送密文包
   - 等待最终 `OK/ERR`

**Error Handling**

- 保留现有 `MysqlError` 分类，新增 full auth 细化错误信息：
  - 公钥请求失败
  - PEM 解析失败
  - RSA 加密失败
  - full auth 最终响应异常
- `sync` / `async` 错误文案保持语义一致，便于上层服务定位

**Testing**

- 先用现有 `T4-sync_mysql_client.cc` 和 `T3-async_mysql_client.cc` 做真实 MySQL 连接失败复现
- 再扩展/复用同类真实集成测试验证：
  - `caching_sha2_password` 用户可连接
  - 基本 `CREATE/INSERT/SELECT/UPDATE/DELETE` 正常
- 对 `blog` 链路仅做 smoke check，确认 `db-server` 不再因认证失败阻塞

**Out of Scope**

- TLS 握手与证书配置
- `auth switch (0xFE)` 新分支支持
- 远端镜像构建脚本本身的修改
