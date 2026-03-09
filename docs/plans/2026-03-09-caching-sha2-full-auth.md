# Caching SHA2 Full Auth Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Support non-TLS `caching_sha2_password` full authentication in both sync and async MySQL clients.

**Architecture:** Reuse the existing handshake pipeline and extend only the authentication branch that currently stops on `0x01 0x04`. Keep protocol helpers shared, but let sync and async each own their send/receive state transitions.

**Tech Stack:** C++23, OpenSSL, MySQL protocol v41, existing `galay-mysql` sync/async clients, real MySQL integration tests.

---

### Task 1: Capture the sync failure with a real MySQL 8 user

**Files:**
- Modify: `test/T4-sync_mysql_client.cc`
- Test: `test/T4-sync_mysql_client.cc`

**Step 1: Write the failing test**

- Make `T4-sync_mysql_client.cc` print the active auth config and fail on `caching_sha2_password` full auth using the current test user.

**Step 2: Run test to verify it fails**

Run: `cmake --build build_reinstall --target T4-sync_mysql_client --parallel && GALAY_MYSQL_HOST=127.0.0.1 GALAY_MYSQL_PORT=3306 GALAY_MYSQL_USER=<user> GALAY_MYSQL_PASSWORD=<pass> GALAY_MYSQL_DB=<db> build_reinstall/test/T4-sync_mysql_client`

Expected: FAIL with full auth related error.

**Step 3: Write minimal implementation**

- No production code in this task.

**Step 4: Run test to verify it still fails for the right reason**

- Confirm the failure is still at the authentication phase, not build/runtime setup.

### Task 2: Add shared full-auth crypto helper

**Files:**
- Modify: `galay-mysql/protocol/MysqlAuth.h`
- Modify: `galay-mysql/protocol/MysqlAuth.cc`
- Test: `test/T2-mysql_auth.cc`

**Step 1: Write the failing test**

- Add a focused test that loads a PEM public key and verifies `cachingSha2FullAuth(...)` returns encrypted bytes instead of an error.

**Step 2: Run test to verify it fails**

Run: `cmake --build build_reinstall --target T2-mysql_auth --parallel && build_reinstall/test/T2-mysql_auth`

Expected: FAIL because helper does not exist or cannot parse/encrypt.

**Step 3: Write minimal implementation**

- Add helper that:
  - strips optional leading protocol marker
  - builds `password + '\0'`
  - XORs with salt
  - loads PEM public key via OpenSSL
  - encrypts with RSA OAEP

**Step 4: Run test to verify it passes**

Run the same command and confirm PASS.

### Task 3: Implement sync full-auth branch

**Files:**
- Modify: `galay-mysql/sync/MysqlClient.cc`
- Test: `test/T4-sync_mysql_client.cc`

**Step 1: Write the failing test**

- Use the existing sync integration test against a `caching_sha2_password` user.

**Step 2: Run test to verify it fails**

Expected: FAIL during auth or the first query after auth.

**Step 3: Write minimal implementation**

- When server returns `0x01 0x04`:
  - send `0x02`
  - read PEM key packet
  - encrypt password with the shared helper
  - send encrypted packet
  - wait for final `OK/ERR`

**Step 4: Run test to verify it passes**

- Re-run `T4-sync_mysql_client.cc` against the same user.

### Task 4: Capture the async failure

**Files:**
- Modify: `test/T3-async_mysql_client.cc`
- Test: `test/T3-async_mysql_client.cc`

**Step 1: Write the failing test**

- Ensure async test uses the same MySQL 8 user and fails specifically on full auth.

**Step 2: Run test to verify it fails**

Run: `cmake --build build_reinstall --target T3-async_mysql_client --parallel && GALAY_MYSQL_HOST=127.0.0.1 GALAY_MYSQL_PORT=3306 GALAY_MYSQL_USER=<user> GALAY_MYSQL_PASSWORD=<pass> GALAY_MYSQL_DB=<db> build_reinstall/test/T3-async_mysql_client`

Expected: FAIL with full auth not supported or equivalent auth error.

### Task 5: Implement async full-auth state transitions

**Files:**
- Modify: `galay-mysql/async/AsyncMysqlClient.h`
- Modify: `galay-mysql/async/AsyncMysqlClient.cc`
- Test: `test/T3-async_mysql_client.cc`

**Step 1: Write the failing test**

- Reuse `T3-async_mysql_client.cc` red state from Task 4.

**Step 2: Run test to verify it fails**

- Confirm failure is still in the async auth phase.

**Step 3: Write minimal implementation**

- Extend connect awaitable state so it can:
  - detect `0x01 0x04`
  - send public-key request
  - read public key
  - send encrypted password packet
  - read final auth result
- Reuse existing send/read contexts where possible.

**Step 4: Run test to verify it passes**

- Re-run `T3-async_mysql_client.cc` and confirm the async CRUD flow passes.

### Task 6: Validate downstream `blog` dependency and handoff

**Files:**
- Modify: `docs/00-快速开始.md` or `docs/07-常见问题.md` if auth caveats need documenting
- Test: `service/db/test/T1-UserCrudApi.sh`, `service/auth/test/T1-AuthApi.sh` in downstream repo

**Step 1: Run integration verification**

- Build `galay-mysql`
- Launch downstream `db-server` with the rebuilt dylib
- Run DB CRUD and auth smoke tests

**Step 2: Document operational handoff**

- Note that remote image must be rebuilt and pushed after library update
- Capture install/runtime requirements if `/usr/local/lib` is not updated in place
