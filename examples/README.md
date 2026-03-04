# Example

目录结构（参考 `galay-redis/examples`）：

- `common/`：示例公共配置
- `include/`：头文件包含写法示例（E1~E5）
- `import/`：模块导入写法示例（E1~E5）

构建 include 示例：

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DGALAY_MYSQL_BUILD_EXAMPLES=ON
cmake --build build --parallel
```

运行 include 示例：

```bash
./build/examples/E1-AsyncQuery-Include
./build/examples/E2-SyncQuery-Include
./build/examples/E3-AsyncPool-Include
./build/examples/E4-SyncPreparedTx-Include
./build/examples/E5-AsyncPipeline-Include
```

尝试构建 import 示例（工具链支持时）：

```bash
cmake -S . -B build-import \
  -DCMAKE_BUILD_TYPE=Release \
  -DGALAY_MYSQL_BUILD_EXAMPLES=ON \
  -DGALAY_MYSQL_ENABLE_IMPORT_COMPILATION=ON \
  -DGALAY_MYSQL_BUILD_MODULE_EXAMPLES=ON
cmake --build build-import --parallel
```

运行 import 示例：

```bash
./build-import/examples/E1-AsyncQuery-Import
./build-import/examples/E2-SyncQuery-Import
./build-import/examples/E3-AsyncPool-Import
./build-import/examples/E4-SyncPreparedTx-Import
./build-import/examples/E5-AsyncPipeline-Import
```
