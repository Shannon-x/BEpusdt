# 升级指南

BEpusdt 的升级方式一直是"替换二进制 / 镜像后重启"：启动时自动迁移数据库结构、为新增配置项写入默认值。本文说明升级时会发生什么、如何自检，以及从旧版本（尤其是 MySQL 部署）升级的注意事项。

## 升级步骤

1. 备份数据库（SQLite 直接复制 `.db` 文件；MySQL / PostgreSQL 用 `mysqldump` / `pg_dump`）。
2. 替换二进制或拉取新镜像（`ghcr.io/shannon-x/bepusdt:latest`），保持原有的 `SQLITE` / `MYSQL_DSN` / `POSTGRESQL_DSN` / `LOG` / `LISTEN` 参数不变。
3. 启动。日志与控制台会输出：
   - `数据库升级：新建数据表 …`——本次新建的表；
   - `配置升级：新增配置项并写入默认值 …`——本次补齐的配置键；
   - `BEpusdt 已从 vX 升级到 vY`——同时通过已配置的通知渠道（如 Telegram）发送一条升级提示。
4. 运行一次自检：

   ```bash
   bepusdt doctor --sqlite /var/lib/bepusdt/sqlite.db      # 或 --mysql / --postgres
   ```

   自检会检查：数据表是否齐全、已执行的迁移、缺失的默认配置、每条链的钱包数与 RPC / API 节点数（只有 1 个节点会提醒补备用）、交易所钱包是否配置了凭证、TronGrid Key、回调积压与 dead 数、扫描任务 failed 数、各链游标、通知渠道。发现阻断性问题时退出码为 1。

## 数据库支持

| 数据库 | 参数 / 环境变量 | 说明 |
|---|---|---|
| SQLite（默认） | `--sqlite` / `SQLITE` | 单机部署首选 |
| MySQL 5.7+ / 8.x、MariaDB 10.x、TiDB | `--mysql` / `MYSQL_DSN` | v1.26.0 起恢复并全面支持。DSN 缺少 `parseTime` / `charset` / `loc` 时自动补齐 |
| PostgreSQL | `--postgres` / `POSTGRESQL_DSN` | |

MySQL DSN 示例：

```
MYSQL_DSN=user:password@tcp(127.0.0.1:3306)/bepusdt?charset=utf8mb4&parseTime=True&loc=Local
```

同时指定多个时优先级为 PostgreSQL > MySQL > SQLite。

### 从旧版 MySQL 部署升级

上游 v1.24 曾移除 MySQL 支持，导致停留在旧版 MySQL 部署的用户无法升级。本项目已恢复：**直接用原来的 `MYSQL_DSN` 启动新版即可**，启动时会在原库上补齐新表与新列（`bep_scan_cursor`、`bep_scan_job`、`bep_chain_transfer`、`bep_notify_outbox`、`bep_wallet.credentials` 等），并把尚未回调成功的历史订单登记到回调 outbox。

### 换库（SQLite ⇄ MySQL ⇄ PostgreSQL）

```bash
# 先停止运行中的 bepusdt，再执行整库复制（目标库自动建表，按主键去重，可重复执行）
bepusdt db copy --from-sqlite /var/lib/bepusdt/sqlite.db --to-mysql "user:password@tcp(127.0.0.1:3306)/bepusdt"
bepusdt db copy --from-mysql "user:password@tcp(127.0.0.1:3306)/bepusdt" --to-postgres "postgres://user:password@localhost:5432/bepusdt?sslmode=disable"
```

复制完成后把启动参数指向新库即可。

## 各版本升级要点

### → v1.26.0

- 新增 4 张表与 `bep_wallet.credentials` 列，自动创建。
- `rpc_endpoint_*` 支持逗号分隔多个节点；建议每条链至少配 2 个不同服务商的节点（见 [RPC 节点配置指南](./faq/rpc-endpoint.md)）。
- 新增币安 / 欧易内部转账收款（见 [配置说明](./faq/exchange.md)）。
- 收银台新增内置主题「苏菲家宽 · Editorial Warm」（`sufe`）；如果你之前把它放在外部 `static/checkout/` 目录，现在可以删掉外部目录改用内置版——注意外部目录存在时会**完全取代**内置主题列表。
- 收银台提示改为明确的"手续费由付款方承担，实际到账必须为 X"。
- `payment_lookback_hour` 建议设为 `24`。

### ≤ v1.23 → 新版

- 收银台模板格式从 `static/payment/`（每币种一页）改为 `static/checkout/<主题>/`；旧模板不再加载，请改用内置主题或按新格式迁移。
- 订单表移除了 `trade_type_reselect` 列（迁移自动完成）。

## 常见问题

- **升级后没有收到 Telegram 升级提示**：检查后台「通知设置」是否配置了渠道；`bepusdt doctor` 会提示未配置。
- **MySQL 报 `Error 1071: Specified key was too long`**：请使用 `utf8mb4` 字符集且 MySQL 5.7+（`innodb_large_prefix` 默认开启）。
- **想回退**：数据库结构向前兼容（只增不删），回退到 v1.25.0 可直接替换二进制；更早版本请用备份恢复。
