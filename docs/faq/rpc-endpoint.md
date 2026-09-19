# RPC 节点配置指南

## Tron 节点

### TronGrid Api Key（强烈推荐）

> 系统默认内置的 Tron 公共 RPC 节点为 `grpc.trongrid.io:50051`，虽然目前无明显频率限制，但长期使用后被限流是必然趋势。  
> 强烈建议配置 TronGrid Api Key，以提高 Tron 扫块稳定性，避免因节点频率限制导致订单确认失败等问题；基础计划完全免费，足以满足个人需求，无需额外付费！

#### 获取 Api Key

1. 访问 https://www.trongrid.io/register 使用邮箱注册账号并完成登录。
2. 登录后找到 `API Keys` 选项，点击 `Create API Key`，填写名称后提交即可。

#### 配置 Api Key

你的 Api Key 应类似：`648870c0-xxxx-xxxx-xxxx-c7ac4ec263b0`

拿到之后登录 BEpusdt 后台，进入 `系统管理` -> `区块节点` -> `Tron 网络`，将 Api Key 填入 `TronGrid Api Key` 输入框，保存即可生效。

---

## EVM 链节点

### RPC 节点在 BEpusdt 中的作用

BEpusdt 在区块扫描过程中，所有区块数据均通过 RPC 节点获取。因此，**RPC 节点的性能和稳定性直接影响系统的收款体验**。

### 内置公益节点

BEpusdt 默认内置了一批公益 RPC
节点，详见[源代码配置](https://github.com/v03413/BEpusdt/blob/0e1e22cebbf4a2127786e62b2b8b4d2175054c0b/app/model/conf.go#L140)。

#### 公益节点的局限性

- **稳定性难以保证**：公益节点由第三方维护，服务质量无法长期保障
- **网络质量差异大**：部分 VPS 的网络状况本身较差，容易导致请求失败
- **覆盖面不完整**：虽然这些节点已在开发中测试，但无法覆盖所有场景

这也是常见问题"为什么无法收款"或"为什么换台服务器就好了"的主要原因。

#### 建议

- 优先选择大型云服务提供商的服务器部署 BEpusdt，确保国际网络连接质量
- 在非必要情况下，考虑禁用钱包交易监控功能，以降低 RPC 节点的请求压力

### 第三方 RPC 服务

#### Chainlist

**官网**：https://chainlist.org/

Chainlist 是一个聚合各类区块链网络 RPC 节点信息的平台，用户可以：

- 查找适合自己需求的 RPC 节点
- 一键复制配置信息
- 直接集成到应用中

> **注意**：第三方提供的节点质量需自行测试和评估。

#### Nodies

**官网**：https://www.nodies.app/

Nodies 是一个商业 RPC 服务提供商，具有以下特点：

- 提供企业级的高质量 RPC 节点服务
- 性能和稳定性有保障，适合生产环境使用
- 提供免费配额，可满足个人单节点的使用需求
- 需付费才能获得更高的配额和优先级支持

#### Alchemy

**官网**：https://www.alchemy.com/

Alchemy 是业界知名的商业 RPC 基础设施提供商，具有以下特点：

- 支持 Ethereum、Polygon、Arbitrum、Base、BSC 等主流 EVM 链，也提供 Solana 节点
- 免费套餐按计算单元（CU）计费，额度以官网当前数字为准（写作时约 3000 万 CU/月）；重方法（`eth_getLogs`、Solana `getBlock`）消耗更多 CU
- 提供实时 WebSocket 推送、增强型 API 及交易模拟等高级功能
- 稳定性和响应速度均属业界一流水准，适合生产环境

#### Infura

**官网**：https://www.infura.io/

Infura 是由 ConsenSys 旗下的老牌商业 RPC 服务商，具有以下特点：

- 支持 Ethereum、Polygon、Arbitrum、Base 等主流 EVM 链
- 免费套餐每天提供 10 万次请求，可满足个人及小型项目需求
- 服务历史悠久、稳定可靠，被大量 DApp 和工具在生产环境中长期使用
- 按量付费，超出免费额度后可灵活扩容

---

## 多节点与自动切换（推荐配置）

后台 `系统管理` -> `区块节点` 中每条链的节点输入框都支持填写 **多个节点**，用英文逗号（或空格、换行）分隔，例如：

```
https://polygon.drpc.org, https://polygon-bor-rpc.publicnode.com, https://1rpc.io/matic
```

规则：

- 第一个是主节点，程序启动后先使用主节点。
- 请求因节点原因失败（连接失败 / 超时、HTTP 429 / 5xx、返回非法 JSON、JSON-RPC error、区块数据缺失或与请求不一致）时，自动切换到下一个节点，之后的重试与新区块都走新节点。切换会记录在 `task.log` 中（`rpc endpoint failed, switched to next`）。
- 切换后不会自动切回主节点：只要当前节点可用就一直使用，直到它再次失败。因此节点顺序只影响“启动后先用谁”。
- 单个区块失败会以指数退避（1 秒起、最长 60 秒、±25% 抖动）重试最多 10 次，期间每次失败都会尝试切换节点；10 次仍失败才放弃，并通过已配置的通知渠道（如 Telegram）告警，之后可以用下面的补扫接口重新扫描。
- 订单回溯与手动补扫走独立的低优先级队列，不会阻塞实时新区块的扫描。
- Tron 使用 gRPC，节点写法为 `host:port`，多个同样用逗号分隔；TronGrid API Key 与节点独立配置。
- TON 使用 global config 文件，不适用多节点配置。

### 多节点之间的高度差

同一条链配多个节点（以及 PublicNode / dRPC 这类本身就是负载均衡节点池的地址）时，不同节点的高度不一致，v1.26.0 起按以下规则处理：

- **链头只前进**：切到落后节点后它报告的更低链头会被忽略，不回退、不重复下发区块。
- **陈旧节点检测**：头部同步时校验最新区块的时间（EVM 取 `latest` 区块时间戳、Solana `getBlockTime`、Tron 区块头时间、Aptos `ledger_timestamp`），落后超过 3 分钟的节点视为停止同步，立即切换并跳过本轮，不从陈旧链头下发。
- **"区块尚未可用"不算节点故障**：EVM 返回 `null`、Solana `-32004`、Aptos 分片交易数不足、Tron 返回空块，都是节点还没同步到而已——前两次在原节点等待重试，第三次仍拿不到才切换节点；首次等待不计入失败率。
- **EVM 日志按区块哈希查询**：`eth_getLogs` 不再按区间，而是对每个已校验的区块用 `blockHash` 查询（合并在一个批量请求里，HTTP 次数不变）。节点池里某个节点没有该块会明确报错进入重试，不会像按区间查询那样静默少返回日志。
- **完整性校验**：Tron 校验返回区块头的高度、Aptos 校验分片数量，不完整一律重试而不是当作空区块成功。

### 免费额度的估算方式

只有存在待支付订单（或开启了钱包监控 / MQTT 订阅）的链才会扫块，空闲时不消耗额度；没有配置钱包的链完全不会请求节点。扫块期间的请求量大致为：

| 链 | 出块速度 | 持续扫块时的请求量 |
|---|---|---|
| Solana | ~2.5 slot/s | 每个 slot 1 次 `getBlock`，约 9000 次/小时 |
| Polygon / BSC / Base / Arbitrum / X Layer / Plasma | 0.25~2 块/s | 每 `block_batch_size`（默认 3）个区块：1 次批量 `eth_getBlockByNumber` + 1 次带合约地址过滤的 `eth_getLogs` |
| Ethereum | ~12 s/块 | 约 600 次/小时 |
| Tron | ~3 s/块 | 每块 1 次 `GetBlockByNum2`，约 1200 次/小时 |
| Aptos | 按版本区间 | 每 100 个版本 1 次 `transactions` 查询 |

交易确认阶段每 5 秒对每个“确认中”的订单额外查询 1 次。据此对照服务商的免费额度选择主备节点。

### 推荐搭配

以下为写作时（2026-09）的免费方案，额度请以各官网当前数字为准。原则：**主备来自不同服务商**，至少配置 2 个节点，避免同一家限流时一起失效。

| 链 | 首选（注册免费 API Key） | 备用（无需注册的公共节点） | 说明 |
|---|---|---|---|
| Tron | `grpc.trongrid.io:50051` + TronGrid API Key（免费 10 万次/天、15 QPS） | 需为 gRPC 节点 | 无 Key 的 TronGrid 正在逐步降低 QPS，务必配置 Key；按 1200 次/小时估算，免费额度足够 |
| Solana | Alchemy 免费 Key；Helius 免费版 1M credits / 10 RPS 仅够低流量 | `https://solana-rpc.publicnode.com`、`https://api.mainnet-beta.solana.com` | `getBlock` 是重方法，按 9000 次/小时估算额度。已实测两者对事故 slot 448096702 在 `maxSupportedTransactionVersion: 1` 下正常返回；dRPC 免费计划**不含** Solana |
| Polygon | dRPC（`https://polygon.drpc.org`，已实测可取历史区块与过滤日志）或 Alchemy 免费 Key | `https://polygon-bor-rpc.publicnode.com`、`https://1rpc.io/matic` | `eth_getLogs` 已按合约地址过滤（事故区间 2556 条 → 242 条）。PublicNode Polygon 是**裁剪节点**，几天前的区块会返回 `-32701 History has been pruned`，只适合实时扫描，请放在备用位；`polygon-rpc.com` 已停止服务 |
| BSC | dRPC 或 Alchemy 免费 Key | `https://bsc-rpc.publicnode.com`、`https://1rpc.io/bnb`、`https://bsc-dataseed.bnbchain.org` | |
| Ethereum | Alchemy / Infura（免费 10 万次/天）/ dRPC | `https://ethereum-rpc.publicnode.com`、`https://1rpc.io/eth` | 出块慢，公共节点通常够用 |
| Arbitrum | Alchemy / dRPC | `https://arbitrum-one-rpc.publicnode.com`、`https://arb1.arbitrum.io/rpc` | 官方节点有限流，建议放备用 |
| Base | Alchemy / dRPC | `https://base-rpc.publicnode.com`、`https://mainnet.base.org` | 同上 |
| X Layer | — | `https://xlayerrpc.okx.com`、`https://rpc.xlayer.tech` | |
| Plasma | — | `https://rpc.plasma.to` | 公共节点较少 |
| Aptos | Geomi（Aptos Labs）免费 Key | `https://api.mainnet.aptoslabs.com`、`https://aptos-rest.publicnode.com` | |

### 相关配置

| 配置键 | 建议 | 说明 |
|---|---|---|
| `block_batch_size` | `3`（免费节点报 batch 限制时可设为 `1`） | EVM 链每次批量请求的区块数 |
| `payment_lookback_hour` | 覆盖商户允许“重新打开订单”的时长，如 `24` | 订单过期后仍可被链上入账匹配的时间窗口，同时决定回溯扫描范围 |
| `notify_max_retry` | `10` 或更大 | 商户回调失败的最大重试次数，第 N 次在确认时间后 2^N 分钟重试 |

### 扫描状态与补扫接口

登录后台后可调用（需携带后台登录会话）：

- `GET /api/scan/status`：各链最新高度、最近成功区块与时间、成功率、实时 / 回溯队列长度、累计放弃区块数、当前节点与节点列表。
- `POST /api/scan/replay`，请求体 `{"network":"solana","from":448096702,"to":448096702}`：把区间重新送入低优先级队列扫描，单次最多 5000 个区块。补扫是幂等的：已成功的订单不会重复处理，非订单通知按交易哈希去重，不会产生重复回调。

告警（需先在 `系统管理` -> `通知设置` 配置渠道）：

- 区块放弃：某区块重试 10 次仍失败（同一链 5 分钟最多 1 条）。
- 扫描停滞：存在待支付订单但超过 2 分钟没有任何成功扫描（同一链 10 分钟最多 1 条）。
- 队列拥堵：实时队列达到 100 个区块上限（同一链 10 分钟最多 1 条）。

---

## 持久化扫描状态与补扫（v1.25.0+）

升级后会自动创建 4 张表，无需手工迁移：

| 表 | 作用 |
|---|---|
| `bep_scan_cursor` | 每条链**已连续扫描完成**的最高高度。N 成功后才推进到 N，即使 N+1、N+2 已成功也不会越过失败的 N；每 2 秒落盘，进程退出时强制落盘 |
| `bep_scan_job` | 扫描任务：`abandoned`（重试 10 次仍失败的区块）、`gap`（链头跳跃 / 重启导致未扫描的区间）、`replay`（手动补扫）。`pending` 任务每分钟自动重试一次，任务级退避 5m → 10m → … → 6h，最多 20 次后标记 `failed` 并告警 |
| `bep_chain_transfer` | 打到本系统钱包地址的链上入账流水，唯一键 `network + tx_hash + event_index`。**先落库再匹配订单**，同一区块重复扫描只产生一条记录 |
| `bep_notify_outbox` | 商户回调事件。订单标记成功与回调事件在**同一事务**内写入；发送任务独立运行，记录每次响应状态码、响应摘要、失败原因与下次重试时间（1m → 2m → 4m → … → 6h），达到 `notify_max_retry` 次后标记 `dead` 并告警 |

### 重启与链头跳跃的处理

- 重启时若游标与链头差距 ≤ `block_height_max_diff`（默认 1000），从游标续扫，不丢块。
- 差距更大（停机很久）或运行中链头跳跃超过该值：从链头继续，跳过的区间记为 `gap` 任务（不自动扫描）并告警；期间的待支付订单仍由订单回溯覆盖。如需完整补扫，用下面的补扫接口分段回放。
- 进程异常退出时执行中的任务会在下次启动时重新排队。

### 对账

每 5 分钟把最近 48 小时内仍未匹配订单的入账重新跑一遍订单匹配（订单范围放宽到 48 小时内过期的订单）。付款发生在订单过期**之前**但被延迟扫到时，订单会进入确认流程并正常回调（迟到支付恢复）；付款发生在过期之后不会匹配。对账补认单时会发送告警，提示实时匹配阶段曾出现问题。

### 运维接口

后台接口（需登录会话）：

- `GET /api/scan/status`：各链头高度、连续游标、最近成功区块/时间、成功率、实时/回溯队列、回溯状态、放弃区块数、任务统计、当前节点；以及回调 outbox 待发送/dead 数量、最早待发送等待秒数、24 小时内未认单入账数。
- `POST /api/scan/replay` `{"network":"solana","from":448096702,"to":448096702}`：补扫，单次 ≤ 5000 区块，登记为 `replay` 任务可跟踪结果。
- `POST /api/scan/jobs` `{"network":"","limit":50}`：最近的任务列表。

命令行（直接操作数据库，运行中的 `start` 进程 1 分钟内接手）：

```bash
bepusdt scan status                      # 游标、任务统计、outbox 状态
bepusdt scan replay --network solana --from 448096702 --to 448096702
bepusdt scan replay --network polygon --from 93363010 --to 93363019
bepusdt scan jobs --network polygon --limit 30
```

与 `start` 一样通过 `--sqlite` / `--postgres`（或环境变量 `SQLITE` / `POSTGRESQL_DSN`）指定数据库。

### 告警一览

需先在 `系统管理` -> `通知设置` 配置渠道。同一类告警对同一链限频。

| 告警 | 触发条件 | 限频 |
|---|---|---|
| 区块扫描放弃 | 区块重试 10 次仍失败 | 5 分钟 |
| 扫描任务重试耗尽 | 任务自动重试 20 次仍失败 | 每任务 1 次 |
| 扫描区间跳过 | 重启 / 链头跳跃导致区间被跳过 | 10 分钟 |
| 区块扫描停滞 | 有待支付订单但 2 分钟内没有成功扫描 | 10 分钟 |
| 扫描队列拥堵 | 实时队列达到 100 个区块 | 10 分钟 |
| 商户回调积压 | 最早待发送回调等待超过 30 分钟 | 10 分钟 |
| 订单回调重试耗尽 | 单个订单回调达到 `notify_max_retry` 次 | 每订单 1 次 |
| 对账补认单 | 对账任务为此前未匹配的入账补认了订单 | 10 分钟 |
