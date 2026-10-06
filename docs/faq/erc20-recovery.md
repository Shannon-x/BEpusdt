# ERC20 扫描故障与历史付款恢复

当最新高度查询正常，但 `task.log` 持续出现 HTTP 403、RPC 套餐限制或 `eth_getBlockByNumber` 错误时，仍可能无法识别付款。检查必须覆盖区块、Transfer 日志和交易回执，不能只测试 `eth_blockNumber`。

## 本次修复的行为

- ERC20 使用 `eth_getBlockByNumber(..., false)` 获取区块号、哈希、时间，再按区块哈希查询 `eth_getLogs`。USDT 不再依赖节点允许完整 ETH 交易读取。参数定义见 [Ethereum JSON-RPC 文档](https://ethereum.org/developers/docs/apis/json-rpc/#eth_getblockbynumber)。
- ETH 原生币按收款和监控需求独立扫描；失败范围先登记为 `native` 任务，再允许主扫描继续。历史回放保留原生币恢复能力。
- 本系统收款流水持久化成功后，扫描器才确认完成。写库失败会重试；失败范围登记失败时不允许游标越过。
- 补扫按付款当时的订单有效期选择候选，旧订单不受当前普通回溯窗口限制。付款必须仍符合币种、地址和金额条件，并发生在创建之后、过期之前。
- 历史订单进入确认中后仍保持链头同步；开启区块确认偏移时，不会因订单超出普通回溯窗口而停在确认中。
- 流水领取与订单进入确认中在同一事务内完成。重复扫描同一事件不会把同一付款用于第二张订单；确认成功后仍通过已有回调 outbox 发送商户通知。
- 对账同时覆盖近期补录的旧流水，并分页处理。异常重启遗留的执行中任务会重新排队；任务状态写库失败时保留完成进度继续尝试。

扫描游标表示此前区块已经扫描完成，或者已有可靠的失败任务负责恢复。它不证明每笔付款已经认单；仍需检查 `native`、`failed`、`deferred` 任务和回调结果。

## 恢复步骤

1. 备份数据库和配置，停止旧进程后再部署包含本修复的二进制或镜像。启动时自动补充扫描任务的分段进度字段，并迁移原生币流水索引以保留旧付款的认领状态。若旧原生币流水存在同一付款关联不同订单的冲突，升级会停止并保留原始记录，需先核对该冲突。
2. 在后台 RPC 设置中配置从收款服务器实际可用的节点。HTTP 403 仍需处理节点鉴权或访问限制；解除完整交易依赖无法绕过 HTTP 拦截。支持多个节点时可配置主备。收 ETH 还需有节点支持完整交易读取。
3. 核对链上交易、收款地址、实际币种和订单有效期。过期订单不一定已经付款，不应批量手工改为成功。
4. 查看积压任务，恢复失败任务；需要追补跳过的大区间时，另行恢复对应的 `deferred` 任务。恢复后由运行中的 `start` 进程接手，每轮最多派发 5000 个高度，保留分段进度。

```bash
bepusdt scan status --sqlite /实际路径/sqlite.db
bepusdt scan jobs --network ethereum --limit 30 --sqlite /实际路径/sqlite.db
bepusdt scan retry --network ethereum --status failed --sqlite /实际路径/sqlite.db
# 将 123 替换为 scan jobs 中核实过的跳过区间任务 ID
bepusdt scan retry --network ethereum --status deferred --id 123 --sqlite /实际路径/sqlite.db
```

对未包含在任务表中的历史付款，可按交易所在区块登记定向回放。将下面的起止高度替换为核实后的范围：

```bash
bepusdt scan replay --network ethereum --from 26119000 --to 26119999 --sqlite /实际路径/sqlite.db
```

命令必须连接运行服务使用的同一个数据库。容器内应使用容器可见的数据库路径；MySQL 或 PostgreSQL 使用相应的 `--mysql` / `--postgres` 参数。`scan retry` 仅把任务重新排队，不会直接改变订单状态或发送回调。

5. 检查任务最终为 `done`，目标流水为 `matched`，订单经过确认进入成功状态，并检查商户回调 outbox 的结果。旧付款在订单过期之后到账、币种错误或金额不符合匹配规则时，补扫也不会认单。

如需扫描故障告警，在已配置通知渠道的情况下将 `notifier_alerts` 设为 `important` 或 `all`。此修复不会自动改变现有通知偏好。
