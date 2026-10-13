# 收件箱配置代际热切换 Implementation Plan

## Repository Research

### 当前更新链路（问题所在）

1. `cmd/inboxes.go::handleUpdateInbox` 先调 `Manager.Update`（开事务、`SELECT ... FOR UPDATE`、合并占位密钥、提交 DB、提交别名行），再调 `reloadInbox`。
2. `reloadInbox`（cmd/init.go）→ `Manager.ReloadInbox`（internal/inbox/inbox.go）：
   - 先 `stopInbox`：取消接收器 → **等待 goroutine 退出** → `Close()` 旧对象并从 map 删除；
   - 再读 DB、用 `initFn` 构造新对象、放入 map、`startReceiver`。
3. 后果：DB 提交发生在新运行时被验证之前；一旦候选构造失败（如 SMTP auth protocol 非法、建池失败），旧接收器已停、旧对象已关，库里却是新配置——保存"成功"但收发已中断。`ReloadInbox` 还在持有 `m.mu` 时调用 `initFn`。

### 各渠道资源与关闭责任

- **email**（internal/inbox/channel/email）：
  - `email.New` 调 `NewSmtpPool` 构造 SMTP 池；`smtppool.New` 不拨号，构造成功即代表配置可用；IMAP 无长连接，每次轮询在 `processMailbox` 内拨号、`defer Logout`。
  - `NewSmtpPool` 中途失败时，此前已建的 pool **不会被关闭**（泄漏）。
  - fork 版 `smtppool.Pool.Close()` 非幂等：二次调用 `close(stopBorrow)` 会 **panic**。
  - `Send` 在 OAuth token 变化时自行关旧池、建新池；`Email.Close` 目前无 `sync.Once` 保护。
- **livechat**：`Close()` 断开所有 widget 客户端连接（`Client.CloseChannel` 有 once），并重置 map，二次调用实际为空操作，但无显式 once 契约。
- **whatsapp**：对象不持有需关闭资源，`Close()` 为 no-op；共享的 `*whatsapp.Client` 绝不能被收件箱关闭；入站走独立的 Redis stream ingester。
- 关机路径：main.go 先 `CloseLiveChatClients()`（关 livechat），后 `Manager.Close()`（关全部），热切换后更依赖"每对象恰好关闭一次"。

### 其他会改变运行时的入口

- Create（handler 后接 reload）、Toggle（启用/禁用）、SoftDelete；
- `cmd/oauth.go` OAuth 重连：`UpdateConfig` 后 `reloadInbox`；
- `cmd/custom_attributes.go` 剥离 livechat prechat 自定义属性后 `reloadInbox`；
- OAuth token 运行时刷新走 `Manager.UpdateConfig`（代内演化，不触发热切换，本次不动）。

### 并发模型现状

- 无按收件箱串行机制；两个并发 PATCH 各自走"提交→停旧→建新"，初始化完成顺序不受控。
- DB 表 `inboxes` 无代际列；内存中以 `map[int]Inbox` 为唯一 active runtime。

## 方案总览

引入**按收件箱串行的配置代际（generation）**：同一收件箱的更新在进程级互斥锁内完成"旧运行时服务中构造候选 → 候选成功才提交同一代配置 → 单一受控交接点切换"；代际同时落库（`inboxes.runtime_generation`）与内存，DB 记录与唯一 active runtime 永远同代。

## Files and Modules

- `schema.sql`：`inboxes` 增加 `runtime_generation BIGINT NOT NULL DEFAULT 0`。
- `internal/migrations/v2.9.0-rc.11.go`（新建）：幂等 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS runtime_generation BIGINT NOT NULL DEFAULT 0`。
- `cmd/upgrade.go`：migList 注册 v2.9.0-rc.11。
- `internal/inbox/models/models.go`：`Inbox` 增加 `RuntimeGeneration int64 `db:"runtime_generation" json:"-"``。
- `internal/inbox/queries.sql`：select 列表补列；`update` 语句自增代际并 RETURNING；新增 `bump-runtime-generation`。
- `internal/inbox/inbox.go`（核心）：串行锁、代际 map、候选-提交-交接流程、Toggle/SoftDelete/StartInbox/ReloadInbox 重构。
- `internal/inbox/channel/email/email.go`：`Close()` 加 `sync.Once`；struct 加 `closeOnce`/关闭标记。
- `internal/inbox/channel/email/smtp.go`：`NewSmtpPool` 失败时关闭已建 pool。
- `internal/inbox/channel/livechat/livechat.go`：`Close()` 加 `sync.Once`。
- `cmd/inboxes.go`：update/create/toggle/delete handler 改为一次 Manager 调用完成持久化+切换；认证错误标记与 ingester 的触发点随结果移动。
- `cmd/init.go`：`reloadInbox` 包装保留（OAuth 重连、自定义属性）；`Toggle` 新签名的接线。
- `i18n/en-US.json`：新增 `inbox.runtimeInitFailed`（含 `{error}`，提示旧配置仍在服务）。
- `internal/inbox/inbox_test.go`：适配 `Update` 新签名；新增热切换代际测试。
- 新增/补充 channel 单测：`smtp_test.go`（建池失败清理、Close 幂等）、`livechat_test.go`（Close 幂等）。

## Implementation Steps

1. **DB 代际基础**
   - schema.sql 加列；新建 rc.11 迁移并注册；model 加字段。
   - queries.sql：`get-active-inboxes`/`get-all-inboxes`/`get-inbox`/`get-inbox-by-uuid` 列表补 `runtime_generation`；`-- name: update` 增加 `runtime_generation = runtime_generation + 1`（仍 `RETURNING *`）；新增：
     ```sql
     -- name: bump-runtime-generation
     UPDATE inboxes SET runtime_generation = runtime_generation + 1, updated_at = NOW()
     WHERE id = $1 AND deleted_at IS NULL RETURNING runtime_generation;
     ```

2. **Manager 串行与代际设施（inbox.go）**
   - `Manager` 增加 `switchLocks map[int]*sync.Mutex` + `switchLocksMu sync.Mutex`；`runtimeGen map[int]int64`（与 `inboxes`/`receivers` 同受 `m.mu` 保护）。
   - `lockInbox(id) func()`：惰性取锁，入口全程持有（不同收件箱仍可并行）。
   - 内部原语（调用方均已持收件箱锁）：
     - `stopRuntime(id)`：取消接收器 → 锁外等 `done` → 持锁摘 map、记录代际、锁外 `Close()` 旧对象一次；对象为空时均为 no-op。
     - `handoff(ctx, id, cand, gen)`：**先**取消并等待旧接收器完全退出 → 持 `m.mu` 校验 `gen > runtimeGen[id]`（陈旧候选：关闭候选、返回 conflict，不动当前运行时）→ 发布 `inboxes[id]=cand`、写 `runtimeGen[id]=gen`、`startReceiver(ctx, cand)`（旧接收器已确认退出，杜绝两个接收器并行）→ 锁外对旧对象 `Close()` 恰好一次。
     - `activateRecord(ctx, id, initFn)`：读解密记录；`!Enabled` → `stopRuntime`；否则**在旧运行时继续服务期间**调 `initFn` 构造候选（不持 `m.mu`、不起接收器），失败则关闭候选已建资源并返回错误，旧运行时保持不动；成功后 `bump-runtime-generation` 取新代际 → `handoff`。

3. **`Update` 重构为"候选先行 + 同代提交 + 受控交接"**
   - 新签名：`Update(ctx context.Context, id int, patch imodels.Inbox, initFn initFn) (imodels.Inbox, error)`；`initFn == nil` 时仅持久化（测试用）。
   - 流程：取收件箱锁 → 开事务、`FOR UPDATE`、现有全部合并/校验/别名/密钥占位逻辑保持不变 →
     - 若 `Enabled && initFn != nil`：用合并后的明文记录（`current` 行覆盖 Config/From/Name/Aliases/Secret 等）在提交前构造候选；
       - 构造失败：`cand != nil` 则 `cand.Close()`（回收半成品资源），事务回滚，返回 `envelope.InputError`（`inbox.runtimeInitFailed` 带底层 error，可操作）；**持久配置、别名、密钥占位值、旧接收器、旧发送对象全部保持保存前状态**；
       - 构造成功：执行原提交（代际 +1，RETURNING 得 `gen`），提交后完成别名状态读取等既有收尾 → `handoff(ctx, id, cand, gen)`。
     - 若 `!Enabled`：提交后 `stopRuntime`（禁用仍有意停止）。
   - 两个并发更新因收件箱锁严格串行，提交序=构造序；`handoff` 内 `gen > runtimeGen` 校验兜底：即使未来代码放松串行，较旧候选也不能覆盖较新提交。

4. **其余生命周期入口统一到同一串行/代际机制**
   - `StartInbox(ctx, id, initFn)`（Create 后）：收件箱锁内 `activateRecord`（含 bump+handoff）；构造失败关闭候选并返回错误（行已存在的既有语义不变）。
   - `ReloadInbox(ctx, id, initFn)` 保留签名（OAuth 重连、自定义属性用）：收件箱锁内 `activateRecord`——同样改为候选先行，旧运行时在候选构造期间继续服务。
   - `Toggle(ctx, id, initFn)`：锁内执行 toggle SQL；变为禁用 → `stopRuntime`；变为启用 → `activateRecord`（重新启用只启动一个接收器；构造失败返回可操作错误，DB 状态为启用、无运行时，与当前失败类别一致）。
   - `SoftDelete(id)`：锁内执行后 `stopRuntime`。
   - `InitInboxes`/`Start`：启动时把记录的 `runtime_generation` 写入 `runtimeGen`，保证重启后代际连续。
   - `Close()`：保持取消全部接收器并关闭对象；每个对象仅从 map 摘除一次、关闭一次。

5. **渠道关闭责任加固**
   - email：`Email` 增加 `closeOnce sync.Once`，`Close()` 在 once 内关闭全部 pool 并置空 slice；`Send` 内池重建路径保持（旧池仅在该处关闭一次，不复用 `Close`）。
   - smtp.go：`NewSmtpPool` 任一池构造失败时，先关闭已构造的 pools 再返回错误（失败候选不泄漏）。
   - livechat：`LiveChat.Close()` 用 `sync.Once` 包裹（被替换/禁用/删除对象、关机重复路径不重复关闭）。
   - whatsapp：维持 no-op，共享 client 不触碰；失败候选（`New` 校验失败返回 nil 对象）无需清理。

6. **cmd 接线**
   - `handleUpdateInbox`：用新 `Update(ctx, id, inbox, makeInboxInitializer(...))` 一次完成；删除其后的 `reloadInbox`；成功后再 `inboxAuthErrors.Delete(id)`、`ensureWhatsAppIngester`、`postSaveWhatsAppTasks`；失败直接 `sendErrorEnvelope`（候选失败为 400 可操作错误）。WhatsApp 提交前的实时凭据校验逻辑与位置不变。
   - `handleCreateInbox`：`Create` 后改调 `StartInbox`；失败仍返回 500。
   - `handleToggleInbox`/`handleDeleteInbox`：改用新签名方法，删除各自的 `reloadInbox`；ingester 确保与 WhatsApp 删除后 webhook 重指逻辑保留。
   - `reloadInbox` 包装保留给 oauth.go / custom_attributes.go。

7. **i18n**
   - en-US.json 增加：
     `"inbox.runtimeInitFailed": "The inbox could not start with the new settings: {{ .error }}. The previous configuration is still active."`

## Dependencies and Considerations

- `initEmailInbox` 向全局 `ko` koanf 实例 Load 配置是既有跨收件箱并发隐患；本次保持现状，仅保证候选构造不在 `m.mu` 内执行。
- 不为 email 新增 IMAP/SMTP 实时探活：该渠道收发就绪本就由构造（SMTP 池）+ 轮询建连（IMAP）体现，新增网络探测会改变轮询语义并引入超时/不稳定测试。"候选可用"边界 = 构造函数成功返回（WhatsApp 另有既有 handler 实时校验）。
- `UpdateConfig`（OAuth token 刷新）属代内演化，不 bump 代际；它与并发 `Update` 之间的整配置覆盖是**既有竞态**，不在本次范围。
- 交接时等待旧接收器退出，若恰有在途 IMAP 拨号可能耗时——与现状 `stopInbox` 相同，不回退。
- 迁移名 v2.9.0-rc.11 经 `semver.Compare` 大于 rc.10，升级路径正常。

## Validation

- `go build ./...`、`gofmt -l`、`go vet ./internal/inbox/... ./cmd/...`。
- `go test ./internal/inbox/...`（需 Postgres，不可用时 testutil 自动 skip）：
  - 候选构造失败：DB 配置/别名/密钥占位不变、map 中旧对象仍是同一实例且接收器存活、候选被 Close；
  - 成功更新：DB `runtime_generation` 与 `runtimeGen`/map 一致、map 中唯一对象为候选、旧对象恰好 Close 一次、接收器先停旧后启新（用可控 initFn/假 Inbox 记录 Receive 启停顺序，无并行窗口）；
  - 反转完成顺序的两个串行更新后，active runtime 与 DB 同为较新代际；
  - 禁用→运行时移除；删除→移除；重新启用→仅一个接收器；
  - 既有 `TestInboxEmailAddresses`、`TestLivechatSecretStaysEncryptedOnUpdate` 等保持通过（适配新签名，传 nil initFn）。
- channel 单测：`NewSmtpPool` 部分失败后已建 pool 被关闭；`Email.Close()` 重复调用不 panic；`LiveChat.Close()` 重复调用安全。
- `go test ./cmd/...` 确认接线编译与 whatsapp ingester 既有测试不回归。

## Risks

- **风险：交接点引入死锁/接收器并行** → 严格约束：等待 `done` 在 `m.mu` 之外；发布与 `startReceiver` 在同一把 `m.mu` 的同一临界区内；旧接收器 `done` 之后才进入发布临界区。
- **风险：关闭 panic（smtppool 二次 Close）导致进程崩溃** → once 保护 + Manager 每对象单次摘除关闭 + 建池失败清理，三处兜底。
- **风险：handler 行为回归（WhatsApp 校验、认证错误展示、ingester）** → 这些逻辑原样保留，仅把"先提交后重载"合并为一次 Manager 调用；认证错误标记改为成功切换后清除（语义更准），失败时保留旧标记。
- **风险：迁移/列遗漏导致 sqlx Scan 报错** → schema.sql 与 4 条 select 同步加列，model 字段对齐，测试库直接走 schema.sql 可立即暴露。
