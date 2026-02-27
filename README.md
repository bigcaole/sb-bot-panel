# sb-bot-panel

本项目现在包含节点侧一键部署方案：`sing-box + sb-agent + UFW + systemd + ACME 证书检查`，并提供中文数字菜单，体验接近 x-ui/s-ui。

详细手册（零基础可用）：`/Users/cwzs/Documents/sb-bot-panel/docs/零基础部署-测试-使用-排障手册.md`

## 目录新增

- `agent/sb_agent.py`
  - 节点侧 agent（Python 3.11+），轮询 controller `/nodes/{node_code}/sync`，生成并热更新 `sing-box` 配置。
  - 当 UFW 已启用时，agent 会自动放行 TUIC 相关 UDP 端口（含监听端口与端口池范围）。
- `scripts/install.sh`
  - Debian/Ubuntu 中文交互式安装/更新脚本（含域名解析检查、防火墙、服务部署、证书检查 timer）。
- `scripts/menu.sh`
  - 中文数字菜单（安装/配置/启停/日志/证书刷新/卸载/SSH 与 fail2ban 安全加固）。
- `scripts/sb_cert_check.sh`
  - 证书状态检查脚本（供菜单与 timer 调用）。

## 节点一键安装（Debian/Ubuntu）

### 方式 A：仓库内执行（推荐）

```bash
sudo bash /path/to/sb-bot-panel/scripts/install.sh
```

### 方式 B：一条命令（示例）

```bash
git clone <你的仓库地址> sb-bot-panel && cd sb-bot-panel && sudo bash scripts/install.sh
```

安装过程为中文交互，会询问并写入 `/etc/sb-agent/config.json`：

1. `controller_url`（支持省略协议；例如可直接填 `panel.example.com:8080`）
2. `node_code`（例如 `JP1`）
3. `auth_token`
4. `tuic_domain`（留空则不启用 TUIC）
5. `acme_email`（启用 TUIC 时必填）
6. `tuic_listen_port`（默认 `8443`）
7. `poll_interval`（默认 `15` 秒）

## 中文菜单管理

```bash
sudo bash /path/to/sb-bot-panel/scripts/menu.sh
```

菜单项：

【日常运维】
1. 配置（重写 `/etc/sb-agent/config.json`）
2. 启动 sb-agent
3. 停止 sb-agent
4. 重启 sb-agent
5. 查看 sb-agent 状态
6. 查看 sb-agent 日志（tail -f）
7. 重启 sing-box
8. 查看 sing-box 状态与最近日志
9. 证书状态检查
10. 触发证书重新申请/刷新（先备份再清理）
【安全工具】
11. 安装/启用 fail2ban（SSH 防爆破）
12. 查看 fail2ban 状态与封禁列表
13. 解封 fail2ban 封禁 IP
14. 生成 SSH 密钥（ed25519）
15. SSH 安全状态总览（只读）
16. 一键安全修复（半自动）
17. 启用 SSH 仅密钥登录（禁用密码）
18. 恢复 SSH 密码登录（应急）
【系统级操作（谨慎）】
19. 更新同步（保留原配置，自动 `git pull`，无交互）
20. 卸载
21. 退出

说明：

- 首次安装后，后续更新建议直接用菜单 `19`，不会重复询问端口/域名等参数。
- 需要改参数时用菜单 `1`。
- 安全建议：先用菜单 `14` 生成并部署公钥，再用菜单 `15` 做只读安全检查；可先执行菜单 `16` 做半自动修复，最后再执行菜单 `17` 启用仅密钥登录。
- 菜单 `15` 会输出 SSH 风险等级（低/中/高）和修复建议清单；菜单 `16` 会自动修复低风险项（fail2ban/UFW），高风险项仍需人工确认。
- 也可命令行直接执行：

```bash
sudo bash scripts/install.sh --sync-only
```

- 安装后可用快捷命令打开节点菜单：

```bash
sb-node
```

说明：历史版本可能残留 `sb-bot-panel` 快捷命令，最新安装脚本会自动清理，仅保留 `sb-node`。

## 域名证书前置条件（TUIC）

启用 TUIC 前请确保：

- 域名 A 记录指向节点公网 IPv4。
- 如果使用 Cloudflare，请关闭代理（小黄云置灰）。
- 放行端口：
  - `443/tcp`（VLESS+REALITY）
  - `${tuic_listen_port}/udp`（TUIC）
  - `22/tcp`（SSH）

安装脚本会自动检查解析并给出中文提示，可反复重试。

## sb-agent 行为说明

- 配置文件：`/etc/sb-agent/config.json`
- 运行方式：`systemd` 服务 `sb-agent.service`
- 日志文件：`/var/log/sb-agent/agent.log`
- 轮询接口：`GET {controller_url}/nodes/{node_code}/sync`（携带 `Authorization: Bearer {auth_token}`）
- 每次配置变更后自动执行：
  - `sing-box check -c /etc/sing-box/config.json`
  - `systemctl reload-or-restart sing-box`

REALITY 密钥策略：

- 若本地缺少 `private_key/public_key/short_id`，`sb-agent` 会在节点本机自动生成并持久化。
- 私钥仅存本机（`/etc/sb-agent/state.json`），不会上传到 controller。

## 证书检查与续期

- `sing-box` 使用内置 ACME（certmagic）自动申请/续期。
- 安装脚本会创建：
  - `sb-cert-check.service`
  - `sb-cert-check.timer`（每日执行，可选启用）
- 手动检查：

```bash
/usr/local/bin/sb-cert-check.sh
```

该检查会输出：

- 域名与本机公网 IP 对比
- 证书文件是否存在
- 证书剩余天数（可读时）
- 最近 ACME 相关日志关键词
- “是否近期成功签发”的判断

## 常见故障排查

### 1) 没有 TUIC 日志 / 无法连接

- 确认 `tuic_domain` 非空且配置正确：`cat /etc/sb-agent/config.json`
- 确认 `tuic_listen_port/udp` 已放行：`ufw status`
- 看服务日志：
  - `journalctl -u sing-box -f`
  - `tail -f /var/log/sb-agent/agent.log`

### 2) 证书申请失败

- 先看：`/usr/local/bin/sb-cert-check.sh`
- 重点检查：
  - 域名 A 记录是否指向当前公网 IP
  - Cloudflare 代理是否关闭（小黄云置灰）
  - UDP/TCP 端口是否放行
- 可通过菜单 `10` 执行“备份 + 刷新重申领”。

### 3) 域名解析不对

- 用命令确认：
  - `dig +short A 你的域名`
  - `curl -4 ifconfig.me`
- 两者必须一致。

### 4) 端口未放行

- `ufw allow 443/tcp`
- `ufw allow 8443/udp`（或你的 TUIC 端口）
- `ufw status`

## 管理服务器一键安装（controller + bot）

管理服务器侧新增脚本目录：`scripts/admin/`，用于一键安装、配置、服务管理和迁移。

### 新装（推荐从菜单进入）

```bash
git clone <你的仓库地址> sb-bot-panel && cd sb-bot-panel && sudo bash scripts/admin/menu_admin.sh
```

菜单里选择 `17) 安装/更新` 即可完成：

- 依赖安装（apt）
- venv 创建与 `pip install -r requirements.txt`
- 写入 `.env`
- 可选启用 Caddy（自动申请 HTTPS 证书并自动续期）
- 写入并启用 systemd：
  - `sb-controller.service`
  - `sb-bot.service`

说明：菜单 `17) 安装/更新` 默认复用现有 `.env` 配置，不会每次重复询问端口/域名/token/chat_id。需要改参数时使用菜单 `1) 配置向导`。

### 仅重新配置（改 token/chat id）

```bash
sudo bash scripts/admin/install_admin.sh --configure-only
```

或通过菜单：

- `2) 配置向导（修改参数并重启）`

说明：执行安装/配置向导时，会自动写入完整 `.env` 字段（包括 `BOT_MENU_TTL`、`BOT_NODE_MONITOR_INTERVAL`、`BOT_NODE_OFFLINE_THRESHOLD`、`BOT_LOG_VIEW_COOLDOWN`、`BOT_LOG_VIEW_MAX_PAGES`），无需手工补字段。
并且 URL 字段支持省略协议（`http://` / `https://`），脚本会自动补全。

### 管理服务器 HTTPS 证书（申请+自动续期）

- 建议在安装向导里启用 `ENABLE_HTTPS=1`，并填写：
  - `HTTPS_DOMAIN`（例如 `panel.example.com`）
  - `HTTPS_ACME_EMAIL`（可选，建议填写）
- 启用后脚本会自动：
  - 安装 `caddy`
  - 写入 `/etc/caddy/Caddyfile`
  - 反向代理到 `127.0.0.1:${CONTROLLER_PORT}`
  - 自动申请并自动续期 Let's Encrypt 证书
- 管理菜单新增：
  - `9) HTTPS 证书状态（Caddy）`
  - `10) HTTPS 证书刷新（重载 Caddy）`

## 管理服务器菜单（中文数字）

```bash
sudo bash scripts/admin/menu_admin.sh
```

菜单新增 `手动安全清理（过期封禁 + 审计日志）`，可在 SSH 下直接触发一次安全维护清理，不必进入 bot。

## Bot 远程管理（管理服务器）

在 Telegram 的 `管理服务器` 菜单中已支持：

- 安装/更新（后台执行 `install_admin.sh --reuse-config`，仅作用于当前管理服务器）
- 配置向导（远程修改 `.env` 后后台重载）
- 一键验收自检（语法检查 + unittest + API 冒烟）
- 启动/停止 controller
- 状态查看（controller/bot/caddy）
- 查看日志（controller/bot/caddy）
- HTTPS 证书状态 / 刷新
- 迁移导出 / 迁移导入（导入走非交互模式）
- 操作日志（`/admin/audit`，查看最近审计记录）
- 访问安全（整合节点来源 IP 白名单状态 + 全局安全配置状态）

## Bot 远程管理（节点服务器）

在 Telegram 的 `节点与线路 -> 节点远程运维` 中可按节点下发任务：

- 同步更新（节点执行 `scripts/install.sh --sync-only`，仅作用于你当前选中的该节点）
- 重启 sing-box
- 查看 sing-box / sb-agent 状态
- 查看 sing-box / sb-agent 日志（通过任务回传）
- 修改节点参数（写入 `/etc/sb-agent/config.json`，如 `poll_interval`、`tuic_domain`、`tuic_listen_port` 等）
- 任务生命周期：controller 自动处理超时任务（默认 120 秒）、失败重试（按任务重试次数）与历史清理（默认保留 7 天）

安全建议：

- 强烈建议配置 `AUTH_TOKEN`（否则上述远程接口在开放端口下无鉴权）。
- 每个节点设置 `agent_ip` 白名单，并在管理服务器 UFW 仅放行节点 IP 到 controller 端口。

安装后可直接用快捷命令打开菜单：

```bash
sb-admin
```

说明：历史版本可能残留 `sb-bot-panel` 快捷命令，最新安装脚本会自动清理，仅保留 `sb-admin`。

如果系统里没有现成的 `s-ui` 命令冲突，脚本也会自动创建 `s-ui` 作为同一菜单入口。

菜单项：

【日常运维】
1. 配置向导（修改参数并重启）
2. 启动 controller
3. 停止 controller
4. 启动 bot
5. 停止 bot
6. 状态查看（controller/bot）
7. 查看日志（controller/bot）
8. HTTPS 证书状态（Caddy）
9. HTTPS 证书刷新（重载 Caddy）
10. 迁移：导出迁移包
11. 迁移：导入迁移包
12. 一键验收自检（语法/单测/API）
13. 数据库一致性校验（迁移前建议）
14. 安全加固向导（token 轮换 + 8080 收敛）
15. 收敛 AUTH_TOKEN（新旧双token -> 单token）
16. 手动安全清理（过期封禁 + 审计日志）
17. SSH 安全状态总览（只读）
18. SSH 一键安全修复（半自动）
【系统级操作（谨慎）】
19. 安装/更新（git pull + 依赖 + venv + 重启）
20. 卸载
21. 退出

统一验收命令（管理服务器）：

```bash
bash /root/sb-bot-panel/scripts/admin/smoke_test.sh --require-api
```

说明：

- 默认会读取 `.env` 中的 `CONTROLLER_PORT` / `AUTH_TOKEN`
- 检查项：Python 语法、`tests/` 单元测试、controller API 鉴权冒烟
- 访问收敛检查：会读取 `/admin/node_access/status`，默认仅告警；若设置 `SMOKE_REQUIRE_NODE_LOCK=1`，当存在“启用但未锁定来源IP”的节点会直接判失败
- 退出码：`0=通过`，`10=代码检查失败`，`20=API检查失败`，`30=代码+API均失败`

数据库迁移前一致性校验（管理服务器）：

```bash
bash /root/sb-bot-panel/scripts/admin/db_consistency_check.sh
```

说明：

- 会依次执行：`/admin/db/export` -> `/admin/db/verify_export`（与当前库比对）-> `/admin/db/integrity`
- 任何一步失败会返回非 0 退出码，避免带问题做迁移

## GitHub 自动验收（CI）

仓库已新增：`.github/workflows/ci.yml`

- 触发时机：`push main`、`pull request`
- 检查内容：
  - 管理/节点脚本 `bash -n` 语法检查
  - `scripts/admin/smoke_test.sh --skip-api`（语法 + 单元测试）
- Python 版本：`3.11`、`3.12`

## Python 与系统兼容

- 生产基线：`Python 3.11`（推荐）
- 兼容版本：`Python 3.12`
- 不再建议：`Python 3.9`（已退役，缺少安全支持）

安装脚本已内置 Python 3.11 适配逻辑：

- Debian 12：直接使用系统 Python 3.11
- Debian 13：系统 Python 通常 >=3.11，直接可用
- Debian 11：自动尝试 `bullseye-backports` 安装 Python 3.11
- Ubuntu（20.04/22.04/24.04）：自动尝试系统包，必要时回退 `deadsnakes` 安装 Python 3.11

## 迁移导出/导入（管理服务器）

默认迁移包目录：`/var/backups/sb-migrate/`

默认保留策略：

- 控制器备份：保留最近 `30` 个（`BACKUP_RETENTION_COUNT`）
- 迁移包：保留最近 `20` 个（`MIGRATE_RETENTION_COUNT`）

### 旧机导出

- 进入菜单后执行：`11) 迁移：导出迁移包`
- 脚本会停止 `sb-controller` 与 `sb-bot`，导出完成后可选是否自动拉起
- 生成文件名示例：`sb-migrate-YYYYmmdd-HHMMSS.tar.gz`
- 导出内容：
  - `data/`（必须）
  - `.env`（必须）
  - `scripts/`（建议）
  - `sb-controller.service` / `sb-bot.service`（如果存在则附带）

### 传输迁移包

```bash
scp root@旧IP:/var/backups/sb-migrate/sb-migrate-xxxx.tar.gz root@新IP:/root/
```

### 新机导入

- 在新机先准备项目目录（建议先 `git clone`）
- 进入菜单执行：`12) 迁移：导入迁移包`
- 导入脚本会：
  - 备份旧项目目录到 `/var/backups/sb-migrate/restore-backup-*.tar.gz`
  - 恢复 `data/.env/scripts`
  - 进入参数修正向导（CONTROLLER_URL / BOT_TOKEN / ADMIN_CHAT_IDS）
  - 自动安装依赖、重建 venv、重写 systemd、重启服务
  - 自检 `/health` 与 bot 服务状态

## .env 配置项（管理服务器）

参考：`/Users/cwzs/Documents/sb-bot-panel/.env.example`

- `CONTROLLER_URL=http://127.0.0.1:8080`
- `CONTROLLER_PUBLIC_URL=http://your-public-ip:8080`（可选，对外地址）
- `PANEL_BASE_URL=https://panel.example.com`（建议填域名，bot 订阅链接将使用该地址）
- `ENABLE_HTTPS=1`（1=启用 Caddy 自动证书，0=关闭）
- `HTTPS_DOMAIN=panel.example.com`（启用 HTTPS 时填写域名）
- `HTTPS_ACME_EMAIL=admin@example.com`（可选，证书账号邮箱）
- `CONTROLLER_PORT=8080`
- `CONTROLLER_PORT_WHITELIST=`（可选；逗号分隔 IP/CIDR，用于限制 8080 访问来源）
- `SECURITY_BLOCK_PROTECTED_IPS=`（可选；逗号分隔 IP/CIDR，manual/auto 封禁会跳过这些来源）
- `AUTH_TOKEN=随机长串`（可空；空值表示关闭接口鉴权；支持 `new_token,old_token` 过渡轮换）
- `SUB_LINK_SIGN_KEY=`（可选；设置后可生成带签名订阅链接）
- `SUB_LINK_REQUIRE_SIGNATURE=0`（可选；1=强制订阅必须带签名）
- `SUB_LINK_DEFAULT_TTL_SECONDS=604800`（可选；签名默认有效期）
- `API_RATE_LIMIT_ENABLED=0`（可选；controller 轻量限流开关）
- `API_RATE_LIMIT_WINDOW_SECONDS=60`（可选；限流窗口）
- `API_RATE_LIMIT_MAX_REQUESTS=120`（可选；单个 IP+路径窗口内请求上限）
- `SECURITY_EVENTS_EXCLUDE_LOCAL=1`（可选；安全统计默认过滤本机测试来源）
- `UNAUTHORIZED_AUDIT_SAMPLE_SECONDS=30`（可选；未授权审计采样窗口，防止被扫描时审计日志爆涨）
- `SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS=60`（可选；到期封禁自动清理周期）
- `AUDIT_LOG_RETENTION_DAYS=30`（可选；审计日志保留天数）
- `AUDIT_LOG_CLEANUP_INTERVAL_SECONDS=3600`（可选；审计日志自动清理周期）
- `AUDIT_LOG_CLEANUP_BATCH_SIZE=2000`（可选；单次清理批量）
- `BOT_TOKEN=xxxxxxxx`（建议填写；留空会写入占位值并跳过启动 sb-bot）
- `ADMIN_CHAT_IDS=123,456`（可空）
- `VIEW_ADMIN_CHAT_IDS=`（可选，只读管理员）
- `OPS_ADMIN_CHAT_IDS=`（可选，运维管理员）
- `SUPER_ADMIN_CHAT_IDS=`（可选，超级管理员）
- `MIGRATE_DIR=/var/backups/sb-migrate`
- `BACKUP_RETENTION_COUNT=30`（可选，控制器备份保留数量）
- `MIGRATE_RETENTION_COUNT=20`（可选，迁移包保留数量）
- `BOT_MENU_TTL=60`（可选，bot 菜单按钮自动清理秒数）
- `BOT_NODE_MONITOR_INTERVAL=60`（可选，节点在线检测周期秒数）
- `BOT_NODE_OFFLINE_THRESHOLD=120`（可选，判定离线阈值秒数）
- `CONTROLLER_HTTP_TIMEOUT=10`（可选，bot 调 controller 超时秒数）
- `BOT_ACTOR_LABEL=sb-bot`（可选，bot 调 controller 时用于审计的操作者标识）
- `BOT_LOG_VIEW_COOLDOWN=1`（可选，日志翻页冷却秒数，防止触发 Telegram 限流）
- `BOT_MUTATION_COOLDOWN=1`（可选，写操作按钮防抖秒数，防止重复点击重复执行）
- `BOT_LOG_VIEW_MAX_PAGES=100`（可选，日志查看最大页数，超出后请用服务器命令查看全量）
- `TRUST_X_FORWARDED_FOR=0`（默认不信任 XFF）
- `TRUSTED_PROXY_IPS=127.0.0.1,::1`（仅当启用 XFF 信任时生效）
- `NODE_TASK_RUNNING_TIMEOUT=120`（节点任务超时秒数）
- `NODE_TASK_RETENTION_SECONDS=604800`（节点任务历史保留秒数）

## 节点在线监控（Bot）

已支持节点在线状态监控：

- 每个节点在“节点详情”页可单独开启/关闭监控。
- 开启后 bot 周期检查（默认每 `60` 秒）。
- 在线时不推送，掉线时推送一次，恢复后再推送一次。
- 节点在线状态基于 agent 心跳（`/nodes/{node_code}/sync` 自动写入 `last_seen_at`）。

建议配置：

- 在管理服务器 `.env` 中设置：
  - `SUPER_ADMIN_CHAT_IDS=你的chat_id`
  - `BOT_NODE_MONITOR_INTERVAL=60`
  - `BOT_NODE_OFFLINE_THRESHOLD=120`
  - `BOT_LOG_VIEW_COOLDOWN=1`
  - `BOT_LOG_VIEW_MAX_PAGES=100`
- 重启 bot：`systemctl restart sb-bot`

快速验证：

1. 在 bot 中打开节点详情，点击“开启节点监控”
2. 在节点服务器停止 agent：`systemctl stop sb-agent`
3. 等待 1~2 分钟，收到“节点掉线”
4. 启动 agent：`systemctl start sb-agent`
5. 再等待 1 分钟，收到“节点恢复”

## 安全提示

- 若 `ADMIN_CHAT_IDS` 与 `VIEW/OPS/SUPER_ADMIN_CHAT_IDS` 全部为空，任何 Telegram 账号都可使用管理菜单（仅建议测试环境）。
- 建议生产环境至少配置 `SUPER_ADMIN_CHAT_IDS`；推荐按职责拆分：
  - `VIEW_ADMIN_CHAT_IDS`：只读
  - `OPS_ADMIN_CHAT_IDS`：运维写操作
  - `SUPER_ADMIN_CHAT_IDS`：高危操作（删除/迁移导入/远程运维）
- Controller API 支持可选 Bearer 鉴权（全局中间件）：
  - `AUTH_TOKEN` 为空：不校验，保持兼容行为
  - `AUTH_TOKEN` 非空：除 `/health`、`/sub/*`、`/docs`、`/openapi.json`、`/redoc` 外其余接口都必须带 `Authorization: Bearer <AUTH_TOKEN>`
- 支持订阅签名：
  - 配置 `SUB_LINK_SIGN_KEY` 后，可通过 `/admin/sub/sign/{user_code}` 生成带签名 URL
  - 开启 `SUB_LINK_REQUIRE_SIGNATURE=1` 后，`/sub/*` 必须携带 `exp` + `sig`
- 支持轻量限流（默认关闭）：
  - `API_RATE_LIMIT_ENABLED=1` 后，会对高风险管理路径按 IP+路径限流，超限返回 429
- 安全状态检查：
  - `GET /admin/security/status` 可查看当前鉴权、订阅签名、XFF 信任、限流等配置状态与告警提示
  - `GET /admin/security/events?window_seconds=3600&top=5` 可按窗口查看未授权来源统计（适合观察加固后的实时效果）
  - 可选参数：`include_local=1`（临时包含本机测试来源）
  - `POST /admin/security/maintenance_cleanup` 可手动触发一次“过期封禁 + 审计日志保留”清理（bot 安全事件页也可一键触发）
  - `POST /admin/security/auto_block/run` 可手动执行一次“自动封禁阈值检查”（bot 安全事件页也可触发）
  - `GET /admin/node_access/status` 会返回 `whitelist_missing_nodes`，用于检查节点 `agent_ip` 是否已被 controller 端口白名单覆盖
- 自动封禁策略（默认关闭）：
  - `SECURITY_AUTO_BLOCK_ENABLED=1` 开启后，controller 会按窗口阈值自动封禁高频未授权来源
  - 关键参数：`SECURITY_AUTO_BLOCK_WINDOW_SECONDS`、`SECURITY_AUTO_BLOCK_THRESHOLD`、`SECURITY_AUTO_BLOCK_DURATION_SECONDS`、`SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL`
  - 建议先完成 `agent_ip + CONTROLLER_PORT_WHITELIST` 收敛，再启用自动封禁
- 数据库迁移检查：
  - `POST /admin/db/export` 生成逻辑导出快照（json.gz）
  - `POST /admin/db/verify_export` 校验快照并可选对比当前数据库
  - `GET /admin/db/integrity` 查看 SQLite 完整性与外键状态
- 节点同步接口 `/nodes/{node_code}/sync` 已支持来源 IP 白名单：
  - 在节点创建/编辑时设置 `agent_ip`
  - `agent_ip` 已设置时，只有该 IP 才能拉取该节点 sync（其余返回 403）
  - 默认仅信任 TCP 直连源地址，不信任 `X-Forwarded-For`（可通过 `TRUST_X_FORWARDED_FOR=1` + `TRUSTED_PROXY_IPS` 显式开启）
- 建议同时使用防火墙限制管理端口来源（仅节点 IP）：
  - `ufw allow from <节点IP> to any port <CONTROLLER_PORT> proto tcp`
  - `ufw deny <CONTROLLER_PORT>/tcp`
- 公有仓库通常不需要 token。
- 私有仓库建议使用 deploy key 或 PAT（Personal Access Token）。

### AUTH_TOKEN 开关与验证

1. 启用鉴权：在 `.env` 设置非空 `AUTH_TOKEN`，重启 `sb-controller`
2. 关闭鉴权：将 `.env` 里的 `AUTH_TOKEN=` 留空，重启 `sb-controller`

验证命令（启用鉴权时）：

```bash
# 不带 token，应返回 401
curl -i -X POST http://127.0.0.1:8080/admin/backup

# 带 token，应返回 200
curl -i -X POST http://127.0.0.1:8080/admin/backup \
  -H "Authorization: Bearer your_auth_token"
```
