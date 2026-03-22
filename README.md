# sb-bot-panel

本项目现在包含节点侧一键部署方案：`sing-box + sb-agent + UFW(可选) + systemd/OpenRC + ACME 证书检查`，并提供中文数字菜单，体验接近 x-ui/s-ui。

详细手册（零基础可用）：`docs/零基础部署-测试-使用-排障手册.md`
推荐先读（最少切换上线流程）：`docs/一条龙落地SOP.md`
菜单逐项说明与示例：见零基础手册第「菜单功能说明与示例」章节（HTTPS 状态为“强判定”模式）
节点证书检查同样为“强判定摘要”输出，日志需手动执行命令查看。
节点自检（菜单 24）会自动修复常见权限问题（例如 sing-box 日志写入失败）。
若 sing-box 以 DynamicUser 运行，自检会自动放宽日志目录权限以避免 `permission denied`。
sing-box 未运行时，菜单会提示使用自检进行自动修复。
自检还会提示配置校验失败与端口冲突，并给出具体占用进程信息。
sing-box 日志默认输出到 `/var/lib/sing-box/sing-box.log`（避免系统包的 DynamicUser 权限问题）。
自检还会自动写入 systemd override（ReadWritePaths/StateDirectory），进一步避免日志权限问题。
若 sync 失败（404/403/401），自检会提示先修复 controller_url/鉴权，TUIC 相关检查会暂时跳过以避免误判。
节点侧配置完成后，会提示立即执行一次自检修复（默认推荐执行）。
自检会自动尝试修复 fail2ban 未运行与证书定时检查未启用（systemd 环境）。
若 sing-box 报 `certmagic ... permission denied`，自检会自动修复证书目录权限并重启。
若 sing-box 仍未恢复运行，自检会输出最新错误日志，便于继续排查。
自检的修复结果采用分级提示：✅已修复 / ⚠️需要人工 / ❌致命错误。
自检在尝试修复 sing-box 后会等待稳定运行再给结论；若恢复后仍退出，会提示“恢复后立即退出”并附最新错误。
若 ACME 触发限流（HTTP 429），自检会提示最早重试时间并建议等待或更换 TUIC 域名。
若 tuic_domain 已清空但配置仍残留 TUIC 入站，自检会自动清理并尝试恢复 sing-box。
节点支持通过配置开关彻底禁用 TUIC 或 VLESS（enable_tuic/enable_vless）。
节点菜单的组件管理支持卸载 sing-box/fail2ban，子菜单支持输入 q 返回主菜单；管理端子菜单同样支持 q 返回。

## 目录新增

- `agent/sb_agent.py`
  - 节点侧 agent（Python 3.11+），轮询 controller `/nodes/{node_code}/sync`，生成并热更新 `sing-box` 配置。
  - 当 UFW 已启用时，agent 会自动放行 TUIC 相关 UDP 端口（含监听端口与端口池范围）。
- `scripts/install.sh`
  - Debian/Ubuntu/Alpine 中文交互式安装/更新脚本（含域名解析检查、防火墙、服务部署、证书检查 timer；Alpine 使用 OpenRC）。
- `scripts/menu.sh`
  - 中文数字菜单（安装/配置/启停/日志/证书刷新/卸载/SSH 与 fail2ban 安全加固）。
- `scripts/sb_cert_check.sh`
  - 证书状态检查脚本（供菜单与 timer 调用）。
- `scripts/ops_snapshot.sh`
  - 节点运维快照导出脚本（汇总服务状态、联通检查、防火墙与配置摘要）。
- `scripts/ai_context_export.sh`
  - 节点 AI 诊断包导出脚本（固定格式，可直接粘贴给任意 AI）。
- `scripts/admin/ai_context_export.sh`
  - 管理端 AI 诊断包导出脚本（固定格式，可直接粘贴给任意 AI）。

## 新手部署总览（先看这里）

下面只给你“最短可用路径”，并明确每条命令在哪台服务器执行。

### 第 0 步：准备两台机器

- `管理服务器`：运行 `controller + bot`（可选 Caddy HTTPS）
- `节点服务器`：运行 `sing-box + sb-agent`

### 第 1 步：在管理服务器执行（先做）

执行位置：`管理服务器` 终端

```bash
cd /root
git clone <你的仓库地址> sb-bot-panel
cd /root/sb-bot-panel
sudo bash scripts/admin/menu_admin.sh
```

进入菜单后先选：`24) 安装/重装`

安装成功后可用以下命令随时打开管理菜单：

```bash
sb-admin
```

### 第 2 步：在节点服务器执行（每台节点都要做一次）

执行位置：`节点服务器` 终端

```bash
cd /root
git clone <你的仓库地址> sb-bot-panel
cd /root/sb-bot-panel
sudo bash scripts/install.sh
```

安装成功后可用以下命令随时打开节点菜单：

```bash
sb-node
```

### 第 3 步：最短联通验证

执行位置：`管理服务器` 终端

```bash
cd /root/sb-bot-panel
set -a; . ./.env; set +a
curl -fsSL "http://127.0.0.1:${CONTROLLER_PORT:-8080}/health"
```

若输出 `{"ok":true}`，表示管理端已启动。  
节点是否连通可在 bot 的“管理服务器 -> 状态查看”里看节点在线状态。

### 常见误区（务必看）

- `scripts/admin/*` 只在 `管理服务器` 执行
- `scripts/install.sh` / `scripts/menu.sh` 只在 `节点服务器` 执行
- 节点不会自动“发现管理端”，必须在节点安装向导里填写 `controller_url + 节点鉴权 token + node_code`

---

## 节点服务器：一键安装（Debian/Ubuntu/Alpine）

执行位置：`节点服务器` 终端

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
2. `node_code`（例如 `N1`）
3. `auth_token`（建议填写 `NODE_AUTH_TOKEN`）
4. `enable_vless`（是否启用 VLESS+Reality，默认 `true`）
5. `enable_tuic`（是否启用 TUIC，默认 `true`）
6. `tuic_domain`（留空则不启用 TUIC）
5. `acme_email`（启用 TUIC 时必填）
6. `tuic_listen_port`（默认 `24443`，建议高位 UDP 端口）
7. `poll_interval`（默认 `15` 秒）

补充：

- 快速配置模式也支持直接启用/修改 `tuic_domain + acme_email + tuic_listen_port`，不需要再切换到高级模式。
- Alpine 需确保已安装 `bash`（首次执行脚本前可先 `apk add --no-cache bash curl git jq`）。
- Alpine 服务查看可用：`rc-service sb-agent status`、`rc-service sing-box status`。

### 节点最低配置建议

- **最低可运行**：`1C / 0.5G 内存 / 5G 硬盘`（轻量使用可行）
- **更稳妥**：`1C / 1G+ 内存 / 10G+ 硬盘`（并发/多用户更稳定）
- 说明：极低配机型在高并发或大量连接下可能出现抖动，建议按实际用户规模预留余量。
- 节点侧 TUIC 证书由 `sing-box` 内置 ACME 处理，不依赖 Caddy；不建议在节点安装 Caddy 抢占 `443` 端口。

若安装/配置失败，默认自动导出 AI 诊断包到 `/tmp/sb-install-node-ai-context-on-fail-*.md`（可用 `INSTALL_NODE_EXPORT_AI_CONTEXT_ON_FAIL=0` 关闭）。

## 节点服务器菜单管理

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
11. fail2ban 管理（安装/卸载）
12. 查看 fail2ban 状态与封禁列表
13. 解封 fail2ban 封禁 IP
   - systemd 系统会自动使用 `backend=systemd`（避免缺少 auth.log 导致启动失败）
   - 若 fail2ban 启动失败，会自动切换 backend 并重试一次
   - 若仍失败，会自动选择可用日志文件并创建缺失日志文件
14. 生成 SSH 密钥（ed25519）
15. SSH 安全状态总览（只读）
16. 一键安全修复（半自动）
17. 启用 SSH 仅密钥登录（禁用密码）
18. 恢复 SSH 密码登录（应急）
【系统级操作（谨慎）】
19. 节点运维快照（导出关键状态）
20. AI诊断包导出（可粘贴给任意AI）
21. 更新同步（保留原配置，自动 `git pull --ff-only origin main`，无交互）
22. 深度卸载
23. sing-box 管理（安装/更新/卸载）
24. 退出

说明：

- 菜单默认是“简化视图”（仅常用项）。
- 输入 `A` 可切到“高级视图”（显示全部功能）。
- 在高级视图输入 `B` 可切回简化视图。

- 首次安装后，后续更新建议直接用菜单 `21`，不会重复询问端口/域名等参数。
- 菜单 `21` 若拉取失败会直接中止并提示，不会继续执行后续更新步骤。
- 需要改参数时用菜单 `1`（支持“参数单项修改”，列表按 `中文说明｜参数名` 显示）。
- 若后续仅缺失或需要升级 sing-box，可直接执行菜单 `23`（交互安装/更新）。
- 默认部署会设置开机自启：`sb-agent` 与 `sing-box`（即使当前配置待下发，`sing-box` 也会先写入开机自启）。
- 安全建议：先用菜单 `14` 生成并部署公钥，再用菜单 `15` 做只读安全检查；可先执行菜单 `16` 做半自动修复，最后再执行菜单 `17` 启用仅密钥登录。
- 排障建议先执行菜单 `19` 导出节点运维快照，再贴快照内容定位问题。
- 若需要跨 AI 对话排障，建议直接执行菜单 `20` 导出标准 AI 诊断包并整体粘贴。
- 若你在本地电脑生成 SSH 密钥，请把公钥上传到节点服务器：
  - `root` 用户路径：`/root/.ssh/authorized_keys`
  - 普通用户路径：`/home/<用户名>/.ssh/authorized_keys`
  - 建议权限：`.ssh` 目录 `700`，`authorized_keys` 文件 `600`
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
- 节点侧不需要 Caddy 参与 TUIC 证书签发；若 Caddy 占用 `443`，可能影响 sing-box。
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

- 先看：`/usr/local/bin/sb-cert-check.sh`（已简化为“核心原因 + 建议操作”，不再输出一大段日志）
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
- `ufw allow 24443/udp`（或你的 TUIC 端口）
- `ufw status`

### 5) Caddy 启动失败：`:443 address already in use`

- 这通常不是证书问题，而是 443 端口已被其他进程占用。
- 排查：
  - `ss -lntp | grep ':443'`
  - `systemctl status nginx apache2 caddy --no-pager`
- 处理：
  - 临时不启 HTTPS：将 `ENABLE_HTTPS=0`，先保证 controller/bot 正常迁移；
  - 需要 HTTPS：停掉占用 443 的服务后再 `systemctl restart caddy`。

## 管理服务器：一键安装（controller + bot）

执行位置：`管理服务器` 终端

管理服务器侧新增脚本目录：`scripts/admin/`，用于一键安装、配置、服务管理和迁移。

### 新装（推荐从菜单进入）

```bash
git clone <你的仓库地址> sb-bot-panel && cd sb-bot-panel && sudo bash scripts/admin/menu_admin.sh
```

菜单里选择 `24) 安装/重装` 即可完成：

- 依赖安装（apt）
- venv 创建与 `pip install -r requirements.txt`
- 写入 `.env`
- 可选启用 Caddy（自动申请 HTTPS 证书并自动续期）
- 写入并启用 systemd：
  - `sb-controller.service`
  - `sb-bot.service`
- 若安装/重装失败，默认自动导出 AI 诊断包到 `/tmp/sb-install-admin-ai-context-on-fail-*.md`
  - 可用 `INSTALL_ADMIN_EXPORT_AI_CONTEXT_ON_FAIL=0` 关闭

说明：
- 菜单 `22) 组件自检与自动修复`：会检查 controller/bot/caddy，缺失时自动修复。
- 菜单 `23) 部署参数自检与修复向导`：按“必需未配置/可选未配置/配置错误”分类并循环修复（含开机自启状态检查与修复）。
- 菜单 `24) 安装/重装` 会进入交互配置，适合首装或重装。
- 菜单 `25) 更新` 会强制执行 `git pull --ff-only origin main` 并执行 `--reuse-config`，默认复用现有 `.env` 配置，不会重复询问端口/域名/token/chat_id。
- 若 `git pull` 失败（如本地有冲突改动），更新会直接中止并提示处理，不会继续走后续重装步骤。
- 需要改参数时使用菜单 `1) 配置（快速默认 / 高级变量向导）`。
- 默认部署会设置开机自启：`sb-controller`；若 `BOT_TOKEN` 已配置则 `sb-bot`；启用 HTTPS 时 `caddy`。

### 仅重新配置（改 token/chat id）

```bash
sudo bash scripts/admin/install_admin.sh --configure-only
```

快速配置（推荐默认值，最少提问）：

```bash
sudo bash scripts/admin/install_admin.sh --configure-quick
```

或通过菜单 `1) 配置` 进入后选择：

- `1) 快速配置（推荐默认值，最少提问）`
- `2) 高级变量设置向导（逐项说明，全部可调）`
- `3) 查看当前关键配置（只读，含建议）`
- `4) 参数单项修改（点选一项直接改；改一项无需重跑全向导）`

单项修改列表会显示为 `中文说明｜参数名 = 当前值`，便于直接按中文定位参数再修改。
布尔项（如 `enable_tuic`/`enable_vless`）的 `false` 会正常显示，不会被误判为“未设置”；若回显异常，菜单会自动识别并强制纠正显示。

说明：执行安装/配置向导时，会自动写入完整 `.env` 字段（包括 `BOT_MENU_TTL`、`BOT_NODE_MONITOR_INTERVAL`、`BOT_NODE_OFFLINE_THRESHOLD`、`BOT_NODE_TIME_SYNC_INTERVAL`、`BOT_LOG_VIEW_COOLDOWN`、`BOT_LOG_VIEW_MAX_PAGES`），无需手工补字段。
并且 URL 字段支持省略协议（`http://` / `https://`），脚本会自动补全。
配置完成后，脚本会自动调用 `POST /admin/nodes/sync_agent_defaults`，下发节点默认参数（`auth_token`=节点鉴权主 token、`poll_interval`、可选 `controller_url`），降低管理端与节点端参数不一致风险。
快速配置模式下，若未设置 `SECURITY_BLOCK_PROTECTED_IPS`，脚本会自动填入当前 SSH 来源 IP 作为封禁保护白名单，减少误封风险。

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

- 更新（后台执行 `install_admin.sh --reuse-config`，仅作用于当前管理服务器）
- 配置向导（远程修改 `.env` 后后台重载）
- 一键验收自检（语法检查 + unittest + API 冒烟）
- 启动/停止 controller
- 状态查看（controller/bot/caddy）
- 查看日志（controller/bot/caddy）
- 日志归档（打包最近日志到服务器本地，支持保留策略）
- 运维审计（仅看 `ops.*` 脚本运维事件）
- HTTPS 证书状态 / 刷新
- 迁移导出 / 迁移导入（导入为交互向导，逐项给出推荐值）
- 操作日志（`/admin/audit`，查看最近审计记录）
- 访问安全（整合节点来源 IP 白名单状态 + 全局安全配置状态）
- 订阅安全预设（签名+限流 / 仅签名 / 开放测试）
- 节点时间同步（一键下发 `sync_time`，使节点系统时间对齐管理服务器）
- 任务幂等概览（24h 去重命中率，辅助观察重复下发）
- 紧急停止（先预演影响范围，再确认禁用全部 active 用户）

在 Telegram 的 `限速管理` 菜单中已支持：
- 设置限速（用户级）
- 切换限速模式（用户级 `tc/off`，`off` 时节点侧不再对该用户执行限速）

补充：

- 可单独执行 bot 分发回归检查：

```bash
sudo bash scripts/admin/bot_regression_check.sh
```

- 日志归档脚本支持以下可选环境变量（写入 `.env`）：
  - `LOG_ARCHIVE_WINDOW_HOURS`（默认 24）
  - `LOG_ARCHIVE_RETENTION_COUNT`（默认 30）
  - `LOG_ARCHIVE_DIR`（默认 `/var/backups/sb-controller/logs`）
- 已知长期技术债（含 VLESS 按用户限速未解决）见：`docs/待办与技术债.md`

## Bot 远程管理（节点服务器）

在 Telegram 的 `节点与线路 -> 节点远程运维` 中可按节点下发任务：

- 同步更新（节点执行 `scripts/install.sh --sync-only`，仅作用于你当前选中的该节点）
- 重启 sing-box
- 查看 sing-box / sb-agent 状态
- 查看 sing-box / sb-agent 日志（通过任务回传）
- 修改节点参数（写入 `/etc/sb-agent/config.json`，如 `poll_interval`、`tuic_domain`、`tuic_listen_port` 等）
- 任务生命周期：controller 自动处理超时任务（默认 120 秒）、失败重试（按任务重试次数）与历史清理（默认保留 7 天）
- 节点详情支持“同步预览（排障）”：调用 `/admin/nodes/{node_code}/sync_preview`，可在管理端直接查看该节点将收到的下发内容（不受 `agent_ip` 限制）
- 通过 bot 新建节点后，会自动尝试为该节点下发初始化 `config_set`（`auth_token`/`poll_interval`/可用时 `controller_url`），减少手工同步步骤。

新机上线最短路径（推荐）：

1. 在新节点执行一次安装命令（见“节点一键安装”）
2. 在 bot 中进入 `节点与线路 -> 节点远程运维`
3. 后续使用受控任务完成更新/重启/日志查看/参数调整（无需每次手动 SSH）

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
1. 配置（快速默认 / 高级变量向导）
2. 启动 controller
3. 停止 controller
4. 启动 bot
5. 停止 bot
6. 状态查看（controller/bot + 节点连接统计）
7. 查看日志（controller/bot/归档/运维审计）
8. HTTPS 证书状态（Caddy）
9. HTTPS 证书刷新（重载 Caddy）
10. 迁移：导出迁移包
11. 迁移：导入迁移包
12. 一键验收自检（语法/单测/API）
13. 数据库一致性校验（迁移前建议）
14. 节点同步（默认参数 / Token / 时间 / 对齐参数，支持自动验证下发）
15. 安全加固向导（token 轮换 + 8080 收敛）
16. Token 工具（收敛 token / 拆分迁移 / 完整显示）
17. 手动安全清理（过期封禁 + 审计日志）
18. SSH 安全状态总览（只读）
19. SSH 一键安全修复（半自动）
20. 运维快照（导出关键状态）
21. AI诊断包导出（可粘贴给任意AI）
【系统级操作（谨慎）】
22. 组件自检与自动修复（controller/bot/caddy）
23. 部署参数自检与修复向导（循环到通过）
24. 安装/重装（交互配置 + 依赖 + venv + 重启）
25. 更新（git pull + 复用现有配置 + 重启）
26. 深度卸载
27. 退出

说明：菜单 `14` 支持五类操作：  
1) 默认参数同步（`/admin/nodes/sync_agent_defaults`）  
2) 节点 Token 同步（`/admin/auth/sync_node_tokens`）  
3) 节点时间同步（`/admin/nodes/sync_time`）  
4) 节点部署对齐参数查看（输出 `controller_url/auth_token/poll_interval` 建议值）  
5) 新节点上线一条龙向导（减少管理端/节点端来回切换）  
用于节点参数或 token 快速对齐。  
菜单 `6` 会附带输出节点连接统计（含 `last_seen` 视角），用于判断新节点是否已接入管理端。  
菜单 `16 -> 3` 支持查看完整 token（有二次确认，高风险操作）。  
管理端排障时，可使用 `GET /admin/nodes/{node_code}/sync_preview` 预览该节点下发内容（不受 `agent_ip` 限制，且不刷新 `last_seen_at`）。  
执行 `安全加固向导` 并轮换 token 后，脚本会自动触发一次节点 token 同步（默认包含禁用节点并强制新建），降低节点鉴权不同步风险。  
同样地，执行 `Token 收敛` 后也会自动触发一次节点 token 同步任务，进一步降低节点掉线风险。  
Token 优先级、拆分迁移、收敛规则与严格验收，统一见下文 `Token 模型与生命周期（管理/节点）` 章节。
执行 `安全加固向导` 时，脚本会提示并可自动把“当前 SSH 来源 IP”加入 `CONTROLLER_PORT_WHITELIST`、`ADMIN_API_WHITELIST` 与 `SECURITY_BLOCK_PROTECTED_IPS`，减少误封与误锁风险。

菜单视图说明：

- 管理菜单默认是“简化视图”（常用项）。
- 输入 `A` 可切换到“高级视图”（全功能）。
- 在高级视图输入 `B` 可切回简化视图。

统一验收命令（管理服务器）：

```bash
bash /root/sb-bot-panel/scripts/admin/smoke_test.sh --require-api
# 严格要求通过 token 拆分验收（推荐）
bash /root/sb-bot-panel/scripts/admin/smoke_test.sh --require-api --require-token-split
# 严格要求启用管理接口来源白名单（推荐）
bash /root/sb-bot-panel/scripts/admin/smoke_test.sh --require-api --require-admin-api-whitelist
```

说明补充：

- 当验收失败时，脚本默认会自动导出 AI 诊断包到 `/tmp/sb-admin-ai-context-on-fail-*.md`
- 可通过环境变量关闭：`SMOKE_EXPORT_AI_CONTEXT_ON_FAIL=0`

运维快照导出（管理服务器，排障建议先执行）：

```bash
bash /root/sb-bot-panel/scripts/admin/ops_snapshot.sh
```

默认输出目录：`/var/backups/sb-controller/ops-snapshots/`
也可在 Telegram bot 中进入 `管理服务器 -> 证书与备份 -> 运维快照` 一键生成（返回 `/tmp/...` 路径与审计记录）。
同菜单还支持 `诊断打包`（一次生成“运维快照 + AI诊断包”）。

AI 诊断包导出（管理服务器，可直接粘贴给任意 AI）：

```bash
bash /root/sb-bot-panel/scripts/admin/ai_context_export.sh
```

默认输出目录：`/var/backups/sb-controller/ai-context/`
也可在 Telegram bot 中进入 `管理服务器 -> 证书与备份 -> AI诊断包` 一键生成（返回 `/tmp/...` 路径与审计记录）。

AI 诊断包导出（节点服务器，可直接粘贴给任意 AI）：

```bash
bash /root/sb-bot-panel/scripts/ai_context_export.sh
```

默认输出目录：`/var/backups/sb-agent/ai-context/`

说明：

- 默认会读取 `.env` 中的 `CONTROLLER_PORT` 与鉴权 token（`ADMIN_AUTH_TOKEN` / `NODE_AUTH_TOKEN` / `AUTH_TOKEN`）
- 检查项：Python 语法、`tests/` 单元测试、controller API 鉴权冒烟
- token 选择优先级、拆分判定规则见下文 `Token 模型与生命周期（管理/节点）`
- token 拆分检查：默认告警；启用 `--require-token-split`（或环境变量 `SMOKE_REQUIRE_TOKEN_SPLIT=1`）时，若仍为兼容模式会直接判失败
- 管理接口来源白名单检查：默认告警；启用 `--require-admin-api-whitelist`（或环境变量 `SMOKE_REQUIRE_ADMIN_API_WHITELIST=1`）时，若未启用 `ADMIN_API_WHITELIST` 会直接判失败
- API 冒烟检查遇到 `429` 会按 `Retry-After` 自动重试（默认最多 3 次、单次最多等待 20 秒；可通过 `SMOKE_RATE_LIMIT_RETRY_ATTEMPTS` / `SMOKE_RATE_LIMIT_RETRY_MAX_WAIT_SECONDS` 调整）
- 访问收敛检查：会读取 `/admin/node_access/status`，默认仅告警；若设置 `SMOKE_REQUIRE_NODE_LOCK=1`，当存在“启用但未锁定来源IP”的节点会直接判失败
- 退出码：`0=通过`，`10=代码检查失败`，`20=API检查失败`，`30=代码+API均失败`

数据库迁移前一致性校验（管理服务器）：

```bash
bash /root/sb-bot-panel/scripts/admin/db_consistency_check.sh
```

说明：

- 会依次执行：`/admin/db/export` -> `/admin/db/verify_export`（与当前库比对）-> `/admin/db/integrity`
- 任何一步失败会返回非 0 退出码，避免带问题做迁移
- 失败时默认自动导出 AI 诊断包到 `/tmp/sb-db-check-ai-context-on-fail-*.md`（可用 `DB_CHECK_EXPORT_AI_CONTEXT_ON_FAIL=0` 关闭）

安全加固向导失败自动诊断包：

- 脚本：`bash /root/sb-bot-panel/scripts/admin/harden_security.sh`
- 失败时默认自动导出到 `/tmp/sb-harden-security-ai-context-on-fail-*.md`
- 可用 `HARDEN_EXPORT_AI_CONTEXT_ON_FAIL=0` 关闭

## GitHub 自动验收（CI）

仓库已新增：`.github/workflows/ci.yml`

- 触发时机：`push main`、`pull request`
- 检查内容：
  - 文档同步检查（若改动功能代码，必须同步更新 `README.md` 与 `docs/零基础部署-测试-使用-排障手册.md`）
  - 管理/节点脚本 `bash -n` 语法检查
  - `scripts/admin/smoke_test.sh --skip-api`（语法 + 单元测试）
- Python 版本：`3.11`、`3.12`

本地也可手动执行同样检查：

```bash
bash scripts/admin/check_docs_sync.sh
```

## Python 与系统兼容

- 生产基线：`Python 3.11`（推荐）
- 兼容版本：`Python 3.12`
- 不再建议：`Python 3.9`（已退役，缺少安全支持）

安装脚本已内置 Python 3.11 适配逻辑：

- Debian 12：直接使用系统 Python 3.11
- Debian 13：系统 Python 通常 >=3.11，直接可用
- Debian 11：自动尝试 `bullseye-backports` 安装 Python 3.11
- Ubuntu（20.04/22.04/24.04）：自动尝试系统包，必要时回退 `deadsnakes` 安装 Python 3.11
- Alpine：使用系统仓库 `apk add python3 py3-virtualenv py3-pip`（要求 Python >=3.11）

## 迁移导出/导入（管理服务器）

默认迁移包目录：`/var/backups/sb-migrate/`

默认保留策略：

- 控制器备份：保留最近 `30` 个（`BACKUP_RETENTION_COUNT`）
- 迁移包：保留最近 `20` 个（`MIGRATE_RETENTION_COUNT`）

### 旧机导出

- 进入菜单后执行：`10) 迁移：导出迁移包`
- 脚本会停止 `sb-controller` 与 `sb-bot`，导出完成后可选是否自动拉起
- 可选包含 `项目代码快照`（推荐开启；新机可免 `git clone` 直接导入）
- 生成文件名示例：`sb-migrate-YYYYmmdd-HHMMSS.tar.gz`
- 导出内容：
  - `data/`（必须）
  - `.env`（必须）
  - `scripts/`（建议）
  - `project-code.tar.gz`（可选；项目代码快照，排除 `.git/venv/data/.env`）
  - `sb-controller.service` / `sb-bot.service`（如果存在则附带）

### 传输迁移包

```bash
scp root@旧IP:/var/backups/sb-migrate/sb-migrate-xxxx.tar.gz root@新IP:/root/
```

### 新机导入

- 新机两种方式：
  - 若迁移包包含 `project-code.tar.gz`：无需先 `git clone`，可直接导入
  - 若迁移包不含代码快照：需先准备项目目录（建议先 `git clone`）
- 进入菜单执行：`11) 迁移：导入迁移包`
- 导入脚本会：
  - 备份旧项目目录到 `/var/backups/sb-migrate/restore-backup-*.tar.gz`
  - 恢复 `data/.env/scripts`（如包含代码快照会先恢复代码）
  - 进入参数修正向导（每项带推荐值）
  - 自动安装依赖、重建 venv、重写 systemd、重启服务
  - 自检 `/health` 与 bot 服务状态，并输出“迁移后必须检查参数”清单

## .env 配置项（管理服务器）

参考：`.env.example`

- `CONTROLLER_URL=http://127.0.0.1:8080`
- `CONTROLLER_PUBLIC_URL=http://your-public-ip:8080`（可选，对外地址）
- `PANEL_BASE_URL=https://panel.example.com`（建议填域名，bot 订阅链接将使用该地址）
- `ENABLE_HTTPS=1`（1=启用 Caddy 自动证书，0=关闭）
- `HTTPS_DOMAIN=panel.example.com`（启用 HTTPS 时填写域名）
- `HTTPS_ACME_EMAIL=admin@example.com`（可选，证书账号邮箱）
- `CONTROLLER_PORT=8080`
- `CONTROLLER_PORT_WHITELIST=`（可选；逗号分隔 IP/CIDR，用于限制 8080 访问来源）
- `ADMIN_API_WHITELIST=`（可选；逗号分隔 IP/CIDR，用于限制“管理接口”来源，应用层二次限制）
  - 留空时会回退使用 `CONTROLLER_PORT_WHITELIST`（建议后续显式配置）
- `SECURITY_BLOCK_PROTECTED_IPS=`（可选；逗号分隔 IP/CIDR，manual/auto 封禁会跳过这些来源；无效项会在安全状态中告警）
- `ADMIN_AUTH_TOKEN=随机长串`（管理接口 token）
- `NODE_AUTH_TOKEN=随机长串`（节点接口 token）
- `AUTH_TOKEN=随机长串`（兼容兜底；建议留空）
  - 安装脚本默认会自动生成并写入 `ADMIN_AUTH_TOKEN` 与 `NODE_AUTH_TOKEN`（默认拆分）
  - token 轮换/拆分迁移/收敛规则见下文 `Token 模型与生命周期（管理/节点）`
- `SUB_LINK_SIGN_KEY=`（可选；设置后可生成带签名订阅链接）
- `SUB_LINK_REQUIRE_SIGNATURE=0`（可选；1=强制订阅必须带签名）
- `SUB_LINK_DEFAULT_TTL_SECONDS=604800`（可选；签名默认有效期）
- `API_RATE_LIMIT_ENABLED=1`（可选；controller 轻量限流开关，默认开启）
- `API_RATE_LIMIT_WINDOW_SECONDS=60`（可选；限流窗口）
- `API_RATE_LIMIT_MAX_REQUESTS=120`（可选；单个 IP+路径窗口内请求上限）
- `API_RATE_LIMIT_TRUSTED_LOOPBACK_ACTORS=sb-bot,sb-admin`（可选；本机回环 + 已鉴权 + `X-Actor` 命中名单时可跳过限流，避免 bot 连点误触发 429）
- `ADMIN_OVERVIEW_CACHE_TTL_SECONDS=5`（可选；`/admin/overview` 缓存秒数，0=关闭缓存）
- `ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS=5`（可选；`/admin/security/status` 缓存秒数，0=关闭缓存）
- `ADMIN_SECURITY_EVENTS_CACHE_TTL_SECONDS=5`（可选；`/admin/security/events` 缓存秒数，0=关闭缓存）
- `RATE_LIMIT_STATE_MAX_KEYS=20000`（可选；轻量限流内存状态键数上限，防止极端扫描导致内存持续增长）
- `SECURITY_EVENTS_EXCLUDE_LOCAL=1`（可选；安全统计默认过滤本机测试来源）
- `API_DOCS_ENABLED=0`（可选；是否启用 `/docs` `/redoc` `/openapi.json`，生产建议保持 0）
- `UNAUTHORIZED_AUDIT_SAMPLE_SECONDS=30`（可选；未授权审计采样窗口，防止被扫描时审计日志爆涨）
- `UNAUTHORIZED_AUDIT_STATE_MAX_KEYS=20000`（可选；未授权采样内存状态键数上限，防止极端扫描导致内存持续增长）
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
- `BOT_NODE_TIME_SYNC_INTERVAL=86400`（可选，节点自动时间对齐周期；0=关闭）
- `CONTROLLER_HTTP_TIMEOUT=10`（可选，bot 调 controller 超时秒数）
- `BOT_ACTOR_LABEL=sb-bot`（可选，bot 调 controller 时用于审计的操作者标识）
- `BOT_LOG_VIEW_COOLDOWN=1`（可选，日志翻页冷却秒数，防止触发 Telegram 限流）
- `BOT_MUTATION_COOLDOWN=1`（可选，写操作按钮防抖秒数，防止重复点击重复执行）
- `BOT_LOG_VIEW_MAX_PAGES=100`（可选，日志查看最大页数，超出后请用服务器命令查看全量）
- `BOT_OPS_AUDIT_WINDOW_SECONDS=604800`（可选，bot“运维审计”默认时间窗口，单位秒）
- `TRUST_X_FORWARDED_FOR=0`（默认不信任 XFF）
- `TRUSTED_PROXY_IPS=127.0.0.1,::1`（仅当启用 XFF 信任时生效）
- `NODE_TASK_RUNNING_TIMEOUT=120`（节点任务超时秒数）
- `NODE_TASK_RETENTION_SECONDS=604800`（节点任务历史保留秒数）

## Token 模型与生命周期（管理/节点）

### 1) 三个字段与优先级

- 管理接口鉴权优先级：`ADMIN_AUTH_TOKEN -> AUTH_TOKEN -> NODE_AUTH_TOKEN`
- 节点接口鉴权优先级：`NODE_AUTH_TOKEN -> AUTH_TOKEN -> ADMIN_AUTH_TOKEN`
- 仅当 `ADMIN_AUTH_TOKEN` 与 `NODE_AUTH_TOKEN` 都已设置为单值，且不再依赖 `AUTH_TOKEN` 回退时，才算“已拆分完成”。

### 2) 新装默认（推荐）

- 安装/配置脚本默认会生成并写入：`ADMIN_AUTH_TOKEN`、`NODE_AUTH_TOKEN`
- `AUTH_TOKEN` 建议保持空（仅用于历史兼容过渡）

### 3) 过渡轮换（不中断）

- 支持多值过渡：`new_token,old_token`
- 管理 token 轮换示例：`ADMIN_AUTH_TOKEN=new_admin,old_admin`
- 节点 token 轮换示例：`NODE_AUTH_TOKEN=new_node,old_node`
- 完成节点同步后再收敛到单值

### 4) 从旧版 `AUTH_TOKEN` 一键拆分迁移

```bash
bash /root/sb-bot-panel/scripts/admin/auth_token_split_migrate.sh --yes
```

- 自动将兼容模式迁移到 `ADMIN_AUTH_TOKEN + NODE_AUTH_TOKEN`
- 自动重启服务并触发节点 token 同步
- 失败时默认导出诊断包：`/tmp/sb-token-split-ai-context-on-fail-*.md`

### 5) token 收敛（结束过渡）

```bash
bash /root/sb-bot-panel/scripts/admin/auth_token_collapse.sh --yes
```

- 同时处理 `AUTH_TOKEN` / `ADMIN_AUTH_TOKEN` / `NODE_AUTH_TOKEN` 的多值过渡
- 即使已是单值（No-Op），也会尝试执行一次节点 token 对齐同步
- 失败时默认导出诊断包：`/tmp/sb-token-collapse-ai-context-on-fail-*.md`

### 6) 严格验收（推荐）

```bash
# 常规验收
bash /root/sb-bot-panel/scripts/admin/smoke_test.sh --require-api

# 严格要求已完成 token 拆分
bash /root/sb-bot-panel/scripts/admin/smoke_test.sh --require-api --require-token-split
```

- `--require-token-split` 下，若仍处于兼容模式会直接失败
- 验收脚本会自动选择可用管理/节点 token 并给出来源告警

## 节点在线监控（Bot）

已支持节点在线状态监控：

- 每个节点在“节点详情”页可单独开启/关闭监控。
- 开启后 bot 周期检查（默认每 `60` 秒）。
- 在线时不推送，掉线时推送一次，恢复后再推送一次。
- 节点在线状态基于 agent 心跳（`/nodes/{node_code}/sync` 自动写入 `last_seen_at`）。
- bot 还支持“节点自动时间对齐”任务（默认每 24 小时一次，下发 `sync_time`）。

建议配置：

- 在管理服务器 `.env` 中设置：
  - `SUPER_ADMIN_CHAT_IDS=你的chat_id`
  - `BOT_NODE_MONITOR_INTERVAL=60`
  - `BOT_NODE_OFFLINE_THRESHOLD=120`
  - `BOT_NODE_TIME_SYNC_INTERVAL=86400`
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
- Controller API 支持可选 Bearer 鉴权（全局中间件），详细优先级与迁移策略见上文 `Token 模型与生命周期（管理/节点）`。
- API 文档入口默认关闭：
  - `API_DOCS_ENABLED=0` 时，`/docs`、`/redoc`、`/openapi.json` 返回 404
  - 仅在排障场景临时开启：`API_DOCS_ENABLED=1`
- 支持订阅签名：
  - 配置 `SUB_LINK_SIGN_KEY` 后，可通过 `/admin/sub/sign/{user_code}` 生成带签名 URL
  - 开启 `SUB_LINK_REQUIRE_SIGNATURE=1` 后，`/sub/*` 必须携带 `exp` + `sig`
- 支持轻量限流（默认开启）：
  - `API_RATE_LIMIT_ENABLED=1` 后，会对高风险管理路径按 `IP+路径+鉴权桶(auth/anon/open)` 限流，超限返回 429
  - 对 `127.0.0.1/::1` 且已鉴权并携带受信 `X-Actor`（默认 `sb-bot,sb-admin`）的调用，默认放宽限流，避免 bot 高频交互误伤
- 节点任务接口的管理侧返回已对敏感字段做脱敏（如 `auth_token`、`Authorization: Bearer ...` 显示为 `***`），避免误泄露；节点拉取任务时仍使用真实值。
- 安全状态检查：
  - `GET /admin/security/status` 可查看当前鉴权、订阅签名、XFF 信任、限流等配置状态与告警提示
  - 额外包含 `admin_auth_source/node_auth_source/auth_token_split_active`，可快速判断是否仍在兼容模式
  - 包含 `weak_auth_token_count/weak_auth_token_risks`（不泄露 token 内容）用于提示弱 token 风险
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
- 可选启用管理接口应用层来源白名单（即使端口策略误配也可二次拦截）：
  - `ADMIN_API_WHITELIST=运维IP1,运维网段CIDR`
  - 启用后，非白名单来源访问管理接口会返回 `403 source_not_allowed`（本机 `127.0.0.1/::1` 默认允许）
- 公有仓库通常不需要 token。
- 私有仓库建议使用 deploy key 或 PAT（Personal Access Token）。

### 鉴权 Token 开关与验证

1. 启用管理鉴权：设置非空 `ADMIN_AUTH_TOKEN`，重启 `sb-controller`
2. 启用节点鉴权：设置非空 `NODE_AUTH_TOKEN`，重启 `sb-controller`
3. 关闭鉴权：将 `ADMIN_AUTH_TOKEN/NODE_AUTH_TOKEN/AUTH_TOKEN` 全部留空后重启
4. 历史环境从兼容模式迁移，建议按上文 `Token 模型与生命周期（管理/节点）` 先拆分、再收敛

验证命令（启用鉴权时）：

```bash
# 不带 token，应返回 401
curl -i -X POST http://127.0.0.1:8080/admin/backup

# 带管理 token，应返回 200
curl -i -X POST http://127.0.0.1:8080/admin/backup \
  -H "Authorization: Bearer your_admin_token"
```
