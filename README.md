# sb-bot-panel

本项目现在包含节点侧一键部署方案：`sing-box + sb-agent + UFW + systemd + ACME 证书检查`，并提供中文数字菜单，体验接近 x-ui/s-ui。

详细手册（零基础可用）：`/Users/cwzs/Documents/sb-bot-panel/docs/零基础部署-测试-使用-排障手册.md`

## 目录新增

- `agent/sb_agent.py`
  - 节点侧 agent（Python 3.9），轮询 controller `/nodes/{node_code}/sync`，生成并热更新 `sing-box` 配置。
- `scripts/install.sh`
  - Debian 12 中文交互式安装/更新脚本（含域名解析检查、防火墙、服务部署、证书检查 timer）。
- `scripts/menu.sh`
  - 中文数字菜单（安装/配置/启停/日志/证书刷新/卸载）。
- `scripts/sb_cert_check.sh`
  - 证书状态检查脚本（供菜单与 timer 调用）。

## 节点一键安装（Debian 12）

### 方式 A：仓库内执行（推荐）

```bash
sudo bash /path/to/sb-bot-panel/scripts/install.sh
```

### 方式 B：一条命令（示例）

```bash
git clone <你的仓库地址> sb-bot-panel && cd sb-bot-panel && sudo bash scripts/install.sh
```

安装过程为中文交互，会询问并写入 `/etc/sb-agent/config.json`：

1. `controller_url`（例如 `http://面板IP:8080`）
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

1. 安装/更新（调用 `install.sh`）
2. 配置（重写 `/etc/sb-agent/config.json`）
3. 启动 sb-agent
4. 停止 sb-agent
5. 重启 sb-agent
6. 查看 sb-agent 状态
7. 查看 sb-agent 日志（tail -f）
8. 重启 sing-box
9. 查看 sing-box 状态与最近日志
10. 证书状态检查
11. 触发证书重新申请/刷新（先备份再清理）
12. 卸载

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
- 可通过菜单 `11` 执行“备份 + 刷新重申领”。

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

菜单里选择 `1) 安装/更新` 即可完成：

- 依赖安装（apt）
- venv 创建与 `pip install -r requirements.txt`
- 写入 `.env`
- 写入并启用 systemd：
  - `sb-controller.service`
  - `sb-bot.service`

### 仅重新配置（改 token/chat id）

```bash
sudo bash scripts/admin/install_admin.sh --configure-only
```

或通过菜单：

- `2) 配置向导（仅写 .env 并重启）`

说明：执行安装/配置向导时，会自动写入完整 `.env` 字段（包括 `BOT_MENU_TTL`、`BOT_NODE_MONITOR_INTERVAL`、`BOT_NODE_OFFLINE_THRESHOLD`），无需手工补字段。

## 管理服务器菜单（中文数字）

```bash
sudo bash scripts/admin/menu_admin.sh
```

菜单项：

1. 安装/更新（git pull + 依赖 + venv + 重启）
2. 配置向导（仅写 `.env` 并重启）
3. 启动 controller
4. 停止 controller
5. 启动 bot
6. 停止 bot
7. 状态查看（controller/bot）
8. 查看日志（controller/bot）
9. 迁移：导出迁移包
10. 迁移：导入迁移包
11. 卸载
12. 退出

## 迁移导出/导入（管理服务器）

默认迁移包目录：`/var/backups/sb-migrate/`

### 旧机导出

- 进入菜单后执行：`9) 迁移：导出迁移包`
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
- 进入菜单执行：`10) 迁移：导入迁移包`
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
- `CONTROLLER_PORT=8080`
- `AUTH_TOKEN=devtoken123`（可空；空值表示关闭 `/admin/*` 鉴权）
- `BOT_TOKEN=xxxxxxxx`（必填）
- `ADMIN_CHAT_IDS=123,456`（可空）
- `MIGRATE_DIR=/var/backups/sb-migrate`
- `BOT_MENU_TTL=60`（可选，bot 菜单按钮自动清理秒数）
- `BOT_NODE_MONITOR_INTERVAL=60`（可选，节点在线检测周期秒数）
- `BOT_NODE_OFFLINE_THRESHOLD=120`（可选，判定离线阈值秒数）

## 节点在线监控（Bot）

已支持节点在线状态监控：

- 每个节点在“节点详情”页可单独开启/关闭监控。
- 开启后 bot 周期检查（默认每 `60` 秒）。
- 在线时不推送，掉线时推送一次，恢复后再推送一次。
- 节点在线状态基于 agent 心跳（`/nodes/{node_code}/sync` 自动写入 `last_seen_at`）。

建议配置：

- 在管理服务器 `.env` 中设置：
  - `ADMIN_CHAT_IDS=你的chat_id`
  - `BOT_NODE_MONITOR_INTERVAL=60`
  - `BOT_NODE_OFFLINE_THRESHOLD=120`
- 重启 bot：`systemctl restart sb-bot`

快速验证：

1. 在 bot 中打开节点详情，点击“开启节点监控”
2. 在节点服务器停止 agent：`systemctl stop sb-agent`
3. 等待 1~2 分钟，收到“节点掉线”
4. 启动 agent：`systemctl start sb-agent`
5. 再等待 1 分钟，收到“节点恢复”

## 安全提示

- `ADMIN_CHAT_IDS` 为空时，任何 Telegram 账号都可看到并操作管理菜单。
- 建议生产环境务必填写 `ADMIN_CHAT_IDS`（逗号分隔），仅允许指定 chat_id 使用 bot 管理功能。
- `/admin/backup`、`/admin/migrate/export` 支持可选 Bearer 鉴权：
  - `AUTH_TOKEN` 为空：不校验，保持兼容行为
  - `AUTH_TOKEN` 非空：必须带 `Authorization: Bearer <AUTH_TOKEN>`
- 建议同时使用防火墙限制管理端口来源（仅可信 IP）。
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
