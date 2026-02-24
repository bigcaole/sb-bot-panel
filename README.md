# sb-bot-panel

本项目现在包含节点侧一键部署方案：`sing-box + sb-agent + UFW + systemd + ACME 证书检查`，并提供中文数字菜单，体验接近 x-ui/s-ui。

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
