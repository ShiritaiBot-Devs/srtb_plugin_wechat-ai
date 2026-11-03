# srtb_plugin_wechat-ai

ShiritaiBot 独立微信通知群插件。通过 WebUI 从本仓库安装，不需要修改主仓库。

ShiritaiBot 的 `wechat` 插件通过 [ChisBread/wechat-ai](https://github.com/ChisBread/wechat-ai)
提供的 Streamable HTTP MCP 读取个人微信中的指定群聊，将新消息送入主动性引擎。
ClawBot / iLink 机器人接入不提供个人好友和群聊历史，不能替代此数据源。

```text
官方 Linux 微信（本人扫码登录）
    → wechat-ai（本地数据库 → 只读 MCP 工具）
    → ShiritaiBot 微信插件（关注群、基线、去重、轮询）
    → ExternalEvent → Filter → Proactive Loop → 主人通知 / 整理
```

## 准备微信数据源

插件不安装或启动微信，不接管登录，也不直接读取或解密微信数据库。
下载桌面微信安装包并不等于已经具备 MCP 数据源。

1. 按 [上游部署说明](https://github.com/ChisBread/wechat-ai#快速开始) 部署 `wechat-ai`。
   推荐在独立容器中运行其官方 Linux 微信桌面，避免与日常使用的桌面实例混淆。
2. 配置随机的 `WECHAT_AI_MCP_TOKEN`，以及上游管理台、远程桌面的登录认证。
   保持 `WECHAT_AI_MCP_WRITE_ENABLED=false`、`WECHAT_AI_MCP_ADMIN_ENABLED=false`。
3. 上游 Compose 默认发布桌面端口 3100 / 3101 和 MCP 端口 8100，部署时将宿主绑定改为
   `127.0.0.1`，例如 `127.0.0.1:8100:8000`；如需远程操作，使用已有 VPN 或端口转发。
4. 打开上游桌面，本人扫码登录。管理台确认数据库可用、联系人已加载，目标群有消息记录。
   上游通过读取微信进程内存发现数据库密钥，其当前 Compose 使用 `privileged: true`；
   插件本身只需要访问 MCP，不需要特权。

微信必须保持在线；手机旧历史不会自动全部同步到 Linux 客户端。只有该客户端本地已有的记录可查。
同一个后端固定使用同一个微信账号；切换账号应使用独立的后端地址和插件数据目录，重新建立基线。

## 配置 ShiritaiBot

下载本仓库 ZIP，在 WebUI「插件 → 安装插件」上传，插件 ID 填 `wechat`，安装后默认停用。
私有仓库请在已登录 GitHub 的浏览器中选择 Code → Download ZIP，或使用有权限的本机 Git 克隆后打包。
宿主的 GitHub 地址安装只支持公开仓库；仓库公开后，也可以在安装入口填写：

```text
https://github.com/ShiritaiBot-Devs/srtb_plugin_wechat-ai
```

运行时只依赖 ShiritaiBot 的公共 `plugin_api` 和宿主已有的 `httpx`，无需修改主仓库或安装额外运行时依赖。
本插件自带 Streamable HTTP MCP 客户端，不依赖宿主内部 MCP 模块。

安装后找到「微信通知群」，配置：

- 地址：`http://127.0.0.1:8100/mcp`。
- Token：填上游的 `WECHAT_AI_MCP_TOKEN` 原文，不带 `Bearer` 前缀。保存时自动加密。
- 关注群：每行一个精确群名或 `…@chatroom` 群 ID；留空不采集。
- 自动收取：开启，默认每 30 秒检查一次。

先保持关注列表为空，配置 Token 并启用插件，再在主人私聊 / WebUI Agent 中说：

> 用 wechat_groups 查找名字含「通知」的微信群，列出群名和群 ID。

将要关注的群 ID 填入配置，保存。精确群名也可用，但遇到同名群时插件会拒绝随机选择。
成功解析后绑定群 ID，后续群改名不会切换订阅对象。

配置示例（Token 应在 WebUI 输入，不要把凭据提交到 Git）：

```toml
[[plugins]]
id = "wechat"
enabled = true
[plugins.settings]
url = "http://127.0.0.1:8100/mcp"
token = "secret:由WebUI自动生成"
groups = ["123456789@chatroom"]
poll = true
poll_seconds = 30
timeout = 30
```

插件自己建立 MCP 连接，不需要再在「MCP 服务」重复注册上游所有工具。
启用主动性引擎后，通知群新消息进入已有的 Filter 与 Proactive Loop；
引擎可以判断重要程度、整理消息、按已有权限创建待办，并通过现有主人渠道提醒。
插件的定时采集任务不调用模型，也不自行通知主人。

## 行为与边界

- **只收新消息**：每个群首次成功读取时建立基线，已有消息不产生事件。
  重启或重载沿用原基线及去重状态，可补收停机期间仍在最近消息窗口内的消息。
  配置移除并保存后再次添加，或更换后端 URL，建立新基线。
- **批量事件**：每轮轮询把所有关注群的新消息合并为一个 `wechat.messages.received` 事件，
  `payload.groups` 按群列出新消息，`payload.count` 为总条数。
  事件包含群 ID、群名、发送者、消息 ID、时间、文字或非文本类型，不包含数据库路径与媒体内容。
  微信内容作为不可信资料交给核心处理，不作为工具指令执行。
- **持久化与恢复**：状态保存在 `<home>/plugin_data/wechat/state_<后端地址摘要>.json`，
  权限 0600。先写入待投递批次，再提交事件，成功后更新进度。失败保留批次，
  重启重试沿用相同事件去重键，不因后续到达新消息改变旧批次。
- **上游窗口限制**：`get_recent_messages` 最多 200 条，没有分页或增量游标。
  如果满额窗口与上次窗口不重叠，插件在同一事件的 `payload.gaps` 中指出可能漏收的群，仍提交可读取的新消息；
  若该轮没有新消息，则单独生成 `wechat.sync.gap`。
  这不是无损归档方案；高流量群、长时间离线、旧消息重新排序等情况不能保证无遗漏。
- **离线与错误**：读取失败不推进该群进度；下一轮重新连接，其他群继续同步。
  每轮所有失败合并为一个 `wechat.sync.failed` 诊断事件，相同失败组合每天最多一个。
- **查询工具**：`wechat_status` 查看基线和最近成功时间；`wechat_groups(query)` 查找群；
  `wechat_history(group, limit?)` 查看关注群最近消息；
  `wechat_search(group, query, start_timestamp?, end_timestamp?, limit?)` 检索本地文本历史。
  显式历史查询不受自动采集基线限制，也不改变收取进度。
- **访问范围**：所有查询仅允许主人私聊、WebUI 或后台 Agent；拒绝群聊和非主人调用。
  不提供发送、删除消息、退群、群管理等工具。媒体自动下载与解密未接入。

接口契约依据上游提交 `638e98e51fd373f11d62965a41bccb603d63954c` 的
[MCP 实现](https://github.com/ChisBread/wechat-ai/blob/638e98e51fd373f11d62965a41bccb603d63954c/src/wechat_ai_bot/mcp/app.py)
核对。协议测试使用模拟 HTTP MCP 服务；真实账号需要本人扫码后完成端到端联调。

## 开发与测试

需要 Python 3.12+，以及包含插件 API v1 与主动性引擎的 ShiritaiBot。已在宿主提交
`ad9af867fb159a1191350cdbbed6f1521b8b44cc` 验证。测试使用临时数据目录与模拟 MCP，不访问真实微信账号。

在虚拟环境安装宿主与开发依赖后运行：

```bash
python -m pip install -e ../ShiritaiBot
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m ruff check .
```

测试会把插件复制到临时宿主插件目录，验证独立安装、群列表、首次基线、重启去重、
投递恢复、消息窗口缺口、访问权限以及主动性引擎衔接。

## 许可证与来源

AGPL-3.0-or-later，见 [LICENSE](LICENSE)。MCP 客户端与部分测试夹具适配自
[ShiritaiBot](https://github.com/ShiritaiBot-Devs/ShiritaiBot)，来源见 [NOTICE](NOTICE)。
