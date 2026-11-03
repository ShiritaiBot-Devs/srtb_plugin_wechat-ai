import asyncio
import io
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
import wechat_ai_plugin as main
from shiritaibot.config import PluginConfig
from shiritaibot.core import Context, PublicError
from shiritaibot.plugin_api import PluginContext
from wechat_ai_plugin.mcp_client import HttpTransport

from .support import Agent, Judge, Summarizer, eventually

OWNER = Context("web:owner", "owner", "agent", True)
GROUP = {"username": "100@chatroom", "display_name": "通知群", "is_chatroom": True}
OTHER = {"username": "300@chatroom", "display_name": "课程群", "is_chatroom": True}
NOW = int(datetime.now(UTC).timestamp())
ROOT = Path(__file__).resolve().parents[1]


def message(number, **extra):
    return {
        "server_id": str(number),
        "local_id": number,
        "sort_seq": number,
        "create_time": NOW + number,
        "text": f"通知 {number}",
        "type": 1,
        "sender_username": "wxid_teacher",
        **extra,
    }


@pytest.fixture
async def wechat(app, monkeypatch, tmp_path):
    box = {
        "rows": [],
        "others": [],
        "calls": [],
        "fail": False,
        "duplicate": False,
        "token": "private-token",
    }

    def respond(request):
        assert request.headers["Authorization"] == "Bearer " + box["token"]
        if request.method == "DELETE":
            return httpx.Response(204)
        rpc = json.loads(request.content)
        if "id" not in rpc:
            return httpx.Response(202)
        method = rpc["method"]
        if method == "initialize":
            result = {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "wechat-ai", "version": "test"},
            }
        elif method == "tools/call":
            name = rpc["params"]["name"]
            args = rpc["params"]["arguments"]
            box["calls"].append((name, args))
            if box["fail"]:
                return httpx.Response(503)
            if name == "get_timestamp":
                data = NOW * 1000 + 500
            elif name == "search_contacts":
                contacts = (
                    [GROUP, {**GROUP, "username": "200@chatroom"}] if box["duplicate"] else [GROUP]
                )
                data = {"status": "ok", "contacts": [*contacts, OTHER]}
            elif name == "get_contact_detail":
                contact = OTHER if args["contact_name"] == OTHER["username"] else GROUP
                data = {"status": "ok", "contact": contact}
            elif name in {"get_recent_messages", "search_text_messages"}:
                other = args["contact_name"] == OTHER["username"]
                data = {
                    "status": "ok",
                    "contact": OTHER if other else GROUP,
                    "messages": box["others"] if other else box["rows"],
                }
            else:
                raise AssertionError("unexpected tool: " + name)
            # FastMCP string returns may appear both in text and a structured result wrapper.
            encoded = json.dumps(data)
            result = {
                "content": [{"type": "text", "text": encoded}],
                "structuredContent": {"result": encoded},
            }
        else:
            raise AssertionError(method)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": rpc["id"], "result": result})

    def transport(url, headers, timeout):
        instance = HttpTransport(url, headers, timeout)
        instance.client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        return instance

    monkeypatch.setattr(main, "HttpTransport", transport)
    directory = tmp_path / "wechat-data"
    directory.mkdir()
    ctx = PluginContext(
        "wechat",
        {"groups": ["通知群"], "token": "ref", "poll": False},
        directory,
        AsyncMock(return_value="private-token"),
        app.proactive.emit,
    )
    plugin = main.Entry(ctx)
    await plugin.start()
    try:
        yield app, plugin, box
    finally:
        await plugin.close()


async def test_first_sync_is_baseline_then_batches_new_messages_and_survives_restart(wechat):
    app, plugin, box = wechat
    box["rows"] = [message(1)]
    assert await plugin.sync_once() == 0
    assert not await app.store.list("event")
    box["rows"] = [message(3), message(2), message(1)]
    assert await plugin.sync_once() == 2
    assert await plugin.sync_once() == 0
    events = await app.store.list("event")
    assert len(events) == 1
    event = events[0]["event"]
    assert event["kind"] == "wechat.messages.received"
    [group] = event["payload"]["groups"]
    assert [m["text"] for m in group["messages"]] == ["通知 2", "通知 3"]
    assert group["group_id"] == GROUP["username"]
    await plugin.close()
    await plugin.start()
    box["rows"] = [message(4), *box["rows"]]
    assert await plugin.sync_once() == 1
    assert len(await app.store.list("event")) == 2
    assert all(name in main.READ_TOOLS for name, _ in box["calls"])


async def test_empty_subscriptions_do_not_connect_and_adding_group_gets_fresh_baseline(wechat):
    app, plugin, box = wechat
    plugin.context.settings["groups"] = []
    await plugin.start()
    assert await plugin.sync_once() == 0
    assert box["calls"] == []
    plugin.context.settings["groups"] = ["通知群"]
    await plugin.start()
    box["rows"] = [message(2)]
    assert await plugin.sync_once() == 0
    assert not await app.store.list("event")


async def test_failed_emit_replays_durable_batch_before_reading_new_rows(wechat, monkeypatch):
    app, plugin, box = wechat
    box["rows"] = [message(1)]
    await plugin.sync_once()
    original_emit = plugin.context.emit

    async def emit_then_fail(event):
        await original_emit(event)
        raise RuntimeError("crashed after ingress")

    box["rows"] = [message(2), message(1)]
    monkeypatch.setattr(plugin.context, "emit", emit_then_fail)
    with pytest.raises(RuntimeError):
        await plugin.sync_once()
    assert json.loads(plugin.state_path.read_text())[main.PENDING]["events"]
    await plugin.close()
    await plugin.start()
    monkeypatch.setattr(plugin.context, "emit", original_emit)
    box["rows"] = [message(3), *box["rows"]]
    assert await plugin.sync_once() == 1
    events = await app.store.list("event")
    assert len(events) == 2
    ids = [e["event"]["payload"]["groups"][0]["messages"][0]["server_id"] for e in events]
    assert sorted(ids) == ["2", "3"]


async def test_backend_errors_leave_cursor_and_recover_without_rebaselining(wechat):
    app, plugin, box = wechat
    box["rows"] = [message(1)]
    await plugin.sync_once()
    before = plugin.state_path.read_text()
    box["fail"] = True
    assert await plugin.sync_once() == 0
    assert await plugin.sync_once() == 0
    assert plugin.state_path.read_text() == before
    failures = [
        e for e in await app.store.list("event") if e["event"]["kind"] == "wechat.sync.failed"
    ]
    assert len(failures) == 1
    assert "private-token" not in json.dumps(failures)
    box["fail"] = False
    box["rows"] = [message(2), message(1)]
    assert await plugin.sync_once() == 1
    assert not plugin.last_error


async def test_non_overlapping_full_window_reports_gap_and_forwards_available_messages(wechat):
    app, plugin, box = wechat
    box["rows"] = [message(1)]
    await plugin.sync_once()
    box["rows"] = [message(i) for i in range(500, 700)]
    assert await plugin.sync_once() == 200
    [event] = [e["event"] for e in await app.store.list("event")]
    assert event["kind"] == "wechat.messages.received"
    assert event["payload"]["gaps"][0]["group_id"] == GROUP["username"]
    assert event["payload"]["groups"][0]["count"] == 200
    assert await plugin.sync_once() == 0
    assert len(await app.store.list("event")) == 1


async def test_ambiguous_name_does_not_collect_a_random_group(wechat):
    _, plugin, box = wechat
    box["duplicate"] = True
    with pytest.raises(PublicError, match="精确群 ID"):
        await plugin.collect("通知群")
    assert not any(name == "get_recent_messages" for name, _ in box["calls"])
    assert not plugin.state


@pytest.mark.parametrize(
    "ctx",
    [
        Context("group:1", "other", "agent"),
        Context("group:1", "owner", "agent", True, True),
        Context("web:owner", "owner", "chat", True),
    ],
)
async def test_owner_group_and_mode_boundaries(wechat, ctx):
    _, plugin, box = wechat
    for name, args in [
        ("status", {}),
        ("find_groups", {"query": "通知"}),
        ("history", {"group": "通知群"}),
        ("search", {"group": "通知群", "query": "考试"}),
    ]:
        with pytest.raises(PublicError, match="仅限主人"):
            await getattr(plugin, name)(ctx, args)
    assert box["calls"] == []


async def test_query_tools_enforce_allowlist_and_never_change_subscription_cursor(wechat):
    _, plugin, box = wechat
    before = plugin.state_path.read_text()
    with pytest.raises(PublicError, match="关注列表"):
        await plugin.history(OWNER, {"group": "其他群"})
    with pytest.raises(PublicError, match="只读"):
        await plugin.rpc("send_text_msg", {})
    box["rows"] = [message(1)]
    assert (await plugin.history(OWNER, {"group": "通知群"}))["messages"] == box["rows"]
    await plugin.search(OWNER, {"group": "通知群", "query": "通知", "start_timestamp": NOW})
    assert plugin.state_path.read_text() == before


async def test_plugin_is_discovered_disabled_and_registers_tools_when_enabled(app):
    meta = next(p for p in await app.plugins.listing() if p["id"] == "wechat")
    assert not meta["enabled"] and meta["defaults"]["groups"] == []
    app.config.value.plugins = [PluginConfig(id="wechat", enabled=True, settings={"poll": False})]
    await app.plugins.reload("wechat")
    result = json.loads(await app.registry.execute(OWNER, "wechat_status", {}))
    assert result["groups"] == []


async def test_new_group_batch_enters_one_proactive_turn(wechat):
    app, plugin, box = wechat
    for task in app.tasks:
        task.cancel()
    await asyncio.gather(*app.tasks, return_exceptions=True)
    app.config.value.proactive.enabled = True
    app.proactive.llm = Judge(importance=2, reason="群通知待整理")
    app.runtime.llm = Agent()
    app.short_term.llm = Summarizer()
    box["rows"] = [message(1)]
    await plugin.sync_once()
    box["rows"] = [message(1), message(2), message(3)]
    assert await plugin.sync_once() == 2
    await app.proactive.tick()

    async def completed():
        return [turn for turn in await app.store.list("turn") if turn["status"] == "completed"]

    assert len(await eventually(completed)) == 1
    assert app.proactive.llm.calls == 1
    context = app.runtime.llm.inputs[0][0][0]["content"]
    assert "通知 2" in context and "通知 3" in context


async def test_invalid_message_does_not_advance_baseline_or_cursor(wechat):
    _, plugin, box = wechat
    box["rows"] = [message(1)]
    await plugin.sync_once()
    before = plugin.state_path.read_text()
    box["rows"] = [message(2, create_time="not-a-timestamp")]
    with pytest.raises(ValueError):
        await plugin.collect("通知群")
    assert plugin.state_path.read_text() == before


async def test_new_message_in_baseline_second_is_not_lost(wechat):
    _, plugin, box = wechat
    box["rows"] = [message(1, create_time=NOW)]
    assert await plugin.sync_once() == 0
    box["rows"].append(message(2, create_time=NOW))
    assert await plugin.sync_once() == 1


async def test_repository_zip_installs_and_loads_without_host_changes(app):
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as bundle:
        for name in ["plugin.toml", "main.py", "mcp_client.py", "README.md", "LICENSE", "NOTICE"]:
            bundle.write(ROOT / name, "srtb_plugin_wechat-ai-main/" + name)
    await app.plugins.install("wechat_archive", archive.getvalue())
    cfg = next(p for p in app.config.value.plugins if p.id == "wechat_archive")
    assert not cfg.enabled
    cfg.enabled = True
    cfg.settings["poll"] = False
    await app.plugins.reload("wechat_archive")
    result = json.loads(await app.registry.execute(OWNER, "wechat_status", {}))
    assert result["groups"] == []


async def test_messages_from_all_groups_merge_into_one_event_per_poll(wechat):
    app, plugin, box = wechat
    plugin.context.settings["groups"] = ["通知群", OTHER["username"]]
    await plugin.start()
    box["rows"], box["others"] = [message(1)], [message(11)]
    assert await plugin.sync_once() == 0
    box["rows"] = [message(2), message(1)]
    box["others"] = [message(13), message(12), message(11)]
    assert await plugin.sync_once() == 3
    [event] = [e["event"] for e in await app.store.list("event")]
    assert event["kind"] == "wechat.messages.received"
    assert event["payload"]["count"] == 3
    groups = {g["group_id"]: [m["text"] for m in g["messages"]] for g in event["payload"]["groups"]}
    assert groups == {GROUP["username"]: ["通知 2"], OTHER["username"]: ["通知 12", "通知 13"]}
    box["others"] = [message(14), *box["others"]]
    assert await plugin.sync_once() == 1
    latest = [e["event"] for e in await app.store.list("event")]
    assert len(latest) == 2


async def test_legacy_per_group_outbox_is_replayed(wechat):
    app, plugin, box = wechat
    box["rows"] = [message(1)]
    await plugin.sync_once()
    state = json.loads(plugin.state_path.read_text())
    legacy = main.ExternalEvent(
        source="wechat", kind="wechat.messages.received", dedup_key="legacy", title="旧批次"
    )
    state["通知群"]["pending"] = {
        "events": [legacy.model_dump(mode="json")],
        "next": {**state["通知群"], "seen": state["通知群"]["seen"] + ["server:2"]},
    }
    plugin.state_path.write_text(json.dumps(state))
    await plugin.close()
    await plugin.start()
    box["rows"] = [message(2), message(1)]
    assert await plugin.sync_once() == 0
    assert [e["event"]["dedup_key"] for e in await app.store.list("event")] == ["legacy"]
    assert main.PENDING not in json.loads(plugin.state_path.read_text())
