"""Synthetic model replies for host integration tests; adapted from ShiritaiBot."""

import asyncio
import json

from shiritaibot.core import Reply


class Judge:
    def __init__(self, **decision):
        self.decision, self.calls, self.systems = decision, 0, []

    async def complete(self, route, system, messages, tools, **kwargs):
        assert route == "light" and tools == []
        self.calls += 1
        self.systems.append(system)
        return Reply(text=json.dumps(self.decision, ensure_ascii=False))

    async def decide(self, *args, **kwargs):
        return None


class Agent:
    def __init__(self, notify=True):
        self.notify, self.inputs, self.systems = notify, [], []

    async def complete(self, route, system, messages, tools, **kwargs):
        assert route == "agent"
        self.inputs.append((list(messages), tools))
        self.systems.append(system)
        if not any(m["role"] == "tool" for m in messages):
            return Reply(
                text="我先查一下日程。",
                calls=[
                    {
                        "id": "check",
                        "type": "function",
                        "function": {"name": "routine_list", "arguments": "{}"},
                    }
                ],
            )
        return Reply(
            text=json.dumps(
                {
                    "notify": self.notify,
                    "message": "作业周五截止，已查过日程。",
                    "reason": "核查完成",
                },
                ensure_ascii=False,
            )
        )


class Summarizer:
    def __init__(self):
        self.inputs = []

    async def complete(self, route, system, messages, tools, **kwargs):
        assert route == "light" and not tools
        self.inputs.append(messages)
        return Reply(text="收到作业提醒，核查日程后结束本轮。")


async def eventually(check):
    async with asyncio.timeout(3):
        while not (result := await check()):  # noqa: ASYNC110 -- bounded polling of persisted state
            await asyncio.sleep(0.01)
        return result
