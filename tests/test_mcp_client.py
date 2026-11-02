import json

import httpx
from wechat_ai_plugin.mcp_client import HttpTransport, McpClient


async def test_streamable_http_json_and_sse():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            seen.append(("DELETE", request.headers.get("mcp-session-id")))
            return httpx.Response(200)
        message = json.loads(request.content)
        seen.append((message.get("method"), request.headers.get("mcp-session-id")))
        if "id" not in message:
            return httpx.Response(202)
        if message["method"] == "initialize":
            result = {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}}}
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": message["id"], "result": result},
                headers={"Mcp-Session-Id": "abc"},
            )
        if message["method"] == "tools/list":
            tools = [{"name": "t", "inputSchema": {"type": "object"}}]
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": message["id"], "result": {"tools": tools}}
            )
        body = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": message["id"],
                "result": {"content": [{"type": "text", "text": "ok"}]},
            }
        )
        return httpx.Response(
            200,
            content=f"event: message\ndata: {body}\n\n".encode(),
            headers={"Content-Type": "text/event-stream"},
        )

    transport = HttpTransport("https://mcp.example/mcp", {"Authorization": "Bearer x"}, 10)
    transport.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = McpClient(transport, "remote", 10)
    await client.start()
    assert [t["name"] for t in await client.list_tools()] == ["t"]
    result = await client.call_tool("t", {})
    assert result["content"][0]["text"] == "ok"
    assert transport.protocol == "2025-03-26"
    assert seen[0] == ("initialize", None) and seen[-1] == ("tools/call", "abc")
    await client.close()
    assert seen[-1] == ("DELETE", "abc")
