"""最小同步 ASGI 测试客户端：直接驱动 ASGI 应用，收集响应。

仅用于测试，支持 JSON、二进制 body 与常用 header，不依赖外部服务器。
"""

from __future__ import annotations

import asyncio
import json as json_module
import threading
from typing import Any


class SyncASGIClient:
    def __init__(self, app: Any, base_url: str = "http://obs.local"):
        self.app = app
        self.base_url = base_url
        self._local = threading.local()
        self._all_loops: list[asyncio.AbstractEventLoop] = []
        self._loops_lock = threading.Lock()

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        loop = getattr(self._local, "loop", None)
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._local.loop = loop
            with self._loops_lock:
                self._all_loops.append(loop)
        return loop

    def close_thread_loop(self) -> None:
        """工作线程结束时调用：关闭本线程持有的事件循环。"""
        loop = getattr(self._local, "loop", None)
        if loop is not None and not loop.is_closed():
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            loop.close()
            self._local.loop = None

    def close(self) -> None:
        """统一关闭所有仍存活的事件循环（主线程兜底）。"""
        with self._loops_lock:
            loops = list(self._all_loops)
        for loop in loops:
            if not loop.is_closed():
                try:
                    loop.run_until_complete(loop.shutdown_asyncgens())
                except Exception:
                    pass
                loop.close()

    def request(self, method: str, url: str, *, json: Any = None,
                content: bytes | None = None, params: dict | None = None,
                headers: dict | None = None) -> "SyncResponse":
        if params:
            from urllib.parse import urlencode

            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}{urlencode(params)}"
        hdrs = [(k.lower(), v) for k, v in (headers or {}).items()]
        if json is not None:
            content = json_module.dumps(json).encode("utf-8")
            if not any(k == "content-type" for k, _ in hdrs):
                hdrs.append(("content-type", "application/json"))
        if content is None:
            content = b""
        hdrs = [(k, v if isinstance(v, str) else str(v)) for k, v in hdrs]
        if not any(k == "content-length" for k, _ in hdrs):
            hdrs.append(("content-length", str(len(content))))
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": method.upper(),
            "scheme": "http",
            "path": url.split("?")[0],
            "raw_path": url.split("?")[0].encode(),
            "query_string": url.split("?", 1)[1].encode() if "?" in url else b"",
            "headers": [(k.encode(), v.encode()) for k, v in hdrs],
            "client": ("testclient", 12345),
            "server": ("obs.local", 80),
        }
        body_chunks = [content]

        async def receive():
            if body_chunks:
                return {"type": "http.request", "body": body_chunks.pop(0),
                        "more_body": False}
            return {"type": "http.disconnect"}

        messages: list[dict] = []

        async def send(message: dict):
            messages.append(message)

        loop = self._get_loop()
        loop.run_until_complete(self.app(scope, receive, send))

        status = next((m["status"] for m in messages if m["type"] == "http.response.start"), 500)
        resp_headers = {k.decode(): v.decode()
                        for m in messages if m["type"] == "http.response.start"
                        for k, v in m.get("headers", [])}
        body = b"".join(m.get("body", b"")
                        for m in messages if m["type"] == "http.response.body")
        return SyncResponse(status, resp_headers, body)

    def get(self, url: str, **kw) -> "SyncResponse":
        return self.request("GET", url, **kw)

    def post(self, url: str, **kw) -> "SyncResponse":
        return self.request("POST", url, **kw)

    def put(self, url: str, **kw) -> "SyncResponse":
        return self.request("PUT", url, **kw)

    def delete(self, url: str, **kw) -> "SyncResponse":
        return self.request("DELETE", url, **kw)


class SyncResponse:
    def __init__(self, status_code: int, headers: dict, content: bytes):
        self.status_code = status_code
        self.headers = headers
        self.content = content
        self._json: Any = None

    def json(self) -> Any:
        if self._json is None:
            self._json = json_module.loads(self.content.decode("utf-8"))
        return self._json

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")
