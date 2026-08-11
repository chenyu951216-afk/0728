from __future__ import annotations

import asyncio
import importlib
import logging
import os
import traceback
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

LOG = logging.getLogger('eth-adaptive-bootstrap')
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s %(levelname)s %(message)s')


def _resolve_port() -> int:
    """Resolve Zeabur's port without allowing a literal ${WEB_PORT} to crash import."""
    for raw in (os.getenv('PORT', ''), os.getenv('WEB_PORT', ''), '8080'):
        try:
            value = int(str(raw).strip())
            if 1 <= value <= 65535:
                os.environ['PORT'] = str(value)
                return value
        except (TypeError, ValueError):
            continue
    os.environ['PORT'] = '8080'
    return 8080


PORT = _resolve_port()
PRODUCTION_APP: Any | None = None
PRODUCTION_MODULE: Any | None = None
PRODUCTION_LIFESPAN: Any | None = None
PRODUCTION_LOAD_TASK: asyncio.Task | None = None
STARTUP_ERROR_TYPE: str | None = None
STARTUP_ERROR_TEXT: str | None = None
STARTUP_STATUS = 'BOOTING'


async def _load_production() -> None:
    """Load the heavyweight runtime only after the bootstrap server has bound its port.

    Import is sent to a worker thread so SQLite migrations/provenance recovery cannot
    block the ASGI event loop. Once imported, enter the production FastAPI lifespan in
    the main event loop so all existing startup/background workers still run exactly
    as they would when uvicorn hosted the production app directly.
    """
    global PRODUCTION_APP, PRODUCTION_MODULE, PRODUCTION_LIFESPAN
    global STARTUP_ERROR_TYPE, STARTUP_ERROR_TEXT, STARTUP_STATUS
    STARTUP_STATUS = 'LOADING_PRODUCTION_RUNTIME'
    try:
        production = await asyncio.to_thread(importlib.import_module, 'server_v19')
        prod_app = production.app

        lifespan_cm = prod_app.router.lifespan_context(prod_app)
        await lifespan_cm.__aenter__()

        PRODUCTION_MODULE = production
        PRODUCTION_LIFESPAN = lifespan_cm
        PRODUCTION_APP = prod_app
        STARTUP_STATUS = 'PRODUCTION_READY'
        LOG.info('production runtime ready after bootstrap bind: version=%s', getattr(prod_app, 'version', 'unknown'))
    except Exception as exc:
        STARTUP_ERROR_TYPE = type(exc).__name__
        STARTUP_ERROR_TEXT = f'{type(exc).__name__}: {exc}'
        STARTUP_STATUS = 'PRODUCTION_FAILED'
        LOG.error('production runtime failed during background initialization: %s', STARTUP_ERROR_TEXT)
        LOG.error('%s', traceback.format_exc())


@asynccontextmanager
async def _bootstrap_lifespan(_: FastAPI):
    global PRODUCTION_LOAD_TASK, PRODUCTION_LIFESPAN, STARTUP_STATUS
    # Schedule, do not await: yielding immediately is what lets uvicorn finish startup
    # and expose the HTTP port before the heavyweight research runtime is ready.
    PRODUCTION_LOAD_TASK = asyncio.create_task(_load_production(), name='load-production-runtime')
    yield

    if PRODUCTION_LOAD_TASK and not PRODUCTION_LOAD_TASK.done():
        PRODUCTION_LOAD_TASK.cancel()
        try:
            await PRODUCTION_LOAD_TASK
        except asyncio.CancelledError:
            pass
    if PRODUCTION_LIFESPAN is not None:
        try:
            await PRODUCTION_LIFESPAN.__aexit__(None, None, None)
        except Exception:
            LOG.exception('production lifespan shutdown failed')
    STARTUP_STATUS = 'STOPPED'


bootstrap = FastAPI(
    title='ETH Adaptive AI bootstrap',
    version='9.0.2-bootstrap',
    lifespan=_bootstrap_lifespan,
)


@bootstrap.get('/healthz')
def healthz() -> JSONResponse:
    ready = PRODUCTION_APP is not None
    return JSONResponse(
        status_code=200 if ready else 503,
        content={
            'ok': ready,
            'mode': 'PRODUCTION' if ready else 'FAIL_CLOSED_BOOTSTRAP',
            'startup_status': STARTUP_STATUS,
            'startup_error_type': STARTUP_ERROR_TYPE,
            'trading_enabled': ready,
        },
    )


@bootstrap.get('/', response_class=HTMLResponse)
def bootstrap_dashboard() -> str:
    status = STARTUP_STATUS
    err = STARTUP_ERROR_TEXT or '—'
    return f'''<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>ETH Adaptive AI</title><style>body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#071426;color:#e8f0ff;margin:0;padding:28px}}.card{{max-width:760px;margin:40px auto;padding:24px;border:1px solid #29466d;border-radius:20px;background:#0b1b31}}h1{{margin-top:0}}.warn{{color:#ffd36e}}.bad{{color:#ff7187}}code{{word-break:break-word}}</style></head><body><div class="card"><h1>ETH Adaptive AI 9.0.2</h1><h2 class="{'bad' if STARTUP_ERROR_TYPE else 'warn'}">HTTP 已啟動 · 正式 Runtime {status}</h2><p>Zeabur Port 已先完成監聽。正式研究、認證與交易 Runtime 正在背景初始化；完成前所有新訊號與交易皆 fail-closed。</p><p>錯誤：<code>{err}</code></p><p>既有 SQLite / Volume 不會因 bootstrap 被清除。</p></div></body></html>'''


@bootstrap.api_route('/{path:path}', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD'])
async def bootstrap_fallback(path: str, request: Request) -> JSONResponse:
    _ = path, request
    return JSONResponse(
        status_code=503,
        content={
            'ok': False,
            'mode': 'FAIL_CLOSED_BOOTSTRAP',
            'startup_status': STARTUP_STATUS,
            'startup_error_type': STARTUP_ERROR_TYPE,
            'trading_enabled': False,
        },
    )


class DynamicProductionApp:
    """ASGI switch: bootstrap owns lifespan; HTTP/WebSocket move to production when ready."""

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'lifespan':
            await bootstrap(scope, receive, send)
            return
        target = PRODUCTION_APP if PRODUCTION_APP is not None else bootstrap
        await target(scope, receive, send)


app = DynamicProductionApp()


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=PORT)
