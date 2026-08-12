from __future__ import annotations

import asyncio
import importlib
import logging
import os
import threading
import traceback
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

LOG = logging.getLogger('eth-adaptive-bootstrap')
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s %(levelname)s %(message)s')


def _resolve_port() -> int:
    raw = os.getenv('PORT', '8080')
    try:
        value = int(str(raw).strip())
        if 1 <= value <= 65535:
            return value
    except (TypeError, ValueError):
        pass
    os.environ['PORT'] = '8080'
    return 8080


PORT = _resolve_port()
PRODUCTION_APP: Any | None = None
PRODUCTION_LIFESPAN: Any | None = None
STARTUP_STATUS = 'BOOTING'
STARTUP_ERROR_TYPE: str | None = None
STARTUP_ERROR_TEXT: str | None = None


def _prepare_92_generation(production: Any) -> None:
    """One-time migration: make old certification due under the 9.2 learner.

    Historical/raw/derived samples are preserved. Only the previous certification
    timestamp is invalidated once so the new sealed-holdout evolution actually runs
    immediately after upgrade instead of inheriting a stale fixed-pair rejection result.
    """
    try:
        core = production.core
        version = str(production.signal_evolution.VERSION)
        marker_key = 'v21_historical_evolution_migration'
        if core.get_state(marker_key, '') == version:
            return
        state = core.get_state('v18_final_system_state', None)
        state = dict(state) if isinstance(state, dict) else {}
        state['last_cert_completed_at'] = 0
        state['status'] = 'READY_FOR_SIGNAL_CERTIFICATION'
        state['reason'] = '9.2 lineage-aware Signal evolution requires one fresh certification pass after schema-7 replay'
        core.set_state('v18_final_system_state', state)
        core.set_state(marker_key, version)
        LOG.info('9.2 evolution migration armed: preserved raw history; invalidated certification timestamp only')
    except Exception:
        LOG.exception('9.2 evolution migration preparation failed; production remains fail-closed')


def _import_production_blocking() -> tuple[Any, Any]:
    production = importlib.import_module('server_v19')
    _prepare_92_generation(production)
    return production, production.app


async def _load_production() -> None:
    global PRODUCTION_APP, PRODUCTION_LIFESPAN
    global STARTUP_STATUS, STARTUP_ERROR_TYPE, STARTUP_ERROR_TEXT
    STARTUP_STATUS = 'LOADING_PRODUCTION_RUNTIME'
    try:
        _, prod_app = await asyncio.to_thread(_import_production_blocking)
        lifespan_cm = prod_app.router.lifespan_context(prod_app)
        await lifespan_cm.__aenter__()
        PRODUCTION_LIFESPAN = lifespan_cm
        PRODUCTION_APP = prod_app
        STARTUP_STATUS = 'PRODUCTION_READY'
        LOG.info('production runtime ready version=%s', getattr(prod_app, 'version', 'unknown'))
    except BaseException as exc:
        STARTUP_ERROR_TYPE = type(exc).__name__
        STARTUP_ERROR_TEXT = f'{type(exc).__name__}: {exc}'
        STARTUP_STATUS = 'PRODUCTION_FAILED'
        LOG.error('production runtime initialization failed: %s', STARTUP_ERROR_TEXT)
        LOG.error('%s', traceback.format_exc())


@asynccontextmanager
async def _bootstrap_lifespan(_: FastAPI):
    LOG.info(
        'BOOTSTRAP_BIND pid=%s thread=%s host=0.0.0.0 port=%s PORT_ENV=%r',
        os.getpid(), threading.current_thread().name, PORT, os.getenv('PORT'),
    )
    task = asyncio.create_task(_load_production(), name='load-production-runtime')
    yield
    if not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    if PRODUCTION_LIFESPAN is not None:
        try:
            await PRODUCTION_LIFESPAN.__aexit__(None, None, None)
        except Exception:
            LOG.exception('production lifespan shutdown failed')


bootstrap = FastAPI(title='ETH Adaptive AI bootstrap', version='9.2.0-bootstrap', lifespan=_bootstrap_lifespan)


@bootstrap.get('/healthz')
def healthz() -> JSONResponse:
    return JSONResponse(status_code=200, content={
        'ok': True, 'alive': True, 'startup_status': STARTUP_STATUS,
        'startup_error_type': STARTUP_ERROR_TYPE, 'port': PORT,
    })


@bootstrap.get('/readyz')
def readyz() -> JSONResponse:
    ready = PRODUCTION_APP is not None
    return JSONResponse(status_code=200 if ready else 503, content={
        'ok': ready, 'ready': ready, 'startup_status': STARTUP_STATUS,
        'startup_error_type': STARTUP_ERROR_TYPE, 'port': PORT,
    })


@bootstrap.get('/', response_class=HTMLResponse)
def bootstrap_dashboard() -> str:
    err = STARTUP_ERROR_TEXT or '—'
    return f'''<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>ETH Adaptive AI</title><style>body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#071426;color:#e8f0ff;margin:0;padding:28px}}.card{{max-width:760px;margin:40px auto;padding:24px;border:1px solid #29466d;border-radius:20px;background:#0b1b31}}h1{{margin-top:0}}.warn{{color:#ffd36e}}.bad{{color:#ff7187}}code{{word-break:break-word}}</style></head><body><div class="card"><h1>ETH Adaptive AI 9.2.0</h1><h2 class="{'bad' if STARTUP_ERROR_TYPE else 'warn'}">Bootstrap HTTP ONLINE · {STARTUP_STATUS}</h2><p>Listening: <code>0.0.0.0:{PORT}</code></p><p>正式 Runtime 完成前，新訊號與交易維持 fail-closed。</p><p>錯誤：<code>{err}</code></p></div></body></html>'''


@bootstrap.api_route('/{path:path}', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD'])
async def bootstrap_fallback(path: str, request: Request) -> JSONResponse:
    _ = path, request
    return JSONResponse(status_code=503, content={
        'ok': False, 'mode': 'FAIL_CLOSED_BOOTSTRAP', 'startup_status': STARTUP_STATUS,
        'startup_error_type': STARTUP_ERROR_TYPE, 'port': PORT,
    })


class DynamicProductionApp:
    async def __call__(self, scope, receive, send):
        if scope['type'] == 'lifespan':
            await bootstrap(scope, receive, send)
            return
        target = PRODUCTION_APP if PRODUCTION_APP is not None else bootstrap
        await target(scope, receive, send)


app = DynamicProductionApp()


if __name__ == '__main__':
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s', PORT)
    uvicorn.run(app, host='0.0.0.0', port=PORT, access_log=True, log_level='info')
