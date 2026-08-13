from __future__ import annotations

import asyncio
import importlib
import logging
import os
import threading
import traceback
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

# Bound native math-library parallelism before production imports numpy/sklearn.
# This changes resource usage only; it does not reduce data, features, candidates,
# validation folds, or holdout rules.
os.environ.setdefault('OMP_NUM_THREADS', '1')
os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
os.environ.setdefault('MKL_NUM_THREADS', '1')
os.environ.setdefault('NUMEXPR_NUM_THREADS', '1')
os.environ.setdefault('VECLIB_MAXIMUM_THREADS', '1')
os.environ.setdefault('MALLOC_ARENA_MAX', '2')

try:
    import fcntl
except Exception:  # pragma: no cover - Zeabur production is Linux
    fcntl = None  # type: ignore[assignment]

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import runtime_identity

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
_BOOTSTRAP_LEADER_FH: Any | None = None
_BOOTSTRAP_ROLE = 'UNKNOWN'


def _claim_bootstrap_role() -> str:
    """Fence import-time DB/preflight work during rolling replica overlap.

    v26 already fences lifespan workers, but server_v19 has an import-time provenance
    preflight thread. Acquire a second, earlier lock before importing production so
    only one overlapping Zeabur instance can run that mutating/heavy preflight.
    This is a safety fence, not a replacement for configuring the service to 1 replica.
    """
    global _BOOTSTRAP_LEADER_FH, _BOOTSTRAP_ROLE
    if _BOOTSTRAP_ROLE != 'UNKNOWN':
        return _BOOTSTRAP_ROLE
    if fcntl is None:
        _BOOTSTRAP_ROLE = 'LEADER_NO_FCNTL'
        return _BOOTSTRAP_ROLE
    db_path = Path(os.getenv('DATABASE_PATH', '/data/eth_adaptive.db'))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = Path(str(db_path) + '.bootstrap-leader.lock')
    fh = open(lock_path, 'a+', encoding='utf-8')
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        _BOOTSTRAP_ROLE = 'FOLLOWER_READ_ONLY'
        LOG.warning('bootstrap replica follower: import-time DB preflight will be suppressed; lock=%s', lock_path)
        return _BOOTSTRAP_ROLE
    except Exception as exc:
        fh.close()
        # Fail closed rather than assume leadership when the shared-volume lock cannot
        # be proven. The web bootstrap still comes online for diagnosis.
        _BOOTSTRAP_ROLE = 'FOLLOWER_LOCK_ERROR'
        LOG.error('bootstrap leader lock failed: %s', exc)
        return _BOOTSTRAP_ROLE
    _BOOTSTRAP_LEADER_FH = fh
    _BOOTSTRAP_ROLE = 'LEADER'
    try:
        fh.seek(0); fh.truncate(0); fh.write(f'{os.getpid()}\n'); fh.flush()
    except Exception:
        pass
    LOG.info('bootstrap replica leader acquired lock=%s', lock_path)
    return _BOOTSTRAP_ROLE


def _prepare_100_generation(production: Any) -> None:
    """Record the current migration without overriding the replay safety gate."""
    if _BOOTSTRAP_ROLE.startswith('FOLLOWER'):
        return
    try:
        core = production.core
        version = str(production.signal_evolution.VERSION)
        marker_key = 'v22_hierarchical_evolution_migration'
        if core.get_state(marker_key, '') == version:
            return
        core.set_state(marker_key, version)
        LOG.info('%s causal full-history migration recorded; replay safety gate remains authoritative', runtime_identity.RUNTIME_VERSION)
    except Exception:
        LOG.exception('%s evolution migration preparation failed; production remains fail-closed', runtime_identity.RUNTIME_VERSION)


def _import_production_blocking() -> tuple[Any, Any]:
    role = _claim_bootstrap_role()
    os.environ['ETH_RUNTIME_BOOTSTRAP_ROLE'] = role
    production = importlib.import_module('server_v19')
    # Install after server_v19 has composed the final fixed-horizon/certification
    # authority, but before its lifespan starts any background workers.
    transition = importlib.import_module('v26_replay_transition_stability')
    transition.install(production.core)
    production.core.state['bootstrap_replica_role'] = {
        'role': role,
        'pid': os.getpid(),
        'import_preflight_allowed': not role.startswith('FOLLOWER'),
    }
    _prepare_100_generation(production)
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
        LOG.info('production runtime ready version=%s role=%s', getattr(prod_app, 'version', 'unknown'), _BOOTSTRAP_ROLE)
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


bootstrap = FastAPI(title='ETH Adaptive AI bootstrap', version=f'{runtime_identity.API_VERSION}-bootstrap', lifespan=_bootstrap_lifespan)


@bootstrap.get('/healthz')
def healthz() -> JSONResponse:
    return JSONResponse(status_code=200, content={
        'ok': True, 'alive': True, 'startup_status': STARTUP_STATUS,
        'startup_error_type': STARTUP_ERROR_TYPE, 'port': PORT,
        'bootstrap_replica_role': _BOOTSTRAP_ROLE,
    })


@bootstrap.get('/readyz')
def readyz() -> JSONResponse:
    ready = PRODUCTION_APP is not None
    return JSONResponse(status_code=200 if ready else 503, content={
        'ok': ready, 'ready': ready, 'startup_status': STARTUP_STATUS,
        'startup_error_type': STARTUP_ERROR_TYPE, 'port': PORT,
        'bootstrap_replica_role': _BOOTSTRAP_ROLE,
    })


@bootstrap.get('/', response_class=HTMLResponse)
def bootstrap_dashboard() -> str:
    err = STARTUP_ERROR_TEXT or '—'
    return f'''<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{runtime_identity.PRODUCT_NAME}</title><style>body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#071426;color:#e8f0ff;margin:0;padding:28px}}.card{{max-width:760px;margin:40px auto;padding:24px;border:1px solid #29466d;border-radius:20px;background:#0b1b31}}h1{{margin-top:0}}.warn{{color:#ffd36e}}.bad{{color:#ff7187}}code{{word-break:break-word}}</style></head><body><div class="card"><h1>{runtime_identity.PRODUCT_NAME} {runtime_identity.API_VERSION}</h1><h2 class="{'bad' if STARTUP_ERROR_TYPE else 'warn'}">Bootstrap HTTP ONLINE · {STARTUP_STATUS}</h2><p>Listening: <code>0.0.0.0:{PORT}</code></p><p>Replica role: <code>{_BOOTSTRAP_ROLE}</code></p><p>正式 Runtime 完成前，新訊號與交易維持 fail-closed。</p><p>錯誤：<code>{err}</code></p></div></body></html>'''


@bootstrap.api_route('/{path:path}', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD'])
async def bootstrap_fallback(path: str, request: Request) -> JSONResponse:
    _ = path, request
    return JSONResponse(status_code=503, content={
        'ok': False, 'mode': 'FAIL_CLOSED_BOOTSTRAP', 'startup_status': STARTUP_STATUS,
        'startup_error_type': STARTUP_ERROR_TYPE, 'port': PORT,
        'bootstrap_replica_role': _BOOTSTRAP_ROLE,
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
