from __future__ import annotations

import logging
import os
import traceback

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

LOG = logging.getLogger('eth-adaptive-bootstrap')
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s %(levelname)s %(message)s')


def _resolve_port() -> int:
    """Resolve Zeabur's port before importing the application.

    Some deployment environments leave values such as ``${WEB_PORT}`` literal. The
    legacy app parses PORT with ``int()`` at import time, so sanitize it first and
    always leave a numeric PORT in the environment.
    """
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
STARTUP_ERROR_TYPE: str | None = None

try:
    # 9.0.1 moves heavyweight source-provenance recovery out of the synchronous
    # import path. HTTP can bind immediately while certification/live orders remain
    # fail-closed until the background preflight is complete.
    import server_v19 as production
    app = production.app
except Exception as exc:  # HTTP must stay available even when the research layer fails.
    STARTUP_ERROR_TYPE = type(exc).__name__
    LOG.error('production runtime failed during import: %s', STARTUP_ERROR_TYPE)
    LOG.error('%s', traceback.format_exc())

    app = FastAPI(title='ETH Adaptive AI bootstrap fail-closed', version='9.0.1-bootstrap')

    @app.get('/healthz')
    def healthz() -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={
                'ok': False,
                'mode': 'FAIL_CLOSED_BOOTSTRAP',
                'startup_error_type': STARTUP_ERROR_TYPE,
                'trading_enabled': False,
            },
        )

    @app.get('/', response_class=HTMLResponse)
    def bootstrap_dashboard() -> str:
        return f'''<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>ETH Adaptive AI</title><style>body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#071426;color:#e8f0ff;margin:0;padding:28px}}.card{{max-width:760px;margin:40px auto;padding:24px;border:1px solid #29466d;border-radius:20px;background:#0b1b31}}h1{{margin-top:0}}.bad{{color:#ff7187}}code{{word-break:break-word}}</style></head><body><div class="card"><h1>ETH Adaptive AI 9.0.1</h1><h2 class="bad">FAIL-CLOSED 啟動保護</h2><p>HTTP 服務已啟動，但正式研究/認證 Runtime 在初始化時發生錯誤，因此新訊號與交易功能已停用。</p><p>錯誤類型：<code>{STARTUP_ERROR_TYPE or 'Unknown'}</code></p><p>請查看 Zeabur 運作紀錄中的完整 traceback。資料庫不會因這個保護頁面被自動清除。</p></div></body></html>'''


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=PORT)
