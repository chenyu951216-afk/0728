from pathlib import Path

import uvicorn
from fastapi.responses import HTMLResponse

import app as core
from v5_runtime import install

install(core)

# Legacy v4 models mixed LONG/SHORT in one model. Keep their records for audit,
# but archive them so only v5 direction-separated Champions can be selected/displayed.
con = core.db()
con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION' AND direction NOT IN ('LONG','SHORT')")
con.commit()
con.close()

app = core.app
PORT = core.PORT

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    return Path('dashboard.html').read_text(encoding='utf-8')


if __name__ == '__main__':
    uvicorn.run('server:app', host='0.0.0.0', port=PORT)
