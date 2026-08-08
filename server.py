from pathlib import Path

import uvicorn
from fastapi.responses import HTMLResponse

import app as core
from v5_async_runtime import install_async
from v5_runtime import install

install(core)
install_async(core)

# v5 changes the learning target itself: strategies are trained separately by
# LONG/SHORT and use strategy-specific passive-fill outcomes. Reusing v4 rows via
# INSERT OR IGNORE would silently contaminate v5 with old mixed/generic labels.
# Preserve the old samples for audit, but rebuild the active learning set from the
# already-cached 2020->present market data exactly once.
if core.get_state('v5_sample_schema') != 2:
    con = core.db()
    con.execute('DROP TABLE IF EXISTS learning_samples_v4_archive')
    con.execute('CREATE TABLE learning_samples_v4_archive AS SELECT * FROM learning_samples')
    con.execute('DELETE FROM learning_samples')
    con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION'")
    con.commit()
    con.close()
    core.set_state('last_learning_sample_ts_v2', core.START_TS)
    core.set_state('v5_last_train_sample_total', 0)
    core.set_state('last_train_ts_v5', 0)
    core.set_state('v5_sample_schema', 2)

# Any legacy v4 Champion that survived a pre-migration database must never be
# selected/displayed by the v5 runtime. The record is retained for audit only.
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
