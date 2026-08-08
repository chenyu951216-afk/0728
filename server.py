from pathlib import Path

import uvicorn
from fastapi.responses import HTMLResponse

import app as core
from v5_async_runtime import install_async
from v5_runtime import install as install_v5
from v6_runtime import install as install_v6
from v6_scan_runtime import install as install_v6_scan

install_v5(core)
install_async(core)

# v5 changed the signal-learning target: strategies are trained separately by
# LONG/SHORT and use strategy-specific passive-fill outcomes. Preserve old samples
# for audit, but rebuild the active signal-model training set once.
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

# Legacy non-directional Champions remain available for audit only.
con = core.db()
con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION' AND direction NOT IN ('LONG','SHORT')")
con.commit()
con.close()

# v6 adds a second independent certification layer for the exact execution plan.
# New signals require BOTH the signal Champion and its matching execution Champion.
install_v6(core)
install_v6_scan(core)

app = core.app
PORT = core.PORT

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    return Path('dashboard.html').read_text(encoding='utf-8')


if __name__ == '__main__':
    uvicorn.run('server:app', host='0.0.0.0', port=PORT)
