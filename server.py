from pathlib import Path

import uvicorn
from fastapi.responses import HTMLResponse

import app as core
from v5_async_runtime import install_async
from v5_runtime import install as install_v5
from v7_timesafe_learning import install as install_timesafe_learning
from v7_signal_learner import install as install_signal_learner
from v7_execution_alignment import install as install_execution_alignment
from v7_fine_execution import install as install_fine_execution
from v7_runtime import install as install_v7
from v7_reentry_guard import install as install_reentry_guard
from v7_discord_runtime import install as install_discord_runtime
from v7_live_health import install as install_live_health
from v7_learning_guard import install as install_learning_guard
from v7_trade_monitor import install as install_trade_monitor
from v7_timeout_guard import install as install_timeout_guard
from v7_trade_feed import install as install_trade_feed
from v7_monitor_gate import install as install_monitor_gate

install_v5(core)
install_async(core)

if core.get_state('v5_sample_schema') != 2:
    con = core.db()
    con.execute('DROP TABLE IF EXISTS learning_samples_v4_archive')
    con.execute('CREATE TABLE learning_samples_v4_archive AS SELECT * FROM learning_samples')
    con.execute('DELETE FROM learning_samples')
    con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION'")
    con.commit(); con.close()
    core.set_state('last_learning_sample_ts_v2', core.START_TS)
    core.set_state('v5_last_train_sample_total', 0)
    core.set_state('last_train_ts_v5', 0)
    core.set_state('v5_sample_schema', 2)

con = core.db()
con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION' AND direction NOT IN ('LONG','SHORT')")
con.commit(); con.close()

# Historical features are close-time safe; old contaminated samples/Champions are archived.
install_timesafe_learning(core)
# Champion evolution compares only stored clean OOS metrics and a fresh recent fold.
install_signal_learner(core)
# Execution 30m/1h structures obey the same historical close-time eligibility.
install_execution_alignment()
# Entry/SL/TP lifecycle is replayed on 5m history instead of ambiguous 15m paths;
# missing 5m paths are excluded from audit rather than mislabeled.
install_fine_execution()
# Point-in-time Signal OOF + independent validation + untouched execution audit.
install_v7(core)
# Losing-stop cooldown and structural reset / whipsaw quarantine.
install_reentry_guard()
# Dynamic runtime-labelled Discord delivery, no stale v5 footer.
install_discord_runtime(core)
# Deployment drift circuit breaker from separate live execution outcomes.
install_live_health(core)
# Throttled signal learning plus daily fresh-data execution re-audit.
install_learning_guard(core)
# Ordered public-trade lifecycle monitor, with the same max-hold timeout used in audit.
install_trade_monitor(core)
install_timeout_guard()
# Paginated catch-up prevents deploy/restart gaps from silently missing a stop.
install_trade_feed(core)
# Fail closed: no new position unless the risk feed proves complete coverage.
install_monitor_gate(core)

app = core.app
PORT = core.PORT

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    return Path('dashboard_v7.html').read_text(encoding='utf-8')


if __name__ == '__main__':
    uvicorn.run('server:app', host='0.0.0.0', port=PORT)
