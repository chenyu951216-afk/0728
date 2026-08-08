from pathlib import Path

import uvicorn
from fastapi.responses import HTMLResponse

import app as core
from v5_async_runtime import install_async
from v5_runtime import install as install_v5
from v7_timesafe_learning import install as install_timesafe_learning
from v7_signal_learner import install as install_signal_learner
from v7_execution_alignment import install as install_execution_alignment
from v7_runtime import install as install_v7
from v7_reentry_guard import install as install_reentry_guard
from v7_discord_runtime import install as install_discord_runtime
from v7_live_health import install as install_live_health
from v7_learning_guard import install as install_learning_guard
from v7_trade_monitor import install as install_trade_monitor

install_v5(core)
install_async(core)

# v5 changed the signal-learning target: strategies are trained separately by
# LONG/SHORT. Preserve the old v4 set for audit once.
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

con = core.db()
con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION' AND direction NOT IN ('LONG','SHORT')")
con.commit()
con.close()

# Critical point-in-time correction: exchange timestamps are candle OPEN times.
# Historical HTF features now include a 1H/4H/1D bar only after its close was
# actually knowable. Existing contaminated samples/Champions are archived.
install_timesafe_learning(core)
# Champion replacement compares stored clean OOS metrics rather than letting an
# old final model score historical rows it may already have seen.
install_signal_learner(core)
# The execution simulator uses the same close-time eligibility for 30m/1h
# structural invalidation levels.
install_execution_alignment()

# v7 retires v6 execution metrics, uses point-in-time historical validation,
# and separates live execution outcomes from signal-model labels.
install_v7(core)
# Stop-loss re-entry requires both elapsed cooldown and a genuinely new market
# structure; repeated directional whipsaws trigger a longer quarantine.
install_reentry_guard()
# Every Discord alert reports the active runtime instead of a stale v5 footer.
install_discord_runtime(core)
# Clean historical validation is necessary but not sufficient: if a deployed
# version materially degrades in paper/live outcomes it is temporarily quarantined.
install_live_health(core)
# Avoid repeating identical CPU-heavy execution searches when no model/data changed,
# while still doing a forced fresh-data execution re-audit once per day.
install_learning_guard(core)
# Live Entry/TP/SL lifecycle is driven by ordered Gate public trades, not closed
# 15m candles, so a stopped position is recognized within the monitor interval.
install_trade_monitor(core)

app = core.app
PORT = core.PORT

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    return Path('dashboard.html').read_text(encoding='utf-8')


if __name__ == '__main__':
    uvicorn.run('server:app', host='0.0.0.0', port=PORT)
