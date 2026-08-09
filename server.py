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
from v7_post_exit import install as install_post_exit
from v7_live_health import install as install_live_health
from v7_learning_guard import install as install_learning_guard
from v7_trade_monitor import install as install_trade_monitor
from v7_timeout_guard import install as install_timeout_guard
from v7_trade_feed import install as install_trade_feed
from v7_monitor_gate import install as install_monitor_gate
from v8_migration import install as install_evolution_migration
from v8_evolution import install as install_evolution
from v8_execution_oof import install as install_execution_oof
from v8_execution_walkforward import install as install_execution_walkforward
from v8_notice import install as install_evolution_notice
from v8_storage_guard import install as install_storage_guard, install_early as install_storage_guard_early
from v8_stability import install as install_stability

# Before any migration, production must never silently switch from the mounted
# database to /tmp.
install_storage_guard_early(core)
install_v5(core)
install_async(core)

# Legacy v5 metadata can occasionally be absent even though the newer point-in-time
# schema and its samples are already valid. Never destructively clear newer samples
# just because the legacy bookkeeping key is missing.
if core.get_state('v5_sample_schema') != 2:
    pit_schema = int(core.get_state('point_in_time_sample_schema', 0) or 0)
    con = core.db()
    sample_count = int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0])
    if pit_schema >= 4 and sample_count > 0:
        con.close()
        core.set_state('v5_sample_schema', 2)
    else:
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

install_timesafe_learning(core)
install_signal_learner(core)
install_execution_alignment()
install_fine_execution()
install_v7(core)
install_reentry_guard()
install_discord_runtime(core)
install_post_exit(core)
install_live_health(core)
install_learning_guard(core)
install_trade_monitor(core)
install_timeout_guard()
install_trade_feed(core)
install_monitor_gate(core)
install_evolution_migration(core)
install_evolution(core)
install_execution_oof()
install_execution_walkforward(core)
install_evolution_notice(core)
# Storage guard wraps the final learning/signal implementations.
install_storage_guard(core)
# Stability must be last: it isolates scan/risk/learning/execution/Discord failures
# and supervises the already-installed 7.2 runtime without bypassing safety gates.
install_stability(core)

app = core.app
PORT = core.PORT

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    return Path('dashboard_v72.html').read_text(encoding='utf-8')


if __name__ == '__main__':
    uvicorn.run('server:app', host='0.0.0.0', port=PORT)
