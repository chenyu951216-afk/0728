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
from v9_final import install as install_strict_final
from v9_readiness import install as install_replay_readiness
from v9_training_store import install as install_training_store
from v9_live_parity import install as install_live_parity

install_storage_guard_early(core)
install_v5(core)
install_async(core)

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
install_storage_guard(core)
install_stability(core)
# Final layers are deliberately installed last so no legacy module can overwrite
# strict event-time replay, DEV-only evolution, derivative readiness, full-span
# training storage, live feature parity, or runtime identity.
install_strict_final(core)
install_replay_readiness(core)
install_training_store(core)
install_live_parity(core)

app = core.app
PORT = core.PORT

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    html = Path('dashboard_v721.html').read_text(encoding='utf-8')
    return (
        html.replace('ETH Adaptive AI 7.2.1', 'ETH Adaptive AI 8.0.2 Final Strict Replay')
        .replace(
            'Walk-Forward Evolution · Storage Identity Guard · Subsystem-Isolated Fail-Closed',
            'Strict Event-Time Replay · Full-Span Learning · Live/History Parity · Untouched Audit · Fail-Closed',
        )
    )


if __name__ == '__main__':
    uvicorn.run('server:app', host='0.0.0.0', port=PORT)
