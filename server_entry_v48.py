from __future__ import annotations

"""Final production entry with V48 runtime-continuity overlay.

V48 is installed after the validated V47 research stack. It does not alter the V47
Stage-6 dataset/code fingerprint, candidate search space, trade simulator, OOS rules or
no-lookahead semantics. It only makes resource scheduling and rolling-restart status
truthful/stable on the 4C/8GB production host.
"""

import importlib
import logging
import os
import time

import server_entry_v27 as v27

LOG = logging.getLogger('eth-adaptive.v48-entry')
_ORIGINAL_IMPORT = v27.base._import_production_blocking
_BOOT_ID = f"bootstrap-{os.getpid()}-{int(time.time())}"


def _bootstrap_payload(kind: str) -> dict:
    return {
        'ok': False, 'loading': True, 'mode': 'ROLLING_BOOTSTRAP_FAIL_CLOSED', 'kind': kind,
        'boot_id': _BOOT_ID, 'startup_status': v27.base.STARTUP_STATUS,
        'startup_error_type': v27.base.STARTUP_ERROR_TYPE,
        'bootstrap_replica_role': getattr(v27.base, '_BOOTSTRAP_ROLE', 'UNKNOWN'),
        'data_deleted': False, 'storage_truth_known': False,
        'reason': 'production runtime is loading; endpoint unavailability is not evidence of SQLite loss',
    }


def _install_bootstrap_compatibility_routes() -> None:
    app = v27.base.bootstrap
    catchalls = [r for r in list(app.router.routes) if getattr(r, 'path', None) == '/{path:path}']
    if catchalls:
        app.router.routes = [r for r in app.router.routes if r not in catchalls]
    paths = {getattr(r, 'path', None) for r in app.router.routes}

    if '/api/latest/runtime-snapshot' not in paths:
        @app.get('/api/latest/runtime-snapshot')
        def bootstrap_runtime_snapshot() -> dict:
            p = _bootstrap_payload('runtime-snapshot')
            p.update({'storage': {'status': 'BOOTING', 'truth_known': False, 'data_deleted': False},
                'replay': {'status': 'BOOTING', 'complete': None},
                'autonomous': {'status': 'BOOTING_RUNTIME', 'progress': {'evolution_percent': 0.0}, 'champions': [], 'active': {}},
                'stage6': {'status': 'BOOTING_RUNTIME'}, 'exact_integrity': {'status': 'BOOTING_RUNTIME'},
                'candidate_watchdog': {'running': False}, 'memory': {'ratio': None}})
            return p

    if '/api/v30/autonomous' not in paths:
        @app.get('/api/v30/autonomous')
        def bootstrap_autonomous() -> dict:
            p = _bootstrap_payload('autonomous')
            p.update({'status': 'BOOTING_RUNTIME', 'progress': {'evolution_percent': 0.0, 'oos_percent': 0.0},
                'champions': [], 'research_best': [], 'active': {}, 'live_ready': False, 'paper_notional_usdt': 20000})
            return p

    if '/api/v46/stage6-throughput' not in paths:
        @app.get('/api/v46/stage6-throughput')
        def bootstrap_v46() -> dict:
            p = _bootstrap_payload('stage6-throughput')
            p.update({'state': {'status': 'BOOTING_RUNTIME'}, 'memory': {'ratio': None}, 'checkpoint_counts': {}})
            return p

    if '/api/v47/dataset-integrity' not in paths:
        @app.get('/api/v47/dataset-integrity')
        def bootstrap_v47() -> dict:
            p = _bootstrap_payload('dataset-integrity')
            p.update({'state': {'status': 'BOOTING_RUNTIME'}, 'manifest': {}, 'replay': {'complete': None}})
            return p

    if '/api/storage/status' not in paths:
        @app.get('/api/storage/status')
        def bootstrap_storage() -> dict:
            p = _bootstrap_payload('storage')
            p.update({'status': 'BOOTING_RUNTIME', 'healthy': None, 'persistent_ok': None, 'database_exists': None,
                'database_size_bytes': None, 'market_bars': None, 'learning_samples': None})
            return p

    if '/api/v12/baseline' not in paths:
        @app.get('/api/v12/baseline')
        def bootstrap_baseline() -> dict:
            p = _bootstrap_payload('baseline')
            p.update({'status': 'BOOTING', 'clean': None, 'dataset_id': None, 'certification_allowed': False})
            return p

    if '/api/v17/certification' not in paths:
        @app.get('/api/v17/certification')
        def bootstrap_certification() -> dict:
            p = _bootstrap_payload('certification')
            p.update({'audit': {'status': 'BOOTING', 'valid': False, 'learning_samples': 0, 'decision_timestamps': 0,
                'partial_decision_timestamps': 0}, 'certification': {'status': 'NOT_STARTED', 'reason': 'runtime loading'},
                'pipeline': {'signal_champions': 0, 'execution_champions': 0}})
            return p

    if '/api/latest/pipeline' not in paths:
        @app.get('/api/latest/pipeline')
        def bootstrap_pipeline() -> dict:
            p = _bootstrap_payload('pipeline')
            p.update({'stage': 'BOOTING_RUNTIME', 'active_stage': 'runtime bootstrap', 'overall_percent': 0.0,
                'operational': False, 'stages': [], 'replay': {'complete': None}})
            return p

    if '/api/latest/progress-detail' not in paths:
        @app.get('/api/latest/progress-detail')
        def bootstrap_progress_detail() -> dict:
            p = _bootstrap_payload('progress-detail')
            p.update({'replay': {'complete': None, 'pending_eligible_decisions': None}, 'signal_certification': {},
                'execution_audit': {}, 'live_handoff': {'ready': False, 'percent': 0.0},
                'trading_contract': {'paper_notional_usdt': 20000}})
            return p

    app.router.routes.extend(catchalls)


_install_bootstrap_compatibility_routes()


def _import_production_blocking_v48():
    production, app = _ORIGINAL_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    throughput = importlib.import_module('v46_stage6_throughput_liveness')
    integrity = importlib.import_module('v47_dataset_integrity_authority')
    continuity = importlib.import_module('v48_runtime_continuity')
    continuity.install(production, autonomous, throughput, integrity)
    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role['continuity_runtime'] = continuity.VERSION
        role['stable_runtime_snapshot'] = True
        role['stage6_candidate_process_watchdog'] = True
        role['rolling_restart_does_not_imply_storage_loss'] = True
        role['v47_exact_resume_identity_unchanged_by_v48'] = True
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v48
app = v27.base.app


if __name__ == '__main__':
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V48_CONTINUITY', v27.base.PORT)
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT, access_log=True, log_level='info')
