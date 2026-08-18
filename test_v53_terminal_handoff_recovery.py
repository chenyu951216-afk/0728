from __future__ import annotations

import time
from types import SimpleNamespace

import v53_terminal_handoff_recovery as v53


class DummyCore:
    def __init__(self):
        self.state = {}
        self.saved = {}

    def get_state(self, key, default=None):
        return self.saved.get(key, default)

    def set_state(self, key, value):
        self.saved[key] = value


def _autonomous(registry=None):
    rows = list(registry or [])
    return SimpleNamespace(
        CHECKPOINT_KEY='autonomous_evolution_checkpoint_v30',
        STATE_KEY='autonomous_strategy_discovery_v30',
        SCHEMA=30,
        POPULATION=48,
        GENERATIONS=8,
        _load_registry=lambda _core, active_only=True: list(rows),
    )


def _pipeline52(counts=None):
    truth = dict(counts or {'saved': 0, 'finalists': 0, 'audited': 0, 'champions': 0})
    return SimpleNamespace(
        _run=lambda _core, _throughput: 'run-v52',
        _counts=lambda _core, _run='': dict(truth),
    )


def test_complete_checkpoint_outranks_stale_oos_progress_marker():
    core = DummyCore(); a = _autonomous([{'strategy_id': 'AUTO_OK'}])
    core.saved[a.CHECKPOINT_KEY] = {
        'status': 'COMPLETE', 'generation': 7, 'finalists': 1, 'champions': 1,
        'updated_at': 200,
    }
    core.state['autonomous_live_progress'] = {
        'stage': 'ONE_TIME_COMPLETE_PACKAGE_OOS', 'updated_at': 100,
    }

    result = v53.reconcile(core, a, SimpleNamespace(), _pipeline52(), SimpleNamespace(_CERT_FUTURE=None))

    assert result['terminal'] is True
    assert result['status'] == 'TERMINAL_CHECKPOINT_AUTHORITATIVE'
    assert core.state['v53_terminal_progress_archive']['stage'] == 'ONE_TIME_COMPLETE_PACKAGE_OOS'
    assert core.state['autonomous_live_progress']['stage'] == 'HISTORICAL_CERTIFICATION_COMPLETE'


def test_newer_active_run_is_not_hidden_by_older_complete_checkpoint():
    core = DummyCore(); a = _autonomous()
    core.saved[a.CHECKPOINT_KEY] = {'status': 'COMPLETE', 'updated_at': 100}
    core.state['autonomous_live_progress'] = {
        'stage': 'DIRECT_R_AUTONOMOUS_EVOLUTION', 'updated_at': 200,
    }

    result = v53.reconcile(core, a, SimpleNamespace(), _pipeline52(), SimpleNamespace(_CERT_FUTURE=None))

    assert result['terminal'] is False
    assert core.state['autonomous_live_progress']['stage'] == 'DIRECT_R_AUTONOMOUS_EVOLUTION'


def test_crash_after_all_oos_audits_recovers_only_terminal_commit():
    core = DummyCore()
    champion = {
        'strategy_id': 'AUTO_1', 'direction': 'LONG', 'behavior_label': 'state',
        'metrics': {'profit_factor': 2.7, 'expectancy_r': .305},
    }
    a = _autonomous([champion])
    core.saved[a.CHECKPOINT_KEY] = {'status': 'RUNNING', 'generation': 7, 'updated_at': 100}
    core.state['autonomous_live_progress'] = {
        'stage': 'ONE_TIME_COMPLETE_PACKAGE_OOS',
        'updated_at': int(time.time()) - (v53.STALE_TAIL_SECONDS + 10),
    }
    core.state['v49_stage6_atomic_orchestration'] = {
        'run_id': 'run-v52',
        'checkpoint_counts': {'persisted': 384, 'scored': 384, 'no_result': 0},
        'error': None,
        'future_error': None,
    }
    p52 = _pipeline52({'saved': 10, 'finalists': 1, 'audited': 1, 'champions': 1})

    result = v53.reconcile(core, a, SimpleNamespace(), p52, SimpleNamespace(_CERT_FUTURE=None))

    cp = core.saved[a.CHECKPOINT_KEY]
    assert result['status'] == 'TERMINAL_COMMIT_RECOVERED_FROM_DURABLE_OOS'
    assert result['terminal'] is True
    assert cp['status'] == 'COMPLETE'
    assert cp['v53_tail_recovered'] is True
    assert cp['finalists'] == 1
    assert cp['champions'] == 1
    assert core.state[a.STATE_KEY]['status'] == 'COMPLETE'
    assert core.state['autonomous_live_progress']['stage'] == 'HISTORICAL_CERTIFICATION_COMPLETE'


def test_tail_recovery_refuses_while_certification_future_is_active():
    class ActiveFuture:
        @staticmethod
        def done():
            return False

    core = DummyCore(); a = _autonomous([{'strategy_id': 'AUTO_1'}])
    core.saved[a.CHECKPOINT_KEY] = {'status': 'RUNNING', 'updated_at': 100}
    core.state['autonomous_live_progress'] = {
        'stage': 'ONE_TIME_COMPLETE_PACKAGE_OOS',
        'updated_at': int(time.time()) - 1000,
    }
    core.state['v49_stage6_atomic_orchestration'] = {
        'checkpoint_counts': {'persisted': 384, 'scored': 384, 'no_result': 0},
    }
    p52 = _pipeline52({'saved': 3, 'finalists': 1, 'audited': 1, 'champions': 1})

    result = v53.reconcile(core, a, SimpleNamespace(), p52,
                           SimpleNamespace(_CERT_FUTURE=ActiveFuture()))

    assert result['terminal'] is False
    assert core.saved[a.CHECKPOINT_KEY]['status'] == 'RUNNING'
