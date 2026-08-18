from __future__ import annotations

from types import SimpleNamespace

import v54_terminal_runtime_authority as v54


class DummyExecutor:
    def __init__(self):
        self.closed = False

    def shutdown(self, wait=False, cancel_futures=False):
        self.closed = True


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
        CHECKPOINT_KEY='v30_autonomous_evolution_checkpoint',
        POPULATION=48,
        GENERATIONS=8,
        _load_registry=lambda _core, active_only=True: list(rows),
    )


def _throughput(counts=None, executor=None):
    truth = dict(counts or {'persisted': 384, 'scored': 384, 'no_result': 0})
    return SimpleNamespace(
        _RUN_ID=None,
        _EXECUTOR=executor,
        MAX_WORKERS=3,
        CHUNK=48,
        _counts=lambda _core, _run: dict(truth),
    )


def _modules():
    integrity = SimpleNamespace(STATE_KEY='v47_dataset_integrity_authority', SCHEMA=47, VERSION='V47_DATASET_INTEGRITY_AUTHORITY')
    orchestration = SimpleNamespace(STATE_KEY='v49_stage6_atomic_orchestration', SCHEMA=49, VERSION='V49_STAGE6_ATOMIC_ORCHESTRATION')
    pipeline52 = SimpleNamespace()
    performance = SimpleNamespace(_memory=lambda: {'ratio': 0.61})
    return integrity, orchestration, pipeline52, performance


def test_terminal_run_converges_all_post_restart_surfaces_without_research_reset():
    core = DummyCore()
    champion = {'strategy_id': 'AUTO_OK', 'direction': 'LONG'}
    a = _autonomous([champion])
    core.saved[a.CHECKPOINT_KEY] = {
        'status': 'COMPLETE', 'generation': 7, 'finalists': 1, 'champions': 1,
        'updated_at': 1234,
    }
    core.saved['v47_last_stage6_manifest'] = {
        'full_sha256': 'abc123', 'run_id': 'exact-run', 'dataset_id': 'dataset-a',
        'hash_scope': 'EVERY_STAGE6_BYTE',
    }
    core.saved['final_dataset_baseline_v1'] = {'clean': True, 'dataset_id': 'dataset-a'}
    core.saved['v49_stage6_outer_cursor'] = {
        'generation': 8, 'candidate': 48, 'status': 'SCORED', 'run_id': 'exact-run',
    }
    core.state['autonomous_live_progress'] = {
        'stage': 'HISTORICAL_CERTIFICATION_COMPLETE', 'generation': 8,
    }
    executor = DummyExecutor()
    throughput = _throughput(executor=executor)
    integrity, orchestration, pipeline52, performance = _modules()

    state = v54.reconcile(core, a, throughput, integrity, orchestration, pipeline52, performance)

    active = core.state['autonomous_live_progress']
    assert state['status'] == 'CURRENT_PAPER_MONITORING'
    assert state['stage6_percent'] == 100.0
    assert state['stage7_percent'] == 100.0
    assert state['stage8_percent'] == 100.0
    assert state['stage9_percent'] == 100.0
    assert active['generation'] == 8 and active['generations'] == 8
    assert active['candidate'] == 48 and active['population'] == 48
    assert active['outer_status'] == 'COMMITTED'
    assert throughput._RUN_ID == 'exact-run'
    assert throughput._EXECUTOR is None and executor.closed is True
    assert core.state[integrity.STATE_KEY]['full_sha256'] == 'abc123'
    assert core.state[orchestration.STATE_KEY]['run_id'] == 'exact-run'
    assert core.state[orchestration.STATE_KEY]['checkpoint_counts']['persisted'] == 384
    assert core.state['v52_current_paper_handoff']['ready'] is True
    assert core.state['learning']['phase'] == 'CURRENT_PAPER_MONITORING'
    assert core.state['learning']['certification_pipeline']['signal_champions'] == 1
    assert core.state['learning']['certification_pipeline']['execution_champions'] == 1
    assert core.state['subsystem_health']['learning']['status'] == 'OK'
    assert core.state['subsystem_health']['execution_audit']['status'] == 'OK'
    assert state['historical_data_deleted'] is False
    assert state['replay_reset'] is False
    assert state['future_peeking_enabled'] is False


def test_incomplete_research_is_not_faked_terminal():
    core = DummyCore(); a = _autonomous([])
    core.saved[a.CHECKPOINT_KEY] = {'status': 'RUNNING', 'generation': 3}
    core.state['autonomous_live_progress'] = {
        'stage': 'DIRECT_R_AUTONOMOUS_EVOLUTION', 'generation': 4,
        'generations': 8, 'candidate': 11, 'population': 48,
    }
    throughput = _throughput({'persisted': 155, 'scored': 155, 'no_result': 0})
    integrity, orchestration, pipeline52, performance = _modules()

    state = v54.reconcile(core, a, throughput, integrity, orchestration, pipeline52, performance)

    assert state['terminal'] is False
    assert state['status'] == 'AUTONOMOUS_RESEARCH_ACTIVE'
    assert core.state['autonomous_live_progress']['stage'] == 'DIRECT_R_AUTONOMOUS_EVOLUTION'
    assert throughput._RUN_ID is None
    assert 'v52_current_paper_handoff' not in core.state


def test_terminal_boot_gate_requires_both_complete_checkpoint_and_champion():
    core = DummyCore(); a = _autonomous([{'strategy_id': 'AUTO_OK'}])
    core.saved[a.CHECKPOINT_KEY] = {'status': 'COMPLETE'}
    gate = v54.terminal_boot_gate(core, a, source='boot')
    assert gate and gate['suppressed'] is True

    core2 = DummyCore(); a2 = _autonomous([])
    core2.saved[a2.CHECKPOINT_KEY] = {'status': 'COMPLETE'}
    assert v54.terminal_boot_gate(core2, a2, source='boot') is None


def test_terminal_progress_authority_fixes_875_percent_marker():
    core = DummyCore(); a = _autonomous([{'strategy_id': 'AUTO_OK'}])
    core.saved[a.CHECKPOINT_KEY] = {'status': 'COMPLETE'}
    core.state['autonomous_live_progress'] = {
        'stage': 'HISTORICAL_CERTIFICATION_COMPLETE',
        'generation': 8, 'candidate': 0, 'population': 48,
    }
    base = lambda _c, _a, _t: {
        'total_candidates': 384, 'completed_candidates': 336,
        'evolution_percent': 87.5, 'error': None,
    }
    p52 = SimpleNamespace(_run_progress=base)
    old = v54._BASE_RUN_PROGRESS
    try:
        v54._BASE_RUN_PROGRESS = None
        v54._install_progress_authority(core, a, p52)
        out = p52._run_progress(core, a, SimpleNamespace())
        assert out['completed_candidates'] == 384
        assert out['evolution_percent'] == 100.0
        assert out['terminal'] is True
    finally:
        v54._BASE_RUN_PROGRESS = old


def test_resource_governor_caps_parallelism_and_falls_back_under_memory_pressure():
    throughput = SimpleNamespace(MAX_WORKERS=3, CHUNK=48, _workers=lambda: 3)
    memory = {'ratio': 0.40}
    performance = SimpleNamespace(_memory=lambda: dict(memory))
    old = v54._BASE_WORKERS
    try:
        v54._BASE_WORKERS = None
        v54._install_resource_governor(throughput, performance)
        assert throughput.MAX_WORKERS == 2
        assert throughput.CHUNK == 32
        assert throughput._workers() == 2
        memory['ratio'] = 0.75
        assert throughput._workers() == 1
    finally:
        v54._BASE_WORKERS = old
