from __future__ import annotations

import hashlib
import json

import v55_autonomous_champion_authority as v55


GENOME = {
    'direction': 'LONG',
    'feature_names': ['wick_ratio', 'atr_pct'],
    'entry_market': False,
    'entry_offset_atr': -0.25,
    'stop_atr': 1.8,
    'target_rr': [1.0, 2.0],
    'allocations': [40.0, 60.0],
    'expire_bars': 4,
    'max_hold_bars': 192,
    'breakeven_after_r': 0.8,
    'trail_start_r': 1.5,
    'trail_lock_r': 0.5,
    'cooldown_bars': 2,
}
METRICS = {
    'gate_thresholds': [{'feature': 'wick_ratio', 'op': 'LE', 'value': 0.495}],
    'direct_r_threshold': 0.12,
    'profit_factor': 1.70,
    'expectancy_r': 0.305,
    'test_win': 0.48,
    'oos_fills': 179,
    'max_drawdown_r': 8.38,
    'bootstrap_ci05_r': 0.02,
    'historical_no_lookahead': True,
}
CHAMPION = {
    'strategy_id': 'AUTO_9837F11BD1040E',
    'direction': 'LONG',
    'behavior_label': 'AI_STATE[wick_ratio ≤ 0.495] · LONG · hold≤48h',
    'genome': GENOME,
    'metrics': METRICS,
    'active': True,
    'status': 'CHAMPION',
}


class Core:
    def __init__(self):
        self.state = {}
        self.saved = {'checkpoint': {'status': 'COMPLETE'}}
        self.created = 0
        self.create_signal = self.base_create

    def get_state(self, key, default=None):
        return self.saved.get(key, default)

    def base_create(self, analysis, m15):
        self.created += 1
        return {'ok': True, 'strategy': analysis.get('selection', {}).get('strategy')}


class Autonomous:
    CHECKPOINT_KEY = 'checkpoint'
    PAPER_NOTIONAL_USDT = 20000.0
    LIVE_MIN_PREDICTED_EV_R = 0.04
    LIVE_MAX_OOD_FRACTION = 0.35

    def __init__(self, champions=None):
        self.champions = list(champions if champions is not None else [CHAMPION])

    def _load_registry(self, core, active_only=True):
        return list(self.champions)

    @staticmethod
    def _hash_payload(payload, n=18):
        raw = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(raw.encode()).hexdigest()[:n]


class Execution:
    VERSION = 'V52_SAFE_LEVERAGE_EXECUTION_AUTHORITY'
    LEVERAGE_MODE = 'MAX_SAFE_WITH_STOP_HEADROOM_AT_ORDER_TIME'


def test_autonomous_champion_projects_into_both_legacy_surfaces():
    core, auto, ex = Core(), Autonomous(), Execution()
    state = v55._reconcile(core, auto, ex)
    assert state['current_paper_ready'] is True
    assert state['champion_ids'] == ['AUTO_9837F11BD1040E']
    assert core.state['v52_current_paper_handoff']['strategy_ids'] == ['AUTO_9837F11BD1040E']
    assert core.state['learning']['certification_pipeline']['signal_champions'] == 1
    assert core.state['learning']['certification_pipeline']['execution_champions'] == 1

    sig = v55._signal_rows(core, auto, ex)
    exe = v55._execution_rows(core, auto, ex)['registry']
    assert sig[0]['strategy'] == exe[0]['strategy'] == 'AUTO_9837F11BD1040E'
    assert sig[0]['profit_factor'] == exe[0]['metrics']['profit_factor'] == 1.70
    assert sig[0]['threshold'] is None  # never mislabel direct-R threshold as probability


def test_logic_is_read_from_persisted_genome_not_invented():
    logic = v55._logic(CHAMPION, Autonomous(), Execution())
    assert logic['state_gate']['text'] == 'wick_ratio ≤ 0.495'
    assert logic['entry']['order_type'] == 'ATR_OFFSET_LIMIT'
    assert logic['entry']['entry_offset_atr'] == -0.25
    assert logic['stop_loss']['stop_atr'] == 1.8
    assert logic['take_profit']['targets'] == [
        {'rr': 1.0, 'allocation_pct': 40.0},
        {'rr': 2.0, 'allocation_pct': 60.0},
    ]
    assert logic['management']['max_hold_hours'] == 48.0
    assert logic['oos']['historical_no_lookahead'] is True


def test_stage9_accepts_only_exact_persisted_certified_genome():
    core, auto, ex = Core(), Autonomous(), Execution()
    previous = v55._BASE_CREATE
    try:
        v55._BASE_CREATE = None
        v55._install_execution_guard(core, auto, ex)
        good = {
            'selection': {
                'tradeable': True,
                'strategy': 'AUTO_9837F11BD1040E',
                'genome': dict(GENOME),
            }
        }
        assert core.create_signal(good, [])['ok'] is True
        assert core.created == 1
        assert core.state['v55_execution_binding']['strategy_id'] == 'AUTO_9837F11BD1040E'

        bad = {
            'selection': {
                'tradeable': True,
                'strategy': 'AUTO_9837F11BD1040E',
                'genome': {**GENOME, 'stop_atr': 9.9},
            }
        }
        assert core.create_signal(bad, []) is None
        assert core.created == 1
        assert 'differs from persisted certified Champion' in core.state['v55_execution_fail_closed']['reason']
    finally:
        v55._BASE_CREATE = previous


def test_no_champion_never_claims_current_paper_ready():
    core, auto, ex = Core(), Autonomous([]), Execution()
    state = v55._reconcile(core, auto, ex)
    assert state['current_paper_ready'] is False
    assert state['champion_count'] == 0
