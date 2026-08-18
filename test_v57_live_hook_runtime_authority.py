from __future__ import annotations

from types import SimpleNamespace

import v57_live_hook_runtime_authority as v57


class FakeCore:
    def __init__(self):
        self.state = {}
        self.analysis_calls = []
        self.create_calls = []
        self.update_calls = []


class FakeAutonomous:
    def __init__(self, core):
        def analysis(bundle):
            core.analysis_calls.append(bundle)
            return {'bundle': bundle}

        def create(analysis, m15):
            core.create_calls.append((analysis, m15))
            return {'created': True, 'analysis': analysis, 'm15': m15}

        def update(bar):
            core.update_calls.append(bar)
            return {'updated': bar}

        self._autonomous_analysis = analysis
        self._autonomous_create_signal = create
        self._autonomous_update_signal = update


def _installed():
    # isolate module singleton for this focused regression test
    v57._INSTALLED = False
    core = FakeCore()
    autonomous = FakeAutonomous(core)
    production = SimpleNamespace(core=core)
    v56 = SimpleNamespace(VERSION='V56_TEST')
    v57.install(production, autonomous, v56)
    return core, autonomous


def test_analysis_accepts_bound_and_legacy_module_style_calls():
    core, autonomous = _installed()
    assert autonomous._autonomous_analysis({'x': 1})['bundle'] == {'x': 1}
    assert autonomous._autonomous_analysis(core, {'x': 2})['bundle'] == {'x': 2}
    assert core._analysis_from_bundle({'x': 3})['bundle'] == {'x': 3}
    assert core.analysis_calls == [{'x': 1}, {'x': 2}, {'x': 3}]


def test_create_accepts_bound_and_legacy_module_style_calls():
    core, autonomous = _installed()
    a1, a2 = {'selection': 1}, {'selection': 2}
    m1, m2 = [{'ts': 1}], [{'ts': 2}]
    assert autonomous._autonomous_create_signal(a1, m1)['created'] is True
    assert autonomous._autonomous_create_signal(core, a2, m2)['created'] is True
    assert core.create_signal({'selection': 3}, [{'ts': 3}])['created'] is True
    assert len(core.create_calls) == 3


def test_update_accepts_bound_and_legacy_module_style_calls():
    core, autonomous = _installed()
    assert autonomous._autonomous_update_signal({'ts': 1})['updated']['ts'] == 1
    assert autonomous._autonomous_update_signal(core, {'ts': 2})['updated']['ts'] == 2
    assert core.update_signal_with_bar({'ts': 3})['updated']['ts'] == 3
    assert [x['ts'] for x in core.update_calls] == [1, 2, 3]


def test_bad_arity_fails_loudly_instead_of_silently_misbinding_core():
    core, autonomous = _installed()
    try:
        autonomous._autonomous_analysis(core, {'x': 1}, {'x': 2})
    except TypeError as exc:
        assert 'expects' in str(exc)
    else:
        raise AssertionError('bad analysis arity must fail')
