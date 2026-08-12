import asyncio
from pathlib import Path

import runtime_identity
import v5_runtime
import v6_runtime
import v7_runtime
import v7_discord_runtime
import v8_evolution
import v8_execution_walkforward
import v8_stability
import v9_derivative_gate
import v9_final
import v9_live_parity
import v9_multisource_derivatives
import v9_readiness
import v9_training_store
import v10_final_integrity
import v10_overfit_guard
import v11_sqlite_stability
import v12_clean_baseline
import v13_replay_cursor_integrity
import v14_operational_throughput
import v15_data_resilience
import v16_runtime_integrity
import v17_certification_orchestrator
import v18_final_system
import v20_historical_signal_evolution
import v21_coinglass_standard
import v22_hierarchical_pipeline


def test_every_runtime_module_exports_the_single_public_version():
    versions = [
        v5_runtime.V5_VERSION,
        v6_runtime.V6_VERSION,
        v7_runtime.V7_VERSION,
        v8_evolution.EVOLUTION_VERSION,
        v8_execution_walkforward.WALKFORWARD_VERSION,
        v8_stability.STABILITY_VERSION,
        v9_derivative_gate.GATE_VERSION,
        v9_final.FINAL_VERSION,
        v9_live_parity.PARITY_VERSION,
        v9_multisource_derivatives.VERSION,
        v9_readiness.READINESS_VERSION,
        v9_training_store.STORE_VERSION,
        v10_final_integrity.VERSION,
        v10_overfit_guard.VERSION,
        v11_sqlite_stability.VERSION,
        v12_clean_baseline.VERSION,
        v13_replay_cursor_integrity.VERSION,
        v14_operational_throughput.VERSION,
        v15_data_resilience.VERSION,
        v16_runtime_integrity.VERSION,
        v17_certification_orchestrator.VERSION,
        v18_final_system.VERSION,
        v20_historical_signal_evolution.VERSION,
        v21_coinglass_standard.VERSION,
        v22_hierarchical_pipeline.VERSION,
    ]
    assert set(versions) == {runtime_identity.RUNTIME_VERSION}


def test_public_text_rewrites_obsolete_runtime_labels():
    text = runtime_identity.public_text(
        'ETH Adaptive AI 8.1 Final Replay Integrity｜v5 策略×方向｜v7 Execution Champion｜v5 runtime'
    )
    assert 'ETH Adaptive AI 10.2' in text
    assert '因果策略×方向' in text
    assert 'Execution Champion' in text
    assert '10.2 runtime' in text
    assert '8.1' not in text
    assert 'v5 策略' not in text
    assert 'v7 Execution' not in text


def test_active_dashboard_uses_latest_api_and_hides_legacy_schema_label():
    html = Path('dashboard_v721.html').read_text(encoding='utf-8')
    assert 'ETH Adaptive AI 10.2' in html
    assert '/api/latest/champions' in html
    assert '/api/latest/execution' in html
    assert '/api/latest/trade-monitor' in html
    assert 'Causal sample schema' in html
    assert 'Legacy v5 schema' not in html
    assert '/api/v5/champions' not in html


def test_unified_discord_boot_notice_is_deduplicated(monkeypatch):
    class Core:
        def __init__(self):
            self.state = {}
            self.saved = {}

        def get_state(self, key, default=None):
            return self.saved.get(key, default)

        def set_state(self, key, value):
            self.saved[key] = value

    sent = []

    async def fake_send(_core, title, body, _color):
        sent.append((title, body))
        await asyncio.sleep(0)
        return True

    core = Core()
    monkeypatch.setattr(v22_hierarchical_pipeline, 'price_collection_gate', lambda _core: {'percent': 42.0})
    monkeypatch.setattr(v5_runtime, 'robust_send_discord', fake_send)

    async def run():
        await asyncio.gather(
            v22_hierarchical_pipeline._unified_boot_notice(core),
            v22_hierarchical_pipeline._unified_boot_notice(core),
        )
        await v22_hierarchical_pipeline._unified_boot_notice(core)

    asyncio.run(run())
    assert len(sent) == 1
    assert '10.2' in sent[0][0]
    assert core.saved['discord_boot_public_runtime'] == runtime_identity.RUNTIME_VERSION


def test_production_discord_sender_rewrites_old_title_and_footer(monkeypatch):
    captured = []

    class Response:
        def raise_for_status(self):
            return None

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, _url, **kwargs):
            captured.append(kwargs['json'])
            return Response()

    class Core:
        state = {}

    monkeypatch.setenv('DISCORD_WEBHOOK_URL', 'https://discord.invalid/test')
    monkeypatch.setattr(v7_discord_runtime.httpx, 'AsyncClient', Client)
    ok = asyncio.run(v7_discord_runtime.robust_send(
        Core(),
        '🛡️ ETH Adaptive AI 8.1 Final Replay Integrity 已啟動',
        'v5 策略×方向｜v7 Execution Champion',
    ))
    assert ok is True
    embed = captured[0]['embeds'][0]
    assert 'ETH Adaptive AI 10.2' in embed['title']
    assert '8.1' not in embed['title']
    assert '因果策略×方向' in embed['description']
    assert 'v7 Execution' not in embed['description']
    assert embed['footer']['text'].startswith('ETH Adaptive AI 10.2.0-20260813')
    assert Core.state['discord']['runtime'] == runtime_identity.RUNTIME_VERSION
