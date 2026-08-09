from __future__ import annotations

from typing import Any

import v5_runtime
import v9_final


READINESS_VERSION = '8.0.1-20260809'


def _coinglass_ready_through(core: Any) -> int | None:
    history = core.derivative_history
    if not getattr(history, 'coinglass_key', ''):
        return None
    keys = ('cg_cursor:oi_usd', 'cg_cursor:liq_long_usd', 'cg_cursor:book_imbalance')
    cursors = [int(history._get_state(k, core.START_TS) or core.START_TS) for k in keys]
    # Each cursor points to the next interval/window that still needs work. With the
    # strict 4h availability lag, using the minimum cursor as the decision watermark
    # is conservative: a decision cannot request an aggregated value from beyond the
    # region already processed (including provider windows explicitly found empty).
    return min(cursors) if cursors else core.START_TS


def install(core: Any) -> None:
    original = v5_runtime.generate_learning_samples_v5

    def watermarked_generate(c: Any, batch: int = 500) -> int:
        ready = _coinglass_ready_through(c)
        current = int(c.get_state(v5_runtime.REPLAY_STATE_KEY, c.START_TS) or c.START_TS)
        if ready is None:
            c.state.setdefault('learning', {})['derivative_replay_watermark'] = {
                'mode': 'explicit_missingness_no_coinglass',
                'blocked': False,
                'ready_through': None,
                'cursor': current,
            }
            return int(original(c, batch) or 0)

        stride_seconds = int(v9_final.REPLAY_STRIDE_BARS) * 900
        # Keep two decision strides of safety room so alignment to the next eligible
        # 15m decision can never jump past the processed derivative watermark.
        room = int(ready) - current - 2 * stride_seconds
        allowed = max(0, room // max(stride_seconds, 1))
        if allowed <= 0:
            c.state.setdefault('learning', {})['derivative_replay_watermark'] = {
                'mode': 'coinglass_watermarked',
                'blocked': True,
                'ready_through': int(ready),
                'cursor': current,
                'reason': 'waiting for historical derivative pages before creating more Signal labels',
            }
            return 0

        n = int(original(c, min(int(batch), int(allowed))) or 0)
        after = int(c.get_state(v5_runtime.REPLAY_STATE_KEY, current) or current)
        if after > int(ready):
            # This should be unreachable because of the safety margin. Fail loudly
            # instead of silently certifying samples whose derivative readiness is
            # uncertain.
            raise RuntimeError(f'strict replay exceeded derivative watermark: cursor={after} ready={ready}')
        c.state.setdefault('learning', {})['derivative_replay_watermark'] = {
            'mode': 'coinglass_watermarked',
            'blocked': False,
            'ready_through': int(ready),
            'cursor': after,
            'batch_limit_from_watermark': int(min(int(batch), int(allowed))),
        }
        return n

    v5_runtime.generate_learning_samples_v5 = watermarked_generate
    core.state['runtime_version'] = READINESS_VERSION
    core.state.setdefault('strict_replay', {})['derivative_watermark_required_when_coinglass_enabled'] = True
    core.state['strict_replay']['runtime'] = READINESS_VERSION
    core.app.version = '8.0.1'

    if not any(getattr(r, 'path', None) == '/api/v9/readiness' for r in core.app.router.routes):
        @core.app.get('/api/v9/readiness')
        def readiness_status() -> dict[str, Any]:
            ready = _coinglass_ready_through(core)
            current = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
            return {
                'runtime': READINESS_VERSION,
                'coinglass_enabled': bool(getattr(core.derivative_history, 'coinglass_key', '')),
                'derivative_ready_through': ready,
                'signal_replay_cursor': current,
                'replay_may_advance': ready is None or current < ready,
                'rule': 'when Coinglass is enabled, historical Signal replay cannot outrun processed derivative history',
            }
