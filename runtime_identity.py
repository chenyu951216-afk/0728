from __future__ import annotations

import re
from typing import Any


PRODUCT_NAME = 'ETH Adaptive AI'
DISPLAY_VERSION = '10.2'
API_VERSION = '10.2.0'
BUILD_DATE = '20260813'
RUNTIME_VERSION = f'{API_VERSION}-{BUILD_DATE}'
PUBLIC_PIPELINE_NAME = 'Causal Full-History Learning'


def public_text(value: Any) -> str:
    """Remove obsolete implementation-version labels from user-facing text."""
    text = str(value or '')
    text = re.sub(
        r'ETH Adaptive AI(?:\s+v?\d+(?:\.\d+){0,3}(?:-\d+)?)?',
        f'{PRODUCT_NAME} {DISPLAY_VERSION}',
        text,
        flags=re.IGNORECASE,
    )
    replacements = {
        'v5 策略×方向': '因果策略×方向',
        'v5 重播': '因果歷史重播',
        'v5 runtime': f'{DISPLAY_VERSION} runtime',
        'v7 Execution Champion': 'Execution Champion',
        'v7 Point-in-Time': 'Point-in-Time',
        'v7 訊號': '正式訊號',
        'v7 持倉': '正式持倉',
        'v7 逐筆': '正式逐筆',
        'v7 TP': 'TP',
        'v7 已啟動': f'{DISPLAY_VERSION} 已啟動',
        'v6 Execution Champion': '舊版 Execution Champion',
        '舊 v6 PF': '舊版 PF',
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def stamp(core: Any) -> None:
    """Publish one active version without changing historical DB compatibility keys."""
    core.state['runtime_version'] = RUNTIME_VERSION
    core.state['public_runtime'] = {
        'product': PRODUCT_NAME,
        'display_version': DISPLAY_VERSION,
        'api_version': API_VERSION,
        'runtime_version': RUNTIME_VERSION,
        'pipeline': PUBLIC_PIPELINE_NAME,
        'single_public_version': True,
        'legacy_state_keys_are_storage_compatibility_only': True,
    }
    core.state.setdefault('strict_replay', {})['runtime'] = RUNTIME_VERSION
    core.app.version = API_VERSION
