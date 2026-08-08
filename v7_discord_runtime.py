from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any

import httpx

import v5_runtime


async def robust_send(core: Any, title: str, body: str, color: int = 6000633) -> bool:
    webhook = os.getenv('DISCORD_WEBHOOK_URL', '') or getattr(core, 'DISCORD_WEBHOOK_URL', '')
    bot = os.getenv('DISCORD_BOT_TOKEN', '') or getattr(core, 'DISCORD_BOT_TOKEN', '')
    channel = os.getenv('DISCORD_CHANNEL_ID', '') or getattr(core, 'DISCORD_CHANNEL_ID', '')
    version = str(core.state.get('runtime_version') or getattr(core.app, 'version', 'v7'))
    payload = {'embeds': [{'title': title[:256], 'description': body[:4000], 'color': color, 'timestamp': datetime.now(timezone.utc).isoformat(), 'footer': {'text': f'ETH Adaptive AI {version} | research/paper only'}}]}
    errors = []
    async with httpx.AsyncClient(timeout=15) as client:
        if webhook:
            for attempt in range(3):
                try:
                    r = await client.post(webhook, json=payload); r.raise_for_status(); core.state['discord'] = {'configured': True, 'ok': True, 'route': 'webhook', 'last_success': datetime.now(timezone.utc).isoformat(), 'error': None, 'runtime': version}; return True
                except Exception as exc:
                    errors.append(f'webhook#{attempt + 1}:{exc}'); await asyncio.sleep(.7 * (attempt + 1))
        if bot and channel:
            for attempt in range(3):
                try:
                    r = await client.post(f'{core.DISCORD_API}/channels/{channel}/messages', headers={'Authorization': f'Bot {bot}'}, json=payload); r.raise_for_status(); core.state['discord'] = {'configured': True, 'ok': True, 'route': 'bot', 'last_success': datetime.now(timezone.utc).isoformat(), 'error': None, 'runtime': version}; return True
                except Exception as exc:
                    errors.append(f'bot#{attempt + 1}:{exc}'); await asyncio.sleep(.7 * (attempt + 1))
    core.state['discord'] = {'configured': bool(webhook or (bot and channel)), 'ok': False, 'route': None, 'last_success': None, 'error': '; '.join(errors)[-1200:] or 'Discord not configured', 'runtime': version}; return False


async def no_legacy_boot_notice(core: Any) -> None:
    return None


def install(core: Any) -> None:
    async def sender(c: Any, title: str, body: str, color: int = 6000633) -> bool:
        return await robust_send(c, title, body, color)
    v5_runtime.robust_send_discord = sender
    v5_runtime.maybe_boot_notice = no_legacy_boot_notice
    core.send_discord = lambda title, body, color=6000633: sender(core, title, body, color)
