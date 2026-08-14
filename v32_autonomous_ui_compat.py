from __future__ import annotations

"""Compatibility bridge so legacy status cards cannot contradict V30/V31 research."""

import time
from typing import Any

import v5_runtime
import v16_runtime_integrity as runtime_integrity
import v17_certification_orchestrator as cert17
import v18_final_system as final_system
import v18_operational_guard as operational_guard

_INSTALLED = False


def install(production: Any, autonomous: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    def autonomous_counts(c: Any) -> tuple[int, int]:
        champions = autonomous._load_registry(c, active_only=True)
        try:
            con = c.db(); samples = int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0] or 0); con.close(); c.state['learning_sample_total'] = samples
        except Exception:
            pass
        n = len(champions)
        # Every V30 Champion is already a complete Signal+Entry+SL+TP+management
        # package, so legacy Signal/Execution cards should show the same count.
        return n, n

    runtime_integrity._champion_counts = autonomous_counts

    base_certify = final_system.certify_and_execute
    def labeled_certify(c: Any, force: bool = False):
        lr = c.state.setdefault('learning', {})
        lr['phase'] = 'AUTONOMOUS_DIRECT_R_EVOLUTION_RUNNING'
        lr['blocker'] = None
        result = base_certify(c, force)
        status = autonomous.autonomous_status(c)
        if status.get('research_complete'):
            lr['phase'] = 'AUTONOMOUS_RESEARCH_COMPLETE' if status.get('champions') else 'AUTONOMOUS_RESEARCH_COMPLETE_NO_CERTIFIED_PACKAGE'
        return result

    final_system.certify_and_execute = labeled_certify
    operational_guard.certify_and_execute = labeled_certify
    cert17.train_v17 = labeled_certify
    v5_runtime.train_v5 = labeled_certify
    core.train_if_due = lambda force=False: labeled_certify(core, force)

    # The old Joint Strategy Research card belongs to the V28 compatibility layer.
    # Hide only that obsolete card; Point-in-Time/data-integrity cards remain visible.
    root = next((r for r in core.app.router.routes if getattr(r, 'path', None) == '/'), None)
    if root is not None and getattr(root, 'name', '') != 'autonomous_v32_dashboard':
        from fastapi.responses import HTMLResponse
        original = root.endpoint
        core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != '/']
        @core.app.get('/', response_class=HTMLResponse, name='autonomous_v32_dashboard')
        def dashboard() -> str:
            raw = original(); html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
            js = '''<script id="autonomous-v32-compat">function hideLegacyJoint(){for(const h of document.querySelectorAll('h2')){if((h.textContent||'').includes('Joint Strategy Research')){const card=h.closest('.card');if(card)card.style.display='none'}}}hideLegacyJoint();setInterval(hideLegacyJoint,5000);</script>'''
            return html.replace('</body>', js + '</body>') if '</body>' in html else html + js

    core.state['autonomous_ui_compat'] = {
        'installed': True,
        'legacy_joint_card_hidden': True,
        'legacy_champion_counts_mapped_to_complete_packages': True,
        'learning_phase_uses_autonomous_names': True,
        'updated_at': int(time.time()),
    }
