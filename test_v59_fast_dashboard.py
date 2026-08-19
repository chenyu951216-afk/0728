from __future__ import annotations

import v59_fast_dashboard as v59


def test_fast_shell_paints_without_waiting_for_api():
    html = v59._fast_shell()
    assert '<body>' in html
    assert '網頁已載入，正在讀取後端' in html
    assert 'Production Runtime V59' in html
    assert '/api/v59/runtime' in html
    assert 'AbortController' in html
    assert str(v59.FETCH_TIMEOUT_MS) in html
    assert 'window.fetch=' not in html


def test_full_dashboard_removes_v58_global_fetch_monkeypatch():
    source = '<html><head><script id="v58-dashboard-governor">window.fetch=function(){}</script></head><body>x</body></html>'
    cleaned = v59._strip_v58_fetch_monkeypatch(source)
    assert 'v58-dashboard-governor' not in cleaned
    assert 'window.fetch=function' not in cleaned
    assert '<body>x</body>' in cleaned


def test_shell_keeps_diagnostics_separate_and_refreshes_slowly():
    html = v59._fast_shell()
    assert '/dashboard/full' in html
    assert v59.REFRESH_MS >= 8000
    assert 'document.hidden' in html
    assert 'Stage 1–9 最終權威 / Runtime Convergence' in html
