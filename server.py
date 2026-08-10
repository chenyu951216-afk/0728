from pathlib import Path

import uvicorn
from fastapi.responses import HTMLResponse

import app as core
from v5_async_runtime import install_async
from v5_runtime import install as install_v5
from v7_timesafe_learning import install as install_timesafe_learning
from v7_signal_learner import install as install_signal_learner
from v7_execution_alignment import install as install_execution_alignment
from v7_fine_execution import install as install_fine_execution
from v7_runtime import install as install_v7
from v7_reentry_guard import install as install_reentry_guard
from v7_discord_runtime import install as install_discord_runtime
from v7_post_exit import install as install_post_exit
from v7_live_health import install as install_live_health
from v7_learning_guard import install as install_learning_guard
from v7_trade_monitor import install as install_trade_monitor
from v7_timeout_guard import install as install_timeout_guard
from v7_trade_feed import install as install_trade_feed
from v7_monitor_gate import install as install_monitor_gate
from v8_migration import install as install_evolution_migration
from v8_evolution import install as install_evolution
from v8_execution_oof import install as install_execution_oof
from v8_execution_walkforward import install as install_execution_walkforward
from v8_notice import install as install_evolution_notice
from v8_storage_guard import install as install_storage_guard, install_early as install_storage_guard_early
from v8_stability import install as install_stability
from v9_final import install as install_strict_final
from v9_readiness import install as install_replay_readiness
from v9_training_store import install as install_training_store
from v9_live_parity import install as install_live_parity
from v9_derivative_gate import install as install_derivative_gate
from v9_multisource_derivatives import install as install_multisource_derivatives
from v9_multisource_integrity import install as install_multisource_integrity
from v10_final_integrity import install as install_final_integrity
from v10_source_freeze import install as install_source_freeze
from v10_overfit_guard import install as install_overfit_guard
from v10_notice import install as install_final_notice
from v11_sqlite_stability import install as install_sqlite_stability
from v12_clean_baseline import install as install_clean_baseline
from v13_replay_cursor_integrity import install as install_replay_cursor_integrity
from v14_operational_throughput import install as install_operational_throughput
from v15_data_resilience import install as install_data_resilience

install_storage_guard_early(core)
install_v5(core)
install_async(core)

if core.get_state('v5_sample_schema') != 2:
    pit_schema = int(core.get_state('point_in_time_sample_schema', 0) or 0)
    con = core.db()
    sample_count = int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0])
    if pit_schema >= 4 and sample_count > 0:
        con.close()
        core.set_state('v5_sample_schema', 2)
    else:
        con.execute('DROP TABLE IF EXISTS learning_samples_v4_archive')
        con.execute('CREATE TABLE learning_samples_v4_archive AS SELECT * FROM learning_samples')
        con.execute('DELETE FROM learning_samples')
        con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION'")
        con.commit(); con.close()
        core.set_state('last_learning_sample_ts_v2', core.START_TS)
        core.set_state('v5_last_train_sample_total', 0)
        core.set_state('last_train_ts_v5', 0)
        core.set_state('v5_sample_schema', 2)

con = core.db()
con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION' AND direction NOT IN ('LONG','SHORT')")
con.commit(); con.close()

install_timesafe_learning(core)
install_signal_learner(core)
install_execution_alignment()
install_fine_execution()
install_v7(core)
install_reentry_guard()
install_discord_runtime(core)
install_post_exit(core)
install_live_health(core)
install_learning_guard(core)
install_trade_monitor(core)
install_timeout_guard()
install_trade_feed(core)
install_monitor_gate(core)
install_evolution_migration(core)
install_evolution(core)
install_execution_oof()
install_execution_walkforward(core)
install_evolution_notice(core)
install_storage_guard(core)
install_stability(core)
install_strict_final(core)
install_replay_readiness(core)
install_training_store(core)
install_live_parity(core)
install_derivative_gate(core)
install_multisource_derivatives(core)
install_multisource_integrity(core)
install_final_integrity(core)
install_source_freeze(core)
install_overfit_guard(core)
install_final_notice(core)
install_sqlite_stability(core)
install_clean_baseline(core)
install_replay_cursor_integrity(core)
install_operational_throughput(core)
# Final authority: provider capability ranges, canonical multi-exchange price fallback,
# targeted gap repair/quarantine, and model-feature source consistency.
install_data_resilience(core)

RUNTIME_VERSION = '8.3.0-20260810'
core.state['runtime_version'] = RUNTIME_VERSION
core.state.setdefault('strict_replay', {})['runtime'] = RUNTIME_VERSION
core.state['strict_replay']['learning_scheduler'] = {
    'price_backfill_nonexclusive': True,
    'price_backfill_failure_isolated': True,
    'derivative_failure_uses_only_previous_safe_watermark': True,
    'readiness_diagnostics_preserved': True,
    'core_sources_frozen_before_replay': True,
    'optional_source_cannot_deadlock': True,
    'provider_retention_limit_cannot_deadlock': True,
    'internal_price_gap_targeted_repair': True,
    'unresolved_price_gap_cannot_advance_replay_cursor': True,
    'confirmed_unrecoverable_gap_can_be_audited_and_omitted_without_fabrication': True,
    'fixed_price_fallback_priority': ['gate', 'bybit', 'binance', 'okx'],
    'feature_builder_contract_verified': True,
    'throughput_io_optimized': True,
    'performance_patch_resets_clean_dataset': False,
}
core.app.version = '8.3.0'

app = core.app
PORT = core.PORT

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    html = Path('dashboard_v721.html').read_text(encoding='utf-8')
    html = (
        html.replace('ETH Adaptive AI 7.2.1', 'ETH Adaptive AI 8.3.0 Final Data Resilience')
        .replace(
            'Walk-Forward Evolution · Storage Identity Guard · Subsystem-Isolated Fail-Closed',
            'Clean Dataset · No-Lookahead Strict Replay · Multi-Exchange Gap Recovery · Provider Capability Guard · Anti-Overfit OOS',
        )
    )
    html = html.replace('2020→現在 K 線覆蓋（直接查 DB）', '原始價格資料覆蓋（必要時框）')
    html = html.replace('無 HTF look-ahead 樣本重播', 'Strict Replay 時間游標進度')
    html = html.replace(
        '.s{font-size:11px;color:var(--muted);margin-top:5px;line-height:1.45}',
        '.s{font-size:11px;color:var(--muted);margin-top:5px;line-height:1.45;overflow-wrap:anywhere;word-break:break-word}',
    )
    html = html.replace(
        '.health{background:var(--card2);border:1px solid #1a3355;border-radius:13px;padding:11px}',
        '.health{background:var(--card2);border:1px solid #1a3355;border-radius:13px;padding:11px;min-width:0;overflow:hidden}',
    )
    html = html.replace(
        '.health .stat{font-size:13px;font-weight:900;margin:6px 0}',
        '.health .stat{font-size:13px;font-weight:900;margin:6px 0;overflow-wrap:anywhere;word-break:break-word;line-height:1.25}',
    )
    html = html.replace(
        '@media(max-width:390px){.healthgrid{grid-template-columns:1fr}.row b{max-width:60%}}',
        '@media(max-width:560px){.healthgrid{grid-template-columns:1fr}.row b{max-width:62%}.health{padding:13px}.health .stat{font-size:14px}.top{display:grid;grid-template-columns:minmax(0,1fr);gap:10px;align-items:start}.top>div{min-width:0}.top h1{overflow-wrap:anywhere;word-break:break-word}.badge{justify-self:start;position:static;padding:7px 10px;font-size:11px}}@media(max-width:360px){.hero{grid-template-columns:1fr}.row b{max-width:58%}}',
    )
    html = html.replace(
        '<div class="k">模型信心 / 門檻</div>',
        '<div id="probTitle" class="k">研究分數（未認證）</div>',
    )
    html = html.replace(
        '<div id="storageBox"></div><div id="storageNotice" class="notice"></div>',
        '<div id="storageBox"></div><div id="storageNotice" class="notice"></div><div id="baselineNotice" class="notice">讀取 Dataset provenance…</div>',
    )
    html = html.replace(
        "function hcard(name,h){h=h||{};let status=h.status||'BOOTING',cls=status==='OK'?'ok':status==='BOOTING'?'warn':'bad';return `<div class=\"health\"><div class=\"name\">${esc(name)}</div><div class=\"stat ${cls}\">${esc(status)}</div><div class=\"tiny\">連續錯誤 ${h.consecutive_errors||0}<br>最後成功 ${tm(h.last_success_at)}${h.last_error?`<br>錯誤：${esc(h.last_error)}`:''}</div></div>`}",
        "function hcard(name,h){h=h||{};let status=h.status||'BOOTING',good=['OK','RUNNING'].includes(status),waiting=status==='BOOTING'||status==='RETRYING'||status.startsWith('WAITING_'),cls=good?'ok':waiting?'warn':'bad';return `<div class=\"health\"><div class=\"name\">${esc(name)}</div><div class=\"stat ${cls}\">${esc(status)}</div><div class=\"tiny\">連續錯誤 ${h.consecutive_errors||0}<br>最後成功 ${tm(h.last_success_at)}${h.last_error?`<br>錯誤：${esc(h.last_error)}`:''}</div></div>`}",
    )
    html = html.replace(
        "$('prob').textContent=pc(sel.probability);$('threshold').textContent=`門檻 ${pc(sel.threshold)} · ${sel.validation_stack||'等待認證'}`;",
        "let certified=(c||[]).some(z=>z.strategy===sel.strategy&&z.direction===sel.direction);$('probTitle').textContent=certified?'Champion 信心 / 門檻':'研究分數（未認證）';$('prob').textContent=pc(sel.probability);$('threshold').textContent=certified?`門檻 ${pc(sel.threshold)} · ${sel.validation_stack||'已認證'}`:'尚無同方向 Signal Champion · 此分數僅供研究，不能觸發下單';",
    )
    html = html.replace(
        '<div id="learnMeta" style="margin-top:12px"></div><div id="learnError"></div></section>',
        '<div id="learnMeta" style="margin-top:12px"></div><div id="learnError"></div><details><summary>查看資料來源 / gap readiness</summary><pre id="derivSources">—</pre></details></section>',
    )
    html = html.replace(
        "row('最新市場',tm(rp.latest_market_ts));$('learnError').innerHTML=lr.error?`<div class=\"notice r\"><b>Learning error：</b>${esc(lr.error)}</div>`:'';",
        "row('最新市場',tm(rp.latest_market_ts))+row('Learning phase',lr.phase||lr.runtime_status||'—')+row('本輪新增樣本',lr.v5_samples_added??0)+row('價格補資料目標',lr.price_backfill_target?(lr.price_backfill_target.asset+' '+lr.price_backfill_target.tf):'無')+row('Core source freeze',(lr.derivative_backfill||{}).core_frozen?'已鎖定':'等待核心來源完成')+row('Frozen OI',((lr.derivative_backfill||{}).frozen_core_oi||[]).join(', ')||'模型全代遮罩')+row('Frozen funding',((lr.derivative_backfill||{}).frozen_core_funding||[]).join(', ')||'模型全代遮罩')+row('Frozen enrichment',((lr.derivative_backfill||{}).frozen_enrichment||[]).join(', ')||'模型全代遮罩')+row('Price gap',((lr.price_gap_repair||{}).status)||(((lr.price_gap_summary||{}).counts||{}).PENDING_REPAIR?'修復中':'無'));let pb=lr.replay_price_blocker||{};$('learnError').innerHTML=lr.error?`<div class=\"notice r\"><b>Learning error：</b>${esc(lr.error)}</div>`:pb.blocked?`<div class=\"notice y\"><b>Strict Replay 正在修復真實價格缺口：</b>${esc(pb.reason||'price gap')}<br>時間：${tm(pb.at_ts)}</div>`:lr.blocker?`<div class=\"notice y\"><b>目前學習狀態：</b>${esc(lr.blocker)}</div>`:'';if($('derivSources'))$('derivSources').textContent=JSON.stringify({derivatives:lr.derivative_backfill||{},resilience:lr.data_resilience||{},gaps:lr.price_gap_summary||{},provider_notices:lr.provider_notices||[]},null,2);",
    )
    html = html.replace(
        "async function setEq(){",
        "async function refreshBaseline(){try{let b=await fetch('/api/v12/baseline').then(r=>r.json()),el=$('baselineNotice');if(!el)return;el.className='notice '+(b.clean?'g':'r');el.innerHTML=b.clean?`<b>Final Clean Baseline：CLEAN</b><br>Dataset ID：${esc(b.dataset_id||'—')}<br>此資料集可進行正式 Champion 認證。`:`<b>Final Clean Baseline：LEGACY_CARRYOVER</b><br>${esc(b.reason||'舊 raw cache 存在')}<br><b>目前正式 Champion 認證與新單已 fail-closed。</b>`}catch(e){let el=$('baselineNotice');if(el){el.className='notice r';el.textContent='Dataset provenance 讀取失敗：'+String(e)}}}\nasync function setEq(){",
    )
    html = html.replace(
        "refresh();setInterval(refresh,5000);",
        "refresh();refreshBaseline();setInterval(refresh,5000);setInterval(refreshBaseline,10000);",
    )
    return html


if __name__ == '__main__':
    uvicorn.run('server:app', host='0.0.0.0', port=PORT)
