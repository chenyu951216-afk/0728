from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime
from typing import Any

import httpx
import runtime_identity

from adaptive_v5 import (
    STRATEGIES, DIRECTIONS, Learner, ModelStore, adaptive_entry,
    baseline_direction_scores, build_features, detect_regime, risk_plan, choose_strategy,
)

V5_VERSION = runtime_identity.RUNTIME_VERSION
REPLAY_STATE_KEY = 'last_learning_sample_ts_v2'


def _fmt(x: Any, digits: int = 2) -> str:
    try:
        return f"{float(x):,.{digits}f}"
    except Exception:
        return '—'


def _signal_row(core: Any, signal_id: str) -> dict[str, Any] | None:
    con = core.db(); raw = con.execute('SELECT * FROM signals WHERE signal_id=?', (signal_id,)).fetchone(); con.close()
    if not raw: return None
    row = dict(raw); row['targets'] = json.loads(row['targets']) if isinstance(row.get('targets'), str) else row.get('targets', []); row['payload'] = json.loads(row['payload']) if isinstance(row.get('payload'), str) else row.get('payload', {})
    return row


def _all_champions(core: Any) -> list[dict[str, Any]]:
    con = core.db(); rows = con.execute("SELECT strategy,direction,version,created_at,metrics FROM model_registry WHERE status='CHAMPION' ORDER BY strategy,direction,version DESC").fetchall(); con.close(); seen=set(); out=[]
    for row in rows:
        key=(row[0],row[1])
        if key in seen: continue
        seen.add(key); meta=json.loads(row[4]); out.append({'strategy':row[0],'direction':row[1],'version':row[2],'created_at':row[3],**meta})
    return out


def _sample_counts(core: Any) -> dict[str, Any]:
    con=core.db(); out={}
    for strategy in STRATEGIES:
        out[strategy]={}
        for direction in DIRECTIONS: out[strategy][direction]=int(con.execute('SELECT COUNT(*) FROM learning_samples WHERE strategy=? AND direction=?',(strategy,direction)).fetchone()[0])
    con.close(); return out


def _replay_progress(core: Any) -> dict[str, Any]:
    cursor=int(core.get_state(REPLAY_STATE_KEY,core.START_TS) or core.START_TS); source=core._best_source('ETH','15m'); rows=core.load_bars('ETH','15m',source,limit=2) if source else []; latest=int(rows[-1]['ts']) if rows else int(time.time()); pct=100*max(0,cursor-core.START_TS)/max(1,latest-core.START_TS)
    return {'cursor_ts':cursor,'latest_market_ts':latest,'percent':round(min(100.,pct),2),'complete':cursor>=latest-30*900,'schema':2}


async def robust_send_discord(core: Any, title: str, body: str, color: int = 6000633) -> bool:
    webhook=os.getenv('DISCORD_WEBHOOK_URL','') or getattr(core,'DISCORD_WEBHOOK_URL',''); bot=os.getenv('DISCORD_BOT_TOKEN','') or getattr(core,'DISCORD_BOT_TOKEN',''); channel=os.getenv('DISCORD_CHANNEL_ID','') or getattr(core,'DISCORD_CHANNEL_ID','')
    title=runtime_identity.public_text(title); body=runtime_identity.public_text(body)
    payload={'embeds':[{'title':title[:256],'description':body[:4000],'color':color,'timestamp':datetime.now(core.timezone.utc).isoformat(),'footer':{'text':f'{runtime_identity.PRODUCT_NAME} {runtime_identity.RUNTIME_VERSION} | research/paper only'}}]}; errors=[]
    async with httpx.AsyncClient(timeout=15) as client:
        if webhook:
            for attempt in range(3):
                try:
                    response=await client.post(webhook,json=payload); response.raise_for_status(); core.state['discord']={'configured':True,'ok':True,'route':'webhook','last_success':datetime.now(core.timezone.utc).isoformat(),'error':None,'runtime':runtime_identity.RUNTIME_VERSION}; return True
                except Exception as exc:
                    errors.append(f'webhook#{attempt+1}:{exc}'); await asyncio.sleep(.7*(attempt+1))
        if bot and channel:
            for attempt in range(3):
                try:
                    response=await client.post(f'{core.DISCORD_API}/channels/{channel}/messages',headers={'Authorization':f'Bot {bot}'},json=payload); response.raise_for_status(); core.state['discord']={'configured':True,'ok':True,'route':'bot','last_success':datetime.now(core.timezone.utc).isoformat(),'error':None,'runtime':runtime_identity.RUNTIME_VERSION}; return True
                except Exception as exc:
                    errors.append(f'bot#{attempt+1}:{exc}'); await asyncio.sleep(.7*(attempt+1))
    core.state['discord']={'configured':bool(webhook or (bot and channel)),'ok':False,'route':None,'last_success':None,'error':'; '.join(errors)[-1200:] or 'Discord not configured','runtime':runtime_identity.RUNTIME_VERSION}; return False


def generate_learning_samples_v5(core: Any, batch: int = 500) -> int:
    src15=core._best_source('ETH','15m'); src1h=core._best_source('ETH','1h'); src4h=core._best_source('ETH','4h'); src1d=core._best_source('ETH','1d'); srcbtc=core._best_source('BTC','1h')
    if not all((src15,src1h,src4h,src1d,srcbtc)): return 0
    m15=core.load_bars('ETH','15m',src15); h1=core.load_bars('ETH','1h',src1h); h4=core.load_bars('ETH','4h',src4h); d1=core.load_bars('ETH','1d',src1d); btc=core.load_bars('BTC','1h',srcbtc)
    if min(map(len,(m15,h1,h4,d1,btc)))<120: return 0
    ts15=[x['ts'] for x in m15];ts1=[x['ts'] for x in h1];ts4=[x['ts'] for x in h4];tsd=[x['ts'] for x in d1];tsb=[x['ts'] for x in btc]
    import bisect
    last_ts=int(core.get_state(REPLAY_STATE_KEY,core.START_TS) or core.START_TS); start_i=max(100,bisect.bisect_right(ts15,last_ts)); con=core.db(); store=ModelStore(con); learner=Learner(store); created=processed=0; newest=last_ts
    for i in range(start_i,len(m15)-25):
        if i%4: continue
        ts=ts15[i]; d1s=core._slice_to(d1,tsd,ts,80,420); h4s=core._slice_to(h4,ts4,ts,100,900); h1s=core._slice_to(h1,ts1,ts,100,1000); btcs=core._slice_to(btc,tsb,ts,50,500); m15s=m15[max(0,i-500):i+1]
        if not all((d1s,h4s,h1s,btcs)): continue
        regime=detect_regime(d1s,h4s,h1s); extras=core.derivative_history.extras_at(ts); extras['source_agreement_bps']=10.; features=build_features(m15s,h1s,btcs,regime,extras); priors=baseline_direction_scores(features,regime); quality=max(60.,(82. if src15=='gate' else 74.)*(.85+.15*extras.get('derivative_coverage',0.)))
        for strategy,dirs in priors.items():
            for direction,prior in dirs.items():
                if prior<.12: continue
                success,pnl_r,mfe_r,mae_r=learner.strategy_outcome(m15,i,strategy,direction,24); store.add_sample({'ts':ts,'strategy':strategy,'direction':direction,'regime':regime['regime'],'phase':regime['phase'],'features':features,'success':success,'pnl_r':pnl_r,'mfe_r':mfe_r,'mae_r':mae_r,'source_quality':quality}); created+=1
        processed+=1;newest=ts
        if processed>=batch: break
    store.commit();con.close()
    if newest>last_ts: core.set_state(REPLAY_STATE_KEY,newest)
    return created


def create_signal_v5(core: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]) -> dict[str, Any] | None:
    selection=analysis['selection']
    if not selection.get('tradeable'): return None
    current=core.latest_signal()
    if current:return current
    con=core.db();store=ModelStore(con);entry=adaptive_entry(store,selection['strategy'],analysis['regime']['regime'],selection['direction'],analysis['price'],m15);plan=risk_plan(store,selection['strategy'],analysis['regime']['regime'],selection['direction'],entry,m15);con.close();signal_id=f"{int(time.time())}-{selection['strategy'][:4]}-{selection['direction'][0]}"
    payload={'initial_plan':plan,'selection':selection,'regime':analysis['regime'],'features':analysis.get('features',{}),'data_quality':float((analysis.get('data_quality') or {}).get('score',0)),'created_from_snapshot':analysis.get('snapshot_ts'),'immutable':True,'model_schema_version':2,'management':{'hit_targets':[],'mfe_r':0.,'mae_r':0.,'trail_reason':None}}
    con=core.db();con.execute('INSERT INTO signals(signal_id,created_at,updated_at,status,strategy,direction,regime,phase,probability,entry,initial_stop,current_stop,targets,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(signal_id,int(time.time()),int(time.time()),'PLANNED',selection['strategy'],selection['direction'],analysis['regime']['regime'],analysis['regime']['phase'],selection['probability'],plan['entry'],plan['stop'],plan['stop'],json.dumps(plan['targets']),json.dumps(payload,ensure_ascii=False)));con.commit();con.close();return core.latest_signal()


def _training_due(core: Any, force: bool = False) -> tuple[bool,int]:
    con=core.db(); total=int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0]);con.close();last_count=int(core.get_state('v5_last_train_sample_total',0) or 0);last_ts=int(core.get_state('last_train_ts_v5',0) or 0);replay=_replay_progress(core)
    if force:return True,total
    if replay['complete'] and total>last_count:return True,total
    if total-last_count>=15000:return True,total
    if total>last_count and time.time()-last_ts>=6*3600:return True,total
    return False,total


def train_v5(core: Any, force: bool = False) -> list[dict[str, Any]]:
    due,total=_training_due(core,force)
    if not due:return []
    con=core.db();store=ModelStore(con);learner=Learner(store);results=learner.train_all();con.close();core.set_state('last_train_ts_v5',int(time.time()));core.set_state('v5_last_train_sample_total',total);out=[x.__dict__ for x in results];core.state['last_training']=out;return out


async def _notify_promotions(core: Any, training: list[dict[str, Any]]) -> None:
    for item in training:
        if not item.get('promoted'):continue
        await robust_send_discord(core,f"🏆 Champion 升級｜{item['strategy']} {item['direction']}",f"新模型已通過 nested purged OOS 驗證。\nPF `{item['profit_factor']:.2f}`｜EV `{item['expectancy_r']:+.3f}R`｜勝率 `{item['test_win']:.1%}`\nOOS 入選 `{item['selected_n']}` 筆｜門檻 `{item['threshold']:.0%}`｜最大回撤 `{item['max_drawdown_r']:.1f}R`\n只有驗證後仍有正期望的模型才會取代舊 Champion。",0x2ECC71)


async def maybe_daily_report(core: Any) -> None:
    now=datetime.now(core.TAIPEI)
    if now.hour<17:return
    day=now.strftime('%Y-%m-%d')
    if core.get_state('last_daily_report_v5')==day:return
    champions=_all_champions(core);counts=_sample_counts(core);start=int(datetime(now.year,now.month,now.day,tzinfo=core.TAIPEI).timestamp());con=core.db();trades=con.execute('SELECT status,strategy,direction,realized_r FROM signals WHERE created_at>=?',(start,)).fetchall();con.close();closed=[x for x in trades if x[0]=='CLOSED'];pnl=sum(float(x[3] or 0) for x in closed);lines=[]
    for c in champions[:12]:lines.append(f"• `{c['strategy']} {c['direction']}` v{c['version']}｜PF {float(c.get('profit_factor') or 0):.2f}｜EV {float(c.get('expectancy_r') or 0):+.3f}R｜門檻 {float(c.get('threshold') or 0):.0%}｜約 {float(c.get('signals_per_day') or 0):.2f}/日")
    total_samples=sum(v for d in counts.values() for v in d.values());replay=_replay_progress(core);body=f"台灣時間 `{now.strftime('%Y-%m-%d %H:%M')}`\n歷史價格覆蓋 `{(core.state.get('learning') or {}).get('progress',{}).get('overall',0):.2f}%`｜因果策略×方向重播 `{replay['percent']:.2f}%`\n總學習樣本 `{total_samples:,}`｜Champion `{len(champions)}` 個\n今日訊號 `{len(trades)}`｜已結束 `{len(closed)}`｜合計 `{pnl:+.2f}R`\n\n"+("\n".join(lines) if lines else '目前尚無通過 OOS 的 Champion。')
    if await robust_send_discord(core,'📊 ETH Adaptive AI 每日 17:00 學習報告',body,0x5865F2):core.set_state('last_daily_report_v5',day)


async def maybe_boot_notice(core: Any) -> None:
    if core.get_state('discord_boot_version')==V5_VERSION:return
    if await robust_send_discord(core,f'✅ {runtime_identity.PRODUCT_NAME} {runtime_identity.DISPLAY_VERSION} 已啟動','方向分流學習、nested OOS 門檻、Regime 盈利過濾、Champion/Challenger、歷史重播與 Discord 交易生命週期通知已啟用。\n沒有通過驗證的策略不會硬發交易訊號。',0x3498DB):core.set_state('discord_boot_version',V5_VERSION)


async def learning_tick_v5(core: Any) -> None:
    live_added=core.ingest_completed_live_samples();con=core.db();progress=core.bootstrap_progress(con);con.close();chosen=None
    for asset,tf in core.BACKFILL_PLAN:
        earliest=core._earliest(asset,tf)
        if earliest is None or earliest>core.START_TS+2*core.TIMEFRAME_SECONDS[tf]:chosen=(asset,tf);break
    backfill_result=derivative_result=None;samples=0;training=[]
    if chosen:backfill_result=await core.backfill_one(*chosen)
    else:
        core.derivative_history.set_db_path(core.DB_PATH);derivative_result=await core.derivative_history.backfill_tick(core.hub,core.START_TS,pages=max(1,min(5,core.BACKFILL_PAGES_PER_TICK)));samples=generate_learning_samples_v5(core);training=train_v5(core)
        if training:await _notify_promotions(core,training)
    con=core.db();progress=core.bootstrap_progress(con);con.close();champions=_all_champions(core);counts=_sample_counts(core);replay=_replay_progress(core);core.state['learning']={'progress':progress,'historical_price_coverage':progress.get('overall',0),'replay_learning_progress':replay,'backfill':backfill_result,'derivatives':core.derivative_history.status(),'derivative_backfill':derivative_result,'live_samples_added':live_added,'causal_samples_added':samples,'v5_samples_added':samples,'sample_counts':counts,'champions':champions,'recent_rejected':[x for x in training if not x.get('promoted')][:12],'learning_order':['1D/4H regime','1H/30M structure','15M/5M execution','derivatives','post-exit review'],'model_schema_version':2};await maybe_boot_notice(core);await maybe_daily_report(core)


def _signal_summary(core: Any, row: dict[str, Any]) -> str:
    sizing=core._notional_for_risk(float(row['entry']),float(row['initial_stop']));targets=row.get('targets') or [];target_text='｜'.join(f"TP{i+1} {_fmt(x.get('price'))} ({_fmt(x.get('rr'))}R/{x.get('allocation')}%)" for i,x in enumerate(targets));notional=sizing.get('notional_usdt')
    return f"`{row['direction']}`｜`{row['strategy']}`｜{row['regime']}/{row['phase']}\n機率 `{float(row['probability']):.1%}`｜Entry `{_fmt(row['entry'])}`｜初始 SL `{_fmt(row['initial_stop'])}`｜目前 SL `{_fmt(row['current_stop'])}`\n{target_text}\n2% 風險名目金額：`{_fmt(notional) if notional is not None else '請先設定帳戶餘額'} USDT`"


async def scan_v5(core: Any) -> dict[str, Any]:
    bundle=await core.hub.live_bundle();core.upsert_live_gate(bundle);analysis=core._analysis_from_bundle(bundle);now=int(time.time());con=core.db();con.execute('INSERT INTO snapshots(ts,payload) VALUES(?,?)',(now,json.dumps(analysis,ensure_ascii=False)));con.execute('DELETE FROM snapshots WHERE ts<?',(now-120*86400,));con.commit();con.close();before=core.latest_signal();before_copy=json.loads(json.dumps(before,ensure_ascii=False)) if before else None;last_bar=bundle['eth_15m'][-1];core.update_signal_with_bar(last_bar);core.post_exit_review(last_bar);after_update=core.latest_signal()
    if before_copy:
        if before_copy['status']=='PLANNED' and after_update and after_update['signal_id']==before_copy['signal_id'] and after_update['status']=='OPEN':await robust_send_discord(core,'📥 ETH 訊號已成交 / 開始持倉監控',_signal_summary(core,after_update),0x3498DB)
        if before_copy['status']=='OPEN':
            current=_signal_row(core,before_copy['signal_id'])
            if current and current['status']=='CLOSED':await robust_send_discord(core,f"✅ ETH 持倉已結束｜{current.get('exit_reason')}",_signal_summary(core,current)+f"\n實現結果 `{float(current.get('realized_r') or 0):+.2f}R`｜出場 `{_fmt(current.get('exit_price'))}`\n系統會再追蹤 24h 檢討是否過早/過晚出場。",0x2ECC71 if float(current.get('realized_r') or 0)>=0 else 0xE74C3C)
            elif current and current['status']=='OPEN':
                old_hits=set((before_copy.get('payload') or {}).get('management',{}).get('hit_targets',[]));new_hits=set((current.get('payload') or {}).get('management',{}).get('hit_targets',[]))
                for idx in sorted(new_hits-old_hits):await robust_send_discord(core,f"🎯 ETH TP{idx+1} 已觸及",_signal_summary(core,current)+'\n剩餘部位依不可放寬止損規則管理。',0x2ECC71)
    active=core.latest_signal()
    if active is None:
        created=create_signal_v5(core,analysis,bundle['eth_15m'])
        if created and created['created_at']>=now-5:await robust_send_discord(core,'🆕 ETH OOS Champion 掛單訊號',_signal_summary(core,created)+'\n不追價；只有目前 Regime 也通過歷史 OOS 的方向模型才可建立。',0x4C8BF5)
    active=core.latest_signal();core.state.update(service='OK',updated_at=datetime.now(core.timezone.utc).isoformat(),error=None,scan_count=core.state['scan_count']+1,analysis=analysis,active_signal=active);return analysis


async def _reply_status(core: Any, text: str) -> None:await robust_send_discord(core,'🤖 Discord 指令回覆',text,0x5865F2)


async def poll_discord_commands(core: Any) -> None:
    token=os.getenv('DISCORD_BOT_TOKEN','') or getattr(core,'DISCORD_BOT_TOKEN','');channel=os.getenv('DISCORD_CHANNEL_ID','') or getattr(core,'DISCORD_CHANNEL_ID','');allowed=os.getenv('DISCORD_ALLOWED_USER_ID','') or getattr(core,'DISCORD_ALLOWED_USER_ID','')
    if not (token and channel):return
    cursor=core.get_state('discord_last_message_id_v5');params={'limit':30}
    if cursor:params['after']=str(cursor)
    try:
        async with httpx.AsyncClient(timeout=15) as client:r=await client.get(f'{core.DISCORD_API}/channels/{channel}/messages',headers={'Authorization':f'Bot {token}'},params=params);r.raise_for_status();messages=r.json()
    except Exception as exc:core.state['discord_commands']=f'ERROR: {exc}';return
    if not cursor:
        if messages:core.set_state('discord_last_message_id_v5',max(messages,key=lambda x:int(x['id']))['id'])
        core.state['discord_commands']='READY';return
    for message in sorted(messages,key=lambda x:int(x['id'])):
        core.set_state('discord_last_message_id_v5',str(message['id']));author=message.get('author') or {}
        if author.get('bot') or message.get('webhook_id'):continue
        if allowed and str(author.get('id',''))!=str(allowed):continue
        content=re.sub(r'\s+','',str(message.get('content','')).strip());match=re.search(r'(\d{3,}(?:\.\d+)?)',content);price=float(match.group(1)) if match else float((core.state.get('analysis') or {}).get('price') or 0)
        if content.startswith(('持倉狀態','持仓状态')):
            row=core.latest_signal();await _reply_status(core,_signal_summary(core,row) if row else '目前沒有 PLANNED / OPEN 訊號。')
        elif content.startswith(('已下單','已下单')):
            row=core.latest_signal(('PLANNED',))
            if not row:await _reply_status(core,'目前沒有經 Champion 驗證的 PLANNED 訊號，因此拒絕手動建立方向。');continue
            if ('多' in content and row['direction']!='LONG') or ('空' in content and row['direction']!='SHORT'):await _reply_status(core,f"方向不符：目前唯一可用訊號是 `{row['direction']}`，不允許用 Discord 繞過 AI 方向。");continue
            core.mark_filled(row['signal_id'],price if price>0 else None);opened=core.latest_signal(('OPEN',));await robust_send_discord(core,'📥 Discord 已確認成交',_signal_summary(core,opened),0x3498DB)
        elif content.startswith(('已出場','已出场')):
            row=core.latest_signal(('OPEN',))
            if not row:await _reply_status(core,'目前沒有 OPEN 持倉。');continue
            if price<=0:await _reply_status(core,'找不到有效出場價格，請輸入例如：`已出場 1920.5`。');continue
            core.mark_closed(row['signal_id'],price);closed=_signal_row(core,row['signal_id']);await robust_send_discord(core,'✅ Discord 已登記完整出場',_signal_summary(core,closed)+f"\n實現 `{float(closed.get('realized_r') or 0):+.2f}R`",0x2ECC71)
        elif content.startswith(('帳戶餘額','账户余额','資金','资金')):
            if price<=0:await _reply_status(core,'請輸入例如：`資金 1000`。');continue
            core.set_state('account_equity_usdt',price);await _reply_status(core,f"帳戶餘額已更新為 `{price:,.2f} USDT`；每筆初始止損風險仍為 `{core.RISK_PER_TRADE:.1%}`。")
        elif content in ('測試','测试','test','TEST'):await robust_send_discord(core,'✅ Discord 連線測試成功',f'{runtime_identity.RUNTIME_VERSION} runtime 正常｜{datetime.now(core.TAIPEI).strftime("%Y-%m-%d %H:%M:%S")}',0x2ECC71)
    core.state['discord_commands']='READY'


async def scan_worker_v5(core: Any) -> None:
    while True:
        try:await scan_v5(core);await poll_discord_commands(core)
        except Exception as exc:core.LOG.exception('v5 scan failed');core.state.update(service='DEGRADED',error=str(exc))
        await asyncio.sleep(core.SCAN_SECONDS)


def install(core: Any) -> None:
    core.STRATEGIES=STRATEGIES;core.ModelStore=ModelStore;core.Learner=Learner;core.choose_strategy=choose_strategy;core.risk_plan=risk_plan;core.send_discord=lambda title,body,color=6000633:robust_send_discord(core,title,body,color);core.generate_learning_samples=lambda batch=500:generate_learning_samples_v5(core,batch);core.create_signal=lambda analysis,m15:create_signal_v5(core,analysis,m15);core.train_if_due=lambda force=False:train_v5(core,force)
    async def learning_tick_wrapper():await learning_tick_v5(core)
    async def scan_wrapper():return await scan_v5(core)
    async def scan_worker_wrapper():await scan_worker_v5(core)
    core.learning_tick=learning_tick_wrapper;core.scan=scan_wrapper;core.scan_worker=scan_worker_wrapper;runtime_identity.stamp(core)
    if not any(getattr(route,'path',None)=='/api/discord/test' for route in core.app.router.routes):
        @core.app.post('/api/discord/test')
        async def discord_test() -> dict[str,Any]:
            ok=await robust_send_discord(core,'✅ Discord 連線測試成功',f'{runtime_identity.PRODUCT_NAME} {runtime_identity.RUNTIME_VERSION}｜台灣時間 {datetime.now(core.TAIPEI).strftime("%Y-%m-%d %H:%M:%S")}',0x2ECC71);return {'ok':ok,'discord':core.state.get('discord')}
    if not any(getattr(route,'path',None)=='/api/v5/champions' for route in core.app.router.routes):
        @core.app.get('/api/v5/champions')
        def v5_champions() -> list[dict[str,Any]]:return _all_champions(core)
