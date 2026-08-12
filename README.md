# ETH Adaptive AI 10.0

ETH 短線（非超短線）策略研究與訊號系統。系統會從 2020 年起做 point-in-time 歷史重播，在牛市、熊市、盤整、壓縮／擴張與反轉等不同 regime 中，分別演化 Signal 與 Entry/SL/TP；沒有通過未觸碰樣本驗證的結果不會進入正式訊號。

## 10.0 分層 Point-in-Time 學習流程

10.0 將學習拆成一條不可跳階的因果流程：先完成 ETH 1D/4H 宏觀趨勢，再學習 1H/30M 市場結構，最後才開放 15M/5M 短線入場。候選策略在 development history 內依 `MACRO_REGIME -> MARKET_STRUCTURE -> SHORT_HORIZON_SIGNAL` 順序演化，每個 lineage 的 sealed OOS 僅能開封一次。

價格來源會依預先固定的 Gate、Bybit、Binance、OKX、Bitget 優先序進行能力檢查與逐時點復原。來源不支援某段歷史時會切換下一來源，不會重複請求必然失敗的 API，也不會插值或拿現在的資料回填過去。

部署 10.0 不需要新的必填環境變數；現有 `COINGLASS_API_KEY` 與 `COINGLASS_PLAN=STANDARD` 可直接沿用。舊版派生樣本與 Champion 會在一次性特徵 schema 遷移後重建，原始市場與衍生品資料不會刪除。

1. 只用當下已收線的多交易所 K 線與當時已存在的衍生品資料建立特徵。
2. 每個策略方向是「族群入口」，不是固定的 14 個模型：族群會演化 feature set、regime scope、recency、模型複雜度與正則化。
3. mutation、選擇與 calibration 全部只在 development history 內完成，並使用 expanding walk-forward + purge。
4. winner 固定後，才打開一次 sealed chronological holdout。失敗後不可拿同一段資料重調再測；至少新增 `SIGNAL_MIN_UNTOUCHED_HOLDOUT` 個成熟決策才可進下一代。
5. 新挑戰者與舊 Champion 在同一段新 holdout 上比較；挑戰者沒有實質改善就保留舊 Champion。
6. Signal Champion 只決定方向與市場適用範圍。Entry/SL/TP 由獨立的 DEV-only Execution Evolution 學習，最後仍需 untouched execution audit。

## 止損／止盈

- Entry、結構止損、ATR／近期 true-range 噪音下限、分批 RR、移動保護與持倉期限都納入 Execution Evolution。
- 正式止損距離不得小於「學到的 ATR floor、近期市場噪音、最低百分比、交易成本倍數」四者最大值。預設 round-trip 成本最多只能占 1R 的 20%，因此像 ETH 1915 進場、1911 止損這種容易被正常波動與成本吃掉的方案不會直接產生。
- 止損下限不是獲利保證；它只排除在成本與噪音尺度上先天不合理的風險計畫。

## CoinGlass Standard

程式會在有 `COINGLASS_API_KEY` 時使用 Standard 可取得的歷史資料：跨交易所 OI、清算量、orderbook、OI-weighted funding、aggregated taker buy/sell、global account long/short 與 top-trader position long/short。每個來源先做 retention/full-span capability audit；只涵蓋近期的來源仍可背景收集，但不會在歷史中途突然加入模型造成假 regime。

CoinGlass liquidation heatmap 目前不是 Standard 權限。`COINGLASS_PLAN=STANDARD` 時程式明確不呼叫該端點；止損仍從歷史 MAE/MFE、清算、orderbook、taker 與 positioning 學習。若未來升級到 Professional/Enterprise，當前熱圖只能用於新單 live veto 和向前快照，不能回填過去或改寫已稽核的 SL。

## 本機啟動

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
python server_entry.py
```

預設服務位址為 `http://localhost:8080`。主要狀態端點：

- `/healthz`、`/readyz`：bootstrap 與 production readiness
- `/api/v18/final-status`：最終認證、Champion 與 regime portfolio
- `/api/v20/historical-evolution`：各 lineage 最近一次 holdout、genome 數與等待新資料狀態
- `/api/v21/coinglass-standard`：Standard capability、資料範圍與可用性
- `/api/v22/pipeline`：八階段真實總進度、目前 blocker、來源證據與 no-lookahead contract

部署時請使用持久磁碟並將 `DATABASE_PATH` 指向該磁碟。不要提交 `.env`、API key、Discord token 或 SQLite 資料庫。

## 重要限制

這是研究與訊號系統，不會保證「最強」或保證獲利。OOS、purge、一次性 holdout、cluster bootstrap、成本與 execution audit 能降低過擬合與不合理成交假設，但不能消除市場風險；上線前仍應 paper trade、限制單筆風險並監控 live drift。
