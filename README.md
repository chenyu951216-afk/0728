# ETH 專屬 SMC / ICT 掃描器

只掃描 Gate `ETH_USDT` 現貨與 USDT 永續合約，產生繁體中文儀表板及 Discord 分級通知。程式不含下單、API 私鑰或提款功能。

## 功能

- Gate 公開 REST 真實資料：4H / 1H / 15M / 5M 已收線、現貨量、永續量、資金費率、訂單簿
- 確認 pivot、BOS、FVG、Fibonacci OTE、15M MSS、RVOL、訂單簿失衡
- 柔性 100 分評分；預設 72 分，避免條件過嚴完全沒訊號
- SQLite 訊號與通知去重；90 天快照保留
- Discord Level 1–4 分級通知
- `/api/backtest?days=30` 使用 Gate 真實歷史 K 線做無未來資料的快速驗證
- 健康檢查 `/health`、JSON 狀態 `/api/status`、互動 API 文件 `/docs`

## 本機啟動

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
copy .env.example .env
python app.py
```

開啟 `http://localhost:8080`。程式只使用公開行情，不需要 Gate API Key。若要 Discord 通知，在部署環境加入 `DISCORD_WEBHOOK_URL`。

## GitHub → Zeabur

1. 把本資料夾全部推到 GitHub。
2. Zeabur 建立 Project → Deploy New Service → GitHub，選擇此 repository。
3. Zeabur 會自動讀取 `Dockerfile`。新增環境變數 `TZ=Asia/Taipei`、`SIGNAL_MIN_SCORE=72`，需要通知時再加 `DISCORD_WEBHOOK_URL`。
4. 建立網域後開啟首頁。健康檢查路徑為 `/health`。
5. 若希望重啟後保留 SQLite 歷史，掛載持久磁碟到 `/data`，並設 `DATABASE_PATH=/data/eth_scanner.db`。

`PORT` 由 Zeabur 自動注入，程式監聽 `0.0.0.0`。若正式環境需要多人存取，建議在 Zeabur 網域層加 Access Control。

## 訊號原則

正式訊號仍要求方向、OTE 與 15M 結構確認；FVG、量能、OI 代理與訂單簿採加權，不會因單一輔助資料短暫不足而整套停擺。`SIGNAL_MIN_SCORE` 建議 68–78；低於 65 容易產生雜訊。

回測端點是核心趨勢回撤的快速 sanity check，並非完整逐筆成交模擬，也不構成績效承諾。本工具僅供量化研究與訊號提示，不構成投資建議。
