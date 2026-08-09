FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py adaptive_engine.py adaptive_v5.py execution_v6.py execution_v7.py market_data.py derivative_data.py v5_runtime.py v5_async_runtime.py v6_runtime.py v6_scan_runtime.py v7_runtime.py v7_timesafe_learning.py v7_signal_learner.py v7_execution_alignment.py v7_fine_execution.py v7_reentry_guard.py v7_discord_runtime.py v7_post_exit.py v7_live_health.py v7_learning_guard.py v7_trade_monitor.py v7_timeout_guard.py v7_trade_feed.py v7_monitor_gate.py v8_migration.py v8_evolution.py v8_execution_oof.py v8_execution_walkforward.py v8_notice.py v8_storage_guard.py v8_stability.py v9_final.py v9_readiness.py v9_training_store.py v9_live_parity.py v9_derivative_gate.py v9_multisource_derivatives.py v9_multisource_integrity.py server.py dashboard.html dashboard_v7.html dashboard_v72.html dashboard_v721.html ./

RUN adduser --disabled-password --gecos "" scanner \
    && mkdir -p /data \
    && chown -R scanner:scanner /app /data

USER scanner
EXPOSE 8080
CMD ["python", "server.py"]
