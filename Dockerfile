FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py adaptive_engine.py adaptive_v5.py market_data.py derivative_data.py v5_runtime.py v5_async_runtime.py server.py dashboard.html ./

RUN adduser --disabled-password --gecos "" scanner \
    && mkdir -p /data \
    && chown -R scanner:scanner /app /data

USER scanner
EXPOSE 8080
CMD ["python", "server.py"]
