FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py adaptive_engine.py market_data.py derivative_data.py ./

RUN adduser --disabled-password --gecos "" scanner \
    && mkdir -p /data \
    && chown -R scanner:scanner /app /data

USER scanner
EXPOSE 8080
CMD ["python", "app.py"]
