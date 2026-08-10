FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Keep the production image aligned with the final runtime without manually maintaining
# a fragile v5/v6/... module list. Runtime modules are v*.py; tests are not.
COPY app.py adaptive_engine.py adaptive_v5.py execution_v6.py execution_v7.py market_data.py derivative_data.py server.py server_v17.py ./
COPY v*.py ./
COPY dashboard*.html ./

RUN adduser --disabled-password --gecos "" scanner \
    && mkdir -p /data \
    && chown -R scanner:scanner /app /data

USER scanner
EXPOSE 8080
CMD ["python", "server_v17.py"]
