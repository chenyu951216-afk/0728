FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8080 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1 \
    VECLIB_MAXIMUM_THREADS=1 \
    MALLOC_ARENA_MAX=2
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Keep the production image aligned with the final runtime without manually maintaining
# a fragile v5/v6/... module list. Runtime modules are v*.py; tests are not.
COPY app.py adaptive_engine.py adaptive_v5.py execution_v6.py execution_v7.py market_data.py derivative_data.py runtime_identity.py server.py server_v17.py server_v18.py server_v19.py server_entry.py server_entry_v27.py server_entry_v48.py server_entry_v49.py server_entry_v50.py server_entry_v51.py server_entry_v52.py server_entry_v53.py server_entry_v54.py server_entry_v55.py server_entry_v56.py server_entry_v57.py server_entry_v58.py server_entry_v59.py server_entry_v60.py server_entry_v61.py server_entry_v62.py ./
COPY v*.py ./
COPY dashboard*.html ./

RUN adduser --disabled-password --gecos "" scanner \
    && mkdir -p /data \
    && chown -R scanner:scanner /app /data

USER scanner
EXPOSE 8080
CMD ["python", "-u", "server_entry_v62.py"]
