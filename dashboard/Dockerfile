# Build context is the repo root (docker-compose.yml). The code is mounted, not copied, so edits show on refresh.
# Base image pinned to a patch release (wk11 [92]: "latest is not always the greatest"); python:3.11-slim moves.
FROM python:3.11.17-slim
WORKDIR /app
COPY dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
