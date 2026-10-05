FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# /data is the Railway volume: database + backups live there and survive redeploys.
# REQUIRE_AUTH makes the server refuse to start without Google login configured.
ENV DATA_DIR=/data REQUIRE_AUTH=1 PYTHONUNBUFFERED=1
CMD ["python", "server.py"]
