FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Run daily at 8am ET (13:00 UTC)
CMD ["sh", "-c", "while true; do python main.py; sleep 86400; done"]
