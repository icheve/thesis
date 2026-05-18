FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY payment_generator.py .

ENTRYPOINT ["python", "payment_generator.py"]
