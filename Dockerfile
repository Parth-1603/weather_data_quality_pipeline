FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY sql ./sql
RUN mkdir -p /app/data/raw /app/data/clean /app/data/quarantine /app/reports
ENV PYTHONUNBUFFERED=1
CMD ["python", "src/pipeline.py"]
