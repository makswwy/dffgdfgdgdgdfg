FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PORT=3000

CMD ["sh", "-c", "exec gunicorn -b 0.0.0.0:${PORT} --access-logfile - --error-logfile - --capture-output --log-level info app:app"]
