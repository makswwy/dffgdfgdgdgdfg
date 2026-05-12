FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV APP_MODE=bot
ENV PORT=7777

CMD ["sh", "-c", "if [ \"$APP_MODE\" = \"web\" ]; then exec gunicorn -b 0.0.0.0:${PORT} app:app; else exec python main.py; fi"]
