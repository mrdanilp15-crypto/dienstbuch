# Wir nutzen Python 3.11 als Basis
FROM python:3.11-slim

# Arbeitsverzeichnis im Container
WORKDIR /app

# System-Abhängigkeiten für PDF-Generierung (falls nötig für deine Reports)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Python-Abhängigkeiten kopieren und installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Den gesamten Code kopieren
COPY . .

# Port 8000 freigeben (passend zu deiner main.py/compose)
EXPOSE 8000

# Startbefehl
CMD ["python", "main.py"]
