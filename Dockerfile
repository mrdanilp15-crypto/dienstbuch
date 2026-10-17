FROM python:3.11-slim

# Zeitzone: Erinnerungen, Ruhezeiten und "heute" richten sich nach der Wehr-Uhrzeit, nicht nach UTC
ENV TZ=Europe/Berlin

WORKDIR /app

# System-Abhängigkeiten (mariadb-client liefert mysqldump für das Auto-Backup)
RUN apt-get update && apt-get install -y \
    gcc \
    mariadb-client \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Python-Pakete installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Den Code kopieren
COPY . .

# Port 8000 für FastAPI/Uvicorn
EXPOSE 8000

# Startbefehl: Uvicorn startet die FastAPI App
# host 0.0.0.0 ist wichtig für den Zugriff aus Docker
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
