FROM python:3.11-slim

WORKDIR /app

# System-Abhängigkeiten
RUN apt-get update && apt-get install -y \
    gcc \
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
