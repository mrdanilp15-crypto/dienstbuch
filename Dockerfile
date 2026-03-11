FROM python:3.11-slim

WORKDIR /app

# Abhängigkeiten installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code kopieren
COPY . .

# Port freigeben (du nutzt laut Compose 8000)
EXPOSE 8000

# Startbefehl (Anpassung falls deine Hauptdatei anders heißt, z.B. main.py)
CMD ["python", "main.py"]
