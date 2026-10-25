# Dockerfile do Pipeline Meshtastic (IAL220 - Atividade 10)
FROM python:3.11-slim

# Evita criação de arquivos .pyc e força flush imediato de stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=America/Sao_Paulo

WORKDIR /app

# Instala dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Instala bibliotecas Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia os códigos do pipeline
COPY schema.sql .
COPY pipeline.py .
COPY consultas.py .

# Executa o pipeline contínuo
CMD ["python", "-u", "pipeline.py"]
