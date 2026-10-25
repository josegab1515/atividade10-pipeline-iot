"""
Simulador de Testes: Validação dos 4 Problemas do Dado Bruto
Disciplina: IAL220 - Atividade 10

Este script permite simular a recepção de pacotes brutos Meshtastic contendo
todas as anomalias do mundo real para comprovar o funcionamento dos 4 tratamentos:
  1. Duplicatas de rádio (mesmo sender e id)
  2. Relógio inválido (timestamp 0 ou 1970)
  3. Buraco / silêncio de transmissão (> 30 minutos)
  4. Violações de esquema / valores físicos fora de faixa (Quarentena)
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "iot")
# Usa coleção de teste local se especificado, ou a padrão
MONGO_COLLECTION = os.getenv("MONGO_TEST_COLLECTION", os.getenv("MONGO_COLLECTION", "sensores"))


def gerar_pacotes_cenario():
    agora = datetime.now(timezone.utc)
    pacotes = []

    # 1. Pacote Válido: Telemetria de Dispositivo (Heltec !8fa724e8)
    pacotes.append({
        "_id": ObjectId(),
        "topic": "sensores/telemetry/2/json/mesh/!8fa724e8",
        "payload_data": {
            "sender": "!8fa724e8",
            "id": 1001,
            "type": "telemetry",
            "timestamp": int(agora.timestamp()),
            "rx_snr": 8.5,
            "rx_rssi": -65,
            "hop_limit": 3,
            "payload": {
                "device_metrics": {
                    "battery_level": 92,
                    "voltage": 4.15,
                    "channel_utilization": 14.2,
                    "air_util_tx": 2.1,
                    "uptime_seconds": 86400
                }
            }
        },
        "received_at": agora - timedelta(minutes=40),
    })

    # 2. Pacote Duplicado do anterior (Problema 1: Duplicata de rádio por outro salto)
    pacotes.append({
        "_id": ObjectId(),
        "topic": "sensores/telemetry/2/json/mesh/!8fa724e8",
        "payload_data": {
            "sender": "!8fa724e8",
            "id": 1001,  # MESMO ID
            "type": "telemetry",
            "timestamp": int(agora.timestamp()),
            "rx_snr": 4.2,
            "rx_rssi": -85,
            "hop_limit": 1,
            "payload": {
                "device_metrics": {
                    "battery_level": 92,
                    "voltage": 4.15
                }
            }
        },
        "received_at": agora - timedelta(minutes=40) + timedelta(seconds=2),
    })

    # 3. Pacote Válido: Leitura de DHT11 com salto temporal (Problema 3: Buraco > 30 min)
    pacotes.append({
        "_id": ObjectId(),
        "topic": "sensores/tempUmid/2/json/mqtt_teste/!8fa724e8",
        "payload_data": {
            "sender": "!8fa724e8",
            "id": 1002,
            "type": "text",
            "timestamp": int(agora.timestamp()),
            "payload": {"t": 23.5, "u": 58.0}
        },
        "received_at": agora,  # Chegou 40 minutos após o anterior -> gera GAP
    })

    # 4. Pacote com Relógio Zerado/1970 (Problema 2: Relógio Inválido)
    pacotes.append({
        "_id": ObjectId(),
        "topic": "sensores/telemetry/2/json/mesh/!3b45c1a0",
        "payload_data": {
            "sender": "!3b45c1a0",
            "id": 2001,
            "type": "telemetry",
            "timestamp": 0,  # Timestamp 0 (1970) -> Deve incrementar relógios inválidos e usar received_at
            "payload": {
                "device_metrics": {
                    "battery_level": 78,
                    "voltage": 3.98,
                    "channel_utilization": 8.0
                }
            }
        },
        "received_at": agora,
    })

    # 5. Pacote Quebrado: Temperatura Impossível (Problema 4: Quarentena - Valor fora de faixa)
    pacotes.append({
        "_id": ObjectId(),
        "topic": "sensores/tempUmid/2/json/mqtt_teste/!3b45c1a0",
        "payload_data": {
            "sender": "!3b45c1a0",
            "id": 2002,
            "type": "text",
            "timestamp": int(agora.timestamp()),
            "payload": {"t": 195.0, "u": 40.0}  # 195 °C -> Fora de faixa física!
        },
        "received_at": agora,
    })

    # 6. Pacote Sem Remetente (Problema 4: Quarentena - Falha de Schema)
    pacotes.append({
        "_id": ObjectId(),
        "topic": "sensores/desconhecido",
        "payload": "{\"status\": \"corrompido\", \"data\": null}",
        "received_at": agora,
    })

    return pacotes


def injetar_no_mongo():
    cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    col = cliente[MONGO_DB][MONGO_COLLECTION]
    pacotes = gerar_pacotes_cenario()
    resultado = col.insert_many(pacotes)
    print(f"Sucesso! {len(resultado.inserted_ids)} pacotes de teste injetados na coleção {col.full_name}.")
    print("Execute agora 'python pipeline.py' para ver o processamento e tratamento ao vivo!")


if __name__ == "__main__":
    try:
        injetar_no_mongo()
    except Exception as e:
        print(f"Não foi possível conectar ao Mongo para simulação: {e}")
        print("Certifique-se de que o MongoDB esteja ativo e configurado no .env.")
