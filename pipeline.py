"""
Pipeline Meshtastic: Do Pacote Bruto ao Dado Útil
Disciplina: IAL220 - Projeto Integrador Big Data / Arquiteturas Cloud p/ Big Data
Professor: Allan Siriani (https://github.com/siriani)

Este serviço executa continuamente a ingestão, limpeza, deduplicação, validação,
modelagem temporal e distribuição dos dados dos nós Meshtastic para 3 destinos:
  1. MariaDB: Série Temporal Limpa ({PREFIX}_serie_limpa)
  2. MariaDB: Agregados por Hora e Dia ({PREFIX}_agregados_hora, {PREFIX}_agregados_dia)
  3. Redis: Último Valor em Tempo Real ({PREFIX}:ultimo:{no}:{metrica})
  4. Quarentena: Isolamento e auditoria de pacotes rejeitados ({PREFIX}_quarentena)
"""

import collections
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import pymysql
import pymysql.cursors
import redis

# Carrega variáveis do .env
load_dotenv()

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")

# ==============================================================================
# CONFIGURAÇÕES E CONSTANTES
# ==============================================================================
PREFIX = os.getenv("STUDENT_PREFIX", "jose_gabriel").strip()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "iot")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "sensores")

MARIADB_HOST = os.getenv("MARIADB_HOST", "localhost")
MARIADB_PORT = int(os.getenv("MARIADB_PORT", "3306"))
MARIADB_USER = os.getenv("MARIADB_USER", "root")
MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD", "")
MARIADB_DATABASE = os.getenv("MARIADB_DATABASE", "fatec_iot")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None) or None
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2.0"))
SILENCE_THRESHOLD_SECONDS = int(os.getenv("SILENCE_THRESHOLD_SECONDS", "1800"))  # 30 minutos

# Faixas de aceitabilidade física (Schema Validation)
METRIC_RANGES = {
    "bateria_tensao": (2.0, 5.5),         # Tensão de bateria Li-Ion / LiPo (V)
    "bateria_nivel": (0.0, 100.0),        # Carga em porcentagem (%)
    "canal_utilizacao": (0.0, 100.0),     # Taxa de ocupação de canal LoRa (%)
    "tempo_ar_tx": (0.0, 100.0),          # Tempo de rádio transmitido (%)
    "tempo_ligado_s": (0.0, 1e9),         # Uptime do nó em segundos
    "temperatura": (-40.0, 85.0),         # Temperatura operacional DHT11 / BME (°C)
    "umidade": (0.0, 100.0),              # Umidade relativa do ar (%)
    "pressao_hpa": (300.0, 1200.0),       # Pressão barométrica em hPa
}

# Timestamp mínimo aceitável para o relógio do nó (01/01/2020 UTC)
MIN_VALID_NODE_TIMESTAMP = 1577836800


class MeshtasticPipeline:
    def __init__(self):
        self.running = True
        self.prefix = PREFIX
        self.mongo_client: Optional[MongoClient] = None
        self.mariadb_conn: Optional[pymysql.Connection] = None
        self.redis_client: Optional[redis.Redis] = None

        # Cache em memória para deduplicação rápida (últimos 50.000 pacotes)
        self.dedup_cache = collections.deque(maxlen=50000)
        self.dedup_set = set()

        # Rastreio de última leitura vista por nó (para detecção de buracos/silêncio)
        self.last_seen_by_node: Dict[str, datetime] = {}

        # Contadores locais em memória (sincronizados no Redis)
        self.stats = {
            "lidos": 0,
            "duplicatas_descartadas": 0,
            "timestamps_invalidos": 0,
            "buracos_detectados": 0,
            "quarentena_total": 0,
            "metricas_inseridas": 0,
        }

    def registrar_sinais(self):
        signal.signal(signal.SIGINT, self._finalizar)
        signal.signal(signal.SIGTERM, self._finalizar)

    def _finalizar(self, signum, frame):
        logger.info("Sinal de interrupção recebido. Encerrando pipeline com segurança...")
        self.running = False

    # --------------------------------------------------------------------------
    # CONEXÕES COM BANCOS DE DADOS
    # --------------------------------------------------------------------------
    def conectar_mongo(self):
        logger.info(f"Conectando ao MongoDB: {MONGO_DB}.{MONGO_COLLECTION}...")
        self.mongo_client = MongoClient(
            MONGO_URI,
            tz_aware=True,
            serverSelectionTimeoutMS=10000,
        )
        self.mongo_client.admin.command("ping")
        logger.info("MongoDB conectado com sucesso (somente leitura).")

    def conectar_mariadb(self):
        logger.info(f"Conectando ao MariaDB: {MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DATABASE}...")
        self.mariadb_conn = pymysql.connect(
            host=MARIADB_HOST,
            port=MARIADB_PORT,
            user=MARIADB_USER,
            password=MARIADB_PASSWORD,
            database=MARIADB_DATABASE,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
            autocommit=True,
        )
        logger.info("MariaDB conectado com sucesso.")

    def conectar_redis(self):
        logger.info(f"Conectando ao Redis: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}...")
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            db=REDIS_DB,
            decode_responses=True,
            socket_timeout=10,
        )
        self.redis_client.ping()
        logger.info("Redis conectado com sucesso.")

    # --------------------------------------------------------------------------
    # CRIAÇÃO IDEMPOTENTE DO ESQUEMA NO MARIADB
    # --------------------------------------------------------------------------
    def inicializar_esquema_mariadb(self):
        logger.info(f"Garantindo a existência das tabelas com prefixo '{self.prefix}'...")
        p = self.prefix
        ddls = [
            f"""
            CREATE TABLE IF NOT EXISTS `{p}_serie_limpa` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `no_id` VARCHAR(32) NOT NULL,
                `timestamp_medicao` DATETIME(3) NOT NULL,
                `timestamp_no` BIGINT NULL,
                `metrica` VARCHAR(64) NOT NULL,
                `valor` DOUBLE NOT NULL,
                `pacote_id` VARCHAR(64) NULL,
                `hop_limit` INT NULL,
                `rx_snr` FLOAT NULL,
                `rx_rssi` INT NULL,
                `criado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_no_pacote_metrica` (`no_id`, `pacote_id`, `metrica`),
                INDEX `idx_no_metrica_tempo` (`no_id`, `metrica`, `timestamp_medicao`),
                INDEX `idx_tempo` (`timestamp_medicao`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{p}_agregados_hora` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `no_id` VARCHAR(32) NOT NULL,
                `metrica` VARCHAR(64) NOT NULL,
                `janela_hora` DATETIME NOT NULL,
                `media_valor` DOUBLE NOT NULL,
                `min_valor` DOUBLE NOT NULL,
                `max_valor` DOUBLE NOT NULL,
                `contagem_leituras` INT NOT NULL,
                `atualizado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_agregado_hora` (`no_id`, `metrica`, `janela_hora`),
                INDEX `idx_hora_busca` (`no_id`, `metrica`, `janela_hora`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{p}_agregados_dia` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `no_id` VARCHAR(32) NOT NULL,
                `metrica` VARCHAR(64) NOT NULL,
                `janela_dia` DATE NOT NULL,
                `media_valor` DOUBLE NOT NULL,
                `min_valor` DOUBLE NOT NULL,
                `max_valor` DOUBLE NOT NULL,
                `contagem_leituras` INT NOT NULL,
                `atualizado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_agregado_dia` (`no_id`, `metrica`, `janela_dia`),
                INDEX `idx_dia_busca` (`no_id`, `metrica`, `janela_dia`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{p}_quarentena` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `mongo_id` VARCHAR(64) NULL,
                `no_id` VARCHAR(32) NULL,
                `pacote_id` VARCHAR(64) NULL,
                `tipo_pacote` VARCHAR(64) NULL,
                `motivo_rejeicao` VARCHAR(255) NOT NULL,
                `payload_bruto` LONGTEXT NULL,
                `recebido_em` DATETIME(3) NULL,
                `criado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX `idx_motivo` (`motivo_rejeicao`),
                INDEX `idx_no_quarentena` (`no_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{p}_pipeline_checkpoint` (
                `chave` VARCHAR(64) PRIMARY KEY,
                `ultimo_mongo_id` VARCHAR(64) NOT NULL,
                `ultimo_timestamp` DATETIME(3) NOT NULL,
                `atualizado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{p}_pipeline_gaps` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `no_id` VARCHAR(32) NOT NULL,
                `inicio_silencio` DATETIME(3) NOT NULL,
                `fim_silencio` DATETIME(3) NOT NULL,
                `duracao_segundos` INT NOT NULL,
                `criado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX `idx_no_gap` (`no_id`, `inicio_silencio`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """,
        ]
        with self.mariadb_conn.cursor() as cur:
            for ddl in ddls:
                cur.execute(ddl)
        self.mariadb_conn.commit()
        logger.info("Esquema do MariaDB pronto.")

    # --------------------------------------------------------------------------
    # CONTROLE DE CHECKPOINT
    # --------------------------------------------------------------------------
    def carregar_checkpoint(self) -> Tuple[Optional[str], Optional[datetime]]:
        sql = f"SELECT ultimo_mongo_id, ultimo_timestamp FROM `{self.prefix}_pipeline_checkpoint` WHERE chave = 'mongo_cursor'"
        try:
            with self.mariadb_conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                if row:
                    logger.info(f"Checkpoint recuperado: Mongo ID={row['ultimo_mongo_id']}, Data={row['ultimo_timestamp']}")
                    return row["ultimo_mongo_id"], row["ultimo_timestamp"]
        except Exception as e:
            logger.warning(f"Falha ao ler checkpoint do MariaDB: {e}")

        try:
            redis_id = self.redis_client.get(f"{self.prefix}:checkpoint:mongo_id")
            redis_dt_str = self.redis_client.get(f"{self.prefix}:checkpoint:timestamp")
            if redis_id:
                dt = datetime.fromisoformat(redis_dt_str) if redis_dt_str else None
                logger.info(f"Checkpoint recuperado do Redis: Mongo ID={redis_id}, Data={dt}")
                return redis_id, dt
        except Exception as e:
            logger.warning(f"Falha ao ler checkpoint do Redis: {e}")

        logger.info("Nenhum checkpoint anterior encontrado. Iniciando do zero.")
        return None, None

    # --------------------------------------------------------------------------
    # PROBLEMA 1: DEDUPLICAÇÃO
    # --------------------------------------------------------------------------
    def eh_duplicata(self, no_id: str, pacote_id: Optional[str]) -> bool:
        if not pacote_id:
            return False
        chave = f"{no_id}:{pacote_id}"
        if chave in self.dedup_set:
            return True

        if len(self.dedup_cache) >= self.dedup_cache.maxlen:
            velho = self.dedup_cache.popleft()
            self.dedup_set.discard(velho)

        self.dedup_cache.append(chave)
        self.dedup_set.add(chave)
        return False

    # --------------------------------------------------------------------------
    # PROBLEMA 2: RELÓGIO ERRADO & AUDITORIA DE TIMESTAMP
    # --------------------------------------------------------------------------
    def extrair_timestamps(self, doc: Dict[str, Any], payload_dict: Dict[str, Any]) -> Tuple[datetime, Optional[int], bool]:
        recebido = doc.get("received_at")
        if isinstance(recebido, str):
            try:
                recebido = datetime.fromisoformat(recebido.replace("Z", "+00:00"))
            except ValueError:
                recebido = None
        if not recebido and "_id" in doc and isinstance(doc["_id"], ObjectId):
            recebido = doc["_id"].generation_time

        if not recebido:
            recebido = datetime.now(timezone.utc)
        elif isinstance(recebido, datetime) and recebido.tzinfo is None:
            recebido = recebido.replace(tzinfo=timezone.utc)

        ts_no = payload_dict.get("timestamp")
        if ts_no is None and isinstance(doc.get("payload_data"), dict):
            ts_no = doc["payload_data"].get("timestamp")

        ts_no_int: Optional[int] = None
        relogio_invalido = False

        if ts_no is not None:
            try:
                ts_no_int = int(ts_no)
                agora_ts = int(time.time())
                if ts_no_int < MIN_VALID_NODE_TIMESTAMP or ts_no_int > (agora_ts + 86400):
                    relogio_invalido = True
            except (ValueError, TypeError):
                relogio_invalido = True
        else:
            relogio_invalido = True

        return recebido, ts_no_int, relogio_invalido

    # --------------------------------------------------------------------------
    # PROBLEMA 4: ESQUEMA QUE MUDA & VALIDAÇÃO FÍSICA
    # --------------------------------------------------------------------------
    def extrair_e_validar_metricas(
        self, doc: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Dict[str, float], Dict[str, Any], Optional[str]]:
        payload_data = doc.get("payload_data")
        payload_raw = doc.get("payload")

        conteudo_dict = {}
        if isinstance(payload_data, dict):
            conteudo_dict = payload_data
        elif isinstance(payload_raw, str):
            try:
                conteudo_dict = json.loads(payload_raw)
            except Exception:
                conteudo_dict = {}
        elif isinstance(payload_raw, dict):
            conteudo_dict = payload_raw

        no_id = (
            conteudo_dict.get("sender")
            or conteudo_dict.get("from")
            or (str(doc.get("topic", "")).split("/")[-1] if str(doc.get("topic", "")).startswith("sensores/") else None)
        )
        if not no_id or no_id == "?":
            return None, None, None, {}, {}, "SEM_REMETENTE_IDENTIFICADO"

        if isinstance(no_id, int):
            no_id = f"!{no_id:08x}"
        else:
            no_id = str(no_id).strip()

        pacote_id = (
            conteudo_dict.get("id")
            or conteudo_dict.get("packet_id")
            or (str(doc.get("_id")) if "_id" in doc else None)
        )
        pacote_id = str(pacote_id) if pacote_id is not None else None
        tipo_pacote = conteudo_dict.get("type", "unknown")

        metadados = {
            "hop_limit": conteudo_dict.get("hop_limit"),
            "rx_snr": conteudo_dict.get("rx_snr"),
            "rx_rssi": conteudo_dict.get("rx_rssi"),
        }

        raw_inner = conteudo_dict.get("payload")
        metricas_extraidas: Dict[str, float] = {}

        if isinstance(raw_inner, str):
            inicio = raw_inner.find("{")
            if inicio >= 0:
                try:
                    inner_json = json.loads(raw_inner[inicio:])
                    if isinstance(inner_json, dict):
                        raw_inner = inner_json
                except Exception:
                    pass

        if isinstance(raw_inner, dict) and "text" in raw_inner:
            txt = raw_inner["text"]
            if isinstance(txt, str):
                inicio = txt.find("{")
                if inicio >= 0:
                    try:
                        raw_inner = json.loads(txt[inicio:])
                    except Exception:
                        pass

        if isinstance(raw_inner, dict):
            dm = raw_inner.get("device_metrics", raw_inner)
            if isinstance(dm, dict):
                if "battery_level" in dm and dm["battery_level"] is not None:
                    metricas_extraidas["bateria_nivel"] = float(dm["battery_level"])
                if "voltage" in dm and dm["voltage"] is not None:
                    metricas_extraidas["bateria_tensao"] = float(dm["voltage"])
                if "channel_utilization" in dm and dm["channel_utilization"] is not None:
                    metricas_extraidas["canal_utilizacao"] = float(dm["channel_utilization"])
                if "air_util_tx" in dm and dm["air_util_tx"] is not None:
                    metricas_extraidas["tempo_ar_tx"] = float(dm["air_util_tx"])
                if "uptime_seconds" in dm and dm["uptime_seconds"] is not None:
                    metricas_extraidas["tempo_ligado_s"] = float(dm["uptime_seconds"])

            em = raw_inner.get("environment_metrics", raw_inner)
            if isinstance(em, dict):
                if "temperature" in em and em["temperature"] is not None:
                    metricas_extraidas["temperatura"] = float(em["temperature"])
                if "relative_humidity" in em and em["relative_humidity"] is not None:
                    metricas_extraidas["umidade"] = float(em["relative_humidity"])
                if "barometric_pressure" in em and em["barometric_pressure"] is not None:
                    metricas_extraidas["pressao_hpa"] = float(em["barometric_pressure"])

            temp_val = raw_inner.get("temp", raw_inner.get("t"))
            if temp_val is not None:
                try:
                    metricas_extraidas["temperatura"] = float(temp_val)
                except (ValueError, TypeError):
                    pass

            umid_val = raw_inner.get("umid", raw_inner.get("u"))
            if umid_val is not None:
                try:
                    metricas_extraidas["umidade"] = float(umid_val)
                except (ValueError, TypeError):
                    pass

        if not metricas_extraidas:
            return no_id, pacote_id, tipo_pacote, {}, metadados, "SEM_METRICAS_UTEIS_NO_PAYLOAD"

        for metrica, val in metricas_extraidas.items():
            if metrica in METRIC_RANGES:
                min_f, max_f = METRIC_RANGES[metrica]
                if val < min_f or val > max_f:
                    return (
                        no_id,
                        pacote_id,
                        tipo_pacote,
                        {},
                        metadados,
                        f"VALOR_FORA_DE_FAIXA: {metrica}={val} (esperado [{min_f}, {max_f}])",
                    )

        return no_id, pacote_id, tipo_pacote, metricas_extraidas, metadados, None

    # --------------------------------------------------------------------------
    # EXECUÇÃO DO LOTE COM TRANSAÇÕES EM MASSA (ALTA PERFORMANCE)
    # --------------------------------------------------------------------------
    def processar_lote(self, documentos: List[Dict[str, Any]]):
        lote_serie = []
        vistos_serie = set()

        lote_quarentena = []
        lote_gaps = []

        pipe_redis = self.redis_client.pipeline()

        ultimo_id = None
        ultimo_ts = None
        min_ts_lote = None

        for doc in documentos:
            self.stats["lidos"] += 1
            pipe_redis.hincrby(f"{self.prefix}:stats", "lidos", 1)

            payload_dict = doc.get("payload_data") if isinstance(doc.get("payload_data"), dict) else {}
            ts_medicao, ts_no, relogio_invalido = self.extrair_timestamps(doc, payload_dict)

            if min_ts_lote is None or ts_medicao < min_ts_lote:
                min_ts_lote = ts_medicao

            if relogio_invalido:
                self.stats["timestamps_invalidos"] += 1
                pipe_redis.hincrby(f"{self.prefix}:stats", "timestamps_invalidos", 1)

            (
                no_id,
                pacote_id,
                tipo_pacote,
                metricas,
                metadados,
                motivo_quarentena,
            ) = self.extrair_e_validar_metricas(doc)

            # Problema 4: Quarentena
            if motivo_quarentena:
                self.stats["quarentena_total"] += 1
                pipe_redis.hincrby(f"{self.prefix}:stats", "quarentena_total", 1)
                motivo_k = motivo_quarentena.split(":")[0].strip().replace(" ", "_").lower()
                pipe_redis.hincrby(f"{self.prefix}:stats", f"quarentena:{motivo_k}", 1)

                mongo_id = str(doc.get("_id", ""))
                payload_str = json.dumps(doc, default=str)
                lote_quarentena.append(
                    (mongo_id, no_id, pacote_id, tipo_pacote, motivo_quarentena[:255], payload_str, ts_medicao)
                )
                ultimo_id = str(doc["_id"])
                ultimo_ts = ts_medicao
                continue

            # Problema 1: Deduplicação
            if self.eh_duplicata(no_id, pacote_id):
                self.stats["duplicatas_descartadas"] += 1
                pipe_redis.hincrby(f"{self.prefix}:stats", "duplicatas_descartadas", 1)
                ultimo_id = str(doc["_id"])
                ultimo_ts = ts_medicao
                continue

            # Problema 3: Buracos e Silêncio
            ultima_vez = self.last_seen_by_node.get(no_id)
            if ultima_vez:
                dt_ant = ultima_vez.replace(tzinfo=timezone.utc) if ultima_vez.tzinfo is None else ultima_vez
                dt_nov = ts_medicao.replace(tzinfo=timezone.utc) if ts_medicao.tzinfo is None else ts_medicao
                delta = (dt_nov - dt_ant).total_seconds()
                if delta > SILENCE_THRESHOLD_SECONDS:
                    self.stats["buracos_detectados"] += 1
                    pipe_redis.hincrby(f"{self.prefix}:stats", "buracos_detectados", 1)
                    lote_gaps.append((no_id, dt_ant, dt_nov, int(delta)))

            self.last_seen_by_node[no_id] = ts_medicao
            pipe_redis.set(f"{self.prefix}:no:{no_id}:ultima_vista", ts_medicao.isoformat())

            # Destinos
            hop = metadados.get("hop_limit")
            snr = metadados.get("rx_snr")
            rssi = metadados.get("rx_rssi")

            for metrica, valor in metricas.items():
                chave_serie = (no_id, pacote_id, metrica)
                if chave_serie not in vistos_serie:
                    vistos_serie.add(chave_serie)
                    lote_serie.append((no_id, ts_medicao, ts_no, metrica, valor, pacote_id, hop, snr, rssi))

                self.stats["metricas_inseridas"] += 1

                # Redis Último Valor
                chave_redis = f"{self.prefix}:ultimo:{no_id}:{metrica}"
                dado_redis = json.dumps({"valor": valor, "timestamp": ts_medicao.isoformat(), "pacote_id": pacote_id})
                pipe_redis.set(chave_redis, dado_redis)
                pipe_redis.hset(f"{self.prefix}:no:{no_id}", metrica, valor)

            pipe_redis.hset(f"{self.prefix}:no:{no_id}", "ultima_atualizacao", ts_medicao.isoformat())
            pipe_redis.hincrby(f"{self.prefix}:stats", "metricas_inseridas", len(metricas))

            ultimo_id = str(doc["_id"])
            ultimo_ts = ts_medicao

        # ----------------------------------------------------------------------
        # GRAVAÇÃO ATÔMICA EM LOTE NO MARIADB
        # ----------------------------------------------------------------------
        sql_quarentena = f"""
        INSERT INTO `{self.prefix}_quarentena`
        (mongo_id, no_id, pacote_id, tipo_pacote, motivo_rejeicao, payload_bruto, recebido_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        sql_serie = f"""
        INSERT IGNORE INTO `{self.prefix}_serie_limpa`
        (no_id, timestamp_medicao, timestamp_no, metrica, valor, pacote_id, hop_limit, rx_snr, rx_rssi)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        sql_gap = f"""
        INSERT INTO `{self.prefix}_pipeline_gaps` (no_id, inicio_silencio, fim_silencio, duracao_segundos)
        VALUES (%s, %s, %s, %s)
        """

        sql_checkpoint = f"""
        INSERT INTO `{self.prefix}_pipeline_checkpoint` (chave, ultimo_mongo_id, ultimo_timestamp)
        VALUES ('mongo_cursor', %s, %s)
        ON DUPLICATE KEY UPDATE
            ultimo_mongo_id = VALUES(ultimo_mongo_id),
            ultimo_timestamp = VALUES(ultimo_timestamp)
        """

        with self.mariadb_conn.cursor() as cur:
            if lote_quarentena:
                cur.executemany(sql_quarentena, lote_quarentena)
            if lote_serie:
                cur.executemany(sql_serie, lote_serie)
                # Atualização analítica das janelas de agregação diretamente via SQL
                cur.execute(f"""
                REPLACE INTO `{self.prefix}_agregados_hora`
                (no_id, metrica, janela_hora, media_valor, min_valor, max_valor, contagem_leituras)
                SELECT
                    no_id,
                    metrica,
                    DATE_FORMAT(timestamp_medicao, '%%Y-%%m-%%d %%H:00:00') AS janela_hora,
                    ROUND(AVG(valor), 4),
                    MIN(valor),
                    MAX(valor),
                    COUNT(*)
                FROM `{self.prefix}_serie_limpa`
                WHERE timestamp_medicao >= %s
                GROUP BY no_id, metrica, janela_hora
                """, (min_ts_lote,))

                cur.execute(f"""
                REPLACE INTO `{self.prefix}_agregados_dia`
                (no_id, metrica, janela_dia, media_valor, min_valor, max_valor, contagem_leituras)
                SELECT
                    no_id,
                    metrica,
                    DATE(timestamp_medicao) AS janela_dia,
                    ROUND(AVG(valor), 4),
                    MIN(valor),
                    MAX(valor),
                    COUNT(*)
                FROM `{self.prefix}_serie_limpa`
                WHERE timestamp_medicao >= %s
                GROUP BY no_id, metrica, janela_dia
                """, (min_ts_lote,))

            if lote_gaps:
                cur.executemany(sql_gap, lote_gaps)
            if ultimo_id and ultimo_ts:
                cur.execute(sql_checkpoint, (ultimo_id, ultimo_ts))


        # Executa operações do Redis em um único roundtrip
        if ultimo_id and ultimo_ts:
            pipe_redis.set(f"{self.prefix}:checkpoint:mongo_id", ultimo_id)
            pipe_redis.set(f"{self.prefix}:checkpoint:timestamp", ultimo_ts.isoformat())
        pipe_redis.execute()

        return ultimo_id, ultimo_ts

    # --------------------------------------------------------------------------
    # LOOP PRINCIPAL DO PIPELINE CONTÍNUO
    # --------------------------------------------------------------------------
    def executar(self):
        self.registrar_sinais()
        self.conectar_mongo()
        self.conectar_mariadb()
        self.conectar_redis()
        self.inicializar_esquema_mariadb()

        col = self.mongo_client[MONGO_DB][MONGO_COLLECTION]
        ultimo_mongo_id, ultimo_ts = self.carregar_checkpoint()

        logger.info("=" * 70)
        logger.info(f"PIPELINE MESHTASTIC INICIADO COM PREFIXO: '{self.prefix}'")
        logger.info(f"Lendo de: {col.full_name} | Polling: {POLL_INTERVAL}s | Lote: {BATCH_SIZE}")
        logger.info("=" * 70)

        ultimo_print_stats = time.time()

        while self.running:
            filtro = {}
            if ultimo_mongo_id:
                try:
                    filtro = {"_id": {"$gt": ObjectId(ultimo_mongo_id)}}
                except Exception:
                    filtro = {}

            try:
                documentos = list(col.find(filtro).sort("_id", 1).limit(BATCH_SIZE))
            except PyMongoError as e:
                logger.error(f"Erro ao consultar MongoDB: {e}. Aguardando {POLL_INTERVAL}s...")
                time.sleep(POLL_INTERVAL)
                continue

            if documentos:
                novo_id, novo_ts = self.processar_lote(documentos)
                if novo_id:
                    ultimo_mongo_id = novo_id
                    ultimo_ts = novo_ts
            else:
                time.sleep(POLL_INTERVAL)

            if time.time() - ultimo_print_stats > 10:
                ultimo_print_stats = time.time()
                logger.info(
                    f"[STATUS] Lidos: {self.stats['lidos']} | "
                    f"Duplicatas: {self.stats['duplicatas_descartadas']} | "
                    f"Relógio Inválido: {self.stats['timestamps_invalidos']} | "
                    f"Buracos: {self.stats['buracos_detectados']} | "
                    f"Quarentena: {self.stats['quarentena_total']} | "
                    f"Séries Inseridas: {self.stats['metricas_inseridas']}"
                )

        logger.info("Pipeline finalizado com sucesso.")


if __name__ == "__main__":
    pipeline = MeshtasticPipeline()
    try:
        pipeline.executar()
    except Exception as exc:
        logger.critical(f"Falha fatal no pipeline: {exc}", exc_info=True)
        sys.exit(1)
