"""
Consultas Analíticas - Atividade 10 (IAL220)
Do pacote bruto ao dado útil — pipeline e modelagem
Professor: Allan Siriani (https://github.com/siriani)

Este script executa e formata as 3 consultas analíticas principais da atividade,
além do relatório de auditoria dos 4 problemas (Quarentena/Duplicatas) e
do snapshot em tempo real do Redis ("E agora?").

Como rodar:
    python consultas.py           (executa todas as consultas formatadas)
    python consultas.py 1         (Consulta 1: Bateria e Autonomia Solar por Dia)
    python consultas.py 2         (Consulta 2: Utilização do Canal LoRa por Hora)
    python consultas.py 3         (Consulta 3: Silêncio e Buracos de Comunicação)
    python consultas.py 4         (Auditoria dos 4 Problemas: Quarentena e Descartes)
    python consultas.py 5         (Redis: Último Valor ao Vivo)
"""

import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
import pymysql
import pymysql.cursors
import redis

load_dotenv()

PREFIX = os.getenv("STUDENT_PREFIX", "jose_gabriel").strip()

MARIADB_HOST = os.getenv("MARIADB_HOST", "localhost")
MARIADB_PORT = int(os.getenv("MARIADB_PORT", "3306"))
MARIADB_USER = os.getenv("MARIADB_USER", "root")
MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD", "")
MARIADB_DATABASE = os.getenv("MARIADB_DATABASE", "fatec_iot")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None) or None
REDIS_DB = int(os.getenv("REDIS_DB", "0"))


def conectar_mariadb():
    return pymysql.connect(
        host=MARIADB_HOST,
        port=MARIADB_PORT,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        database=MARIADB_DATABASE,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def conectar_redis():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True,
        socket_timeout=10,
    )


def imprimir_tabela(linhas, titulo=""):
    if titulo:
        print("\n" + "=" * 80)
        print(f" {titulo.upper()} ")
        print("=" * 80)
    if not linhas:
        print("(Nenhum registro encontrado)\n")
        return

    colunas = list(linhas[0].keys())
    # Calcula larguras das colunas
    larguras = {col: len(col) for col in colunas}
    for l in linhas:
        for col in colunas:
            val_str = str(l[col]) if l[col] is not None else "NULL"
            larguras[col] = max(larguras[col], len(val_str))

    cabecalho = " | ".join(col.ljust(larguras[col]) for col in colunas)
    divisor = "-+-".join("-" * larguras[col] for col in colunas)
    print(cabecalho)
    print(divisor)
    for l in linhas:
        linha_fmt = " | ".join(
            (str(l[col]) if l[col] is not None else "NULL").ljust(larguras[col])
            for col in colunas
        )
        print(linha_fmt)
    print(f"\nTotal: {len(linhas)} linha(s).\n")


# ==============================================================================
# CONSULTA 1: Médias, Mínimos e Máximos Diários por Nó (DHT11 e Telemetria)
# Pergunta: "Como se comportaram a temperatura/umidade do DHT11 e a tensão dos nós por dia?"
# ==============================================================================
def consulta_1_bateria_dia():
    titulo = "Consulta 1: Médias, Mínimos e Máximos Diários por Nó (DHT11 e Telemetria)"
    sql = f"""
    SELECT
        no_id AS `Nó`,
        janela_dia AS `Dia`,
        metrica AS `Métrica`,
        ROUND(media_valor, 2) AS `Média`,
        ROUND(min_valor, 2) AS `Mínimo`,
        ROUND(max_valor, 2) AS `Máximo`,
        contagem_leituras AS `Leituras`
    FROM `{PREFIX}_agregados_dia`
    WHERE metrica IN ('temperatura', 'umidade', 'bateria_tensao', 'bateria_nivel')
    ORDER BY janela_dia DESC, no_id ASC, metrica ASC
    LIMIT 40;
    """
    with conectar_mariadb() as conn, conn.cursor() as cur:
        cur.execute(sql)
        imprimir_tabela(cur.fetchall(), titulo)
    print("Insight: Acompanha os ciclos diários reais de temperatura (°C) e umidade (%)")
    print("do DHT11 (nó !8fa724e8), além da estabilidade de alimentação dos nós da malha.")



# ==============================================================================
# CONSULTA 2: Agregados Horários e Utilização do Canal (Saturação e Ciclo Horário)
# Pergunta: "Como se comportaram as leituras por hora e qual a ocupação da rede?"
# ==============================================================================
def consulta_2_canal_hora():
    titulo = "Consulta 2: Agregados por Hora (Ciclo Horário e Tráfego da Rede)"
    sql = f"""
    SELECT
        janela_hora AS `Janela (Hora)`,
        metrica AS `Métrica`,
        ROUND(AVG(media_valor), 2) AS `Média`,
        ROUND(MIN(min_valor), 2) AS `Mínimo`,
        ROUND(MAX(max_valor), 2) AS `Máximo`,
        SUM(contagem_leituras) AS `Total Leituras`
    FROM `{PREFIX}_agregados_hora`
    WHERE metrica IN ('canal_utilizacao', 'tempo_ar_tx', 'temperatura', 'umidade')
    GROUP BY janela_hora, metrica
    ORDER BY janela_hora DESC
    LIMIT 20;
    """
    with conectar_mariadb() as conn, conn.cursor() as cur:
        cur.execute(sql)
        imprimir_tabela(cur.fetchall(), titulo)
    print("Insight: Demonstra a resolução horária das leituras e permite avaliar picos térmicos")
    print("e saturação do canal de transmissão da malha.")


# ==============================================================================
# CONSULTA 3: Silêncio e Buracos de Comunicação (Gaps)
# Pergunta: "Quanto tempo cada nó ficou em silêncio? Quem está caindo?"
# ==============================================================================
def consulta_3_silencio():
    titulo = "Consulta 3: Detecção de Ausência e Buracos de Sinal (Gaps sem Interpolação)"
    sql = f"""
    SELECT
        no_id AS `Nó`,
        inicio_silencio AS `Início do Silêncio`,
        fim_silencio AS `Fim do Silêncio`,
        ROUND(duracao_segundos / 60, 1) AS `Duração (min)`,
        ROUND(duracao_segundos / 3600, 2) AS `Duração (horas)`
    FROM `{PREFIX}_pipeline_gaps`
    ORDER BY duracao_segundos DESC
    LIMIT 20;
    """
    with conectar_mariadb() as conn, conn.cursor() as cur:
        cur.execute(sql)
        imprimir_tabela(cur.fetchall(), titulo)

    print("--- Nós Atualmente em Silêncio Prolongado (> 30 minutos desde a última leitura) ---")
    sql_silencio_atual = f"""
    SELECT
        no_id AS `Nó`,
        MAX(timestamp_medicao) AS `Último Pacote Recebido`,
        ROUND(TIMESTAMPDIFF(SECOND, MAX(timestamp_medicao), UTC_TIMESTAMP()) / 60, 1) AS `Minutos em Silêncio`
    FROM `{PREFIX}_serie_limpa`
    GROUP BY no_id
    HAVING `Minutos em Silêncio` > 30
    ORDER BY `Minutos em Silêncio` DESC;
    """
    with conectar_mariadb() as conn, conn.cursor() as cur:
        cur.execute(sql_silencio_atual)
        imprimir_tabela(cur.fetchall())
    print("Insight: A ausência de dados é tratada como informação real (deep sleep, queda ou descarregamento),")
    print("sem inventar ou interpolar leituras inexistentes.")


# ==============================================================================
# CONSULTA 4: Auditoria dos 4 Problemas e Quarentena
# Medição do tratamento de Duplicatas, Relógio, Buracos e Esquema Inválido
# ==============================================================================
def consulta_4_auditoria_quarentena():
    titulo = "Consulta 4: Auditoria dos 4 Problemas Tratados e Quarentena"
    print("\n" + "=" * 80)
    print(f" {titulo.upper()} ")
    print("=" * 80)

    try:
        r = conectar_redis()
        stats = r.hgetall(f"{PREFIX}:stats")
        print("\n[Métricas Globais do Pipeline no Redis]")
        for k, v in sorted(stats.items()):
            print(f"  • {k}: {v}")
    except Exception as e:
        print(f"Aviso: Não foi possível ler contadores do Redis: {e}")

    sql = f"""
    SELECT
        motivo_rejeicao AS `Motivo de Descarte (Quarentena)`,
        COUNT(*) AS `Total de Pacotes`,
        MIN(recebido_em) AS `Primeiro Evento`,
        MAX(recebido_em) AS `Último Evento`
    FROM `{PREFIX}_quarentena`
    GROUP BY motivo_rejeicao
    ORDER BY `Total de Pacotes` DESC;
    """
    with conectar_mariadb() as conn, conn.cursor() as cur:
        cur.execute(sql)
        imprimir_tabela(cur.fetchall(), "Distribuição de Pacotes na Quarentena por Motivo")


# ==============================================================================
# CONSULTA 5: Redis - Snapshot em Tempo Real ("E agora?")
# ==============================================================================
def consulta_5_redis_tempo_real():
    titulo = "Consulta 5: Redis - Estado em Tempo Real de cada Nó ('E agora?')"
    print("\n" + "=" * 80)
    print(f" {titulo.upper()} ")
    print("=" * 80)

    try:
        r = conectar_redis()
        chaves_nos = r.keys(f"{PREFIX}:no:*")
        # Filtra apenas hashes de nós (ex: jose_gabriel:no:!8fa724e8)
        nos = [c for c in chaves_nos if not c.endswith(":ultima_vista")]
        if not nos:
            print("(Nenhum nó registrado no Redis ainda)\n")
            return

        linhas = []
        for chave in sorted(nos):
            no_id = chave.split(":")[-1]
            dados = r.hgetall(chave)
            dados["Nó"] = no_id
            linhas.append(dados)

        imprimir_tabela(linhas, "Últimos Valores em Memória (Redis)")
        print("Insight: Latência de leitura < 1ms para responder instantaneamente o estado atual")
        print("sem onerar o banco relacional.")
    except Exception as e:
        print(f"Erro ao consultar Redis: {e}")


def main():
    opcao = sys.argv[1] if len(sys.argv) > 1 else "todas"

    if opcao == "1":
        consulta_1_bateria_dia()
    elif opcao == "2":
        consulta_2_canal_hora()
    elif opcao == "3":
        consulta_3_silencio()
    elif opcao == "4":
        consulta_4_auditoria_quarentena()
    elif opcao == "5":
        consulta_5_redis_tempo_real()
    else:
        consulta_1_bateria_dia()
        consulta_2_canal_hora()
        consulta_3_silencio()
        consulta_4_auditoria_quarentena()
        consulta_5_redis_tempo_real()


if __name__ == "__main__":
    main()
