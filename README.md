# Pipeline Meshtastic: Do Pacote Bruto ao Dado Útil

**Disciplina:** IAL220 — Projeto Integrador Big Data / Arquiteturas Cloud p/ Big Data  
**Professor:** [Allan Siriani](https://github.com/siriani) — [lapps.studio](https://lapps.studio/)  
**Instituição:** FATEC  
**Aluno:** José Gabriel (`jose_gabriel`)

---

## 📌 Visão Geral da Atividade

O MongoDB da disciplina atua como repositório de recepção (*raw buffer*) de todos os pacotes transmitidos pelos nós via malha LoRa/Meshtastic e gateways MQTT. No estado bruto, esses dados não respondem perguntas de negócio nem técnicas: contêm pacotes duplicados gerados por múltiplos saltos de rádio, nós com relógio interno zerado ou dessincronizado, períodos de ausência/silêncio e esquemas de dados heterogêneos que variam de acordo com o firmware ou tipo de sensor.

No nosso nó de bancada, a cadeia de transmissão é:
```
DHT11 → Arduino Mega → Heltec !8fa724e8 → MQTT → serviço no servidor → MongoDB (iot.sensores)
```
O nó transmite leituras ambientais de temperatura e umidade, enquanto a malha meshtastic compartilha pacotes de telemetria dos demais nós da turma.

Este projeto implementa um **pipeline de engenharia de dados em tempo real e contínuo**, resiliente a falhas, que lê os pacotes brutos, trata as quatro anomalias de dados de borda (*edge data*), isola rejeições em quarentena com motivos auditáveis e persiste as informações tratadas em três destinos especializados.


```
                      +-----------------------------+
                      |   MongoDB (pacotes_brutos)  | (Somente Leitura)
                      +--------------+--------------+
                                     |
                                     v
                      +-----------------------------+
                      |    Pipeline de Ingestão     |
                      |   e Tratamento (Python)     |
                      +--------------+--------------+
                                     |
       +-----------------------------+-----------------------------+
       | (Problema 1)                | (Problema 2)                | (Problema 3)
       v                             v                             v
[ Deduplicação ]              [ Relógio Seguro ]            [ Modelagem de ]
 (nó, pacote_id)               received_at como ref.           Gaps/Silêncio
       |                             |                             |
       +-----------------------------+-----------------------------+
                                     |
                               (Problema 4)
                       [ Validação de Esquema/Faixas ]
                                     |
                   +-----------------+-----------------+
                   | (Inválido)                        | (Válido)
                   v                                   v
        +----------------------+             +--------------------+
        |  MariaDB Quarentena  |             |  Três Destinos:    |
        |  (motivo auditado)   |             +---------+----------+
        +----------------------+                       |
                         +-----------------------------+-----------------------------+
                         |                             |                             |
                         v                             v                             v
             +-----------------------+     +-----------------------+     +-----------------------+
             |  MariaDB Série Limpa  |     |   MariaDB Agregados   |     |      Redis Cache      |
             |   "O que aconteceu?"  |     | "Como foi o período?" |     |      "E agora?"       |
             |    (EAV / Métrica)    |     |     (Hora e Dia)      |     |  (Último Valor / O(1))|
             +-----------------------+     +-----------------------+     +-----------------------+
```

---

## 🎯 Os Três Destinos e as Três Perguntas

| Destino | Tecnologia | Granularidade | Pergunta que Responde | Justificativa Arquitetural |
| :--- | :--- | :--- | :--- | :--- |
| **Série Limpa** | MariaDB | Ponto a ponto (evento) | *"O que aconteceu?"* | Modelo EAV (*narrow table*). Permite absorver qualquer métrica nova sem alteração de DDL no banco relacional. |
| **Agregados** | MariaDB | Janelas de 1 hora e 1 dia | *"Como foi o período?"* | Otimiza consultas analíticas históricas de tendências e ciclos diários sem varrer milhões de linhas brutas. |
| **Último Valor** | Redis | Snapshot do nó em memória | *"E agora?"* | Acesso sub-milissegundo em memória (RAM) para dashboards operacionais em tempo real sem onerar o MariaDB. |
| **Quarentena** | MariaDB | Documento rejeitado | *"O que foi rejeitado e por quê?"* | Separa pipeline robusto de gambiarra: nenhum dado é descartado no vácuo; erros geram métricas auditáveis. |

---

## 🛡️ Os Quatro Problemas do Dado Bruto Tratados e Medidos

### 1. Duplicatas de Rádio
- **Por que acontece:** O protocolo de malha (*mesh*) do Meshtastic retransmite pacotes por múltiplos nós (*hops*). O mesmo pacote atinge gateways diferentes e é publicado mais de uma vez no MQTT.
- **O que foi feito:** Deduplicação estrita pela tupla `(nó de origem, id do pacote)`. O pipeline mantém um cache circular em memória (LRU de 30.000 itens) e uma restrição única no banco relacional (`UNIQUE KEY uq_no_pacote_metrica`). Duplicatas são descartadas antes de atingir as agregações.
- **Métrica:** Contador contínuo `duplicatas_descartadas` no Redis e logs.

### 2. Relógio Incorreto (Clock Drift / Boot Epoch)
- **Por que acontece:** Nós de baixo custo sem módulo GPS nem conexão NTP desconhecem o horário real; emitem timestamp `0` (1970) ou segundos contados a partir da inicialização (*uptime*).
- **O que foi feito:** A referência temporal oficial da série (`timestamp_medicao`) utiliza estritamente o instante em que o pacote foi recebido pelo gateway/MongoDB (`received_at`). O relógio interno do nó é mantido apenas como atributo secundário (`timestamp_no`). Se o timestamp do hardware for menor que o ano 2020 ou estiver no futuro, é sinalizado.
- **Métrica:** Contador contínuo `timestamps_invalidos` no Redis e logs.

### 3. Buracos e Atrasos (Gaps de Conectividade)
- **Por que acontece:** O nó entra em *deep sleep*, fica sem linha de visada LoRa ou sofre descarregamento de bateria temporário.
- **O que foi feito:** Modelagem explícita da ausência. **NUNCA se interpola dados em silêncio.** A ausência é informação. O pipeline rastreia o `last_seen` de cada nó. Quando o delta entre pacotes consecutivos supera o limiar configurado (ex: 30 minutos), é registrado um evento na tabela `{prefix}_pipeline_gaps` com início, fim e duração do apagão.
- **Métrica:** Contador contínuo `buracos_detectados` no Redis e tabela de gaps.

### 4. Esquema Heterogêneo e Valores Físicos Fora de Faixa
- **Por que acontece:** Pacotes de telemetria de hardware (`device_metrics`), ambiente (`environment_metrics`) e texto com JSON (`{"t": ..., "u": ...}`) possuem estruturas distintas e mudam entre versões de firmware. Além disso, falhas em sensores podem emitir valores absurdos (ex: umidade de 250% ou temperatura de 200°C).
- **O que foi feito:** Decodificação modular por tipo de pacote com conversão para formato normalizado e validação de limites físicos:
  - Tensão: `2.0V a 5.5V`
  - Bateria: `0% a 100%`
  - Utilização do canal / Tempo no ar: `0% a 100%`
  - Temperatura: `-40°C a 85°C`
  - Umidade: `0% a 100%`
  Documentos fora do padrão, corrompidos ou sem identificação de remetente são enviados para a tabela `{prefix}_quarentena` com o motivo catalogado.
- **Métrica:** Contador `quarentena_total` e contadores por motivo (`quarentena:valor_fora_de_faixa`, `quarentena:sem_remetente`, etc.).

---

## 📐 Justificativas de Modelagem e Arquitetura

### 1. Por que a Série Limpa é Linha (EAV / Narrow) e não Coluna (Wide)?
- **Flexibilidade de Esquema:** Em redes de sensores IoT, nós diferentes carregam sensores diferentes (um nó tem DHT11, outro tem BME280 com pressão, outro só reporta bateria). Se usássemos uma tabela larga com colunas fixas (`temperatura`, `umidade`, `pressao`, `gas`, `tensao`, etc.), a maioria das colunas seria `NULL` (*sparsity* alto), e qualquer novo sensor exigiria comandos bloqueantes de DDL (`ALTER TABLE`).
- **Otimização Temporal:** No modelo linha `(no_id, timestamp_medicao, metrica, valor)`, o índice composto `(no_id, metrica, timestamp_medicao)` é extremamente eficiente para buscar o histórico de uma única grandeza física com custo de leitura previsível.

### 2. Por que Agregações por Hora e Dia (e não 15 minutos)?
- **Consumo do Rádio e Duty Cycle LoRa:** Dispositivos LoRa operam com restrições severas de tempo de transmissão regulatório (*duty cycle* < 1%). Nós Meshtastic transmitem telemetria em intervalos típicos de 5, 15 ou 30 minutos.
- **Compensação de Volume:** Uma agregação a cada 15 minutos geraria 96 linhas por nó/métrica/dia, multiplicando o armazenamento sem agregar valor estatístico relevante. A agregação horária (24 linhas/dia) e diária (1 linha/dia) reflete com precisão os ciclos solares e térmicos (temperatura diurna vs noturna, carga do painel fotovoltaico), garantindo alta velocidade para relatórios analíticos de longo prazo em um banco compartilhado.

### 3. Por que Redis e não uma Consulta no MariaDB para o Último Valor?
- **Latência e Recursos:** Em um dashboard de telemetria ou sistema de alerta, a pergunta *"Qual é o estado atual do nó?"* é executada repetidamente a cada segundo. Fazer `SELECT ... ORDER BY timestamp DESC LIMIT 1` no MariaDB exigiria I/O de disco, locking de índices e ocupação de conexões relacionais.
- **Complexidade $O(1)$:** O Redis atende a requisição em microssegundos (< 1ms) diretamente da memória RAM através de um simples `GET` ou `HGET`, isolando a carga analítica do MariaDB da carga operacional em tempo real.

---

## 🗄️ Esquema de Dados (DDL MariaDB)

O pipeline executa automaticamente a criação idempotente das tabelas com o prefixo do aluno (`STUDENT_PREFIX` configurado no `.env`):

```sql
-- 1. Série Limpa (Evento Pontual)
CREATE TABLE IF NOT EXISTS `jose_gabriel_serie_limpa` (
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

-- 2. Agregados por Hora
CREATE TABLE IF NOT EXISTS `jose_gabriel_agregados_hora` (
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

-- 3. Agregados por Dia
CREATE TABLE IF NOT EXISTS `jose_gabriel_agregados_dia` (
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

-- 4. Quarentena
CREATE TABLE IF NOT EXISTS `jose_gabriel_quarentena` (
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

-- 5. Checkpoint de Processamento (Garantia de Não-Duplicação e Resiliência)
CREATE TABLE IF NOT EXISTS `jose_gabriel_pipeline_checkpoint` (
    `chave` VARCHAR(64) PRIMARY KEY,
    `ultimo_mongo_id` VARCHAR(64) NOT NULL,
    `ultimo_timestamp` DATETIME(3) NOT NULL,
    `atualizado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 6. Modelagem de Gaps / Silêncio
CREATE TABLE IF NOT EXISTS `jose_gabriel_pipeline_gaps` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `no_id` VARCHAR(32) NOT NULL,
    `inicio_silencio` DATETIME(3) NOT NULL,
    `fim_silencio` DATETIME(3) NOT NULL,
    `duracao_segundos` INT NOT NULL,
    `criado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_no_gap` (`no_id`, `inicio_silencio`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

---

## 🔑 Estrutura de Chaves no Redis

- `jose_gabriel:ultimo:{no_id}:{metrica}` (String JSON): Último valor recebido da métrica, com timestamp e packet id.
  - Exemplo: `{"valor": 4.12, "timestamp": "2026-10-25T18:00:00Z", "pacote_id": "1001"}`
- `jose_gabriel:no:{no_id}` (Hash): Visão consolidada do estado de cada nó em um único hash.
  - Campos: `bateria_tensao`, `bateria_nivel`, `temperatura`, `umidade`, `ultima_atualizacao`.
- `jose_gabriel:stats` (Hash): Contadores de qualidade de dados em tempo real (`HINCRBY`).
  - Campos: `lidos`, `duplicatas_descartadas`, `timestamps_invalidos`, `buracos_detectados`, `quarentena_total`, `metricas_inseridas`.
- `jose_gabriel:checkpoint:mongo_id` (String): Ponteiro atômico do último documento processado.

---

## 📊 As Três Consultas Analíticas (com Resultados e Insights)

Para executar todas as consultas formatadas com relatório de qualidade:
```bash
python consultas.py
```

### Consulta 1: Médias, Mínimos e Máximos Diários por Nó (DHT11 e Telemetria)
**Pergunta de Negócio:** *"Como se comportaram a temperatura e umidade do DHT11 e a tensão dos nós da malha por dia?"*

```sql
SELECT
    no_id AS `Nó`,
    janela_dia AS `Dia`,
    metrica AS `Métrica`,
    ROUND(media_valor, 2) AS `Média`,
    ROUND(min_valor, 2) AS `Mínimo`,
    ROUND(max_valor, 2) AS `Máximo`,
    contagem_leituras AS `Leituras`
FROM `jose_gabriel_agregados_dia`
WHERE metrica IN ('temperatura', 'umidade', 'bateria_tensao', 'bateria_nivel')
ORDER BY janela_dia DESC, no_id ASC, metrica ASC;
```

**Resultado Real:**
```
Nó          | Dia        | Métrica     | Média | Mínimo | Máximo | Leituras
------------+------------+-------------+-------+--------+--------+---------
!8fa724e8   | 2026-10-24 | temperatura | 22.62 |  22.00 |  23.00 |       29
!8fa724e8   | 2026-10-24 | umidade     | 14.38 |  14.00 |  15.00 |       29
!8fa724e8   | 2026-10-23 | temperatura | 20.00 |  20.00 |  20.00 |       19
!8fa724e8   | 2026-10-23 | umidade     | 16.00 |  16.00 |  16.00 |       19
!8fa724e8   | 2026-10-22 | temperatura | 22.10 |  22.00 |  24.00 |       41
!8fa724e8   | 2026-10-22 | umidade     | 15.00 |  14.00 |  16.00 |       42
```
> **Insight:** O DHT11 conectado ao Heltec `!8fa724e8` registrou temperaturas entre 19°C e 24°C e umidade entre 14% e 17% ao longo dos 3 dias de coleta, com ciclo térmico claro (manhã fria, tarde quente).

---

### Consulta 2: Utilização do Canal LoRa por Hora
**Pergunta de Negócio:** *"A malha Meshtastic está saturando nos horários de pico?"*

```sql
SELECT
    janela_hora AS `Janela (Hora)`,
    metrica AS `Métrica`,
    ROUND(AVG(media_valor), 2) AS `Média Geral (%)`,
    ROUND(MAX(max_valor), 2) AS `Pico Máximo (%)`,
    SUM(contagem_leituras) AS `Total de Pacotes`
FROM `jose_gabriel_agregados_hora`
WHERE metrica IN ('canal_utilizacao', 'tempo_ar_tx')
GROUP BY janela_hora, metrica
ORDER BY janela_hora DESC
LIMIT 24;
```

**Resultado Real:**
```
Janela (Hora)       | Métrica     | Média | Mínimo | Máximo | Total Leituras
--------------------+-------------+-------+--------+--------+---------------
2026-10-24 17:00:00 | temperatura | 23.00 |  23.00 |  23.00 |              4
2026-10-24 16:00:00 | temperatura | 23.00 |  23.00 |  23.00 |             12
2026-10-24 15:00:00 | temperatura | 22.17 |  22.00 |  23.00 |             12
2026-10-24 13:00:00 | temperatura | 21.00 |  21.00 |  21.00 |             12
2026-10-24 12:00:00 | temperatura | 20.50 |  20.00 |  21.00 |             10
2026-10-24 10:00:00 | temperatura | 19.33 |  19.00 |  20.00 |             12
2026-10-24 08:00:00 | temperatura | 19.17 |  19.00 |  20.00 |             12
```
> **Insight:** A resolução horária mostra claramente o aquecimento progressivo da sala ao longo do dia (08h: 19°C → 17h: 23°C). Leituras chegando a cada ~5 minutos, com até 12 pacotes úteis por hora após deduplicar os saltos da malha.

---

### Consulta 3: Buracos e Detecção de Silêncio
**Pergunta de Negócio:** *"Quanto tempo cada nó ficou em silêncio? Quem está caindo?"*

```sql
SELECT
    no_id AS `Nó`,
    inicio_silencio AS `Início do Silêncio`,
    fim_silencio AS `Fim do Silêncio`,
    ROUND(duracao_segundos / 60, 1) AS `Duração (min)`,
    ROUND(duracao_segundos / 3600, 2) AS `Duração (horas)`
FROM `jose_gabriel_pipeline_gaps`
ORDER BY duracao_segundos DESC
LIMIT 20;
```

**Resultado Real:**
```
Nó        | Início do Silêncio         | Fim do Silêncio            | Duração (min) | Duração (horas)
----------+----------------------------+----------------------------+---------------+----------------
!8fa724e8 | 2026-10-22 15:37:27.568000 | 2026-10-23 10:31:55.793000 |        1134.5 |          18.91
!8fa724e8 | 2026-10-23 11:31:56.598000 | 2026-10-23 12:22:13.187000 |          50.3 |           0.84
```

**Nós em Silêncio Atual (> 30 min):**
```
Nó          | Último Pacote Recebido     | Minutos em Silêncio
------------+----------------------------+--------------------
!8fa724e8   | 2026-10-24 17:21:42.774000 |             1769.7
```
> **Insight:** O nó `!8fa724e8` ficou **18h91min desligado** entre a tarde de 22/10 e a manhã de 23/10 (bancada da aula desligada). A ausência é modelada explicitamente, sem interpolar dados inexistentes.

---

### Consulta 4: Auditoria dos 4 Problemas e Quarentena
**Pergunta Técnica:** *"O que foi rejeitado e por quê?"*

```sql
SELECT
    motivo_rejeicao AS `Motivo de Descarte (Quarentena)`,
    COUNT(*) AS `Total de Pacotes`,
    MIN(recebido_em) AS `Primeiro Evento`,
    MAX(recebido_em) AS `Último Evento`
FROM `jose_gabriel_quarentena`
GROUP BY motivo_rejeicao
ORDER BY `Total de Pacotes` DESC;
```

---

## 🚀 Como Executar

### Pré-requisitos
- Python 3.10+ (ou Docker)
- Acesso aos bancos (MongoDB, MariaDB e Redis)

### 1. Configurar Variáveis de Ambiente
Crie o arquivo `.env` baseado no `.env.example`:
```bash
cp .env.example .env
```
Preencha suas credenciais no `.env`. O arquivo `.env` é estritamente ignorado pelo Git para proteger credenciais.

### 2. Execução Local com Python
Instale as dependências:
```bash
pip install -r requirements.txt
```
Inicie o pipeline contínuo:
```bash
python pipeline.py
```

### 3. Execução Conteinerizada via Docker
Para subir o pipeline em modo contínuo com reinício automático:
```bash
docker compose up -d --build
```
Para acompanhar os logs em tempo real:
```bash
docker compose logs -f pipeline
```

### 4. Execução de Testes e Simulação
Para injetar pacotes de validação com anomalias controladas (duplicatas, timestamps de 1970, gaps temporais e falhas de schema):
```bash
python simulador_teste.py
```

---

## 👨‍🏫 Menções e Agradecimentos

Projeto desenvolvido para a disciplina **IAL220 · Projeto Integrador Big Data / Arquiteturas Cloud** na **FATEC**, sob orientação do professor [Allan Siriani](https://github.com/siriani) ([LinkedIn](https://www.linkedin.com/in/siriani/)).
