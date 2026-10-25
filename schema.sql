-- =============================================================================
-- Modelagem de Dados - Pipeline Meshtastic (IAL220 - Atividade 10)
-- Banco de Destino: MariaDB (fatec_iot)
-- Substitua `{PREFIX}` pelo seu prefixo (ex: jose_gabriel)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. SÉRIE LIMPA (Responde: "O que aconteceu?")
-- Modelo EAV / Tabela Estreita (Narrow Table)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{PREFIX}_serie_limpa` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `no_id` VARCHAR(32) NOT NULL COMMENT 'Identificador único do nó transmissor (ex: !8fa724e8)',
    `timestamp_medicao` DATETIME(3) NOT NULL COMMENT 'Instante oficial de recebimento UTC registrado pelo gateway/consumidor',
    `timestamp_no` BIGINT NULL COMMENT 'Timestamp original reportado pelo hardware do nó (relógio não confiável)',
    `metrica` VARCHAR(64) NOT NULL COMMENT 'Nome padronizado da grandeza física',
    `valor` DOUBLE NOT NULL COMMENT 'Valor numérico medido',
    `pacote_id` VARCHAR(64) NULL COMMENT 'Identificador de sequência do pacote Meshtastic',
    `hop_limit` INT NULL COMMENT 'Limite restante de saltos na malha mesh',
    `rx_snr` FLOAT NULL COMMENT 'Relação sinal-ruído da recepção (dB)',
    `rx_rssi` INT NULL COMMENT 'Intensidade de sinal recebido (dBm)',
    `criado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY `uq_no_pacote_metrica` (`no_id`, `pacote_id`, `metrica`),
    INDEX `idx_no_metrica_tempo` (`no_id`, `metrica`, `timestamp_medicao`),
    INDEX `idx_tempo` (`timestamp_medicao`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 2. AGREGADOS POR HORA (Responde: "Como foi o período?")
-- Agregações horárias pré-calculadas para tendências e tráfego diário
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{PREFIX}_agregados_hora` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `no_id` VARCHAR(32) NOT NULL,
    `metrica` VARCHAR(64) NOT NULL,
    `janela_hora` DATETIME NOT NULL COMMENT 'Início da janela horária (ex: 2026-10-25 18:00:00)',
    `media_valor` DOUBLE NOT NULL,
    `min_valor` DOUBLE NOT NULL,
    `max_valor` DOUBLE NOT NULL,
    `contagem_leituras` INT NOT NULL,
    `atualizado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uq_agregado_hora` (`no_id`, `metrica`, `janela_hora`),
    INDEX `idx_hora_busca` (`no_id`, `metrica`, `janela_hora`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 3. AGREGADOS POR DIA (Responde: "Como foi a semana/mês?")
-- Agregações diárias para análise de autonomia solar e sazonalidade
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{PREFIX}_agregados_dia` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `no_id` VARCHAR(32) NOT NULL,
    `metrica` VARCHAR(64) NOT NULL,
    `janela_dia` DATE NOT NULL COMMENT 'Data do dia (ex: 2026-10-25)',
    `media_valor` DOUBLE NOT NULL,
    `min_valor` DOUBLE NOT NULL,
    `max_valor` DOUBLE NOT NULL,
    `contagem_leituras` INT NOT NULL,
    `atualizado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uq_agregado_dia` (`no_id`, `metrica`, `janela_dia`),
    INDEX `idx_dia_busca` (`no_id`, `metrica`, `janela_dia`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 4. TABELA DE QUARENTENA (Responde: "O que foi rejeitado e por quê?")
-- Isola pacotes malformados, fora de faixa física ou com schema corrompido
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{PREFIX}_quarentena` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `mongo_id` VARCHAR(64) NULL COMMENT '_id original do documento no MongoDB',
    `no_id` VARCHAR(32) NULL,
    `pacote_id` VARCHAR(64) NULL,
    `tipo_pacote` VARCHAR(64) NULL,
    `motivo_rejeicao` VARCHAR(255) NOT NULL COMMENT 'Causa detalhada do descarte',
    `payload_bruto` LONGTEXT NULL COMMENT 'Cópia íntegra do pacote para auditoria',
    `recebido_em` DATETIME(3) NULL,
    `criado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_motivo` (`motivo_rejeicao`),
    INDEX `idx_no_quarentena` (`no_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 5. TABELA DE CHECKPOINT (Garante: "Reinicia sem duplicar nem perder")
-- Persistência transacional do ponteiro de leitura do MongoDB
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{PREFIX}_pipeline_checkpoint` (
    `chave` VARCHAR(64) PRIMARY KEY,
    `ultimo_mongo_id` VARCHAR(64) NOT NULL,
    `ultimo_timestamp` DATETIME(3) NOT NULL,
    `atualizado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 6. MODELAGEM DE BURACOS / SILÊNCIO (Responde: "Quem está caindo e quando?")
-- Registra eventos de blackout/silêncio prolongado sem interpolar dados falsos
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{PREFIX}_pipeline_gaps` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `no_id` VARCHAR(32) NOT NULL,
    `inicio_silencio` DATETIME(3) NOT NULL,
    `fim_silencio` DATETIME(3) NOT NULL,
    `duracao_segundos` INT NOT NULL,
    `criado_em` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_no_gap` (`no_id`, `inicio_silencio`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
