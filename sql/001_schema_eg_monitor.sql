-- Tabela do placar ao vivo (fora da cadeia de hash). As tabelas execucao,
-- intervencao, meta e as views vw_* ja existem no banco dw_trt; adicione aqui
-- seus DDLs de referencia quando quiser versiona-los.

CREATE TABLE IF NOT EXISTS eg_monitor.execucao_atual (
    job              TEXT PRIMARY KEY,
    grau             TEXT,
    numero_tentativa INTEGER,
    inicio           TIMESTAMP,
    status           TEXT NOT NULL,
    atualizado_em    TIMESTAMP NOT NULL DEFAULT now()
);

-- Acompanhamento:
-- SELECT job, grau, numero_tentativa, inicio, status, now()-inicio AS decorrido
-- FROM eg_monitor.execucao_atual ORDER BY inicio;
