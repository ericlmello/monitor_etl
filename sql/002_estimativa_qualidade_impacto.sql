-- Rode no DBeaver (banco dw_trt). Pode ser rodado mais de uma vez.

-- 1) Estimativa de tempo no placar ao vivo
ALTER TABLE eg_monitor.execucao_atual ADD COLUMN IF NOT EXISTS duracao_estimada_s NUMERIC(14,3);
ALTER TABLE eg_monitor.execucao_atual ADD COLUMN IF NOT EXISTS previsao_fim TIMESTAMP;

-- 2) Indicador de qualidade por job, ultimos 30 dias.
--    qualidade_primeira_pct: % das execucoes do job concluidas na 1a tentativa.
--    taxa_conclusao_pct: % que terminou com sucesso, com ou sem retentativa.
CREATE OR REPLACE VIEW eg_monitor.vw_qualidade_job AS
WITH por_execucao AS (
    SELECT id_execucao, job,
           max(numero_tentativa) AS tentativas,
           bool_or(status = 'SUCESSO') AS concluiu,
           bool_or(status = 'SUCESSO' AND numero_tentativa = 1) AS de_primeira
    FROM eg_monitor.execucao
    WHERE inicio >= now() - interval '30 days'
    GROUP BY id_execucao, job
)
SELECT job,
       count(*) AS execucoes,
       round(100.0 * count(*) FILTER (WHERE de_primeira) / count(*), 1) AS qualidade_primeira_pct,
       round(100.0 * count(*) FILTER (WHERE concluiu) / count(*), 1)    AS taxa_conclusao_pct,
       round(avg(tentativas), 2) AS tentativas_medias
FROM por_execucao
GROUP BY job;

-- 3) Frequencia de erros x impacto, ultimos 30 dias.
--    horas_perdidas: soma da duracao das tentativas que falharam.
--    execucoes_sem_conclusao: execucoes em que o job nunca chegou a SUCESSO
--    (impacto alto: a cadeia parou e a remessa nao fechou).
--    impacto: ALTO se houve execucao sem conclusao; MEDIO se perdeu 1h ou mais; BAIXO nos demais.
CREATE OR REPLACE VIEW eg_monitor.vw_erro_impacto AS
WITH concluidas AS (
    SELECT DISTINCT id_execucao, job
    FROM eg_monitor.execucao
    WHERE status = 'SUCESSO'
)
SELECT e.job,
       coalesce(e.categoria_erro, e.status) AS categoria_erro,
       count(*) AS ocorrencias,
       count(DISTINCT e.id_execucao) AS execucoes_afetadas,
       round(sum(e.duracao_s) / 3600.0, 2) AS horas_perdidas,
       count(DISTINCT e.id_execucao) FILTER (WHERE c.id_execucao IS NULL) AS execucoes_sem_conclusao,
       CASE
           WHEN count(DISTINCT e.id_execucao) FILTER (WHERE c.id_execucao IS NULL) > 0 THEN 'ALTO'
           WHEN sum(e.duracao_s) >= 3600 THEN 'MEDIO'
           ELSE 'BAIXO'
       END AS impacto
FROM eg_monitor.execucao e
LEFT JOIN concluidas c ON c.id_execucao = e.id_execucao AND c.job = e.job
WHERE e.status IN ('FALHA', 'BLOQUEADO_JANELA')
  AND e.inicio >= now() - interval '30 days'
GROUP BY e.job, coalesce(e.categoria_erro, e.status)
ORDER BY ocorrencias DESC;

-- Conferencia:
-- SELECT * FROM eg_monitor.vw_qualidade_job;
-- SELECT * FROM eg_monitor.vw_erro_impacto;
-- SELECT job, numero_tentativa, inicio, previsao_fim, now()-inicio AS decorrido FROM eg_monitor.execucao_atual;
