-- Rode no DBeaver (banco dw_trt) depois do 002. Pode ser rodado mais de uma vez.
-- Tendencia e previsao aqui sao regressao linear simples sobre o historico de
-- eg_monitor.execucao. Com poucos dias de historico o resultado e SEM_BASE.

-- 1) Qualidade por dia e job. qualidade_primeira_pct = % das execucoes do dia
--    concluidas na 1a tentativa. faixa_mes separa inicio, meio e fim do mes.
CREATE OR REPLACE VIEW eg_monitor.vw_qualidade_diaria AS
WITH por_execucao AS (
    SELECT id_execucao, job,
           min(inicio)::date AS dia,
           bool_or(status = 'SUCESSO' AND numero_tentativa = 1) AS de_primeira,
           count(*) FILTER (WHERE status IN ('FALHA', 'BLOQUEADO_JANELA')) AS falhas,
           sum(duracao_s) FILTER (WHERE status IN ('FALHA', 'BLOQUEADO_JANELA')) AS segundos_perdidos,
           sum(duracao_s) FILTER (WHERE status = 'SUCESSO') AS segundos_sucesso
    FROM eg_monitor.execucao
    GROUP BY id_execucao, job
)
SELECT dia, job,
       extract(day FROM dia)::int AS dia_do_mes,
       CASE WHEN extract(day FROM dia) <= 10 THEN '1-10 inicio'
            WHEN extract(day FROM dia) <= 20 THEN '11-20 meio'
            ELSE '21-fim' END AS faixa_mes,
       count(*) AS execucoes,
       round(100.0 * count(*) FILTER (WHERE de_primeira) / count(*), 1) AS qualidade_primeira_pct,
       sum(falhas) AS falhas,
       round(coalesce(sum(segundos_perdidos), 0) / 3600.0, 2) AS horas_perdidas,
       round(avg(segundos_sucesso) / 60.0, 1) AS duracao_media_min
FROM por_execucao
GROUP BY dia, job;

-- 2) Frequencia de erros e qualidade por faixa do mes (ultimos 180 dias).
--    Mostra se a qualidade cai perto do fim do mes.
CREATE OR REPLACE VIEW eg_monitor.vw_qualidade_faixa_mes AS
SELECT job, faixa_mes,
       count(*) AS dias,
       round(avg(qualidade_primeira_pct), 1) AS qualidade_primeira_pct,
       round(sum(falhas)::numeric / count(*), 2) AS falhas_por_dia,
       round(sum(horas_perdidas), 2) AS horas_perdidas,
       round(avg(duracao_media_min), 1) AS duracao_media_min
FROM eg_monitor.vw_qualidade_diaria
WHERE dia >= current_date - 180
GROUP BY job, faixa_mes
ORDER BY job, faixa_mes;

-- 3) Tendencia e previsao por job (ultimos 60 dias, regressao linear).
--    qualidade_pp_por_dia: pontos percentuais por dia (negativo = piorando).
--    qualidade_prevista_7d: valor projetado para daqui a 7 dias, limitado a 0-100.
--    duracao_prevista_amanha_min: duracao projetada para o proximo dia.
--    tendencia exige ao menos 10 dias com dados; limiar de 0,2 pp/dia.
CREATE OR REPLACE VIEW eg_monitor.vw_tendencia_qualidade AS
WITH base AS (
    SELECT job, (dia - current_date) AS x,
           qualidade_primeira_pct AS q, duracao_media_min AS d
    FROM eg_monitor.vw_qualidade_diaria
    WHERE dia >= current_date - 60
)
SELECT job,
       count(*) AS dias_com_dados,
       round(regr_slope(q, x)::numeric, 3) AS qualidade_pp_por_dia,
       round(least(100, greatest(0, regr_intercept(q, x) + regr_slope(q, x) * 7))::numeric, 1) AS qualidade_prevista_7d,
       round(regr_slope(d, x)::numeric, 2) AS duracao_min_por_dia,
       round(greatest(0, regr_intercept(d, x) + regr_slope(d, x))::numeric, 1) AS duracao_prevista_amanha_min,
       CASE WHEN count(*) < 10 THEN 'SEM_BASE'
            WHEN regr_slope(q, x) < -0.2 THEN 'PIORANDO'
            WHEN regr_slope(q, x) > 0.2 THEN 'MELHORANDO'
            ELSE 'ESTAVEL' END AS tendencia
FROM base
GROUP BY job;

-- Conferencia:
-- SELECT * FROM eg_monitor.vw_qualidade_faixa_mes;
-- SELECT * FROM eg_monitor.vw_tendencia_qualidade;
