-- Definicoes das views que ja existem em eg_monitor (extraidas de information_schema.views).
-- Referencia/versionamento: CREATE OR REPLACE com a mesma definicao nao altera nada.
-- Ordem respeita as dependencias entre as views.

CREATE OR REPLACE VIEW eg_monitor.vw_macroprocesso AS
SELECT t.job,
    t.etapa_macroprocesso,
    t.ordem
   FROM ( VALUES ('staging'::text,'Staging'::text,1), ('primarias'::text,'Tabelas primárias'::text,2), ('carga_diaria'::text,'Remessa diária'::text,3), ('fecha_remessa'::text,'Fechamento'::text,4)) t(job, etapa_macroprocesso, ordem);

CREATE OR REPLACE VIEW eg_monitor.vw_recuperacao AS
SELECT execucao.id_execucao,
    execucao.job,
    min(execucao.inicio) FILTER (WHERE ((execucao.status)::text = ANY ((ARRAY['FALHA'::character varying, 'TIMEOUT'::character varying])::text[]))) AS primeira_falha,
    max(execucao.fim) FILTER (WHERE ((execucao.status)::text = 'SUCESSO'::text)) AS recuperado_em,
    round((EXTRACT(epoch FROM (max(execucao.fim) FILTER (WHERE ((execucao.status)::text = 'SUCESSO'::text)) - min(execucao.inicio) FILTER (WHERE ((execucao.status)::text = ANY ((ARRAY['FALHA'::character varying, 'TIMEOUT'::character varying])::text[]))))) / (60)::numeric), 1) AS minutos_ate_recuperar
   FROM eg_monitor.execucao
  GROUP BY execucao.id_execucao, execucao.job
 HAVING bool_or(((execucao.status)::text = ANY ((ARRAY['FALHA'::character varying, 'TIMEOUT'::character varying])::text[])));

CREATE OR REPLACE VIEW eg_monitor.vw_indicador_diario AS
SELECT ((e.timestamp_registro AT TIME ZONE 'America/Sao_Paulo'::text))::date AS dia,
    e.job,
    vm.etapa_macroprocesso,
    count(*) AS tentativas,
    count(*) FILTER (WHERE ((e.status)::text = 'SUCESSO'::text)) AS sucessos,
    count(*) FILTER (WHERE ((e.status)::text = 'FALHA'::text)) AS falhas,
    count(*) FILTER (WHERE ((e.status)::text = 'TIMEOUT'::text)) AS timeouts,
    count(*) FILTER (WHERE ((e.status)::text = 'BLOQUEADO_JANELA'::text)) AS bloqueios_por_horario,
    round((avg(e.duracao_s) FILTER (WHERE ((e.status)::text = 'SUCESSO'::text)) / (60)::numeric), 1) AS duracao_media_min
   FROM (eg_monitor.execucao e
     LEFT JOIN eg_monitor.vw_macroprocesso vm USING (job))
  GROUP BY (((e.timestamp_registro AT TIME ZONE 'America/Sao_Paulo'::text))::date), e.job, vm.etapa_macroprocesso;

CREATE OR REPLACE VIEW eg_monitor.vw_indicador_30d AS
SELECT e.job,
    count(DISTINCT e.id_execucao) AS execucoes,
    round(((100.0 * (count(DISTINCT e.id_execucao) FILTER (WHERE ((e.status)::text = 'SUCESSO'::text)))::numeric) / (NULLIF(count(DISTINCT e.id_execucao), 0))::numeric), 1) AS taxa_sucesso_pct,
    round((avg(e.duracao_s) FILTER (WHERE ((e.status)::text = 'SUCESSO'::text)) / (60)::numeric), 1) AS duracao_media_min,
    ( SELECT round(avg(r.minutos_ate_recuperar), 1) AS round
           FROM eg_monitor.vw_recuperacao r
          WHERE (((r.job)::text = (e.job)::text) AND (r.primeira_falha >= (now() - '30 days'::interval)))) AS mttr_medio_min
   FROM eg_monitor.execucao e
  WHERE (e.timestamp_registro >= (now() - '30 days'::interval))
  GROUP BY e.job;

CREATE OR REPLACE VIEW eg_monitor.vw_indicador_valor AS
SELECT vw_indicador_30d.job,
    'taxa_sucesso_pct'::character varying AS indicador,
    vw_indicador_30d.taxa_sucesso_pct AS valor
   FROM eg_monitor.vw_indicador_30d
UNION ALL
 SELECT vw_indicador_30d.job,
    'duracao_media_min'::character varying AS indicador,
    vw_indicador_30d.duracao_media_min AS valor
   FROM eg_monitor.vw_indicador_30d
UNION ALL
 SELECT vw_indicador_30d.job,
    'mttr_medio_min'::character varying AS indicador,
    vw_indicador_30d.mttr_medio_min AS valor
   FROM eg_monitor.vw_indicador_30d;

CREATE OR REPLACE VIEW eg_monitor.vw_amostra_historica AS
SELECT vw_indicador_diario.job,
    'taxa_sucesso_pct'::character varying AS indicador,
    ((100.0 * (vw_indicador_diario.sucessos)::numeric) / (NULLIF(vw_indicador_diario.tentativas, 0))::numeric) AS valor
   FROM eg_monitor.vw_indicador_diario
  WHERE (vw_indicador_diario.tentativas > 0)
UNION ALL
 SELECT vw_indicador_diario.job,
    'duracao_media_min'::character varying AS indicador,
    vw_indicador_diario.duracao_media_min AS valor
   FROM eg_monitor.vw_indicador_diario
  WHERE (vw_indicador_diario.duracao_media_min IS NOT NULL)
UNION ALL
 SELECT vw_recuperacao.job,
    'mttr_medio_min'::character varying AS indicador,
    vw_recuperacao.minutos_ate_recuperar AS valor
   FROM eg_monitor.vw_recuperacao
  WHERE (vw_recuperacao.minutos_ate_recuperar IS NOT NULL);

CREATE OR REPLACE VIEW eg_monitor.vw_indicador_vs_meta AS
SELECT v.job,
    vm.etapa_macroprocesso,
    v.indicador,
    v.valor,
    m.valor_meta,
    m.operador,
    m.descricao,
        CASE
            WHEN (m.valor_meta IS NULL) THEN 'SEM_META'::text
            WHEN (((m.operador)::text = '>='::text) AND (v.valor >= m.valor_meta)) THEN 'ATINGIU'::text
            WHEN (((m.operador)::text = '<='::text) AND (v.valor <= m.valor_meta)) THEN 'ATINGIU'::text
            ELSE 'NAO_ATINGIU'::text
        END AS status_meta
   FROM ((eg_monitor.vw_indicador_valor v
     LEFT JOIN eg_monitor.meta m ON ((((m.job)::text = (v.job)::text) AND ((m.indicador)::text = (v.indicador)::text))))
     LEFT JOIN eg_monitor.vw_macroprocesso vm ON ((vm.job = (v.job)::text)));

CREATE OR REPLACE VIEW eg_monitor.vw_meta_sugerida AS
WITH mediana AS (
         SELECT vw_amostra_historica.job,
            vw_amostra_historica.indicador,
            count(*) AS n,
            percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((vw_amostra_historica.valor)::double precision)) AS mediana
           FROM eg_monitor.vw_amostra_historica
          GROUP BY vw_amostra_historica.job, vw_amostra_historica.indicador
        ), mad AS (
         SELECT a.job,
            a.indicador,
            percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY (abs(((a.valor)::double precision - m_1.mediana)))) AS mad
           FROM (eg_monitor.vw_amostra_historica a
             JOIN mediana m_1 USING (job, indicador))
          GROUP BY a.job, a.indicador
        )
 SELECT m.job,
    m.indicador,
    m.n AS amostras,
    round((m.mediana)::numeric, 2) AS mediana,
    round((((1.4826)::double precision * COALESCE(d.mad, (0)::double precision)))::numeric, 2) AS desvio_robusto,
        CASE m.indicador
            WHEN 'taxa_sucesso_pct'::text THEN '>='::text
            ELSE '<='::text
        END AS operador,
    round((
        CASE m.indicador
            WHEN 'taxa_sucesso_pct'::text THEN GREATEST((0)::double precision, LEAST((100)::double precision, (m.mediana - ((((2)::numeric * 1.4826))::double precision * COALESCE(d.mad, (0)::double precision)))))
            ELSE GREATEST((0)::double precision, (m.mediana + ((((2)::numeric * 1.4826))::double precision * COALESCE(d.mad, (0)::double precision))))
        END)::numeric, 2) AS valor_meta_sugerida
   FROM (mediana m
     JOIN mad d USING (job, indicador))
  WHERE ((m.n >= 10) AND ((m.indicador)::text <> 'taxa_sucesso_pct'::text));

CREATE OR REPLACE VIEW eg_monitor.vw_saude AS
WITH ultima AS (
         SELECT DISTINCT ON (execucao.job) execucao.id,
            execucao.id_execucao,
            execucao.job,
            execucao.grau,
            execucao.numero_tentativa,
            execucao.timestamp_registro,
            execucao.inicio,
            execucao.fim,
            execucao.duracao_s,
            execucao.status,
            execucao.codigo_saida,
            execucao.categoria_erro,
            execucao.retentavel,
            execucao.etapa_origem,
            execucao.mensagem_erro_limpa,
            execucao.resultado_validacao,
            execucao.hash_log_sha256,
            execucao.hash_anterior,
            execucao.verificador_integridade
           FROM eg_monitor.execucao
          ORDER BY execucao.job, execucao.id DESC
        ), ok AS (
         SELECT execucao.job,
            max(execucao.fim) AS fim_ultimo_sucesso
           FROM eg_monitor.execucao
          WHERE ((execucao.status)::text = 'SUCESSO'::text)
          GROUP BY execucao.job
        )
 SELECT u.job,
    vm.etapa_macroprocesso,
    u.grau,
    u.status AS status_ultimo_registro,
    u.timestamp_registro,
    ok.fim_ultimo_sucesso,
    round((EXTRACT(epoch FROM (now() - ok.fim_ultimo_sucesso)) / (3600)::numeric), 1) AS idade_horas,
    u.etapa_origem,
    u.mensagem_erro_limpa,
    u.resultado_validacao,
    "left"((u.verificador_integridade)::text, 12) AS verificador_curto,
    (EXISTS ( SELECT 1
           FROM eg_monitor.intervencao i
          WHERE ((i.id_execucao = u.id_execucao) AND ((i.job)::text = (u.job)::text)))) AS houve_intervencao_humana
   FROM ((ultima u
     LEFT JOIN ok USING (job))
     LEFT JOIN eg_monitor.vw_macroprocesso vm USING (job));
