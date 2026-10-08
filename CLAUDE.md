# monitor_etl

Watchdog em um unico arquivo Python (`monitor_etl.py`) que dispara os jobs Pentaho do e-Gestao/PAI do TRT-2 via `.bat`, valida o resultado nos bancos e grava trilha com hash encadeado.

## Restricoes
- Um unico script Python. Nao dividir em modulos.
- Producao: Windows Server 2008 R2, Python 3.8 (ao lado do 2.7.18). Manter compatibilidade 3.8 (sem `match`, sem `X | Y` em tipos).
- Oracle 19c: modo thin falha (cryptography>=43, DPY-3016) e Instant Client 19c nao carrega (GetOverlappedResultEx). Usa-se thick com Instant Client 11.2 em `C:\oracle\instantclient_11_2` (init preguicoso).
- Credenciais: constantes no inicio do `monitor_etl.py`, preenchidas so no servidor (decisao do usuario, sem .env). No git ficam como placeholder `COLE_A_SENHA_AQUI`; nunca commitar a senha real.
- Consultas ao banco que o usuario deve rodar vao como SQL para o DBeaver.
- Este ambiente (nuvem) nao alcanca Postgres/Oracle internos: aqui so se edita e roda `pytest testes`.
- Texto em portugues do Brasil, sem travessoes nem estilo de IA.

## Cadeia (so 1o grau)
staging -> primarias -> carga_diaria -> fecha_remessa. Lote fixo `10`, remessa D-1 (AAAAMMDD).
- staging e primarias: criticas, retentam a cada 60 s ate 20h (HORA_LIMITE_CRITICOS); validam em `pje_eg.tb_status_carga` (somente leitura).
- carga_diaria: 5 tentativas, backoff [60,300,600] s; valida `eg.egt_info_item` (Oracle).
- fecha_remessa: 5 tentativas; valida so o codigo de saida.
- So falhas transitorias/desconhecidas/timeout/validacao sao retentadas. Timeout mata a arvore Java.

## Banco (dw_trt, schema eg_monitor)
Tabelas: `execucao` (1 linha por tentativa, hash encadeado), `intervencao` (acao humana, motivo >= 20 chars), `meta`, `execucao_atual` (placar ao vivo, FORA da cadeia). Views (DDL em sql/000): vw_macroprocesso, vw_recuperacao, vw_indicador_diario, vw_indicador_30d, vw_indicador_valor, vw_amostra_historica, vw_indicador_vs_meta, vw_meta_sugerida, vw_saude.
Hash: `duracao_s` com 3 casas; CHAR com `rstrip()`. A ancora (12 primeiros caracteres do hash final de `verificar`) deve ser guardada fora do banco. `checar_metas` alerta quando `vw_indicador_vs_meta` da NAO_ATINGIU (valor_meta + operador de `meta`). `vw_meta_sugerida` propoe metas de duracao/MTTR por mediana + 2xMAD (minimo 10 amostras; a taxa de sucesso fica de fora).

## Estimativa, qualidade e impacto
Estimativa = mediana das ultimas 30 execucoes com SUCESSO do job (minimo 3). Gravada em `execucao_atual` (duracao_estimada_s, previsao_fim) e logada. Views `vw_qualidade_job` e `vw_erro_impacto` em sql/002; `relatorio` as imprime.

## Sazonalidade, tendencia e previsao
sql/003: `vw_qualidade_diaria`, `vw_qualidade_faixa_mes` (inicio/meio/fim do mes), `vw_tendencia_qualidade` (regressao linear de 60 dias; SEM_BASE com menos de 10 dias). Previsao simples, nao e modelo de ML.

## Subcomandos
`py -3.8 monitor_etl.py` | `intervir` | `verificar` | `checar_metas` | `relatorio`

## Pendencias de implantacao (lado do usuario)
Criar `execucao_atual` (sql/001), trocar o script em producao, testar timeout, registrar uma intervencao de teste, rodar `verificar` e guardar a ancora, agendar as duas tarefas no Windows, desativar a tarefa do JAR legado. Trocar as senhas do Postgres e do Oracle (estiveram no codigo).
