# monitor_etl

Watchdog PAI em Python 3.8 para os jobs Pentaho de carga do e-Gestao (staging, tabelas primarias, carga diaria, fecha remessa). Detalhes de regras e restricoes em `CLAUDE.md`.

## Instalacao no servidor
1. `py -3.8 -m pip install -r requirements.txt`
2. Edite `POSTGRES_PASSWORD` e `ORACLE_PASSWORD` no inicio do `monitor_etl.py` (no repositorio estao como placeholder).
3. Rode `sql/001_schema_eg_monitor.sql` e depois `sql/002_estimativa_qualidade_impacto.sql` e `sql/003_tendencia_sazonalidade.sql` no DBeaver.
4. Instant Client 11.2 em `C:\oracle\instantclient_11_2`.

## Uso
    py -3.8 monitor_etl.py
    py -3.8 monitor_etl.py intervir --id-execucao X --job Y --acao REEXECUTOU --responsavel "Nome" --papel plantao-setic --motivo "..."
    py -3.8 monitor_etl.py verificar
    py -3.8 monitor_etl.py checar_metas
    py -3.8 monitor_etl.py relatorio
    py -3.8 monitor_etl.py painel            (abre em http://127.0.0.1:8080)

## Testes
    pip install pytest psycopg2-binary oracledb
    python -m pytest testes

## Seguranca
No repositorio as senhas sao placeholder. Preencha so a copia do servidor e nao faca commit dela. Troque as senhas antigas do Postgres e do Oracle.
