# monitor_etl

Watchdog PAI em Python 3.8 para os jobs Pentaho de carga do e-Gestao (staging, tabelas primarias, carga diaria, fecha remessa). Detalhes de regras e restricoes em `CLAUDE.md`.

## Instalacao no servidor
1. `py -3.8 -m pip install -r requirements.txt`
2. Copie `monitor_etl.env.example` para `C:\etl_monitor\monitor_etl.env` e preencha as senhas.
3. Rode `sql/001_schema_eg_monitor.sql` no DBeaver.
4. Instant Client 11.2 em `C:\oracle\instantclient_11_2`.

## Uso
    py -3.8 monitor_etl.py
    py -3.8 monitor_etl.py intervir --id-execucao X --job Y --acao REEXECUTOU --responsavel "Nome" --papel plantao-setic --motivo "..."
    py -3.8 monitor_etl.py verificar
    py -3.8 monitor_etl.py checar_metas

## Testes
    pip install pytest psycopg2-binary oracledb
    python -m pytest testes

## Seguranca
As senhas que ficavam no codigo foram retiradas. Troque as do Postgres e do Oracle, pois constam no historico de versoes anterior a este repositorio.
