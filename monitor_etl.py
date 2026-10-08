# -*- coding: utf-8 -*-
"""
monitor_etl.py - Watchdog PAI em Python 3.8 (substitui o .jar legado)

Ambiente: Windows Server 2008 R2 Standard, Python 3.8 instalado ao lado do
2.7.18 ja existente (nao o substitui). Conecta direto no PostgreSQL
(psycopg2) e no Oracle (oracledb). No Windows Server 2008 R2 o modo thin
falha (cryptography >= 43) e o Instant Client 19c nao carrega; usa-se o
modo thick com o Instant Client 11.2 (ver ORACLE_CLIENT_DIRS).

Cadeia (confirmada, so 1o grau por enquanto):
    staging -> primarias -> carga_diaria -> fecha_remessa

staging e primarias validam contra pje_eg.tb_status_carga (Postgres), com
as mesmas regras de ordem temporal do Java original. carga_diaria valida
contra eg.egt_info_item (Oracle, lote 10, remessa D-1). fecha_remessa nao
tem validacao propria -- seu papel e so fechar a remessa que carga_diaria
ja confirmou carregada; o codigo de saida do proprio script basta.

staging/primarias sao criticas: tentam indefinidamente, a cada 1 minuto,
ate serem validadas ou ate o horario-limite (HORA_LIMITE_CRITICOS).
carga_diaria/fecha_remessa tem retentativa limitada com alerta por e-mail.

Credenciais ficam nas constantes POSTGRES_*/ORACLE_* abaixo, preenchidas
direto no servidor. No git o valor e um placeholder (nunca commitar a senha).

Subcomandos:
    python monitor_etl.py                 executa a cadeia completa (uso normal, 1x/dia)
    python monitor_etl.py intervir ...     registra uma intervencao humana na trilha
    python monitor_etl.py verificar        confere a integridade da trilha
    python monitor_etl.py checar_metas     alerta por e-mail se alguma meta nao for atingida
    python monitor_etl.py relatorio        estimativas, qualidade e erros x impacto (30 dias)
    python monitor_etl.py painel           grafo da cadeia em tempo real (http://127.0.0.1:8080)

Agendamento: Agendador de Tarefas do Windows, opcao "nao iniciar uma nova
instancia se ja estiver em execucao" (o arquivo de lock e uma segunda
camada de protecao, nao a principal).
"""
import hashlib
import json
import os
import re
import smtplib
import statistics
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from datetime import datetime, timedelta
from decimal import Decimal
from email.mime.text import MIMEText
from pathlib import Path

import psycopg2
import psycopg2.extras
import oracledb


# =============================================================================
# CONFIGURACAO -- ajuste os caminhos para o servidor real
# =============================================================================
BASE_DIR = Path(r"C:\etl_monitor")
SCRIPTS_DIR = Path(r"C:\pdi8\scripts")
LOG_DIR = BASE_DIR / "logs"
LOCK_FILE = BASE_DIR / "monitor_etl.lock"

# Credenciais: edite os dois valores SENHA abaixo direto no servidor.
# No repositorio ficam como placeholder; nao faca commit com a senha real.
SENHA_PLACEHOLDER = "COLE_A_SENHA_AQUI"

POSTGRES_HOST = "10.2.2.66"
POSTGRES_PORT = "1036"
POSTGRES_DBNAME = "dw_trt"
POSTGRES_USER = "app_dw_trt"
POSTGRES_PASSWORD = SENHA_PLACEHOLDER

ORACLE_HOST = "clusteroracle.trtsp.jus.br"
ORACLE_PORT = "11521"
ORACLE_SERVICE_NAME = "egestao.trtsp.jus.br"
ORACLE_USER = "eg"
ORACLE_PASSWORD = SENHA_PLACEHOLDER

SCRIPT_STAGING = "staging_pje_1g.bat"
SCRIPT_PRIMARIAS = "tab_primarias_pje_1g.bat"
SCRIPT_CARGA_DIARIA = "carga-diaria-D-1.bat"
SCRIPT_FECHA_REMESSA = "fecha_remessa_diaria_D-1.bat"

NUM_LOTE_REMESSA = "10"        # fixo -- confirmado, nao depende do grau

HORA_LIMITE_CRITICOS = 20              # staging/primarias param de tentar as 20h
ESPERA_RETENTATIVA_CRITICO_S = 60      # 1 minuto entre tentativas dos criticos
ESPERA_VIGILANCIA_INICIAL_S = 15 * 60  # so comeca a checar apos 15 min
ESPERA_VIGILANCIA_PASSO_S = 5 * 60     # checa a cada 5 min

TIMEOUT_SCRIPT_CRITICO_S = 8 * 3600
TIMEOUT_SCRIPT_EGESTAO_S = 2 * 3600

RETENCAO_LOGS_DIAS = 30

SMTP_HOST = "correio3.trtsp.jus.br"    # confirmado: aceita envio sem autenticacao
SMTP_PORT = 25
SMTP_USE_AUTH = False
SMTP_USER = ""
SMTP_PASS = ""
MAIL_FROM = "egestao_pai@trt2.jus.br"
MAIL_TO = ["e178454@trt2.jus.br"]

# Cadeia de execucao, na ordem confirmada. Cada etapa depende da anterior ja
# ter sido validada (garantido pela ordem do loop em cmd_executar).
#
# "critico": True = tenta indefinidamente ate HORA_LIMITE_CRITICOS.
# "critico": False = tentativas_max limitado, com alerta por e-mail ao esgotar.
#
# So 1o grau por enquanto. Para acrescentar o 2o grau depois, duplique o
# par de jobs carga_diaria/fecha_remessa com os nomes de script corretos.
JOBS = [
    {"nome": "staging", "script": SCRIPT_STAGING, "validacao": "staging",
     "critico": True, "timeout_s": TIMEOUT_SCRIPT_CRITICO_S},
    {"nome": "primarias", "script": SCRIPT_PRIMARIAS, "validacao": "primarias",
     "critico": True, "timeout_s": TIMEOUT_SCRIPT_CRITICO_S},
    {"nome": "carga_diaria", "script": SCRIPT_CARGA_DIARIA, "validacao": "remessa_carregada",
     "grau": 1, "critico": False, "tentativas_max": 5, "backoff_s": [60, 300, 600],
     "timeout_s": TIMEOUT_SCRIPT_EGESTAO_S},
    {"nome": "fecha_remessa", "script": SCRIPT_FECHA_REMESSA, "validacao": None,
     "grau": 1, "critico": False, "tentativas_max": 5, "backoff_s": [60, 300, 600],
     "timeout_s": TIMEOUT_SCRIPT_EGESTAO_S},
]

PADROES_TRANSITORIO = [
    r"connection refused", r"could not connect", r"connection reset", r"timed? ?out",
    r"deadlock", r"lock wait timeout", r"too many connections", r"broken pipe",
    r"ORA-12170", r"ORA-12541", r"ORA-00060", r"ORA-03113", r"ORA-03114",
]
PADROES_DADOS = [
    r"duplicate key", r"unique constraint", r"ORA-00001", r"violates foreign key", r"ORA-02291",
]
PADROES_CONFIGURACAO = [
    r"does not exist", r"couldn'?t find", r"syntax error", r"permission denied",
    r"ORA-00942", r"ORA-00904", r"ORA-01017", r"Unable to load the (job|transformation)",
]

GENESIS = "0" * 64
CAMPOS_EXECUCAO = [
    "id_execucao", "job", "grau", "numero_tentativa", "timestamp_registro", "inicio", "fim",
    "duracao_s", "status", "codigo_saida", "categoria_erro", "retentavel", "etapa_origem",
    "mensagem_erro_limpa", "resultado_validacao", "hash_log_sha256",
]
CAMPOS_INTERVENCAO = [
    "id_execucao", "job", "registrado_em", "responsavel_intervencao", "papel_responsavel",
    "acao", "motivo_descarte",
]

_processo_atual = {"popen": None}
_lock_processo = threading.Lock()


# =============================================================================
# CONEXOES
# =============================================================================
def _exigir_senha(valor, nome):
    if not valor or valor == SENHA_PLACEHOLDER:
        raise RuntimeError(f"{nome} nao preenchida: edite a constante no inicio do monitor_etl.py")
    return valor


def conectar_postgres():
    _exigir_senha(POSTGRES_PASSWORD, "POSTGRES_PASSWORD")
    return psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DBNAME,
                            user=POSTGRES_USER, password=POSTGRES_PASSWORD)


# Diretorios onde procurar o Oracle Instant Client, em ordem de preferencia.
# ATENCAO: no Windows Server 2008 R2 o Instant Client 19c NAO carrega -- a
# oraociei19.dll usa GetOverlappedResultEx, API que so existe a partir do
# Windows 8. Use 11.2 ou 12.2 nesse servidor.
ORACLE_CLIENT_DIRS = [
    r"C:\oracle\instantclient_11_2",
    r"C:\oracle\instantclient_12_2",
]

_oracle_thick = {"tentado": False, "ativo": False}


def _init_oracle_thick():
    """Ativa o modo thick uma unica vez, se houver Instant Client utilizavel.

    Chamada preguicosa de proposito: se ficasse no topo do modulo, uma falha
    aqui derrubaria tambem o uso do PostgreSQL, que nao depende do Oracle.
    Se nenhum cliente carregar, segue em modo thin (que exige o pacote
    cryptography e banco Oracle 12.1+).
    """
    if _oracle_thick["tentado"]:
        return _oracle_thick["ativo"]
    _oracle_thick["tentado"] = True
    for diretorio in ORACLE_CLIENT_DIRS:
        if not os.path.isdir(diretorio):
            continue
        try:
            oracledb.init_oracle_client(lib_dir=diretorio)
            _oracle_thick["ativo"] = True
            return True
        except Exception:
            continue
    return False


def conectar_oracle():
    _init_oracle_thick()
    _exigir_senha(ORACLE_PASSWORD, "ORACLE_PASSWORD")
    dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}"
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)


# =============================================================================
# LOG
# =============================================================================
class _Tee:
    def __init__(self, arquivo, original):
        self._arquivo = arquivo
        self._original = original

    def write(self, dado):
        self._original.write(dado)
        self._arquivo.write(dado)
        return len(dado)

    def flush(self):
        self._original.flush()
        self._arquivo.flush()


_arquivos_log = []


def configurar_log():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    saida = open(LOG_DIR / f"app_log_{ts}.log", "a", encoding="utf-8")
    erro_f = open(LOG_DIR / f"log_error_{ts}.log", "a", encoding="utf-8")
    _arquivos_log.extend([saida, erro_f])
    sys.stdout = _Tee(saida, sys.stdout)
    sys.stderr = _Tee(erro_f, sys.stderr)


def log(msg):
    linha = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(linha, flush=True)


def erro(msg):
    linha = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(linha, file=sys.stderr, flush=True)


def compactar_logs_antigos():
    """Zipa e apaga *.log com mais de RETENCAO_LOGS_DIAS dias. Nunca toca nos
    logs do dia (ainda abertos por este processo)."""
    if not LOG_DIR.is_dir():
        return
    limite = time.time() - RETENCAO_LOGS_DIAS * 86400
    abertos = {Path(getattr(a, "name", "")).resolve() for a in _arquivos_log}
    antigos = [p for p in LOG_DIR.glob("*.log")
              if p.resolve() not in abertos and p.stat().st_mtime < limite]
    if not antigos:
        return
    nome_zip = LOG_DIR / f"backup_{datetime.now():%Y%m%d_%H%M%S}.zip"
    with zipfile.ZipFile(nome_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in antigos:
            zf.write(p, p.name)
    for p in antigos:
        p.unlink()
    log(f"{len(antigos)} log(s) antigos compactados em {nome_zip}")


# =============================================================================
# LOCK DE INSTANCIA UNICA
# =============================================================================
def obter_lock():
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.write(fd, str(os.getpid()).encode("ascii"))
        os.close(fd)
        return True
    except FileExistsError:
        return False


def liberar_lock():
    LOCK_FILE.unlink(missing_ok=True)


# =============================================================================
# ENCERRAMENTO DE PROCESSO
# =============================================================================
def _matar_arvore(pid):
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        try:
            os.kill(pid, 9)
        except ProcessLookupError:
            pass


def matar_processo_atual(motivo):
    with _lock_processo:
        p = _processo_atual["popen"]
        if p is not None and p.poll() is None:
            log(f"Encerrando processo em andamento ({motivo}). PID {p.pid}")
            _matar_arvore(p.pid)


# =============================================================================
# TRILHA ENCADEADA (hash) -- sem TSV, os valores ja chegam tipados do banco
# =============================================================================
def _normalizar_campo(v):
    """Representacao canonica de um valor -- usada tanto para calcular o
    hash na gravacao quanto na verificacao posterior. As duas PRECISAM
    coincidir mesmo quando o valor volta do banco com um tipo Python
    diferente do que foi escrito (Decimal em vez de float, texto com
    padding de espacos em colunas CHAR(n))."""
    if v is None:
        return ""
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (float, Decimal)):
        # NUMERIC(14,3) no banco -- Decimal('5.000') na leitura vs float
        # 5.0 na escrita precisam normalizar para a MESMA string.
        return f"{float(v):.3f}"
    return str(v).rstrip()  # rstrip: colunas CHAR(n) vem com padding de espaco


def _calcular_hash(anterior, campos, registro):
    partes = [str(anterior)] + [_normalizar_campo(registro.get(c)) for c in campos]
    conteudo = "|".join(partes)
    return hashlib.sha256(conteudo.encode("utf-8")).hexdigest()


def consultar_ultimo_hash(tabela):
    assert tabela in ("execucao", "intervencao")
    with conectar_postgres() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT verificador_integridade FROM eg_monitor.{tabela} ORDER BY id DESC LIMIT 1")
        linha = cur.fetchone()
    return linha[0] if linha else GENESIS


def gravar_execucao(registro):
    anterior = consultar_ultimo_hash("execucao")
    h = _calcular_hash(anterior, CAMPOS_EXECUCAO, registro)
    valores = [registro.get(c) for c in CAMPOS_EXECUCAO]
    idx_validacao = CAMPOS_EXECUCAO.index("resultado_validacao")
    valores[idx_validacao] = psycopg2.extras.Json(valores[idx_validacao] or {})
    try:
        with conectar_postgres() as conn, conn.cursor() as cur:
            cur.execute(
                f"""INSERT INTO eg_monitor.execucao
                    ({', '.join(CAMPOS_EXECUCAO)}, hash_anterior, verificador_integridade)
                    VALUES ({', '.join(['%s'] * len(CAMPOS_EXECUCAO))}, %s, %s)""",
                valores + [anterior, h])
            conn.commit()
    except Exception as exc:
        erro(f"FALHA AO GRAVAR NA TRILHA (registro perdido): {exc}")
    return h


def gravar_intervencao(registro):
    anterior = consultar_ultimo_hash("intervencao")
    h = _calcular_hash(anterior, CAMPOS_INTERVENCAO, registro)
    valores = [registro.get(c) for c in CAMPOS_INTERVENCAO]
    with conectar_postgres() as conn, conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO eg_monitor.intervencao
                ({', '.join(CAMPOS_INTERVENCAO)}, hash_anterior, verificador_integridade)
                VALUES ({', '.join(['%s'] * len(CAMPOS_INTERVENCAO))}, %s, %s)""",
            valores + [anterior, h])
        conn.commit()
    return h


AMOSTRA_ESTIMATIVA = 30     # ultimas execucoes com sucesso usadas na estimativa
MINIMO_ESTIMATIVA = 3       # abaixo disso nao ha base para estimar


def estimar_duracao(job_nome):
    """Mediana da duracao das ultimas execucoes com SUCESSO do job.
    Retorna (segundos, n_amostras) ou (None, n) se houver poucas amostras.
    Nunca levanta excecao: a estimativa e informativa."""
    try:
        with conectar_postgres() as conn, conn.cursor() as cur:
            cur.execute(
                """SELECT duracao_s FROM eg_monitor.execucao
                   WHERE job = %s AND status = 'SUCESSO' AND duracao_s > 0
                   ORDER BY id DESC LIMIT %s""", (job_nome, AMOSTRA_ESTIMATIVA))
            valores = [float(r[0]) for r in cur.fetchall()]
    except Exception as exc:
        erro(f"falha ao estimar duracao de {job_nome} (nao afeta a execucao): {exc}")
        return None, 0
    if len(valores) < MINIMO_ESTIMATIVA:
        return None, len(valores)
    return statistics.median(valores), len(valores)


def formatar_duracao(segundos):
    if segundos is None:
        return "sem base historica"
    minutos = int(round(segundos / 60.0))
    h, m = divmod(minutos, 60)
    return f"{h}h{m:02d}min" if h else f"{m}min"


def estimativa_restante(jobs_restantes):
    """Soma das medianas dos jobs que ainda faltam. Se algum nao tem base,
    devolve None (estimativa incompleta nao e mostrada como se fosse total)."""
    total = 0.0
    for j in jobs_restantes:
        d, _ = estimar_duracao(j["nome"])
        if d is None:
            return None
        total += d
    return total


def marcar_execucao_atual(job, grau, tentativa, estimada_s=None):
    """Placar de status ao vivo -- NAO faz parte da cadeia de hash da
    trilha de auditoria. Serve so para visibilidade operacional enquanto
    um job esta rodando (a tabela eg_monitor.execucao so recebe o
    registro definitivo quando o job termina)."""
    agora = datetime.now()
    try:
        with conectar_postgres() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO eg_monitor.execucao_atual
                       (job, grau, numero_tentativa, inicio, status, atualizado_em,
                    duracao_estimada_s, previsao_fim)
                   VALUES (%s, %s, %s, %s, 'EM_EXECUCAO', now(), %s, %s)
                   ON CONFLICT (job) DO UPDATE SET
                       grau = EXCLUDED.grau,
                       numero_tentativa = EXCLUDED.numero_tentativa,
                       inicio = EXCLUDED.inicio,
                       status = 'EM_EXECUCAO',
                       atualizado_em = now(),
                       duracao_estimada_s = EXCLUDED.duracao_estimada_s,
                       previsao_fim = EXCLUDED.previsao_fim""",
                (job, grau, tentativa, agora, estimada_s,
                 agora + timedelta(seconds=estimada_s) if estimada_s else None))
            conn.commit()
    except Exception as exc:
        erro(f"falha ao atualizar execucao_atual (nao afeta a trilha): {exc}")


def limpar_execucao_atual(job):
    try:
        with conectar_postgres() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM eg_monitor.execucao_atual WHERE job = %s", (job,))
            conn.commit()
    except Exception as exc:
        erro(f"falha ao limpar execucao_atual (nao afeta a trilha): {exc}")


def listar_execucao():
    with conectar_postgres() as conn, conn.cursor() as cur:
        cur.execute(
            f"""SELECT id, hash_anterior, verificador_integridade, {', '.join(CAMPOS_EXECUCAO)}
                FROM eg_monitor.execucao ORDER BY id""")
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, linha)) for linha in cur.fetchall()]


def listar_indicador_vs_meta():
    """Le eg_monitor.vw_indicador_vs_meta: cada indicador (taxa_sucesso_pct,
    duracao_media_min, mttr_medio_min) comparado com a meta cadastrada em
    eg_monitor.meta (valor_meta + operador). SEM_META nao e quebra, so
    ausencia de meta -- fica de fora daqui."""
    with conectar_postgres() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT job, etapa_macroprocesso, indicador, valor, valor_meta,
                      operador, descricao, status_meta
               FROM eg_monitor.vw_indicador_vs_meta
               WHERE status_meta <> 'SEM_META'
               ORDER BY job, indicador""")
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, linha)) for linha in cur.fetchall()]


# =============================================================================
# STATUS_CARGA (Postgres) -- validacao de staging/primarias
# =============================================================================
def consultar_status_carga():
    with conectar_postgres() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT nm_tarefa, estado_carga, dta_inicio_ultima_carga, dta_fim_ultima_carga
               FROM pje_eg.tb_status_carga""")
        linhas = cur.fetchall()
    return {nm: {"estado": estado, "dt_inicio": ini, "dt_fim": fim}
            for nm, estado, ini, fim in linhas}


def validar_staging(estado):
    if estado is None:
        return False, "consulta a tb_status_carga indisponivel"
    ext = estado.get("extracao1grau")
    sta = estado.get("staging_area_1grau")
    if not ext or not sta:
        return False, "tarefa ausente na tb_status_carga"
    hoje0 = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    ext_ok = (ext["estado"] == "CFS" and ext["dt_inicio"] and ext["dt_inicio"].replace(tzinfo=None) > hoje0
             and ext["dt_fim"] and ext["dt_fim"] > ext["dt_inicio"])
    if not ext_ok:
        return False, f"extracao1grau invalida: {ext}"
    sta_ok = (sta["estado"] == "CFS" and sta["dt_inicio"] and sta["dt_inicio"] > ext["dt_fim"]
             and sta["dt_fim"] and sta["dt_fim"] > sta["dt_inicio"])
    return sta_ok, (None if sta_ok else f"staging_area_1grau invalida: {sta}")


def validar_primarias(estado):
    ok, msg = validar_staging(estado)
    if not ok:
        return False, msg
    ext = estado["extracao1grau"]
    pri = estado.get("tabelas_primarias_1grau")
    if not pri:
        return False, "tabelas_primarias_1grau ausente na tb_status_carga"
    pri_ok = (pri["estado"] == "CFS" and pri["dt_inicio"] and pri["dt_inicio"] > ext["dt_fim"]
             and pri["dt_fim"] and pri["dt_fim"] > pri["dt_inicio"])
    return pri_ok, (None if pri_ok else f"tabelas_primarias_1grau invalida: {pri}")


# =============================================================================
# REMESSA (Oracle) -- validacao de carga_diaria
# =============================================================================
def consultar_remessa_itens(num_remessa, num_lote):
    with conectar_oracle() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT num_item, SUM(num_quantidade_item) AS quantidade
               FROM eg.egt_info_item
               WHERE num_tribunal = 2 AND num_remessa = :num_remessa AND num_lote = :num_lote
               GROUP BY num_item ORDER BY num_item""",
            num_remessa=num_remessa, num_lote=num_lote)
        linhas = cur.fetchall()
    return [(str(item), int(qtd) if qtd is not None else 0) for item, qtd in linhas]


def validar_remessa_carregada(job):
    """Usada so por carga_diaria: confirma que a remessa de D-1 tem itens
    carregados em eg.egt_info_item (lote fixo 10)."""
    num_remessa = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    try:
        itens = consultar_remessa_itens(num_remessa, NUM_LOTE_REMESSA)
    except Exception as exc:
        return False, {"num_remessa": num_remessa, "num_lote": NUM_LOTE_REMESSA}, f"erro Oracle: {exc}"
    total_quantidade = sum(q for _, q in itens)
    detalhe = {
        "num_remessa": num_remessa, "num_lote": NUM_LOTE_REMESSA,
        "qtd_itens": len(itens), "total_quantidade": total_quantidade,
    }
    ok = len(itens) > 0 and total_quantidade > 0
    return ok, detalhe, None


VALIDADORES = {
    "staging": lambda job: validar_staging(consultar_status_carga()),
    "primarias": lambda job: validar_primarias(consultar_status_carga()),
    "remessa_carregada": validar_remessa_carregada,
}


def _validar(job):
    resultado = VALIDADORES[job["validacao"]](job)
    if len(resultado) == 2:
        ok, detalhe = resultado
        return ok, detalhe, None
    return resultado


# =============================================================================
# ANALISE DO LOG DO SCRIPT
# =============================================================================
_RE_CAUSA = re.compile(r"(ORA-\d{5}[^\r\n]*|(?:[\w$]+\.)*[\w$]*(?:Exception|Error):[^\r\n]*)")
_HIGIENIZACAO = [
    (re.compile(r"\b\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b"), "<processo>"),
    (re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b"), "<cpf>"),
    (re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"), "<email>"),
    (re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b"), "<ip>"),
    (re.compile(r"=\s*\([^)]*\)"), "=(<valor>)"),
    (re.compile(r"'[^']*'"), "'<valor>'"),
    (re.compile(r"(?i)(password|senha|pwd)\s*[=:]\s*\S+"), r"\1=<oculto>"),
    (re.compile(r"(?<![A-Z]-)\b\d{5,}\b"), "<n>"),
]


def higienizar(msg, limite=300):
    if not msg:
        return None
    t = re.sub(r"\s+", " ", msg).strip()
    for padrao, subst in _HIGIENIZACAO:
        t = padrao.sub(subst, t)
    return t[:limite]


def classificar_saida(codigo, saida):
    if codigo == 0:
        return None, False
    if "[TIMEOUT: processo encerrado pelo monitor]" in saida:
        return "TIMEOUT", True
    if any(re.search(p, saida, re.I) for p in PADROES_DADOS):
        return "DADOS", False
    if any(re.search(p, saida, re.I) for p in PADROES_CONFIGURACAO):
        return "CONFIGURACAO", False
    if any(re.search(p, saida, re.I) for p in PADROES_TRANSITORIO):
        return "TRANSITORIO", True
    return "DESCONHECIDO", True


def analisar_log(saida):
    m = _RE_CAUSA.search(saida)
    if not m:
        return None, None, None
    causa = m.group(1)
    codigo_falha = causa[:9] if causa.startswith("ORA-") else causa.split(":", 1)[0].rsplit(".", 1)[-1]
    return None, codigo_falha, causa


# =============================================================================
# EXECUCAO DO SCRIPT
# =============================================================================
def executar_bat(job):
    cmd = ["cmd.exe", "/c", str(SCRIPTS_DIR / job["script"])]
    inicio = datetime.now()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            cwd=SCRIPTS_DIR, text=True, encoding="utf-8", errors="replace")
    with _lock_processo:
        _processo_atual["popen"] = proc
    try:
        saida, _ = proc.communicate(timeout=job["timeout_s"])
    except subprocess.TimeoutExpired:
        log(f"{job['nome']} excedeu {job['timeout_s']}s; encerrando grupo de processos")
        _matar_arvore(proc.pid)
        saida, _ = proc.communicate()
        saida = (saida or "") + "\n[TIMEOUT: processo encerrado pelo monitor]\n"
    fim = datetime.now()
    return inicio, fim, proc.returncode, saida


def dentro_da_janela(job):
    if not job.get("critico"):
        return True
    return datetime.now().hour < HORA_LIMITE_CRITICOS


def enviar_alerta(assunto, corpo):
    try:
        msg = MIMEText(corpo, "plain", "utf-8")
        msg["Subject"] = assunto
        msg["From"] = MAIL_FROM
        msg["To"] = ", ".join(MAIL_TO)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
            if SMTP_USE_AUTH:
                s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(MAIL_FROM, MAIL_TO, msg.as_string())
        log(f"e-mail de alerta enviado: {assunto}")
    except Exception as exc:
        erro(f"falha ao enviar e-mail de alerta ({assunto}): {exc}")


def rodar_job(job, id_execucao, cadeia):
    tentativa = 0
    while True:
        tentativa += 1
        if not dentro_da_janela(job):
            log(f"[BLOQUEIO] horario limite atingido para {job['nome']}")
            gravar_execucao({
                "id_execucao": id_execucao, "job": job["nome"], "grau": job.get("grau"),
                "numero_tentativa": tentativa, "timestamp_registro": datetime.now(),
                "inicio": datetime.now(), "fim": datetime.now(), "duracao_s": 0,
                "status": "BLOQUEADO_JANELA", "resultado_validacao": dict(cadeia),
            })
            limpar_execucao_atual(job["nome"])
            return "BLOQUEADO_JANELA"

        log(f"{job['nome']}: tentativa {tentativa}")
        estimada_s, n_amostras = estimar_duracao(job["nome"])
        log(f"{job['nome']}: duracao estimada {formatar_duracao(estimada_s)}"
            + (f" (mediana de {n_amostras} execucoes)" if estimada_s else ""))
        marcar_execucao_atual(job["nome"], job.get("grau"), tentativa, estimada_s)
        inicio, fim, codigo, saida = executar_bat(job)
        categoria, retentavel = classificar_saida(codigo, saida)
        status = "SUCESSO" if codigo == 0 else "FALHA"

        detalhe_validacao = {}
        if status == "SUCESSO" and job.get("validacao"):
            ok, detalhe, saida_validacao = _validar(job)
            detalhe_validacao = detalhe if isinstance(detalhe, dict) else {"mensagem": detalhe}
            if not ok:
                status, categoria, retentavel = "FALHA", "VALIDACAO", True
                saida = saida + "\n[VALIDACAO] " + str(detalhe)

        etapa, codigo_falha, causa = (None, None, None)
        if status == "FALHA":
            etapa, codigo_falha, causa = analisar_log(saida)

        registro = {
            "id_execucao": id_execucao, "job": job["nome"], "grau": job.get("grau"),
            "numero_tentativa": tentativa, "timestamp_registro": datetime.now(),
            "inicio": inicio, "fim": fim, "duracao_s": (fim - inicio).total_seconds(),
            "status": status, "codigo_saida": codigo, "categoria_erro": categoria,
            "retentavel": retentavel, "etapa_origem": etapa,
            "mensagem_erro_limpa": higienizar(causa) if status == "FALHA" else None,
            "resultado_validacao": detalhe_validacao,
            "hash_log_sha256": hashlib.sha256(saida.encode("utf-8", "replace")).hexdigest(),
        }
        gravar_execucao(registro)

        if status == "SUCESSO":
            limpar_execucao_atual(job["nome"])
            return "SUCESSO"

        if job.get("critico"):
            log(f"{job['nome']}: tentativa {tentativa} falhou ({categoria}); "
               f"nova tentativa em {ESPERA_RETENTATIVA_CRITICO_S}s")
            time.sleep(ESPERA_RETENTATIVA_CRITICO_S)
            continue

        maximo = job.get("tentativas_max", 3)
        if not retentavel or tentativa >= maximo:
            enviar_alerta(
                f"[ETL] {job['nome']} falhou",
                f"job: {job['nome']}\nid_execucao: {id_execucao}\ntentativas: {tentativa}\n"
                f"categoria: {categoria}\netapa: {etapa}\nmensagem: {registro['mensagem_erro_limpa']}")
            limpar_execucao_atual(job["nome"])
            return "FALHA"

        backoff = job.get("backoff_s", [60, 300])
        espera = backoff[min(tentativa - 1, len(backoff) - 1)]
        log(f"{job['nome']}: aguardando {espera}s antes da proxima tentativa")
        time.sleep(espera)


# =============================================================================
# VIGILANCIA DA EXTRACAO
# =============================================================================
def thread_vigilancia_extracao():
    time.sleep(ESPERA_VIGILANCIA_INICIAL_S)
    hoje0 = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    while True:
        try:
            estado = consultar_status_carga()
        except Exception as exc:
            erro(f"[vigilancia] consulta a tb_status_carga falhou: {exc}")
            return
        ext = estado.get("extracao1grau")
        if not ext:
            return
        if ext["dt_inicio"] and ext["dt_inicio"].replace(tzinfo=None) > hoje0 and not ext["dt_fim"]:
            log(f"[vigilancia] extracao1grau em andamento: {ext}")
            time.sleep(ESPERA_VIGILANCIA_PASSO_S)
            continue
        break
    if ext["estado"] != "CFS":
        log(f"[vigilancia] extracao1grau concluida com erro ({ext['estado']}); reiniciando staging")
        matar_processo_atual("extracao1grau finalizou com erro")


# =============================================================================
# SUBCOMANDOS
# =============================================================================
def cmd_executar():
    configurar_log()
    if not obter_lock():
        log("outra execucao em andamento (lock presente); saindo")
        return 0
    try:
        id_execucao = str(uuid.uuid4())
        log(f"--- Iniciando execucao {id_execucao} ---")
        try:
            with conectar_postgres() as conn, conn.cursor() as cur:
                cur.execute("DELETE FROM eg_monitor.execucao_atual")
                conn.commit()
        except Exception as exc:
            erro(f"falha ao limpar execucao_atual no inicio do ciclo: {exc}")
        t = threading.Thread(target=thread_vigilancia_extracao, daemon=True)
        t.start()

        total = estimativa_restante(JOBS)
        if total is None:
            log("estimativa do ciclo: sem base historica suficiente em algum job")
        else:
            log(f"estimativa do ciclo completo: {formatar_duracao(total)} "
                f"(previsao de termino {datetime.now() + timedelta(seconds=total):%H:%M})")

        cadeia = {}
        for indice, job in enumerate(JOBS):
            if indice > 0:
                restante = estimativa_restante(JOBS[indice:])
                if restante is not None:
                    log(f"falta estimado para concluir a cadeia: {formatar_duracao(restante)}")
            status = rodar_job(job, id_execucao, cadeia)
            cadeia[job["nome"]] = status
            if job.get("critico") and status != "SUCESSO":
                log(f"{job['nome']} nao concluido ({status}); cadeia critica interrompida")
                break

        compactar_logs_antigos()
        log(f"--- Execucao {id_execucao} finalizada: {cadeia} ---")
        return 0
    finally:
        liberar_lock()


def cmd_intervir(argv):
    campos = {}
    i = 0
    while i < len(argv):
        if argv[i].startswith("--"):
            chave = argv[i][2:].replace("-", "_")
            campos[chave] = argv[i + 1] if i + 1 < len(argv) else ""
            i += 2
        else:
            i += 1
    obrigatorios = ["id_execucao", "job", "acao", "responsavel", "papel", "motivo"]
    faltando = [c for c in obrigatorios if not campos.get(c)]
    if faltando:
        print(f"faltam argumentos: {', '.join(faltando)}", file=sys.stderr)
        print("uso: monitor_etl.py intervir --id-execucao X --job Y --acao "
             "[REEXECUTOU|ABORTOU|ACEITOU_SUSPEITO|DESCARTOU_ALERTA|CORRIGIU_ORIGEM] "
             "--responsavel \"Nome\" --papel plantao-setic --motivo \"...\"", file=sys.stderr)
        return 2
    if len(campos["motivo"].strip()) < 20:
        print("motivo muito curto: descreva a razao em pelo menos 20 caracteres", file=sys.stderr)
        return 2
    registro = {
        "id_execucao": campos["id_execucao"], "job": campos["job"], "registrado_em": datetime.now(),
        "responsavel_intervencao": campos["responsavel"], "papel_responsavel": campos["papel"],
        "acao": campos["acao"], "motivo_descarte": campos["motivo"],
    }
    try:
        h = gravar_intervencao(registro)
    except Exception as exc:
        print(f"falha ao gravar intervencao: {exc}", file=sys.stderr)
        return 1
    print(f"Intervencao registrada. Verificador: {h[:12]}")
    return 0


def cmd_verificar():
    linhas = listar_execucao()
    anterior = GENESIS
    problemas = []
    for l in linhas:
        registro = {c: l.get(c) for c in CAMPOS_EXECUCAO}
        if l.get("hash_anterior") != anterior:
            problemas.append((l.get("id"), "elo quebrado"))
        if _calcular_hash(l.get("hash_anterior", GENESIS), CAMPOS_EXECUCAO, registro) != l.get("verificador_integridade"):
            problemas.append((l.get("id"), "conteudo alterado"))
        anterior = l.get("verificador_integridade", anterior)
    situacao = "INTEGRA" if not problemas else f"{len(problemas)} PROBLEMA(S)"
    print(f"eg_monitor.execucao: {len(linhas)} registros | {situacao} | ancora: {anterior}")
    for id_, desc in problemas[:20]:
        print(f"   id {id_}: {desc}")
    return 1 if problemas else 0


def cmd_checar_metas():
    """Quebra = status_meta NAO_ATINGIU em eg_monitor.vw_indicador_vs_meta
    (valor atual contra valor_meta com o operador >= ou <= da tabela meta).
    As metas de duracao/MTTR podem ser calibradas pela mediana + 2xMAD
    sugerida em eg_monitor.vw_meta_sugerida (minimo 10 amostras)."""
    configurar_log()
    quebras = [l for l in listar_indicador_vs_meta()
              if l.get("status_meta") == "NAO_ATINGIU"]
    if not quebras:
        log("checar_metas: nada fora do esperado (ou sem base para comparar ainda)")
        return 0
    linhas_corpo = [
        f"{l['job']} ({l['etapa_macroprocesso']}) -- {l['indicador']}: valor atual {l['valor']}, "
        f"meta {l['operador']} {l['valor_meta']} [{l['status_meta']}]"
        + (f" -- {l['descricao']}" if l.get("descricao") else "")
        for l in quebras
    ]
    corpo = "Fora do esperado nos ultimos 30 dias:\n\n" + "\n".join(linhas_corpo)
    log(f"checar_metas: {len(quebras)} indicador(es) fora do esperado")
    enviar_alerta(f"[ETL] {len(quebras)} indicador(es) fora do esperado nos ultimos 30 dias", corpo)
    return 1


def _consultar_view(sql):
    with conectar_postgres() as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [d.name for d in cur.description]
        return cols, cur.fetchall()


def _imprimir_tabela(titulo, cols, linhas):
    print(f"\n== {titulo} ==")
    if not linhas:
        print("(sem dados)")
        return
    texto = [[("" if v is None else str(v)) for v in l] for l in linhas]
    larguras = [max(len(c), *(len(l[i]) for l in texto)) for i, c in enumerate(cols)]
    print("  ".join(c.ljust(larguras[i]) for i, c in enumerate(cols)))
    for l in texto:
        print("  ".join(v.ljust(larguras[i]) for i, v in enumerate(l)))


def cmd_relatorio():
    """Estimativa de duracao por job, qualidade e frequencia de erros x impacto
    (ultimos 30 dias). Le as views eg_monitor.vw_qualidade_job e vw_erro_impacto
    (sql/002) e vw_qualidade_faixa_mes, vw_tendencia_qualidade (sql/003)."""
    print("== Duracao estimada por job (mediana das ultimas execucoes com sucesso) ==")
    for j in JOBS:
        d, n = estimar_duracao(j["nome"])
        print(f"{j['nome']:<15} {formatar_duracao(d)}" + (f"  (n={n})" if d else f"  (n={n})"))
    for titulo, view in (("Qualidade por job (30 dias)", "vw_qualidade_job"),
                         ("Frequencia de erros x impacto (30 dias)", "vw_erro_impacto"),
                         ("Qualidade e erros por faixa do mes (180 dias)", "vw_qualidade_faixa_mes"),
                         ("Tendencia e previsao (60 dias)", "vw_tendencia_qualidade")):
        try:
            cols, linhas = _consultar_view(f"SELECT * FROM eg_monitor.{view}")
            _imprimir_tabela(titulo, cols, linhas)
        except Exception as exc:
            print(f"\n{titulo}: indisponivel ({exc}). Rode sql/002 e sql/003 no DBeaver.", file=sys.stderr)
    return 0


# =============================================================================
# PAINEL (grafo da cadeia em tempo real) -- somente leitura
# =============================================================================
ETAPAS = {
    "staging": "Staging",
    "primarias": "Tabelas primárias",
    "carga_diaria": "Remessa diária",
    "fecha_remessa": "Fechamento",
}
PAINEL_ATUALIZA_S = 5
_cache_estimativas = {"quando": 0.0, "valores": {}}
_cache_estado = {"quando": 0.0, "valor": None}
_lock_cache = threading.Lock()


def _iso(v):
    return v.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(v, datetime) else None


def montar_estado(agora, em_execucao, tentativas, historico, estimativas):
    """Monta o estado do painel a partir de dados ja lidos (funcao pura, sem
    banco, para poder ser testada). em_execucao: {job: {inicio, tentativa,
    estimada_s}}; tentativas: lista (ordem cronologica) de dicts do ciclo do
    dia; historico: lista de (dia, job, ok); estimativas: {job: segundos|None}."""
    nos = []
    montante_restante = 0.0
    restante_conhecido = True
    parou_a_montante = False
    for indice, job in enumerate(JOBS):
        nome = job["nome"]
        tent = [t for t in tentativas if t["job"] == nome]
        rodando = em_execucao.get(nome)
        estimada = (rodando or {}).get("estimada_s") or estimativas.get(nome)
        no = {
            "job": nome, "etapa": ETAPAS.get(nome, nome), "ordem": indice + 1,
            "critico": bool(job.get("critico")), "tentativas": len(tent),
            "estimada_s": estimada, "decorrido_s": None, "restante_s": None,
            "progresso_pct": None, "inicio": None, "fim": None,
            "duracao_s": None, "categoria_erro": None, "mensagem": None,
        }
        if rodando:
            ultima = tent[-1] if tent else None
            aguardando = bool(ultima and ultima["fim"] and ultima["fim"] >= rodando["inicio"])
            no["estado"] = "AGUARDANDO_RETENTATIVA" if aguardando else "EM_EXECUCAO"
            no["tentativa_atual"] = rodando["tentativa"]
            no["inicio"] = _iso(rodando["inicio"])
            if not aguardando:
                decorrido = max(0.0, (agora - rodando["inicio"]).total_seconds())
                no["decorrido_s"] = decorrido
                if estimada:
                    no["progresso_pct"] = round(min(99.0, 100.0 * decorrido / estimada), 1)
                    no["restante_s"] = max(0.0, estimada - decorrido)
                    montante_restante += no["restante_s"]
                else:
                    restante_conhecido = False
            else:
                if estimada:
                    montante_restante += estimada
                else:
                    restante_conhecido = False
            if ultima:
                no["categoria_erro"] = ultima.get("categoria_erro")
                no["mensagem"] = ultima.get("mensagem")
        elif tent:
            ultima = tent[-1]
            no["estado"] = {"SUCESSO": "SUCESSO", "FALHA": "FALHA",
                            "BLOQUEADO_JANELA": "BLOQUEADO"}.get(ultima["status"], ultima["status"])
            no["inicio"], no["fim"] = _iso(ultima["inicio"]), _iso(ultima["fim"])
            no["duracao_s"] = ultima.get("duracao_s")
            no["categoria_erro"] = ultima.get("categoria_erro")
            no["mensagem"] = ultima.get("mensagem")
            if no["estado"] in ("FALHA", "BLOQUEADO") and job.get("critico"):
                parou_a_montante = True
        else:
            if parou_a_montante:
                no["estado"] = "NAO_EXECUTADO"
            else:
                no["estado"] = "PENDENTE"
                if estimada:
                    montante_restante += estimada
                else:
                    restante_conhecido = False
        nos.append(no)

    ativos = [n for n in nos if n["estado"] in ("EM_EXECUCAO", "AGUARDANDO_RETENTATIVA")]
    pendentes = [n for n in nos if n["estado"] == "PENDENTE"]
    restante = montante_restante if restante_conhecido else None
    resumo = {"restante_s": restante,
              "previsao_fim": _iso(agora + timedelta(seconds=restante)) if (restante is not None and (ativos or pendentes)) else None,
              "concluido": all(n["estado"] == "SUCESSO" for n in nos)}
    barras = []
    for t in tentativas:
        barras.append({"job": t["job"], "tentativa": t["tentativa"], "status": t["status"],
                       "inicio": _iso(t["inicio"]), "fim": _iso(t["fim"]),
                       "aberta": False})
    for nome, r in em_execucao.items():
        barras.append({"job": nome, "tentativa": r["tentativa"], "status": "EM_EXECUCAO",
                       "inicio": _iso(r["inicio"]), "fim": _iso(agora), "aberta": True})
    return {"agora": _iso(agora), "dia": agora.strftime("%Y-%m-%d"),
            "nos": nos, "resumo": resumo, "barras": barras,
            "historico": [{"dia": str(d), "job": j, "ok": bool(ok)} for d, j, ok in historico]}


def _estimativas_com_cache():
    agora = time.time()
    with _lock_cache:
        if agora - _cache_estimativas["quando"] < 60 and _cache_estimativas["valores"]:
            return dict(_cache_estimativas["valores"])
    valores = {}
    for j in JOBS:
        d, _ = estimar_duracao(j["nome"])
        valores[j["nome"]] = d
    with _lock_cache:
        _cache_estimativas.update(quando=agora, valores=dict(valores))
    return valores


def coletar_estado():
    agora = datetime.now()
    with conectar_postgres() as conn, conn.cursor() as cur:
        try:
            cur.execute("""SELECT job, numero_tentativa, inicio, duracao_estimada_s
                           FROM eg_monitor.execucao_atual""")
            linhas = cur.fetchall()
        except Exception:
            conn.rollback()
            cur.execute("SELECT job, numero_tentativa, inicio, NULL FROM eg_monitor.execucao_atual")
            linhas = cur.fetchall()
        em_execucao = {j: {"tentativa": t, "inicio": i,
                           "estimada_s": float(e) if e is not None else None}
                       for j, t, i, e in linhas}
        if em_execucao:
            dia = min(r["inicio"] for r in em_execucao.values()).date()
        else:
            cur.execute("SELECT max(inicio)::date FROM eg_monitor.execucao")
            dia = cur.fetchone()[0] or agora.date()
        cur.execute("""SELECT id_execucao, job, numero_tentativa, inicio, fim, duracao_s,
                              status, categoria_erro, mensagem_erro_limpa
                       FROM eg_monitor.execucao WHERE inicio::date = %s
                       ORDER BY inicio, id""", (dia,))
        brutas = cur.fetchall()
        cur.execute("""SELECT inicio::date AS dia, job, bool_or(status = 'SUCESSO') AS ok
                       FROM eg_monitor.execucao
                       WHERE inicio >= current_date - 13
                       GROUP BY 1, 2 ORDER BY 1, 2""")
        historico = cur.fetchall()
    ultimo_ciclo = brutas[-1][0] if brutas else None
    tentativas = [{"job": r[1], "tentativa": r[2], "inicio": r[3], "fim": r[4],
                   "duracao_s": float(r[5]) if r[5] is not None else None,
                   "status": r[6], "categoria_erro": r[7], "mensagem": r[8]}
                  for r in brutas if r[0] == ultimo_ciclo]
    estado = montar_estado(agora, em_execucao, tentativas, historico, _estimativas_com_cache())
    estado["dia"] = str(dia)
    return estado


def estado_cacheado():
    agora = time.time()
    with _lock_cache:
        if _cache_estado["valor"] is not None and agora - _cache_estado["quando"] < 3:
            return _cache_estado["valor"]
    valor = coletar_estado()
    with _lock_cache:
        _cache_estado.update(quando=agora, valor=valor)
    return valor


PAINEL_HTML = r"""<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Monitor ETL</title>
<style>
:root{--bg:#f6f8fa;--card:#fff;--tx:#1f2328;--mut:#59636e;--bd:#d1d9e0;
--ok:#1a7f37;--run:#0969da;--fail:#cf222e;--wait:#8250df;--warn:#9a6700;--pend:#8c959f}
@media (prefers-color-scheme:dark){:root{--bg:#0d1117;--card:#161b22;--tx:#e6edf3;--mut:#9198a1;--bd:#30363d;
--ok:#3fb950;--run:#58a6ff;--fail:#f85149;--wait:#bc8cff;--warn:#d29922;--pend:#6e7681}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:14px/1.45 system-ui,Segoe UI,Arial,sans-serif}
header{display:flex;flex-wrap:wrap;gap:8px 24px;align-items:baseline;padding:14px 16px;border-bottom:1px solid var(--bd);background:var(--card)}
h1{font-size:18px;margin:0}h2{font-size:14px;margin:0 0 8px;color:var(--mut);font-weight:600}
main{max-width:1040px;margin:0 auto;padding:16px}
.card{background:var(--card);border:1px solid var(--bd);border-radius:8px;padding:14px;margin-bottom:16px}
.mut{color:var(--mut)}#aviso{display:none;margin-bottom:12px;padding:8px 12px;border-radius:6px;background:var(--fail);color:#fff}
svg{display:block;width:100%;height:auto}
.nome{font-weight:600;font-size:14px;fill:var(--tx)}.sub{font-size:12px;fill:var(--mut)}.est{font-size:12px;font-weight:600}
.pulsa{animation:p 1.4s ease-in-out infinite}@keyframes p{50%{stroke-opacity:.25}}
@media (prefers-reduced-motion:reduce){.pulsa{animation:none}}
table{border-collapse:collapse;width:100%;font-size:12px}td,th{padding:3px 4px;text-align:center}
th{color:var(--mut);font-weight:500}td.job,th.job{text-align:left;white-space:nowrap;padding-right:10px}
td.c{border:2px solid var(--card);border-radius:4px;color:#fff;font-weight:700;min-width:22px}
.leg span{margin-right:14px;white-space:nowrap}.leg i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:4px}
</style></head><body>
<header><h1>Monitor ETL</h1><span id="dia" class="mut"></span><span id="resumo"></span>
<span id="atual" class="mut" style="margin-left:auto"></span></header>
<main>
<div id="aviso"></div>
<div class="card"><h2>Cadeia do dia</h2><svg id="dag" viewBox="0 0 960 190" role="img" aria-label="Grafo da cadeia de jobs"></svg></div>
<div class="card"><h2>Linha do tempo das tentativas</h2><svg id="gantt" viewBox="0 0 960 190" role="img" aria-label="Linha do tempo"></svg></div>
<div class="card"><h2>Últimos 14 dias</h2><div id="grade"></div></div>
<div class="leg mut" id="leg"></div>
</main>
<script>
var COR={SUCESSO:'var(--ok)',EM_EXECUCAO:'var(--run)',AGUARDANDO_RETENTATIVA:'var(--wait)',FALHA:'var(--fail)',BLOQUEADO:'var(--warn)',BLOQUEADO_JANELA:'var(--warn)',PENDENTE:'var(--pend)',NAO_EXECUTADO:'var(--pend)'};
var ROT={SUCESSO:'Concluído',EM_EXECUCAO:'Em execução',AGUARDANDO_RETENTATIVA:'Aguardando nova tentativa',FALHA:'Falhou',BLOQUEADO:'Bloqueado pelo horário',BLOQUEADO_JANELA:'Bloqueado pelo horário',PENDENTE:'Pendente',NAO_EXECUTADO:'Não executado'};
var NS='http://www.w3.org/2000/svg';
function el(t,a,txt){var e=document.createElementNS(NS,t);for(var k in a){e.setAttribute(k,a[k])}if(txt!==undefined){e.textContent=txt}return e}
function limpa(n){while(n.firstChild){n.removeChild(n.firstChild)}}
function ep(s){var m=/^(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)/.exec(s||'');return m?Date.UTC(+m[1],m[2]-1,+m[3],+m[4],+m[5],+m[6])/1000:null}
function dur(s){if(s===null||s===undefined){return '-'}s=Math.round(s);var h=Math.floor(s/3600),m=Math.floor(s%3600/60),x=s%60;
 return h?h+'h'+('0'+m).slice(-2)+'min':(m?m+'min'+(x?(' '+x+'s'):''):x+'s')}
function hm(t){var d=new Date(t*1000);return ('0'+d.getUTCHours()).slice(-2)+':'+('0'+d.getUTCMinutes()).slice(-2)}
function desenhaDag(nos){var g=document.getElementById('dag');limpa(g);
 var df=el('defs',{});var mk=el('marker',{id:'ar',viewBox:'0 0 10 10',refX:'9',refY:'5',markerWidth:'7',markerHeight:'7',orient:'auto'});
 mk.appendChild(el('path',{d:'M0,0 L10,5 L0,10 z',fill:'var(--mut)'}));df.appendChild(mk);g.appendChild(df);
 var W=200,H=150,GAP=44,X0=12,Y=18;
 for(var i=0;i<nos.length;i++){var n=nos[i],x=X0+i*(W+GAP),c=COR[n.estado]||'var(--pend)';
  if(i>0){g.appendChild(el('line',{x1:x-GAP+2,y1:Y+H/2,x2:x-3,y2:Y+H/2,stroke:'var(--mut)','stroke-width':2,'marker-end':'url(#ar)'}))}
  var ativo=(n.estado==='EM_EXECUCAO'||n.estado==='AGUARDANDO_RETENTATIVA');
  g.appendChild(el('rect',{x:x,y:Y,width:W,height:H,rx:8,fill:'var(--card)',stroke:c,'stroke-width':ativo?3:2,'class':ativo?'pulsa':''}));
  g.appendChild(el('rect',{x:x,y:Y,width:6,height:H,rx:3,fill:c}));
  g.appendChild(el('text',{x:x+16,y:Y+24,'class':'nome'},n.ordem+'. '+n.etapa));
  g.appendChild(el('text',{x:x+16,y:Y+42,'class':'sub'},n.job+(n.critico?' (crítico)':'')));
  g.appendChild(el('text',{x:x+16,y:Y+66,'class':'est',fill:c},ROT[n.estado]||n.estado));
  var l1='',l2='';
  if(n.estado==='EM_EXECUCAO'){l1='Decorrido '+dur(n.decorrido_s)+(n.estimada_s?' de ~'+dur(n.estimada_s):'');l2=n.restante_s!==null?'Faltam ~'+dur(n.restante_s):'Sem estimativa ainda'}
  else if(n.estado==='AGUARDANDO_RETENTATIVA'){l1='Tentativa '+n.tentativa_atual+' em seguida';l2=(n.categoria_erro?('Último erro: '+n.categoria_erro):'')}
  else if(n.estado==='SUCESSO'){l1='Duração '+dur(n.duracao_s);l2='Fim '+hm(ep(n.fim))}
  else if(n.estado==='FALHA'||n.estado==='BLOQUEADO'){l1=(n.categoria_erro||'Sem categoria')+' após '+n.tentativas+' tentativa(s)';l2='Fim '+hm(ep(n.fim))}
  else if(n.estimada_s){l1='Estimado ~'+dur(n.estimada_s)}
  g.appendChild(el('text',{x:x+16,y:Y+88,'class':'sub'},l1));g.appendChild(el('text',{x:x+16,y:Y+106,'class':'sub'},l2));
  if(n.tentativas>0&&n.estado!=='SUCESSO'||n.tentativas>1){g.appendChild(el('text',{x:x+16,y:Y+H-10,'class':'sub'},'Tentativas registradas: '+n.tentativas))}
  if(n.progresso_pct!==null){g.appendChild(el('rect',{x:x+16,y:Y+H-26,width:W-32,height:7,rx:3,fill:'var(--bd)'}));
   g.appendChild(el('rect',{x:x+16,y:Y+H-26,width:(W-32)*n.progresso_pct/100,height:7,rx:3,fill:c}));}
  if(n.mensagem){var t=el('title',{},n.mensagem);g.lastChild.appendChild(t)}
 }}
function desenhaGantt(barras,agora,nos){var g=document.getElementById('gantt');limpa(g);
 var jobs=[];for(var i=0;i<nos.length;i++){jobs.push(nos[i].job)}
 var rows=jobs.length,LH=34,L=110,R=20,T=8,Wd=960-L-R,H=T+rows*LH+28;g.setAttribute('viewBox','0 0 960 '+H);
 if(!barras.length){g.appendChild(el('text',{x:L,y:60,'class':'sub'},'Nenhuma tentativa registrada neste dia ainda.'));return}
 var a=ep(agora),mn=a,mx=a;for(var b=0;b<barras.length;b++){var s=ep(barras[b].inicio),f=ep(barras[b].fim);if(s<mn){mn=s}if(f>mx){mx=f}}
 mn=Math.floor(mn/900)*900;mx=Math.max(mx,mn+1800);mx=Math.ceil(mx/900)*900;var esc=Wd/(mx-mn);
 for(var r=0;r<rows;r++){g.appendChild(el('text',{x:4,y:T+r*LH+21,'class':'sub'},jobs[r]));
  g.appendChild(el('line',{x1:L,y1:T+r*LH+LH,x2:L+Wd,y2:T+r*LH+LH,stroke:'var(--bd)'}))}
 var passo=(mx-mn)>6*3600?3600:(mx-mn)>2*3600?1800:600;
 for(var t=Math.ceil(mn/passo)*passo;t<=mx;t+=passo){var xx=L+(t-mn)*esc;
  g.appendChild(el('line',{x1:xx,y1:T,x2:xx,y2:T+rows*LH,stroke:'var(--bd)','stroke-dasharray':'2 3'}));
  g.appendChild(el('text',{x:xx,y:T+rows*LH+16,'class':'sub','text-anchor':'middle'},hm(t)))}
 for(var k=0;k<barras.length;k++){var br=barras[k],ri=jobs.indexOf(br.job);if(ri<0){continue}
  var s2=ep(br.inicio),f2=ep(br.fim),w=Math.max(3,(f2-s2)*esc),st=br.status==='BLOQUEADO_JANELA'?'BLOQUEADO':br.status;
  var rc=el('rect',{x:L+(s2-mn)*esc,y:T+ri*LH+6,width:w,height:LH-14,rx:4,fill:COR[st]||'var(--pend)','class':br.aberta?'pulsa':''});
  rc.appendChild(el('title',{},br.job+' tentativa '+br.tentativa+': '+(ROT[st]||st)+' ('+hm(s2)+' a '+hm(f2)+')'));g.appendChild(rc);
  if(w>26){g.appendChild(el('text',{x:L+(s2-mn)*esc+w/2,y:T+ri*LH+LH/2+4,'text-anchor':'middle',fill:'#fff','font-size':'11','font-weight':'700'},'#'+br.tentativa))}}
 var xa=L+(a-mn)*esc;g.appendChild(el('line',{x1:xa,y1:T,x2:xa,y2:T+rows*LH,stroke:'var(--run)','stroke-width':2}));
 g.appendChild(el('text',{x:xa,y:T+rows*LH+16,'class':'est','text-anchor':'middle',fill:'var(--run)'},'agora'))}
function desenhaGrade(hist,nos){var box=document.getElementById('grade');limpa(box);
 var dias=[],vistos={},m={};for(var i=0;i<hist.length;i++){var h=hist[i];if(!vistos[h.dia]){vistos[h.dia]=1;dias.push(h.dia)}m[h.job+'|'+h.dia]=h.ok}
 dias.sort();if(!dias.length){box.textContent='Sem histórico ainda.';return}
 var t=document.createElement('table'),hd=t.insertRow(-1),c0=document.createElement('th');c0.className='job';hd.appendChild(c0);
 for(var d=0;d<dias.length;d++){var th=document.createElement('th');th.textContent=dias[d].slice(8)+'/'+dias[d].slice(5,7);hd.appendChild(th)}
 for(var j=0;j<nos.length;j++){var tr=t.insertRow(-1),cj=tr.insertCell(-1);cj.className='job';cj.textContent=nos[j].job;
  for(var d2=0;d2<dias.length;d2++){var c=tr.insertCell(-1),v=m[nos[j].job+'|'+dias[d2]];c.className='c';
   if(v===undefined){c.style.background='transparent';c.style.color='var(--mut)';c.textContent='-';c.title=dias[d2]+': sem execução'}
   else{c.style.background=v?'var(--ok)':'var(--fail)';c.textContent=v?'✓':'✗';c.title=dias[d2]+': '+(v?'concluído':'não concluído')}}}
 box.appendChild(t)}
function legenda(){var k=['SUCESSO','EM_EXECUCAO','AGUARDANDO_RETENTATIVA','FALHA','BLOQUEADO','PENDENTE'],h='';
 for(var i=0;i<k.length;i++){h+='<span><i style="background:'+COR[k[i]]+'"></i>'+ROT[k[i]]+'</span>'}document.getElementById('leg').innerHTML=h}
function pinta(e){document.getElementById('dia').textContent='Ciclo de '+e.dia.slice(8)+'/'+e.dia.slice(5,7)+'/'+e.dia.slice(0,4);
 var r=e.resumo,t='';if(r.concluido){t='Cadeia concluída'}else if(r.restante_s!==null&&r.previsao_fim){t='Faltam ~'+dur(r.restante_s)+' (previsão de término '+hm(ep(r.previsao_fim))+')'}else{t='Sem estimativa completa (poucos dados históricos)'}
 document.getElementById('resumo').textContent=t;desenhaDag(e.nos);desenhaGantt(e.barras,e.agora,e.nos);desenhaGrade(e.historico,e.nos);
 document.getElementById('atual').textContent='Atualizado às '+hm(ep(e.agora))+':'+('0'+(ep(e.agora)%60)).slice(-2)}
function busca(){var x=new XMLHttpRequest();x.open('GET','/api/estado?_='+new Date().getTime());
 x.onreadystatechange=function(){if(x.readyState!==4){return}var av=document.getElementById('aviso');
  if(x.status===200){av.style.display='none';try{pinta(JSON.parse(x.responseText))}catch(err){av.textContent='Erro ao desenhar: '+err;av.style.display='block'}}
  else{var m='Sem conexão com o painel';try{m=JSON.parse(x.responseText).erro||m}catch(e2){}av.textContent='Falha ao atualizar: '+m;av.style.display='block'}};x.send()}
legenda();busca();setInterval(busca,__ATUALIZA__);
</script></body></html>
"""


def cmd_painel(argv):
    """Sobe um servidor HTTP local, somente leitura, com o grafo da cadeia em
    tempo real. Padrao: so aceita conexoes da propria maquina (127.0.0.1)."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    host, porta = "127.0.0.1", 8080
    i = 0
    while i < len(argv):
        if argv[i] == "--porta" and i + 1 < len(argv):
            porta = int(argv[i + 1]); i += 2
        elif argv[i] == "--host" and i + 1 < len(argv):
            host = argv[i + 1]; i += 2
        else:
            i += 1
    pagina = PAINEL_HTML.replace("__ATUALIZA__", str(PAINEL_ATUALIZA_S * 1000)).encode("utf-8")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _enviar(self, codigo, tipo, corpo):
            self.send_response(codigo)
            self.send_header("Content-Type", tipo)
            self.send_header("Content-Length", str(len(corpo)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(corpo)

        def do_GET(self):
            caminho = self.path.split("?", 1)[0]
            if caminho == "/":
                self._enviar(200, "text/html; charset=utf-8", pagina)
            elif caminho == "/api/estado":
                try:
                    corpo = json.dumps(estado_cacheado(), ensure_ascii=False, default=str).encode("utf-8")
                    self._enviar(200, "application/json; charset=utf-8", corpo)
                except Exception as exc:
                    corpo = json.dumps({"erro": str(exc)[:300]}, ensure_ascii=False).encode("utf-8")
                    self._enviar(500, "application/json; charset=utf-8", corpo)
            else:
                self._enviar(404, "text/plain; charset=utf-8", b"nao encontrado")

    servidor = ThreadingHTTPServer((host, porta), Handler)
    print(f"Painel em http://{host}:{porta}/  (Ctrl+C para encerrar)")
    if host not in ("127.0.0.1", "localhost"):
        print("ATENCAO: o painel nao tem autenticacao. Exponha apenas na rede interna.")
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        servidor.server_close()
    return 0


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "relatorio":
        return cmd_relatorio()
    if len(sys.argv) > 1 and sys.argv[1] == "painel":
        return cmd_painel(sys.argv[2:])
    if len(sys.argv) > 1 and sys.argv[1] in ("intervir", "verificar", "checar_metas"):
        comando = sys.argv[1]
        if comando == "intervir":
            return cmd_intervir(sys.argv[2:])
        if comando == "verificar":
            return cmd_verificar()
        return cmd_checar_metas()
    return cmd_executar()


if __name__ == "__main__":
    sys.exit(main())
