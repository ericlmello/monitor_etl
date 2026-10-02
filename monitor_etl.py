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
contra eg.egt_info_item (Oracle, lote 09, remessa D-1). fecha_remessa nao
tem validacao propria -- seu papel e so fechar a remessa que carga_diaria
ja confirmou carregada; o codigo de saida do proprio script basta.

staging/primarias sao criticas: tentam indefinidamente, a cada 1 minuto,
ate serem validadas ou ate o horario-limite (HORA_LIMITE_CRITICOS).
carga_diaria/fecha_remessa tem retentativa limitada com alerta por e-mail.

Credenciais NAO ficam no codigo: vem de variaveis de ambiente ou do arquivo
C:/etl_monitor/monitor_etl.env (fora do git; modelo em monitor_etl.env.example).

Subcomandos:
    python monitor_etl.py                 executa a cadeia completa (uso normal, 1x/dia)
    python monitor_etl.py intervir ...     registra uma intervencao humana na trilha
    python monitor_etl.py verificar        confere a integridade da trilha
    python monitor_etl.py checar_metas     alerta por e-mail se alguma meta nao for atingida

Agendamento: Agendador de Tarefas do Windows, opcao "nao iniciar uma nova
instancia se ja estiver em execucao" (o arquivo de lock e uma segunda
camada de protecao, nao a principal).
"""
import hashlib
import json
import os
import re
import smtplib
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

def _carregar_env_arquivo(caminho):
    """Le linhas CHAVE=VALOR (sem dependencias). Variaveis de ambiente ja
    definidas tem precedencia sobre o arquivo."""
    try:
        with open(caminho, encoding="utf-8") as f:
            for linha in f:
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue
                chave, valor = linha.split("=", 1)
                os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))
    except FileNotFoundError:
        pass


_carregar_env_arquivo(os.environ.get("MONITOR_ETL_ENV", str(BASE_DIR / "monitor_etl.env")))


def _env(chave, padrao=None):
    return os.environ.get(chave, padrao)


POSTGRES_HOST = _env("POSTGRES_HOST", "10.2.2.66")
POSTGRES_PORT = _env("POSTGRES_PORT", "1036")
POSTGRES_DBNAME = _env("POSTGRES_DBNAME", "dw_trt")
POSTGRES_USER = _env("POSTGRES_USER", "app_dw_trt")
POSTGRES_PASSWORD = _env("POSTGRES_PASSWORD")

ORACLE_HOST = "clusteroracle.trtsp.jus.br"
ORACLE_PORT = "11521"
ORACLE_SERVICE_NAME = "egestao.trtsp.jus.br"
ORACLE_USER = _env("ORACLE_USER", "eg")
ORACLE_PASSWORD = _env("ORACLE_PASSWORD")

SCRIPT_STAGING = "staging_pje_1g.bat"
SCRIPT_PRIMARIAS = "tab_primarias_pje_1g.bat"
SCRIPT_CARGA_DIARIA = "carga-diaria-D-1.bat"
SCRIPT_FECHA_REMESSA = "fecha_remessa_diaria_D-1.bat"

NUM_LOTE_REMESSA = "09"        # fixo -- confirmado, nao depende do grau

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
    if not valor:
        raise RuntimeError(f"{nome} nao definida (variavel de ambiente ou monitor_etl.env)")
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


def marcar_execucao_atual(job, grau, tentativa):
    """Placar de status ao vivo -- NAO faz parte da cadeia de hash da
    trilha de auditoria. Serve so para visibilidade operacional enquanto
    um job esta rodando (a tabela eg_monitor.execucao so recebe o
    registro definitivo quando o job termina)."""
    try:
        with conectar_postgres() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO eg_monitor.execucao_atual
                       (job, grau, numero_tentativa, inicio, status, atualizado_em)
                   VALUES (%s, %s, %s, %s, 'EM_EXECUCAO', now())
                   ON CONFLICT (job) DO UPDATE SET
                       grau = EXCLUDED.grau,
                       numero_tentativa = EXCLUDED.numero_tentativa,
                       inicio = EXCLUDED.inicio,
                       status = 'EM_EXECUCAO',
                       atualizado_em = now()""",
                (job, grau, tentativa, datetime.now()))
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
    """Le eg_monitor.vw_indicador_vs_meta: taxa_sucesso_pct comparada com a
    meta cadastrada, duracao/MTTR comparados com o padrao historico
    (variancia direta, sem gravar nada). SEM_META e SEM_HISTORICO nao sao
    quebra, so ausencia de base para comparar -- ficam de fora daqui."""
    with conectar_postgres() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT job, etapa_macroprocesso, indicador, valor, referencia,
                      descricao, status_meta
               FROM eg_monitor.vw_indicador_vs_meta
               WHERE status_meta NOT IN ('SEM_META', 'SEM_HISTORICO')
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
    carregados em eg.egt_info_item (lote fixo 09)."""
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
        marcar_execucao_atual(job["nome"], job.get("grau"), tentativa)
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

        cadeia = {}
        for job in JOBS:
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
    """taxa_sucesso_pct: quebra = NAO_ATINGIU (meta fixa em eg_monitor.meta).
    duracao/MTTR: quebra = FORA_DO_PADRAO (desvio da mediana historica maior
    que 2x o MAD -- ver eg_monitor.vw_indicador_vs_meta)."""
    configurar_log()
    quebras = [l for l in listar_indicador_vs_meta()
              if l.get("status_meta") in ("NAO_ATINGIU", "FORA_DO_PADRAO")]
    if not quebras:
        log("checar_metas: nada fora do esperado (ou sem base para comparar ainda)")
        return 0
    linhas_corpo = [
        f"{l['job']} ({l['etapa_macroprocesso']}) -- {l['indicador']}: valor atual {l['valor']}, "
        f"referencia {l['referencia']} [{l['status_meta']}]"
        + (f" -- {l['descricao']}" if l.get("descricao") else "")
        for l in quebras
    ]
    corpo = "Fora do esperado nos ultimos 30 dias:\n\n" + "\n".join(linhas_corpo)
    log(f"checar_metas: {len(quebras)} indicador(es) fora do esperado")
    enviar_alerta(f"[ETL] {len(quebras)} indicador(es) fora do esperado nos ultimos 30 dias", corpo)
    return 1


def main():
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
