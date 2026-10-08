# -*- coding: utf-8 -*-
"""Testes unitarios sem banco. Rodar: python -m pytest testes"""
import os, sys
from datetime import datetime, timedelta
from decimal import Decimal
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import monitor_etl as m


def test_normalizacao_decimal_float_igual():
    assert m._normalizar_campo(Decimal("5.000")) == m._normalizar_campo(5.0) == "5.000"


def test_normalizacao_char_padding():
    assert m._normalizar_campo("SUCESSO   ") == "SUCESSO"


def test_normalizacao_none_e_bool():
    assert m._normalizar_campo(None) == ""
    assert m._normalizar_campo(True) == "True"


def test_hash_encadeado_muda_com_anterior_e_conteudo():
    reg = {c: None for c in m.CAMPOS_EXECUCAO}
    reg["job"] = "staging"
    h1 = m._calcular_hash(m.GENESIS, m.CAMPOS_EXECUCAO, reg)
    h2 = m._calcular_hash(h1, m.CAMPOS_EXECUCAO, reg)
    reg["job"] = "primarias"
    h3 = m._calcular_hash(m.GENESIS, m.CAMPOS_EXECUCAO, reg)
    assert len({h1, h2, h3}) == 3 and len(h1) == 64


def test_classificar_saida():
    assert m.classificar_saida(0, "") == (None, False)
    assert m.classificar_saida(1, "x [TIMEOUT: processo encerrado pelo monitor]") == ("TIMEOUT", True)
    assert m.classificar_saida(1, "ORA-00001 unique constraint") == ("DADOS", False)
    assert m.classificar_saida(1, "ORA-00942") == ("CONFIGURACAO", False)
    assert m.classificar_saida(1, "ORA-03113") == ("TRANSITORIO", True)
    assert m.classificar_saida(1, "algo estranho") == ("DESCONHECIDO", True)


def test_higienizar_remove_dados_sensiveis():
    t = m.higienizar("erro 123.456.789-09 user a@b.com ip 10.2.2.66 senha=abc")
    assert "123.456" not in t and "a@b.com" not in t and "10.2.2.66" not in t and "abc" not in t


def test_analisar_log_extrai_ora():
    _, cod, causa = m.analisar_log("falha\nORA-12541: TNS:no listener\n")
    assert cod == "ORA-12541" and "TNS" in causa


def _estado(agora, ext_estado="CFS"):
    h0 = agora.replace(hour=0, minute=0, second=0, microsecond=0)
    ei, ef = h0 + timedelta(minutes=10), h0 + timedelta(minutes=20)
    si, sf = ef + timedelta(minutes=1), ef + timedelta(minutes=30)
    pi, pf = sf + timedelta(minutes=1), sf + timedelta(minutes=40)
    return {
        "extracao1grau": {"estado": ext_estado, "dt_inicio": ei, "dt_fim": ef},
        "staging_area_1grau": {"estado": "CFS", "dt_inicio": si, "dt_fim": sf},
        "tabelas_primarias_1grau": {"estado": "CFS", "dt_inicio": pi, "dt_fim": pf},
    }


def test_validar_staging_e_primarias_ok():
    e = _estado(datetime.now())
    assert m.validar_staging(e)[0] and m.validar_primarias(e)[0]


def test_validar_staging_extracao_com_erro():
    assert not m.validar_staging(_estado(datetime.now(), "ERR"))[0]


def test_validar_staging_ausente_e_indisponivel():
    assert not m.validar_staging(None)[0]
    assert not m.validar_staging({})[0]


def test_primarias_ausente():
    e = _estado(datetime.now()); del e["tabelas_primarias_1grau"]
    assert not m.validar_primarias(e)[0]


def test_janela_criticos():
    job = {"critico": True}
    assert m.dentro_da_janela({"critico": False})
    assert m.dentro_da_janela(job) == (datetime.now().hour < m.HORA_LIMITE_CRITICOS)


def test_intervir_exige_motivo_longo():
    assert m.cmd_intervir(["--id-execucao", "x", "--job", "j", "--acao", "A",
                           "--responsavel", "R", "--papel", "P", "--motivo", "curto"]) == 2


def test_senha_ausente_falha_claro(monkeypatch):
    monkeypatch.setattr(m, "POSTGRES_PASSWORD", m.SENHA_PLACEHOLDER)
    try:
        m.conectar_postgres()
        assert False
    except RuntimeError as e:
        assert "POSTGRES_PASSWORD" in str(e)


def test_lote_remessa_e_10():
    assert m.NUM_LOTE_REMESSA == "10"


def test_formatar_duracao():
    assert m.formatar_duracao(None) == "sem base historica"
    assert m.formatar_duracao(45 * 60) == "45min"
    assert m.formatar_duracao(3600 + 5 * 60) == "1h05min"


def test_estimativa_restante_soma_e_incompleta(monkeypatch):
    medias = {"a": (600.0, 5), "b": (1200.0, 5), "c": (None, 1)}
    monkeypatch.setattr(m, "estimar_duracao", lambda n: medias[n])
    assert m.estimativa_restante([{"nome": "a"}, {"nome": "b"}]) == 1800.0
    assert m.estimativa_restante([{"nome": "a"}, {"nome": "c"}]) is None


# ---------------------------------------------------------------- painel
def _t(job, n, ini, fim, status, cat=None):
    return {"job": job, "tentativa": n, "inicio": ini, "fim": fim,
            "duracao_s": (fim - ini).total_seconds(), "status": status,
            "categoria_erro": cat, "mensagem": None}


EST = {"staging": 3000.0, "primarias": 1200.0, "carga_diaria": 900.0, "fecha_remessa": 60.0}


def test_painel_em_execucao_com_progresso_e_previsao():
    ag = datetime(2026, 10, 8, 10, 0, 0)
    ini = ag - timedelta(seconds=1500)
    est = m.montar_estado(ag, {"staging": {"tentativa": 1, "inicio": ini, "estimada_s": 3000.0}}, [], [], EST)
    n = {x["job"]: x for x in est["nos"]}
    assert n["staging"]["estado"] == "EM_EXECUCAO" and n["staging"]["progresso_pct"] == 50.0
    assert n["staging"]["restante_s"] == 1500.0
    assert n["primarias"]["estado"] == "PENDENTE"
    assert est["resumo"]["restante_s"] == 1500.0 + 1200 + 900 + 60
    assert est["resumo"]["previsao_fim"] is not None


def test_painel_aguardando_retentativa():
    ag = datetime(2026, 10, 8, 10, 0, 0)
    falha = _t("staging", 1, ag - timedelta(minutes=30), ag - timedelta(minutes=1), "FALHA", "TRANSITORIO")
    run = {"staging": {"tentativa": 2, "inicio": ag - timedelta(minutes=2), "estimada_s": None}}
    est = m.montar_estado(ag, run, [falha], [], EST)
    assert est["nos"][0]["estado"] == "AGUARDANDO_RETENTATIVA"


def test_painel_critico_falho_marca_seguintes_nao_executados():
    ag = datetime(2026, 10, 8, 21, 0, 0)
    t = [_t("staging", 1, ag - timedelta(hours=5), ag - timedelta(hours=4), "BLOQUEADO_JANELA")]
    est = m.montar_estado(ag, {}, t, [], EST)
    estados = [x["estado"] for x in est["nos"]]
    assert estados == ["BLOQUEADO", "NAO_EXECUTADO", "NAO_EXECUTADO", "NAO_EXECUTADO"]
    assert est["resumo"]["concluido"] is False


def test_painel_concluido_e_sem_estimativa():
    ag = datetime(2026, 10, 8, 12, 0, 0)
    ts = [_t(j["nome"], 1, ag - timedelta(hours=3 - i), ag - timedelta(hours=2.5 - i), "SUCESSO")
          for i, j in enumerate(m.JOBS)]
    assert m.montar_estado(ag, {}, ts, [], EST)["resumo"]["concluido"] is True
    sem = m.montar_estado(ag, {}, [], [], {j["nome"]: None for j in m.JOBS})
    assert sem["resumo"]["restante_s"] is None


def test_painel_http(monkeypatch):
    import threading, urllib.request, socket
    ag = datetime(2026, 10, 8, 10, 0, 0)
    est = m.montar_estado(ag, {}, [], [], EST)
    monkeypatch.setattr(m, "estado_cacheado", lambda: est)
    s = socket.socket(); s.bind(("127.0.0.1", 0)); porta = s.getsockname()[1]; s.close()
    threading.Thread(target=m.cmd_painel, args=(["--porta", str(porta)],), daemon=True).start()
    import time as _t2
    for _ in range(50):
        try:
            html = urllib.request.urlopen(f"http://127.0.0.1:{porta}/", timeout=2).read().decode("utf-8")
            break
        except Exception:
            _t2.sleep(0.1)
    assert "Monitor ETL" in html and "__ATUALIZA__" not in html
    api = urllib.request.urlopen(f"http://127.0.0.1:{porta}/api/estado", timeout=2).read().decode("utf-8")
    assert '"nos"' in api
