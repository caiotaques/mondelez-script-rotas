# 04_tasks.py
# -----------------------------------------------------------------------------
# Gera tarefas/missões a partir das rotas (visita roteirizada => tem tarefa)
#
# Entrada:
#   output/rotas_<periodo>.csv
#   output/scores_<periodo>.csv
#
# Saída:
#   output/tarefas_<periodo>.csv
#
# Regras:
# - Se algum KPI (estrela) estiver abaixo do corte -> cria uma ação por KPI ruim
# - Se nenhum KPI estiver abaixo -> cria 1 ação genérica ("Buscar Melhorias")
# - Só para PDVs roteirizados (presentes em rotas)
# -----------------------------------------------------------------------------

from importlib import import_module
import re
from datetime import date, timedelta
from pathlib import Path
import sys
import numpy as np
import pandas as pd

# KPIs (colunas) e nomes "humanos" — igual ao R (sem "batalhas" por design)
MISSAO_NOMES = {
    "estrelas_espaco_em_loja": "Espaço em Loja",
    "estrelas_inovacao": "Inovação",
    "estrelas_ponto_extra": "Ponto Extra",
    "estrelas_preco": "Preço",
    "estrelas_sortimento_prioritario": "Sortimento Prioritário",
}

# Lista completa de KPIs de estrelas e os que têm cap em 0.5
STAR_COLS = [
    "estrelas_batalhas", "estrelas_espaco_em_loja", "estrelas_inovacao",
    "estrelas_ponto_extra", "estrelas_preco", "estrelas_sortimento_prioritario"
]
STAR_HALF_CAP = {"estrelas_batalhas", "estrelas_inovacao"}

# adiciona nome legível para batalhas
MISSAO_NOMES.setdefault("estrelas_batalhas", "Batalhas")

# Função para obter data_inicio e data_fim (30 dias depois) a partir do primeiro dia do mês atual (Duração das missões)
def _month_window_from_today() -> tuple[date, date]:
    """
    data_inicio = primeiro dia do mês atual
    data_fim = data_inicio + 30 dias (aproximação)
    """
    today = date.today()
    data_inicio = date(today.year, today.month, 1)
    data_fim = data_inicio + timedelta(days=30)
    return data_inicio, data_fim

def gerar_tarefas(
    df_scores: pd.DataFrame,
    df_rotas: pd.DataFrame,
    # corte: float = 0.5,
    id_col: str = "cod_cliente",  # recomendação: usar cod_cliente como id do PDV
) -> pd.DataFrame:
    """
    Adaptação do gerar_missoes (R) para Python.
    - "routed" = PDV está em df_rotas
    - Gera 1 linha por ação.
    """

    # --- validações mínimas ---
    if id_col not in df_rotas.columns:
        # fallback: se rotas não tiver cod_cliente, tenta usar cnpj
        if "cnpj" in df_rotas.columns:
            id_col = "cnpj"
        else:
            raise ValueError("df_rotas precisa ter 'cod_cliente' ou 'cnpj' para identificar o PDV roteirizado.")

    if id_col not in df_scores.columns:
        # tenta fallback para cnpj também
        if "cnpj" in df_scores.columns:
            id_col = "cnpj"
        else:
            raise ValueError("df_scores precisa ter 'cod_cliente' ou 'cnpj' para identificar o PDV.")

    # garante strings
    df_rotas = df_rotas.copy()
    df_scores = df_scores.copy()
    df_rotas[id_col] = df_rotas[id_col].astype(str)
    df_scores[id_col] = df_scores[id_col].astype(str)

    # --- routed = está em rotas ---
    routed_ids = set(df_rotas[id_col].dropna().astype(str).unique().tolist())
    df_scores["routed"] = df_scores[id_col].isin(routed_ids)

    # filtra somente roteirizados
    df = df_scores[df_scores["routed"]].copy()

    # datas
    data_inicio, data_fim = _month_window_from_today()
    dstart = data_inicio.strftime("%Y-%m-%d")
    dend = data_fim.strftime("%Y-%m-%d")

    # missão id base (para ficar estável dentro da execução)
    base_date = date.today().strftime("%Y%m%d")

    rows = []
    # itera pdv a pdv (como o map_dfr do R)
    for idx, pdv in df.iterrows():
        kpis_ruins = []
        # Para os KPIs de estrela usamos limites por KPI:
        # - se o KPI pertence a STAR_HALF_CAP, o máximo esperado é 0.5
        # - caso contrário, o máximo esperado é 1.0
        # Geramos missão se o valor registrado for menor que o máximo permitido
        for col in STAR_COLS:
            if col in df.columns:
                val = pdv[col]
                if pd.notna(val):
                    threshold = 0.5 if col in STAR_HALF_CAP else 1.0
                    try:
                        if float(val) < float(threshold):
                            kpis_ruins.append(col)
                    except Exception:
                        # ignora valores que não convertam para float
                        pass

        # gera código MISS-YYYYMMDD-XXXX (4 dígitos)
        codigo = f"MISS-{base_date}-{np.random.randint(1, 10000):04d}"

        # ID do cliente na missão (mantive nome igual ao R: cidcustomer)
        cidcustomer = str(pdv[id_col])
        nome_cliente = pdv.get("nome_cliente", pd.NA)

        if len(kpis_ruins) > 0:
            # 1 ação por KPI ruim
            for a_i, k in enumerate(kpis_ruins, start=1):
                rows.append(
                    {
                        "cidcustomer": cidcustomer,
                        "nome_cliente": nome_cliente,
                        "cidmission": codigo,
                        "cmissionname": MISSAO_NOMES[k],
                        "laction": a_i,
                        "cactionname": f"Verificar {MISSAO_NOMES[k]}",
                        "dstartmissionexecution": dstart,
                        "dendmissionexecution": dend,
                    }
                )
        else:
            # ação genérica
            rows.append(
                {
                    "cidcustomer": cidcustomer,
                    "nome_cliente": nome_cliente,
                    "cidmission": codigo,
                    "cmissionname": "Buscar Melhorias",
                    "laction": 1,
                    "cactionname": "Verificar pontos de melhoria (sellout/tendência)",
                    "dstartmissionexecution": dstart,
                    "dendmissionexecution": dend,
                }
            )

    return pd.DataFrame(rows)

    
if __name__ == "__main__":
    # Ajuste estes paths conforme seu padrão
    sys.path.insert(0, '.')
    spec = import_module('02_score')
    periodo_rotas = spec.periodo_rotas
    periodo_pesquisa = spec.periodo_pesquisa
    path_scores = f"output/{periodo_rotas}/scores_{periodo_pesquisa}.csv"
    path_rotas = f"output/{periodo_rotas}/rotas_{periodo_rotas}.csv"

    df_scores = pd.read_csv(path_scores, low_memory=False)
    df_rotas = pd.read_csv(path_rotas, low_memory=False)

    tarefas = gerar_tarefas(df_scores=df_scores, df_rotas=df_rotas, id_col="cod_cliente")


    out_path = Path(f"output/{periodo_rotas}/tarefas_{periodo_rotas}.csv")
    tarefas.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"[04_tasks] Exportado: {out_path} | shape={tarefas.shape}")
    print(tarefas.head(10))
