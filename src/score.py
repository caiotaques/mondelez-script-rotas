# py_scripts/score_mod.py
import numpy as np
import pandas as pd
from typing import Dict, Optional, List

from src.prep import OUTPUT_DIR

def gerar_scores(
    periodo_pesquisa: str,
    periodo_rotas: str,
    cenario: str = "misto",
    top_n: int = 40,
    group_by: str = "vendedor",             # "vendedor" ou "gerente_area"
    only_groups: Optional[List[str]] = None  
) -> Dict[str, pd.DataFrame]:
    
    print("\n============================ 2 - SCORES ========================")
    """
    Calcula score por grupo (vendedor ou gerente_area).
    - group_by: "vendedor" ou "gerente_area".
    - only_groups: lista de valores do group_by para filtrar (opcional, vendedor ou gerente_area).
    - top_n: número de PDVs por grupo no ranking de prioritários.
    Retorna: {"scores": scores_mes, "prioritarios": prioritarios}
    """

    # Min-max robusto, para evitar problemas em casos degenerados (todos iguais, ou todos NA, ou infinito).
    def _mm(s: pd.Series) -> pd.Series:
        """Min-max por grupo, robusto para casos degenerados."""
        s = s.astype(float)
        if s.notna().sum() <= 1:
            return pd.Series(0.5, index=s.index)
        mn, mx = s.min(), s.max()
        if not np.isfinite(mn) or not np.isfinite(mx) or mx <= mn:
            return pd.Series(0.5, index=s.index)
        return (s - mn) / (mx - mn)

    STAR_COLS = [
    "estrelas_batalhas", "estrelas_espaco_em_loja", "estrelas_inovacao",
    "estrelas_ponto_extra", "estrelas_preco", "estrelas_sortimento_prioritario"
    ]
    STAR_HALF_CAP = {"estrelas_batalhas", "estrelas_inovacao"}


    if (cenario == '1'):
        W_STAR, W_SELL12M, W_TREND6M = 0.50, 0.30, 0.20
        print(f"Cenário 'Misto' selecionado: W_STAR={W_STAR}, W_SELL12M={W_SELL12M}, W_TREND6M={W_TREND6M}")
    elif (cenario == '2'):
        W_STAR, W_SELL12M, W_TREND6M = 1.0, 0.0, 0.0
        print(f"Cenário 'Loja Perfeita' selecionado: W_STAR={W_STAR}, W_SELL12M={W_SELL12M}, W_TREND6M={W_TREND6M}")
    elif (cenario == '3'):
        W_STAR, W_SELL12M, W_TREND6M = 0.0, 0.50, 0.50
        print(f"Cenário 'Sellout/Tendência' selecionado: W_STAR={W_STAR}, W_SELL12M={W_SELL12M}, W_TREND6M={W_TREND6M}")
    else:
        print(f"Cenário '{cenario}' desconhecido, usando misto.")
        W_STAR, W_SELL12M, W_TREND6M = 0.50, 0.30, 0.20

    # periodo_pesquisa = f"P{periodo_rotas-1:02d}"
    # periodo_rotas = f"P{periodo_rotas:02d}"
    # print(periodo_rotas)
    # print(periodo_pesquisa)

    path_csv = OUTPUT_DIR / f"{periodo_rotas}/pdvs_vendedores_{periodo_pesquisa}.csv"

    w_star, w_sell, w_trend = W_STAR, W_SELL12M, W_TREND6M

    # ============= Carrega base ==============
    df = pd.read_csv(path_csv, dtype={"cod_cliente": str, "cnpj": str, "cidcustomer": str, "cidterritory": str, "ciduserowner": str})
    print(f"Número de PDVs por {group_by}:\n{df.groupby(group_by)['cnpj'].nunique()}")

    print(df.columns.tolist())

    # Garantias de chaves
    if "cd_cnpj" not in df.columns and "cnpj" in df.columns:
        df["cd_cnpj"] = df["cnpj"]

    # Normaliza tipos
    for col in ["lat", "long"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Garante colunas de contexto (não explode se ausentes — cria com NA/“SEM_GA”):
    df["uf"] = df["uf"].astype(str)
    df["cidade"] = df["cidade"].astype(str)

    # Sanitiza `group_by`
    if group_by not in {"vendedor", "gerente_area"}:
        raise ValueError("group_by deve ser 'vendedor' ou 'gerente_area'.")

    # Filtro por grupo (opcional)
    if only_groups:
        df = df[df[group_by].isin(only_groups)].copy()

    # Trabalha somente linhas com lat/long válidos
    d = (
        df.dropna(subset=["lat", "long"])
          .copy()
          .sort_values([group_by, "cd_cnpj"])
    )

    # ----- Estrelas -----
    avail = [c for c in STAR_COLS if c in d.columns]
    if avail:
        parts = []
        for c in avail:
            cap = 0.5 if c in STAR_HALF_CAP else 1.0
            parts.append(d[c].astype(float).clip(upper=cap))
        d["estrelas_total_parts"] = pd.concat(parts, axis=1).sum(axis=1, min_count=1)
        d["has_stars"] = (d[avail].notna().sum(axis=1) / float(len(avail))).fillna(0.0)
    else:
        d["estrelas_total_parts"] = np.nan
        d["has_stars"] = 0.0

    d["estrelas_total_eff"] = d["estrelas_total_parts"]
    if "estrelas_total" in d.columns:
        d["estrelas_total_eff"] = d["estrelas_total_eff"].fillna(d["estrelas_total"])
    d["estrelas_total_eff"] = d["estrelas_total_eff"].astype(float).clip(0.0, 5.0)
    d["star_need_abs"] = (5.0 - d["estrelas_total_eff"]) / 5.0
    d.loc[d["estrelas_total_eff"].isna(), "star_need_abs"] = 0.0

    # tem_estrelas garantido
    if "tem_estrelas" not in d.columns:
        d["tem_estrelas"] = ((d["has_stars"] > 0) | d["estrelas_total_eff"].notna()).astype(int)
    else:
        d["tem_estrelas"] = d["tem_estrelas"].fillna(0).astype(int)

    # ----- Sellout / Trend -----
    sell_src = "sellout_ton_ytd"
    if sell_src not in d.columns:
        d[sell_src] = d["sellout"] if "sellout" in d.columns else np.nan
    d[sell_src] = d[sell_src].astype(float)

    d["sell_log1p_12"] = np.log1p(d[sell_src])
    d["s_sell12"] = d.groupby(group_by, group_keys=False)["sell_log1p_12"].apply(_mm)
    d.loc[d[sell_src].isna(), "s_sell12"] = 0.0
    d["s_sell12"] = d["s_sell12"].fillna(0.0)

    trend_src = "trend_l6m"

    d[trend_src] = d[trend_src].astype(float)
    d["trend_need"] = -d[trend_src]
    d["s_trend"] = d.groupby(group_by, group_keys=False)["trend_need"].apply(_mm)
    d.loc[d[trend_src].isna(), "s_trend"] = 0.0
    d["s_trend"] = d["s_trend"].fillna(0.0)

    has_star = (d["has_stars"] > 0).astype(float)
    has_sell = d[sell_src].notna().astype(float)
    has_trnd = d[trend_src].notna().astype(float)
    d["coverage"] = (has_star + has_sell + has_trnd) / 3.0

    W_COVERAGE = 0.05
    W_CORE = 1.0 - W_COVERAGE
    core = (w_sell * d["s_sell12"] + w_star * d["star_need_abs"] + w_trend * d["s_trend"])
    d["score"] = W_CORE * core + W_COVERAGE * d["coverage"]

    # Sem dados em nada -> 0
    mask_no_data = (d["has_stars"].eq(0) & d[sell_src].isna() & d["trend_l6m"].isna())
    d.loc[mask_no_data, ["s_sell12", "star_need_abs", "s_trend", "score"]] = 0.0

    d['cidcustomer'] = d['cod_cliente']  # para facilitar junção posterior
    # ----- Saída -----
    cols_out = [
        "ciduserowner",
        "cnpj", "cod_cliente","cidcustomer", "nome_cliente", "vendedor", "cidterritory", 
        "endereco", "lat", "long", "companyname",
        "uf", "cidade", "gerente_area",
        # estrelas e derivados
        *[c for c in STAR_COLS if c in d.columns],
        "estrelas_total", "estrelas_total_eff", "has_stars", "star_need_abs",
        # métricas
        sell_src, "s_sell12", "trend_l6m", "trend_l6m_holding", "trend_need", "s_trend",
        "score", "tem_estrelas", "depot_lat", "depot_long"
    ]
    # mantém somente as que existem
    cols_out = [c for c in cols_out if c in d.columns]
    scores_mes = d[cols_out].copy()
    
    # rank por grupo (vendedor ou GA)
    prioritarios = (
        scores_mes.sort_values([group_by, "score"], ascending=[True, False])
                  .groupby(group_by, as_index=False, group_keys=False)
                  .head(int(top_n))[["cnpj", group_by]]
                  .assign(priority=1)
    )

    # Chaves como string
    scores_mes["cnpj"] = scores_mes["cnpj"].astype(str)
    prioritarios["cnpj"] = prioritarios["cnpj"].astype(str)

        
    scores_mes['prioritario'] = scores_mes['cnpj'].isin(prioritarios['cnpj']).astype(int)
    # checar quantos prioritários por grupo
    print(f"Contagem de prioritários por {group_by}:\n{scores_mes.groupby(group_by)['prioritario'].sum()}")

    scores_mes['cidcustomer'] = '0' + scores_mes['cidcustomer'].astype(str)
    scores_mes['cidterritory'] = '0' + scores_mes['cidterritory'].astype(str)
    
    scores_mes.to_csv(
        OUTPUT_DIR / f'{periodo_rotas}/scores_{periodo_pesquisa}.csv',
        index=False
    )
    return scores_mes

    # return scores_mes, periodo_pesquisa, periodo_rotas

if __name__ == "__main__":
    scores_mes = compute_scores(periodo_rotas='P03', periodo_pesquisa='P02')
    print(scores_mes.head(10))