import pandas as pd
import numpy as np
import time
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
print(f"BASE_DIR definido como: {BASE_DIR}")

INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"


def prep():
    print("\n\n============================ 1 - PREP ==========================")
    t0 = time.time()

    def _pct(n, d):
        return (100.0 * n / d) if d else 0.0

    # ============= Ingerir e tratar hierarquia ================
    df_hierarquia = pd.read_excel(INPUT_DIR / "hierarquia_feb_26.xlsx", header=6)
    df_hierarquia = df_hierarquia.rename(columns={'Cliente/Costumer': 'id_cliente',
                            'Vendedor/ Salesman': 'cidterritory',
                            'Vendedor/ Salesman.1': 'nome_vendedor',
                            'Sales District': 'distrito_vendas',
                            'Gerente Regional/ Area Manager': 'id_gerente_area',
                            'Gerente de Area/ Area Manager': 'nome_gerente_area',
                            'Gerente Regional/ Regional Manager': 'id_gerente_regional',
                            'Gerente Regional/ Regional Manager.1': 'nome_gerente_regional',
                            'Gerente Nacional/ Country Manager': 'id_gerente_nacional',
                            'Gerente Nacional/Country Manager': 'nome_gerente_nacional',
                            'Cliente/Customer': 'cod_cliente',
                            'Razão Social/ Name': 'nome_cliente',})

    df_hierarquia = df_hierarquia.drop(columns=['Nomenclature', 'Nível 1', 'Nível 2', 'distrito_vendas', 'id_gerente_nacional'
                                                ,'nome_gerente_nacional', 'id_gerente_regional', 'nome_gerente_regional',])

    # ============= Dados de localização dos PDVs ================
    loc_df = pd.read_csv(INPUT_DIR / "end_pdvs_coords.csv", sep=';')

    loc_df.drop(columns=['ger_area', 
                        'vendedor', 
                        'nome_loja'], 
                        inplace=True)

    loc_df.rename(columns={'cliente': 'cod_cliente',
                        'Latitude': 'lat', 
                        'Longitude': 'long'}, 
                        inplace=True)

    # ============= Merge hierarquia com localização ================
    df = df_hierarquia.merge(loc_df, on='cod_cliente', how='left')

    df['cod_cliente'] = df['cod_cliente'].astype(str)

    # ============= Ingerir e tratar DIM_CLIENTE ================
    dim_cliente = pd.read_excel(INPUT_DIR / "dim_cliente.xlsx", header=0)

    dim_cliente.rename(columns={'Masterdata Cliente[sk_customer_sellout]': 'cod_cliente',
                                'Masterdata Cliente[Gerente Area]': 'gerente_area',
                                'Masterdata Cliente[cd_cnpj]': 'cnpj_cliente',}, inplace=True)

    dim_cliente_gus = dim_cliente[dim_cliente['gerente_area'].str.contains('GUSTAVO', case=False, na=False)].copy()

    dim_cliente_gus['cod_cliente'] = dim_cliente_gus['cod_cliente'].str.replace('customer_', '', regex=False)

    # ============ Criar df_gus (hierarquia + loc) ==============
    pdvs_total = df['cod_cliente'].nunique()
    vendedores_total = df['nome_vendedor'].nunique()

    df_gus = df[df['nome_gerente_area'].str.contains('GUSTAVO', na=False)].copy()
    pdvs_gus = df_gus['cod_cliente'].nunique()
    vendedores_gus = df_gus['nome_vendedor'].nunique()

    print(
        f"\n[FILTRO] pdvs_total={pdvs_total} -> pdvs_gus={pdvs_gus} ({_pct(pdvs_gus, pdvs_total):.1f}%) | "
        f"vendedores_total={vendedores_total} -> vendedores_gus={vendedores_gus} ({_pct(vendedores_gus, vendedores_total):.1f}%)\n"
    )

    # ============ Merge df_gus com dim_cliente_gus ==============
    df_merge = df_gus.merge(dim_cliente_gus,
                        left_on='cod_cliente',
                        right_on='cod_cliente',
                        how='inner').copy()

    df_merge = df_merge[['cod_cliente',
                        'cnpj_cliente',
                        'Masterdata Cliente[Holding]',
                        'nome_cliente',
                        'nome_gerente_area',
                        'nome_vendedor',
                        'endereco',
                        'uf',
                        'cidade',
                        'lat',
                        'long']].copy()

    df_merge.rename(columns={'cnpj_cliente': 'cnpj',
                            'Latitude': 'lat',
                            'Longitude': 'long',
                            'Masterdata Cliente[Holding]': 'holding',
                            'nome_gerente_area': 'gerente_area',
                            'nome_vendedor': 'vendedor'}, inplace=True)

    # === Separar "id-nome" em gerente_area e vendedor ===
    ga = df_merge["gerente_area"].astype("string").str.extract(r"^\s*([^-\s]+)\s*-\s*(.*)$")
    df_merge["id_gerente_area"] = ga[0].str.strip()
    df_merge["gerente_area"] = ga[1].str.strip().fillna(df_merge["gerente_area"].astype("string").str.strip())

    vd = df_merge["vendedor"].astype("string").str.extract(r"^\s*([^-\s]+)\s*-\s*(.*)$")
    df_merge["cidterritory"] = vd[0].str.strip()
    df_merge["vendedor"] = vd[1].str.strip().fillna(df_merge["vendedor"].astype("string").str.strip())

    base_order = [
        "cod_cliente",
        "cnpj",
        "holding",
        "nome_cliente",
        "id_gerente_area",
        "gerente_area",
        "cidterritory",
        "vendedor",
        "endereco",
        'uf',
        'cidade',
        "lat",
        "long",
    ]
    df_merge = df_merge[base_order + [c for c in df_merge.columns if c not in base_order]]

    # ============ Ingerir e tratar scores ==============
    scores = pd.read_csv(INPUT_DIR / "score_p10_25_p02_26.csv")
    scores = scores[scores['Equipegerareavendas'].str.contains('gustavo', case=False, na=False)]
    scores['Vendedor'] = scores['Vendedor'].str.upper()

    id_cols  = ["Codigocliente"] # chave única do PDV
    df2 = scores.copy()

    import unicodedata

    def normalize_text(s):
        s = unicodedata.normalize("NFKD", s)
        s = s.encode("ASCII", "ignore").decode("ASCII")
        return s

    df2["_kpi"] = (
        df2["Kpi"]
            .astype(str)
            .str.strip()
            .apply(normalize_text)
            .str.lower()
            .str.replace(r"\W+", "_", regex=True)
)

    for c in ["Qtdestrelas"]:
        if c in df2.columns:
            df2[c] = pd.to_numeric(df2[c], errors="coerce")

    if "Datapesquisa"  in df2.columns:
        df2["Datapesquisa"] = pd.to_datetime(df2["Datapesquisa"], errors="coerce")

        # pega última data por PDV
        max_date_pdv = (
            df2.groupby("Codigocliente")["Datapesquisa"]
                .max()
                .reset_index()
                .rename(columns={"Datapesquisa": "max_data"})
        )

        # junta de volta
        df2 = df2.merge(max_date_pdv, on="Codigocliente", how="left")

        # filtra só registros da última pesquisa daquele PDV
        df2 = df2[df2["Datapesquisa"] == df2["max_data"]].copy()
        most_recent_pesquisa = df2["Datapesquisa"].max().strftime("%Y-%m")
        periodo_pesquisa = "P" + most_recent_pesquisa.split("-")[1]
        periodo_rotas = "P" + str(int(most_recent_pesquisa.split("-")[1]) + 1).zfill(2)

    print('=====================================================')
    print(f"pesquisa mais recente: {periodo_pesquisa}\n periodo_rotas: {periodo_rotas}")
    print('=====================================================')

    df_piv = df2.pivot_table(
        index=id_cols,
        columns="_kpi",
        values=["Qtdestrelas"],
        aggfunc="first"
    )

    value_map = {"Qtdestrelas": "estrelas"}

    df_piv.columns = df_piv.columns.set_levels(
        [value_map.get(l, l) for l in df_piv.columns.levels[0]], level=0
    )

    scores_wide = df_piv.copy()
    scores_wide.columns = [f"{lvl0}_{lvl1}" for (lvl0, lvl1) in scores_wide.columns]
    scores_wide = scores_wide.reset_index()

    kpis = sorted(df2["_kpi"].dropna().unique())
    for k in kpis:
        col_obj = f"objetivo_{k}"
        col_res = f"resultado_{k}"
        if col_obj in scores_wide.columns and col_res in scores_wide.columns:
            objetivo  = pd.to_numeric(scores_wide[col_obj], errors="coerce")
            resultado = pd.to_numeric(scores_wide[col_res], errors="coerce")
            scores_wide[f"porcentagem_{k}"] = np.where(objetivo == 0, np.nan, resultado / objetivo)

    ordered_cols = id_cols.copy()
    for k in kpis:
        for suffix in [f"objetivo_{k}", f"resultado_{k}", f"porcentagem_{k}", f"estrelas_{k}"]:
            if suffix in scores_wide.columns:
                ordered_cols.append(suffix)

    rest = [c for c in scores_wide.columns if c not in ordered_cols]
    scores_wide = scores_wide[ordered_cols + rest]

    scores_wide.columns = scores_wide.columns.str.lower()
    scores_wide.rename(columns={'codigocliente': 'cod_cliente', 'nomecliente': 'nome_cliente',
                            'codigousuario': 'cod_promotor', 'nomeusuario': 'nome_promotor'}, inplace=True)
    # scores_wide.drop(columns=['cod_promotor', 'nome_promotor'], inplace=True)

    # rename
    scores_wide.rename(columns={
        'estrelas_espaço_em_loja': 'estrelas_espaco_em_loja',
        'estrelas_inovação': 'estrelas_inovacao',
        'estrelas_ponto_extra': 'estrelas_ponto_extra',
        'estrelas_preço': 'estrelas_preco',
        'estrelas_sortimento_prioritário': 'estrelas_sortimento_prioritario',
    }, inplace=True)

    # ============ Merge df_merge com scores_wide ==============
    scores_wide['estrelas_total'] = scores_wide[['estrelas_batalhas',
                                        'estrelas_espaco_em_loja',
                                        'estrelas_inovacao',
                                        'estrelas_ponto_extra',
                                        'estrelas_preco',
                                        'estrelas_sortimento_prioritario'
                                        ]].sum(axis=1)

    print(f"scores_wide: {len(scores_wide)} registros")

    scores_wide['cod_cliente'] = scores_wide['cod_cliente'].astype(str)

    overlap = set(df_merge.columns) & set(scores_wide.columns)
    scores_wide.drop(columns=overlap - {'cod_cliente'}, inplace=True)

    df_merge = df_merge.merge(scores_wide, left_on='cod_cliente', right_on='cod_cliente', how='left')

    # % nulos em estrelas_total (df_merge, pós-merge)
    null_stars = int(df_merge["estrelas_total"].isna().sum())
    print(f"[SCORES] PDVs sem score (estrelas_total nulo): {null_stars} de {len(df_merge)} ({_pct(null_stars, len(df_merge)):.1f}%)")

    # ============ Ingerir SELLOUT (parquet) e criar sellout_ton_ytd + trend_l6m ============
    print("\n[PREP] Lendo dados de SELLOUT (OSA) e criando sellout_ton_ytd + trend_l6m...")
    t_sell = time.time()

    sell_path = INPUT_DIR / "osa_p01_p08_25.parquet"
    print(f"[SELLOUT] lendo parquet: {sell_path}")

    sell = pd.read_parquet(sell_path)

    # Padroniza nomes
    # Esperado no parquet: DATA, CNPJ, SELLOUT_TON, SELLOUT_REAIS (+ categoria/familia)
    sell.columns = sell.columns.str.lower()

    # rename para padrão interno
    rename_map = {
        "data": "data",
        "cnpj": "cnpj",
        "sellout_ton": "sellout_ton",
    }
    # só renomeia se existirem
    sell.rename(columns={k: v for k, v in rename_map.items() if k in sell.columns}, inplace=True)

    # validações mínimas
    need_cols = {"data", "cnpj", "sellout_ton"}
    missing = need_cols - set(sell.columns)
    if missing:
        raise ValueError(f"[SELLOUT] colunas ausentes no parquet: {missing}. Colunas atuais: {sell.columns.tolist()}")

    # tipagens
    sell["cnpj"] = sell["cnpj"].astype(str)
    sell["data"] = pd.to_datetime(sell["data"], errors="coerce")
    sell["sellout_ton"] = pd.to_numeric(sell["sellout_ton"], errors="coerce").fillna(0.0)

    # 1) agrega por mês + CNPJ (ignorando categoria/familia)
    sell["mes"] = sell["data"].dt.to_period("M").dt.to_timestamp()

    sell_m = (
        sell.groupby(["cnpj", "mes"], as_index=False)["sellout_ton"]
            .sum()
    )

    # 2) sellout_ton_ytd (por enquanto = soma do histórico todo), qdo tivermos os dados atualizados, será de 2026-01 pra frente. (YTD)
    sellout_ytd = (
        sell_m.groupby("cnpj", as_index=False)["sellout_ton"]
            .sum()
            .rename(columns={"sellout_ton": "sellout_ton_ytd"})
    )

    # 3) trend_l6m (slope dos últimos 6 meses) por CNPJ
    def _slope_last6(g: pd.DataFrame) -> float:
        g = g.sort_values("mes")
        g = g.tail(6)
        if len(g) < 2:
            return np.nan
        y = g["sellout_ton"].to_numpy(dtype=float)
        x = np.arange(len(y), dtype=float)  # 0..n-1
        # slope da reta y = a*x + b
        a = np.polyfit(x, y, 1)[0]
        return float(a)

    trend = (
        sell_m.groupby("cnpj", as_index=False)
            .apply(lambda g: _slope_last6(g), include_groups=False)
            .rename(columns={None: "trend_l6m"})
    )

    # 4) junta sellout + trend
    sell_feats = sellout_ytd.merge(trend, on="cnpj", how="left")

    # 5) merge no df_merge final (via cnpj)
    df_merge["cnpj"] = df_merge["cnpj"].astype(str)
    df_merge = df_merge.merge(sell_feats, on="cnpj", how="left")

    # coverage
    null_sell = int(df_merge["sellout_ton_ytd"].isna().sum())
    null_trnd = int(df_merge["trend_l6m"].isna().sum())
    total = len(df_merge)
    pct_sell = _pct(null_sell, total)
    pct_trnd = _pct(null_trnd, total)

    print(f"[SELLOUT] sellout_ton_ytd: {null_sell} nulos de {total} registros ({pct_sell:.1f}% sem dados)")
    print(f"[SELLOUT] trend_l6m:       {null_trnd} nulos de {total} registros ({pct_trnd:.1f}% sem dados)")

    # ============ Agregar período de pesquisa mais recente no nome do arquivo ==============
    os.makedirs(f'output/{periodo_rotas}', exist_ok=True)

    df_merge.to_csv(f'output/{periodo_rotas}/pdvs_vendedores_{periodo_pesquisa}.csv', index=False)

    print(f"[PREP] tempo total={time.time() - t0:.1f}")

    return periodo_pesquisa, periodo_rotas

if __name__ == "__main__":
    prep()