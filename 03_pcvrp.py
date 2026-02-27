# py_scripts/pcvrp_mod.py

import numpy as np
import pandas as pd
import math
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import time
from osrm_client import build_matrices_with_osrm_or_fallback
from importlib import import_module
import sys

sys.path.insert(0, '.')
spec = import_module('02_score')
periodo_rotas = spec.periodo_rotas
periodo_pesquisa = spec.periodo_pesquisa

path = f"output/{periodo_rotas}/scores_{periodo_pesquisa}.csv"

df_all_vendors = pd.read_csv(path, dtype={"cnpj": str}) 
df_all_vendors = df_all_vendors[df_all_vendors['prioritario'] == 1].copy() # filtra apenas prioritários

# extrai automaticamento o periodo do nome do arquivo scores_p02.csv -> p02 e adiciona 1 ao número para obter o período do arquivo de rotas (p03)
periodo = path.split("_")[-1].split(".")[0]
periodo_num = int(periodo[1:]) + 1
periodo = f"p{periodo_num:02d}"

print(f"número de pdvs por vendedor:\n{df_all_vendors.groupby('vendedor')['cnpj'].nunique()}")

def centroide_vendedor(df_vendor):
    """
    df_vendor precisa ter colunas 'lat' e 'long'.
    Retorna (lat_media, long_media) como tupla simples de floats do Python,
    não numpy scalar.
    """
    lat_c = float(df_vendor["lat"].astype(float).mean())
    lon_c = float(df_vendor["long"].astype(float).mean())
    return (lat_c, lon_c)

# -----------------------------
# Parâmetros padrão
# -----------------------------

NDIAS                = 4 # recebe valor do numericinput do dashboard (padrão = 4)
MAX_STOPS            = 15
MAX_MIN_PER_DAY      = 7 * 60
SERVICE_MIN_PER_STOP = 45.0
AVG_SPEED_KMPH       = 30.0
ROUTE_INFLATION      = 1.0
PENALTY_KM_SCALE     = 20

def _solve_pcvrp_vendor(
    df_vendor: pd.DataFrame,
    n_days=NDIAS,
    max_stops=MAX_STOPS,
    max_min_day=MAX_MIN_PER_DAY,
    route_inflation=ROUTE_INFLATION,
    avg_speed_kmph=AVG_SPEED_KMPH,
    penalty_km_scale=PENALTY_KM_SCALE,
    time_limit_s=2,
    BALANCE_PENALTY_PER_MIN=8,
    SPAN_COST_COEFF=0
) -> list[pd.DataFrame]:
    """Resolve PCVRP somente para um vendedor (df_vendor já filtrado).
       df_vendor precisa ter: ['cnpj' ou 'cnpj', 'nome_cliente', 'lat','long','score'].
    """
    if df_vendor is None or df_vendor.empty:
        return [df_vendor.iloc[[]].copy() for _ in range(n_days)]

    # normaliza nomes / tipos
    df = df_vendor.copy()
    if "cnpj" not in df.columns and "cnpj" in df.columns:
        df["cnpj"] = df["cnpj"]
    df["cnpj"] = df["cnpj"].astype(str)

    for c in ["lat", "long", "score"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    s = df["score"].astype(float)
    s_norm = (s - s.min())/(s.max()-s.min()) if s.max()>s.min() else pd.Series(0.5, index=s.index)
    df["score_norm"] = s_norm

    depot_lat, depot_lon = centroide_vendedor(df)
    pts  = np.vstack([[depot_lat, depot_lon], df[["lat","long"]].to_numpy()])
    Dkm, Tmin = build_matrices_with_osrm_or_fallback(
        pts_latlon=pts, route_inflation=route_inflation, avg_speed_kmph=avg_speed_kmph
    )

    n_nodes  = Tmin.shape[0]
    vehicles = int(min(n_days, max(1, len(df))))
    manager  = pywrapcp.RoutingIndexManager(n_nodes, vehicles, [0]*vehicles, [0]*vehicles)
    routing  = pywrapcp.RoutingModel(manager)

    def time_cb(i, j):
        a, b = manager.IndexToNode(i), manager.IndexToNode(j)
        return int(Tmin[a, b])
    time_idx_cost = routing.RegisterTransitCallback(time_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(time_idx_cost)

    def stops_cb(i, j):
        return 0 if manager.IndexToNode(i) == 0 else 1
    stops_idx = routing.RegisterTransitCallback(stops_cb)
    routing.AddDimension(stops_idx, 0, int(max_stops), True, "Stops")

    service = np.zeros(n_nodes, dtype=int)
    service[1:] = int(SERVICE_MIN_PER_STOP)
    def time_with_service_cb(i, j):
        a, b = manager.IndexToNode(i), manager.IndexToNode(j)
        return int(Tmin[a, b] + service[b])
    time_idx = routing.RegisterTransitCallback(time_with_service_cb)
    routing.AddDimension(time_idx, 0, int(max_min_day), True, "TimeMin")

    for v in range(vehicles):
        routing.GetDimensionOrDie("Stops").CumulVar(routing.End(v)).SetMin(1)

    time_dim = routing.GetDimensionOrDie("TimeMin")
    total_service = service[1:].sum()
    depot_half = sum((Tmin[0,n]+Tmin[n,0])/2.0 for n in range(1, n_nodes))
    alvo_por_dia = int(min(max_min_day, math.ceil((total_service + depot_half)/max(1, vehicles))))
    for v in range(vehicles):
        time_dim.SetCumulVarSoftUpperBound(routing.End(v), alvo_por_dia, int(BALANCE_PENALTY_PER_MIN))
    if SPAN_COST_COEFF > 0:
        time_dim.SetGlobalSpanCostCoefficient(int(SPAN_COST_COEFF))

    scale = 1000; BASE_KM = 5.0
    for node in range(1, n_nodes):
        pdv_idx = node - 1
        pen_km  = BASE_KM + df.iloc[pdv_idx]["score_norm"] * penalty_km_scale
        routing.AddDisjunction([manager.NodeToIndex(node)], int(pen_km * scale))

    p = pywrapcp.DefaultRoutingSearchParameters()
    p.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    p.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    p.time_limit.FromSeconds(time_limit_s)

    sol = routing.SolveWithParameters(p)
    dias = []
    if not sol:
        return [df.iloc[[]].copy() for _ in range(n_days)]

    for v in range(vehicles):
        idx = routing.Start(v); visit=[]
        while not routing.IsEnd(idx):
            node = manager.IndexToNode(idx)
            if node != 0: visit.append(node-1)
            idx = sol.Value(routing.NextVar(idx))

        rota = df.iloc[visit].copy().reset_index(drop=True)
        rota["ordem"] = np.arange(1, len(rota)+1)
        rota["semana"] = v+1

        km_total=min_total=0
        if visit:
            km_total += Dkm[0, visit[0]+1]
            min_total += Tmin[0, visit[0]+1] + service[visit[0]+1]
            for i in range(len(visit)-1):
                a, b = visit[i]+1, visit[i+1]+1
                km_total += Dkm[a, b]
                min_total += Tmin[a, b] + service[b]
            km_total += Dkm[visit[-1]+1, 0]
            min_total += Tmin[visit[-1]+1, 0]
        rota["km_rota"]  = float(km_total)
        rota["min_rota"] = int(min_total)

        dias.append(rota)

    while len(dias) < n_days:
        dias.append(df.iloc[[]].copy())
    return dias

def route_vendor(df_vendor: pd.DataFrame,
                 n_days=NDIAS,
                 max_stops=MAX_STOPS,
                 max_min_day=MAX_MIN_PER_DAY,
                 route_inflation=ROUTE_INFLATION,
                 avg_speed_kmph=AVG_SPEED_KMPH,
                 penalty_km_scale=PENALTY_KM_SCALE,
                 time_limit_s=10) -> pd.DataFrame:
    """API pública: retorna um único DataFrame com todas as semanas."""
    rotas = _solve_pcvrp_vendor(
        df_vendor,
        n_days=n_days,
        max_stops=max_stops,
        max_min_day=max_min_day,
        route_inflation=route_inflation,
        avg_speed_kmph=avg_speed_kmph,
        penalty_km_scale=penalty_km_scale,
        time_limit_s=time_limit_s,
    )
    if not rotas:
        return pd.DataFrame(columns=["cnpj","semana","ordem","km_rota","min_rota","visitas_rota"])

    out = []
    for r in rotas:
        if r is None or r.empty:
            continue
        # garante colunas chave para o join de volta
        for c in ["cnpj", "nome_cliente", "lat", "long", "score", "semana", "ordem", "km_rota", "min_rota"]:
            if c not in r.columns:
                r[c] = np.nan
        r["cnpj"] = r["cnpj"].astype(str)

        out.append(r[["cnpj","nome_cliente","lat","long","score","semana","ordem","km_rota","min_rota"]])

    if not out:
        return pd.DataFrame(columns=["cnpj","semana","ordem","km_rota","min_rota","visitas_rota"])

    df_out = pd.concat(out, ignore_index=True)

    # métrica auxiliar: visitas por rota/semana (inteiro nativo pandas)
    df_out["visitas_rota"] = (
        df_out.groupby("semana", dropna=False)["cnpj"]
              .transform("nunique")
              .astype("Int64")
    )

    # Tipos finais coerentes
    df_out["semana"] = df_out["semana"].astype("Int64")
    df_out["ordem"]  = df_out["ordem"].astype("Int64")
    df_out["min_rota"] = df_out["min_rota"].astype("Int64")

    return df_out


for vendedor in df_all_vendors['vendedor'].unique():
    start_time = time.time()
    # print(f"Processando vendedor: {vendedor}")
    # print(f"número de PDVs para {vendedor}: {df_all_vendors[df_all_vendors['vendedor'] == vendedor]['cnpj'].nunique()}")
    df_vendor = df_all_vendors[df_all_vendors['vendedor'] == vendedor]
    df_rotas = route_vendor(df_vendor)
    # df_rotas.to_csv(f"output/rotas_{vendedor}.csv", index=False)
    end_time = time.time()
    print(f"Tempo de processamento para {vendedor}: {end_time - start_time:.2f} segundos")
    df_rotas["vendedor"] = vendedor
    if vendedor == df_all_vendors['vendedor'].unique()[0]:
        df_all_rotas = df_rotas.copy()
    else:
        df_all_rotas = pd.concat([df_all_rotas, df_rotas], ignore_index=True)

order = ['vendedor', 'cnpj', 'nome_cliente', 'lat', 'long', 'score', 'semana', 'ordem', 'km_rota', 'min_rota', 'visitas_rota']
df_all_rotas = df_all_rotas[order]



df_all_rotas.to_csv(f"output/{periodo_rotas}/rotas_{periodo_rotas}.csv", index=False)
print(f"Total de rotas concatenadas: {len(df_all_rotas)}")

km_total = df_all_rotas["km_rota"].sum()
pdvs_visitados_total = df_all_rotas["cnpj"].nunique()

print(f"Km total: {km_total:.2f} km")
print(f"PDVs visitados: {pdvs_visitados_total}")