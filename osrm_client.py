import requests
import time
import numpy as np
import math
def haversine(lat1, lon1, lat2, lon2, R=6371.0):
    """
    Distância Haversine entre dois pontos (lat/lon em graus).
    Retorna distância em km.
    """
    lat1, lon1, lat2, lon2 = map(float, (lat1, lon1, lat2, lon2))
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c


OSRM_BASE_URL = "https://osrm.operdata.com.br"
OSRM_PROFILE  = "car"
OSRM_BLOCK    = 80

def osrm_is_up(base_url=OSRM_BASE_URL, profile=OSRM_PROFILE, timeout=5):
    """
    Retorna True se o servidor OSRM responder OK.
    Faz um ping leve ao endpoint /nearest.
    """
    try:
        # coord aleatória válida (lon,lat). Pode ser qualquer ponto do mapa carregado.
        url = f"{base_url}/nearest/v1/{profile}/-46.6388,-23.5489"
        r = requests.get(url, timeout=timeout)
        return r.status_code == 200 and "waypoints" in r.json()
    except Exception:
        return False

def _osrm_table_block(src_pts, dst_pts, base_url=OSRM_BASE_URL, profile=OSRM_PROFILE, timeout=20):
    """
    src_pts, dst_pts: listas de (lat, lon)
    Monta a requisição /table com 'coordinates = [src... + dst...]' e
    indices de sources/destinations por faixa.
    Retorna (durations_s: np.array [Ns x Nd], distances_m: np.array [Ns x Nd])
    """
    # OSRM espera lon,lat
    coords_src = ["{:.6f},{:.6f}".format(lon, lat) for (lat, lon) in src_pts]
    coords_dst = ["{:.6f},{:.6f}".format(lon, lat) for (lat, lon) in dst_pts]
    coords_all = coords_src + coords_dst

    Ns = len(src_pts); Nd = len(dst_pts)
    sources = ";".join(str(i) for i in range(Ns))
    dests   = ";".join(str(Ns + j) for j in range(Nd))
    coords  = ";".join(coords_all)

    url = (f"{base_url}/table/v1/{profile}/{coords}"
           f"?annotations=duration,distance&sources={sources}&destinations={dests}")

    # Retry simples (lida com 429)
    for attempt in range(4):
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            data = resp.json()
            dur = np.array(data["durations"], dtype=float)   # segundos
            dist= np.array(data.get("distances", [[math.nan]*Nd]*Ns), dtype=float)  # metros
            return dur, dist
        if resp.status_code == 429 and attempt < 3:
            time.sleep(1.5 * (attempt + 1))
            continue
        resp.raise_for_status()

def osrm_table_full(pts_latlon, block=OSRM_BLOCK, base_url=OSRM_BASE_URL, profile=OSRM_PROFILE):
    """
    pts_latlon: np.array [[lat, lon], ...] incluindo depósito na posição 0.
    Retorna:
        Dkm  (nxn) em km (float)
        Tmin (nxn) em minutos (int)
    Pode levantar exceção se OSRM indisponível.
    """
    import numpy as np
    n = len(pts_latlon)
    Dur = np.full((n, n), np.nan, float)   # s
    Dis = np.full((n, n), np.nan, float)   # m

    # chunking NxN
    for i0 in range(0, n, block):
        for j0 in range(0, n, block):
            src_slice = pts_latlon[i0:i0+block]
            dst_slice = pts_latlon[j0:j0+block]
            src_pts = [(float(a), float(b)) for a,b in src_slice]
            dst_pts = [(float(a), float(b)) for a,b in dst_slice]
            dur, dis = _osrm_table_block(src_pts, dst_pts, base_url=base_url, profile=profile)
            Ns, Nd = dur.shape
            Dur[i0:i0+Ns, j0:j0+Nd] = dur
            Dis[i0:i0+Ns, j0:j0+Nd] = dis

    # conversões finais
    Dkm  = Dis / 1000.0
    Tmin = np.round(Dur / 60.0).astype(int)
    return Dkm, Tmin

def build_matrices_with_osrm_or_fallback(pts_latlon, route_inflation=1.25, avg_speed_kmph=30.0):
    """
    Tenta OSRM. Se falhar, usa haversine + inflação (seu método atual).
    Retorna Dkm (float), Tmin (int).
    """
    import numpy as np
    try:
        Dkm, Tmin = osrm_table_full(pts_latlon)
        # Se o OSRM não devolveu distance, caímos pro tempo e recomputamos Dkm por dur * v
        if not np.isfinite(Dkm).any():
            Dkm = (Tmin.astype(float) / 60.0) * (avg_speed_kmph)  # aproxima
        return Dkm, Tmin
    except Exception as e:
        # Fallback: haversine + inflação + velocidade média
        n = len(pts_latlon)
        Dkm = np.zeros((n, n), float)
        for i in range(n):
            for j in range(i+1, n):
                # haversine_km espera (lat1, lon1, lat2, lon2)
                d = haversine(pts_latlon[i,0], pts_latlon[i,1], pts_latlon[j,0], pts_latlon[j,1])
                Dkm[i,j] = Dkm[j,i] = d * route_inflation
        Tmin = np.round(Dkm / max(1e-9, avg_speed_kmph) * 60.0).astype(int)
        return Dkm, Tmin