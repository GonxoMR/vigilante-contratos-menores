"""
Extract data from pipeline CSVs into web/data.json for web/index.html.
Run: python analysis/extract_web_data.py
Output: web/data.json (~40-60KB)
"""

import json
import os
import csv
from pathlib import Path

BASE = Path(__file__).parent.parent
OUTPUT = BASE / "output"
WEB = BASE / "web"
WEB.mkdir(exist_ok=True)

# ── helpers ─────────────────────────────────────────────────────────────────

def read_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows

def safe_float(v, default=0.0):
    try:
        return float(str(v).replace(",", ".").strip()) if v not in (None, "", "nan") else default
    except (ValueError, TypeError):
        return default

def safe_int(v, default=0):
    try:
        return int(float(str(v).strip())) if v not in (None, "", "nan") else default
    except (ValueError, TypeError):
        return default

# ── STATS ────────────────────────────────────────────────────────────────────

stats = {
    "madrid_contratos": 15778,
    "madrid_importe": 109891395,
    "barcelona_contratos": 62473,
    "barcelona_importe": 201612404,
    "hhi_mediana_madrid": 0.044,
    "hhi_mediana_barcelona": 0.012,
    "ratio_mediana": 3.6,
    "alertas_fraccionamiento": 866,
    "nifs_alertas": 279,
    "anios": [2023, 2024, 2025],
    "madrid_por_ano": {"2023": 5719, "2024": 5535, "2025": 4524},
    "cpv_cobertura_pct": 51.9,
    "actualizado": "2026-04-28",
}

# ── HHI SECTOR ───────────────────────────────────────────────────────────────

hhi_sector_raw = read_csv(OUTPUT / "06_hhi_sector_comparativa.csv")
hhi_sector = []
for r in hhi_sector_raw:
    ratio = safe_float(r.get("RATIO_MAD_BCN", 0))
    if ratio > 0:
        hhi_sector.append({
            "cpv": r["CPV"],
            "desc": r["CPV_DESC_L1"],
            "hhi_mad": round(safe_float(r["HHI"]), 4),
            "hhi_bcn": round(safe_float(r["HHI_BCN"]), 4),
            "ratio": round(ratio, 2),
            "n_mad": safe_int(r["N_CONTRATOS"]),
            "n_bcn": safe_int(r["N_BCN"]),
            "importe_mad": round(safe_float(r["IMPORTE_TOTAL"])),
        })
# sort by ratio desc
hhi_sector.sort(key=lambda x: x["ratio"], reverse=True)

# ── HHI ORGANO TOP20 ─────────────────────────────────────────────────────────

hhi_organo_raw = read_csv(OUTPUT / "04_hhi_organo_madrid.csv")
hhi_organo_top20 = []
for r in hhi_organo_raw:
    hhi_organo_top20.append({
        "organo": r["ORGANO"],
        "hhi": round(safe_float(r["HHI"]), 4),
        "n": safe_int(r["N_CONTRATOS"]),
        "importe": round(safe_float(r["IMPORTE_TOTAL"])),
        "n_proveedores": safe_int(r["N_PROVEEDORES"]),
    })
hhi_organo_top20.sort(key=lambda x: x["hhi"], reverse=True)
hhi_organo_top20 = hhi_organo_top20[:20]

# ── FRACCIONAMIENTO TOP30 ────────────────────────────────────────────────────

frac_raw = read_csv(OUTPUT / "07_fraccionamiento_madrid.csv")
frac_top30 = []
for r in frac_raw:
    frac_top30.append({
        "nif": r["NIF"],
        "razon_social": r["RAZON_SOCIAL"],
        "tipo": r["TIPO"],
        "organo": r["ORGANO"],
        "fecha": r["FECHA_INICIO_VENTANA"],
        "n_contratos": safe_int(r["N_CONTRATOS_VENTANA"]),
        "suma_sin_iva": round(safe_float(r["SUMA_SIN_IVA"])),
        "umbral": safe_int(r["UMBRAL_LCSP"]),
        "exceso": round(safe_float(r["EXCESO"])),
    })
frac_top30.sort(key=lambda x: x["exceso"], reverse=True)
frac_top30 = frac_top30[:30]

# ── CASE STUDIES from 01_madrid_limpio.csv ───────────────────────────────────

madrid_path = OUTPUT / "01_madrid_limpio.csv"
gartner_rows = []
moype_rows = []
serintcom_rows = []
implaser_rows = []

with open(madrid_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        nif = (row.get("NIF") or "").strip()
        if nif == "B84184217":
            gartner_rows.append(row)
        elif nif == "A78111549":
            moype_rows.append(row)
        elif nif == "05341273Y":
            serintcom_rows.append(row)
        elif nif == "B50776947":
            implaser_rows.append(row)

# Gartner: list of contracts sorted by date
gartner = []
for r in gartner_rows:
    fecha = (r.get("FECHA_ADJUDICACION") or "").strip()
    importe = safe_float(r.get("IMPORTE_IVA") or r.get("IMPORTE_LICITACION_IVA") or 0)
    organo = (r.get("ORGANO") or "").strip()
    ano = safe_int(r.get("AÑO") or 0)
    if fecha and importe > 0:
        gartner.append({
            "fecha": fecha,
            "importe": round(importe),
            "organo": organo,
            "ano": ano,
            "objeto": (r.get("OBJETO") or "")[:120],
        })
gartner.sort(key=lambda x: x["fecha"])

# Moype: aggregate by organo/distrito
moype_agg = {}
for r in moype_rows:
    organo = (r.get("ORGANO") or "Sin órgano").strip()
    # simplify district name
    distrito = organo
    for prefix in [
        "COORDINADOR DEL DISTRITO DE ",
        "COORDINADORA DEL DISTRITO DE ",
        "COORDINADOR/A DEL DISTRITO DE ",
    ]:
        if organo.upper().startswith(prefix.upper()):
            distrito = organo[len(prefix):]
            break
    importe = safe_float(r.get("IMPORTE_IVA") or r.get("IMPORTE_LICITACION_IVA") or 0)
    if organo not in moype_agg:
        moype_agg[organo] = {"distrito": distrito, "organo": organo, "n_contratos": 0, "importe": 0}
    moype_agg[organo]["n_contratos"] += 1
    moype_agg[organo]["importe"] = round(moype_agg[organo]["importe"] + importe)

moype_distritos = sorted(moype_agg.values(), key=lambda x: x["importe"], reverse=True)
# Use canonical total from pipeline output (08_casos_narrativa.csv) to avoid IVA rounding drift
moype_total = 711115.41

# Serintcom: list sorted by date with threshold line
serintcom = []
for r in serintcom_rows:
    fecha = (r.get("FECHA_ADJUDICACION") or "").strip()
    importe_sin_iva = safe_float(r.get("IMPORTE_SIN_IVA") or 0)
    tipo = (r.get("TIPO") or "Servicios").strip()
    umbral = 40000 if tipo == "Obras" else 15000
    if fecha and importe_sin_iva > 0:
        serintcom.append({
            "fecha": fecha,
            "importe_sin_iva": round(importe_sin_iva, 2),
            "tipo": tipo,
            "umbral": umbral,
            "objeto": (r.get("OBJETO") or "")[:80],
            "supera_umbral": importe_sin_iva > umbral,
        })
serintcom.sort(key=lambda x: x["fecha"])

# Implaser: obras contracts only, sorted by date
implaser = []
for r in implaser_rows:
    fecha = (r.get("FECHA_ADJUDICACION") or "").strip()
    tipo = (r.get("TIPO") or "").strip()
    importe_sin_iva = safe_float(r.get("IMPORTE_SIN_IVA") or 0)
    importe_iva = safe_float(r.get("IMPORTE_IVA") or 0)
    if fecha and importe_sin_iva > 0 and tipo == "Obras":
        implaser.append({
            "fecha": fecha,
            "importe_sin_iva": round(importe_sin_iva, 2),
            "importe_iva": round(importe_iva, 2),
            "tipo": tipo,
            "umbral": 40000,
            "organo": (r.get("ORGANO") or "")[:60],
            "objeto": (r.get("OBJETO") or "")[:80],
        })
implaser.sort(key=lambda x: x["fecha"])

# ── ASSEMBLE ─────────────────────────────────────────────────────────────────

# Canonical case totals from 08_casos_narrativa.csv (authoritative pipeline output)
case_totals = {
    "gartner_total": 527650.75,
    "gartner_n": 4,
    "gartner_incremento_pct": 75.9,
    "moype_total": 711115.41,
    "moype_n": 91,
    "serintcom_total": 262701.05,
    "serintcom_n": 18,
}

data = {
    "stats": {**stats, "periodo_label": "2023–2025 (3 años)"},
    "case_totals": case_totals,
    "hhi_sector": hhi_sector,
    "hhi_organo_top20": hhi_organo_top20,
    "fraccionamiento_top30": frac_top30,
    "gartner": gartner,
    "moype_distritos": moype_distritos,
    "moype_total": moype_total,
    "serintcom": serintcom,
    "implaser": implaser,
}

out_path = WEB / "data.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

size_kb = out_path.stat().st_size / 1024
print(f"✓ web/data.json generated — {size_kb:.1f} KB")
print(f"  stats: {len(stats)} keys")
print(f"  hhi_sector: {len(hhi_sector)} rows")
print(f"  hhi_organo_top20: {len(hhi_organo_top20)} rows")
print(f"  fraccionamiento_top30: {len(frac_top30)} rows")
print(f"  gartner: {len(gartner)} contracts")
print(f"  moype_distritos: {len(moype_distritos)} districts | total €{moype_total:,}")
print(f"  serintcom: {len(serintcom)} contracts | supera umbral: {sum(1 for s in serintcom if s['supera_umbral'])}")
print(f"  implaser: {len(implaser)} obras contracts")
