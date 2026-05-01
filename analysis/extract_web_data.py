"""
Extract data from pipeline CSVs into web/data.json for web/index.html.
Run: python analysis/extract_web_data.py
Output: web/data.json (~40-60KB) + injects const DATA into index.html
"""

import json
import os
import re
import csv
from pathlib import Path

BASE = Path(__file__).parent.parent
OUTPUT = BASE / "output"
WEB = BASE / "docs" if (BASE / "docs").exists() else BASE / "web"
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

hhi_organo_full = sorted([
    {
        "organo":        r["ORGANO"],
        "hhi":           round(safe_float(r["HHI"]), 4),
        "n":             safe_int(r["N_CONTRATOS"]),
        "importe":       round(safe_float(r["IMPORTE_TOTAL"])),
        "n_proveedores": safe_int(r["N_PROVEEDORES"]),
    }
    for r in hhi_organo_raw
], key=lambda x: x["hhi"], reverse=True)

# ── ESCALADA DE PRECIOS ───────────────────────────────────────────────────────
# NIF+TIPO, avg contract value >20% YoY, both years avg >= €5k
_vendor_year: dict = {}
for _r in read_csv(OUTPUT / "01_madrid_limpio.csv"):
    _k = (_r.get("NIF", "").strip(), _r.get("TIPO", "").strip(), _r.get("RAZON_SOCIAL", "").strip())
    _ano = _r.get("AÑO", "").strip()
    _imp = safe_float(_r.get("IMPORTE_SIN_IVA", 0))
    if _k[0] and _k[1] and _ano:
        _vendor_year.setdefault(_k, {}).setdefault(_ano, []).append(_imp)

escalada = []
for (_nif, _tipo, _razon), _ydata in _vendor_year.items():
    _present = [y for y in ["2023", "2024", "2025"] if y in _ydata]
    if len(_present) < 2:
        continue
    for _i in range(len(_present) - 1):
        _y1, _y2 = _present[_i], _present[_i + 1]
        _avg1 = sum(_ydata[_y1]) / len(_ydata[_y1])
        _avg2 = sum(_ydata[_y2]) / len(_ydata[_y2])
        if _avg1 >= 5000 and _avg2 >= 5000:
            _pct = (_avg2 - _avg1) / _avg1 * 100
            if _pct > 20:
                escalada.append({
                    "nif":           _nif,
                    "razon":         _razon,
                    "tipo":          _tipo,
                    "ano_base":      _y1,
                    "ano_comp":      _y2,
                    "n_base":        len(_ydata[_y1]),
                    "n_comp":        len(_ydata[_y2]),
                    "total_base":    round(sum(_ydata[_y1])),
                    "total_comp":    round(sum(_ydata[_y2])),
                    "avg_base":      round(_avg1),
                    "avg_comp":      round(_avg2),
                    "variacion_pct": round(_pct, 1),
                })
escalada.sort(key=lambda x: x["variacion_pct"], reverse=True)

# ── FRACCIONAMIENTO: build group-date lookup for fecha_fin ───────────────────
# IDS_CONTRATOS are 0-based indices within each (NIF, TIPO) group sorted by date,
# matching pipeline.py's sort_values([NIF, TIPO, FECHA_ADJUDICACION]).
_madrid_sorted = sorted(
    read_csv(OUTPUT / "01_madrid_limpio.csv"),
    key=lambda r: (r.get("NIF", ""), r.get("TIPO", ""), r.get("FECHA_ADJUDICACION", "")),
)
from itertools import groupby as _igroup
_group_dates: dict = {}
for _key, _grp in _igroup(_madrid_sorted, key=lambda r: (r.get("NIF", ""), r.get("TIPO", ""))):
    _group_dates[_key] = [r.get("FECHA_ADJUDICACION", "") for r in _grp]

def _fecha_fin(nif: str, tipo: str, ids_str: str) -> str:
    ids = [int(x) for x in ids_str.split("|") if x.strip().isdigit()]
    dates = _group_dates.get((nif, tipo), [])
    window = [dates[i] for i in ids if i < len(dates)]
    return max(window) if window else ""

# ── FRACCIONAMIENTO TOP30 + FULL DATASET ─────────────────────────────────────

frac_raw = read_csv(OUTPUT / "07_fraccionamiento_madrid.csv")
frac_top30 = []
frac_all = []
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
    frac_all.append({
        "nif":      r.get("NIF", ""),
        "razon":    r.get("RAZON_SOCIAL", ""),
        "tipo":     r.get("TIPO", ""),
        "organo":   r.get("ORGANO", ""),
        "fecha":    r.get("FECHA_INICIO_VENTANA", ""),
        "fecha_fin": _fecha_fin(r.get("NIF", ""), r.get("TIPO", ""), r.get("IDS_CONTRATOS", "")),
        "n":        safe_int(r.get("N_CONTRATOS_VENTANA", 0)),
        "suma":     round(safe_float(r.get("SUMA_SIN_IVA", 0))),
        "umbral":   safe_int(r.get("UMBRAL_LCSP", 0)),
        "exceso":   round(safe_float(r.get("EXCESO", 0))),
        "ids":      r.get("IDS_CONTRATOS", ""),
    })
frac_top30.sort(key=lambda x: x["exceso"], reverse=True)
frac_top30 = frac_top30[:30]
frac_all.sort(key=lambda x: x["exceso"], reverse=True)

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
    "hhi_organo_full": hhi_organo_full,
    "fraccionamiento_top30": frac_top30,
    "fraccionamiento_all": frac_all,
    "escalada_precios": escalada,
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
print(f"✓ {WEB.name}/data.json generated — {size_kb:.1f} KB")
print(f"  stats: {len(stats)} keys")
print(f"  hhi_sector: {len(hhi_sector)} rows")
print(f"  hhi_organo_top20: {len(hhi_organo_top20)} rows")
print(f"  hhi_organo_full: {len(hhi_organo_full)} rows")
print(f"  fraccionamiento_top30: {len(frac_top30)} rows")
print(f"  fraccionamiento_all: {len(frac_all)} rows")
print(f"  escalada_precios: {len(escalada)} rows")
print(f"  gartner: {len(gartner)} contracts")
print(f"  moype_distritos: {len(moype_distritos)} districts | total €{moype_total:,}")
print(f"  serintcom: {len(serintcom)} contracts | supera umbral: {sum(1 for s in serintcom if s['supera_umbral'])}")
print(f"  implaser: {len(implaser)} obras contracts")

# ── INJECT const DATA into index.html ────────────────────────────────────────

index_path = WEB / "index.html"
if index_path.exists():
    html = index_path.read_text(encoding="utf-8")
    injected = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    html_new = re.sub(
        r'const DATA = \{.*?\};',
        f'const DATA = {injected};',
        html,
        flags=re.DOTALL,
    )
    if html_new != html:
        index_path.write_text(html_new, encoding="utf-8")
        print(f"✓ {index_path.name} DATA injected")
    else:
        print(f"⚠ {index_path.name}: const DATA pattern not found — skipped")
else:
    print(f"⚠ {index_path} not found — skipped injection")
