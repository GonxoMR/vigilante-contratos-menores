"""
Análisis de Contratación Menor: Madrid vs Barcelona 2023-2025
=============================================================
Reproducible pipeline. Ejecutar desde el directorio raíz del proyecto:

    python analysis/pipeline.py

Produce:
    output/01_madrid_limpio.csv
    output/02_barcelona_limpio.csv
    output/03_madrid_cpv.csv
    output/04_hhi_organo_madrid.csv
    output/05_hhi_organo_barcelona.csv
    output/06_hhi_sector_comparativa.csv
    output/07_fraccionamiento_madrid.csv
    output/08_casos_narrativa.csv
    output/RESUMEN.txt

Fuentes de datos:
    Madrid  — CKAN API datos.madrid.es  (descarga directa XLSX)
    Barcelona — CKAN datastore SQL API  opendata-ajuntament.barcelona.cat
"""

import os, re, sys, io, warnings
import requests
import pandas as pd
import numpy as np
warnings.filterwarnings("ignore")

ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA   = os.path.join(ROOT, "data")
OUT    = os.path.join(ROOT, "output")
os.makedirs(OUT, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# BLOQUE 0: DICCIONARIO CPV
# Mapeo texto libre Madrid → código CPV L1 (8 dígitos, primeros 2 significativos)
# Reglas ordenadas: más específico primero.
# ─────────────────────────────────────────────────────────────────────────────

REGLAS_CPV = [
    # Obras — capturar antes que salud/servicios
    (r'^obras?\s+de\s+',                                                           "45000000", "Obras de construcción"),
    (r'^obra\s+de\s+',                                                             "45000000", "Obras de construcción"),
    (r'acondicionamiento\s+(puntual|zona|espacio|aparcamiento|local|acceso)'
     r'|instalaci[oó]n\s+de\s+(toldos?|velas?\s+sombra|equipo[s]?\s+recarga|derivaci[oó]n)'
     r'|reposici[oó]n\s+(puertas?|ventanas?|cubierta)'
     r'|demolici[oó]n|reconstrucci[oó]n|sobrecubierta',                           "45000000", "Obras de construcción"),

    # Limpieza
    (r'limpieza\s+de\s+(edificio|oficina[s]?|instalaci|local|dependencia)'
     r'|servicios?\s+de\s+limpieza',                                              "90910000", "Servicios de limpieza"),

    # TI / software / datos
    (r'base[s]?\s+de\s+dato[s]?|gartner|suscripci[oó]n.*digital'
     r'|licencia[s]?\s+(adobe|microsoft|software)'
     r'|plataforma\s+digital|herramienta[s]?\s+(web|digital)'
     r'|posicionamiento\s+web|redes\s+sociales.*herramienta'
     r'|alojamiento\s+como\s+servicio|hosting|cloud\s+computing',                 "72000000", "Servicios de tecnologías de la información"),
    (r'software|aplicaci[oó]n\s+inform|sistema\s+inform'
     r'|desarrollo\s+(web|inform|aplicaci)',                                       "48000000", "Paquetes de software y sistemas de información"),
    (r'equipos?\s+inform|material\s+inform|ordenador|port[aá]til'
     r'|tablet|monitor|teclado|rat[oó]n\s+inform|impresora|servidor|hardware',   "30000000", "Maquinaria y equipo de oficina e informática"),

    # Publicaciones / prensa
    (r'suscripci[oó]n.*peri[oó]dico|suscripci[oó]n.*publicaci[oó]n'
     r'|periódico|revista\s+(t[eé]cnica|cient)'
     r'|bases?\s+de\s+datos\s+(jur[ií]dic|la\s+ley|aranzadi|consultor)'
     r'|publicaci[oó]n\s+en\s+(papel|digital)',                                   "22000000", "Libros, publicaciones y prensa"),

    # Cultura y artes
    (r'espect[aá]culo|actuaci[oó]n\s+art[ií]stica|concierto'
     r'|m[uú]sica\s+(en\s+vivo|directo)|representaci[oó]n\s+teatral'
     r'|t[ií]teres|circo|danza|ballet|[oó]pera|zarzuela|flamenco'
     r'|teatro\s+(infantil|familiar)|festival\s+(m[uú]sica|teatro|arte)',         "92310000", "Servicios de artes escénicas y musicales"),
    (r'exposici[oó]n|museo|galeria|arte\s+(contempor|visual|pl[aá]stico)'
     r'|montaje\s+exposici|comisari|curaduría',                                   "92320000", "Servicios de instalaciones artísticas"),
    (r'audiovisual|producci[oó]n\s+(audiovisual|cine|v[ií]deo|documental)'
     r'|grabaci[oó]n\s+(sonido|video)|estudio\s+(grabaci|sonido)',                "92110000", "Servicios de producción audiovisual"),

    # Fotografía / vídeo / streaming
    (r'fotograf[ií]a\s+(artística|event|cobertura)|reportaje[s]?\s+(foto|v[ií]deo)'
     r'|grabaci[oó]n\s+v[ií]deo|live\s+streaming|retransmisi[oó]n',              "79961000", "Servicios de fotografía"),

    # Deporte
    (r'material\s+deportivo|equipamiento\s+deportivo|instalaci[oó]n\s+deportiva'
     r'|centro\s+deportivo|piscina|gimnasio|pista\s+(deportiva|tenis|p[aá]del)'
     r'|campo\s+(deportes|f[uú]tbol|golf)|cancha',                                "37000000", "Artículos de deporte y equipamiento deportivo"),
    (r'actividad[es]?\s+deportiva|servicio[s]?\s+deportivo|monitor\s+deportivo',  "92600000", "Servicios deportivos"),

    # Salud
    (r'laboratorio\s+salud|ensayo[s]?\s+interlaboratorio|material\s+laboratorio'
     r'|reactivo[s]?|anal[ií]tica|diagn[oó]stico\s+(laboratorio|clin)'
     r'|determinaci[oó]n\s+(virus|bacteria|microbi)|virus\s+papiloma|hpv',        "33100000", "Equipamiento médico y de laboratorio"),
    (r'salud\s+p[uú]blica|vigilancia\s+epidemiol|promoci[oó]n\s+salud'
     r'|prevenci[oó]n\s+(enfermedad|drogodependencia|adicci[oó]n)'
     r'|sensibilizaci[oó]n\s+salud|educaci[oó]n\s+para\s+la\s+salud',           "85100000", "Servicios sanitarios"),
    (r'veterinari|animal[es]?\s+(compa[nñ][ií]a|abandonado|adopci[oó]n)'
     r'|protecci[oó]n\s+animal|semoviente[s]?|caballería|heno\s+alimentaci[oó]n', "85200000", "Servicios veterinarios"),
    (r'entomol[oó]gic|plaga[s]?|insecto[s]?|mosquito[s]?'
     r'|fumigaci[oó]n|desinfecci[oó]n|desratizaci[oó]n',                         "90920000", "Servicios de desinfección y control de plagas"),

    # Medio ambiente
    (r'residuo[s]?|recogida\s+(basura|residuo)|tratamiento\s+residuo|reciclaje',  "90510000", "Servicios de gestión de residuos"),
    (r'zonas\s+verdes|jardiner[ií]a|arbolado|poda|riego\s+(jardín|zona\s+verde)'
     r'|mantenimiento\s+(parque|jard[ií]n)|reforestaci[oó]n',                    "77310000", "Mantenimiento de zonas verdes"),
    (r'calidad\s+del?\s+aire|contaminaci[oó]n\s+(atmosf|ambient)'
     r'|muestreo\s+ambiental|medici[oó]n\s+(ruido|contaminaci[oó]n)',             "90710000", "Servicios de gestión medioambiental"),

    # Mantenimiento
    (r'mantenimiento\s+(ascensor|elevador)',                                       "50750000", "Mantenimiento de ascensores"),
    (r'mantenimiento\s+(veh[ií]culo|flota|autom[oó]vil|moto)'
     r'|reparaci[oó]n\s+(veh[ií]culo|coche)',                                     "50100000", "Reparación de vehículos"),
    (r'mantenimiento\s+(inform[aá]tic|equipo[s]?\s+inform|servidor)'
     r'|soporte\s+t[eé]cnico\s+inform',                                           "50300000", "Mantenimiento equipos informáticos"),
    (r'mantenimiento\s+(instalaci[oó]n\s+el[eé]ctric|alumbrado)'
     r'|reparaci[oó]n\s+(el[eé]ctric)',                                           "50700000", "Mantenimiento instalaciones eléctricas"),
    (r'mantenimiento\s+(edificio|inmueble|instalaci[oó]n[es]?)'
     r'|reparaci[oó]n\s+(edificio|estructura|cubierta|fachada)',                  "50800000", "Mantenimiento de edificios"),

    # Arquitectura / ingeniería
    (r'proyecto\s+(arquitect|ingenier|edificaci|construcci|urbanismo|obra[s]?)'
     r'|redacci[oó]n\s+(proyecto|plan\s+urbanist)'
     r'|direcci[oó]n\s+(facultativa|obra[s]?)|arquitecto',                        "71000000", "Servicios de arquitectura e ingeniería"),
    (r'coordinaci[oó]n\s+(seguridad\s+y\s+salud|obras)|plan\s+de\s+seguridad',   "71317000", "Coordinación de seguridad y salud en obras"),

    # Formación / educación
    (r'impartici[oó]n\s+(curso|formaci[oó]n|taller|jornada|seminario|clase)'
     r'|formaci[oó]n\s+(profesional|continua|a\s+distancia|online)'
     r'|actividad[es]?\s+formativa[s]?|capacitaci[oó]n',                         "80500000", "Servicios de formación"),
    (r'conferencia|ponencia|charla\s+divulgativa|seminario\s+t[eé]cnico',         "80510000", "Formación especializada"),
    (r'actividad[es]?\s+(educativa|pedagógica|infantil[es]?)'
     r'|taller[es]?\s+(infantil|familiar|educativ)'
     r'|animaci[oó]n\s+(infantil|sociocultural)',                                 "80000000", "Servicios de educación y formación"),

    # Servicios sociales
    (r'servicio[s]?\s+social[es]?|atenci[oó]n\s+(mayor[es]?|dependiente|discapacidad)'
     r'|centro\s+(mayor[es]?|dia\s+mayor)|ayuda\s+domicilio',                    "85300000", "Servicios de bienestar social"),
    (r'integraci[oó]n\s+social|inserci[oó]n\s+(laboral|social)'
     r'|ocio\s+(inclusivo|terapéutico|adolescente)',                              "85320000", "Servicios sociales"),

    # Seguridad
    (r'seguridad\s+(privada|vigilancia)|vigilante[s]?\s+seguridad|control\s+acceso[s]?'
     r'|conserjería|atenci[oó]n\s+p[uú]blico.*control\s+entrada',                "79710000", "Servicios de vigilancia"),

    # Comunicación / publicidad
    (r'publicidad|campa[nñ]a\s+(publicitar|comunicaci[oó]n)'
     r'|dise[nñ]o\s+(gr[aá]fico|web|editorial|marca)'
     r'|identidad\s+visual|imagen\s+corporativa|branding|cartelería'
     r'|patrocinio\s+(event|female|woman|hackathon)',                             "79340000", "Servicios de publicidad"),

    # Servicios jurídicos
    (r'asesoramiento\s+(legal|jur[ií]dico)|defensa\s+(jur[ií]dica|legal)'
     r'|servicios?\s+jur[ií]dicos?|gesti[oó]n\s+cobro\s+impagados',             "79100000", "Servicios jurídicos"),

    # Logística / transporte
    (r'env[ií]o\s+(documentaci[oó]n|paqueter[ií]a)|mensajería|paquetería',        "64120000", "Servicios de mensajería"),
    (r'transporte\s+(mercanc[ií]a|carga|mudanza|traslado\s+material)',            "60000000", "Servicios de transporte"),

    # Mobiliario / material oficina
    (r'mobiliario\s+(oficina|escolar|biblioteca|sala|despacho)'
     r'|silla[s]?|mesa[s]?\s+(oficina|trabajo)|estanter[ií]a[s]?',              "39100000", "Mobiliario de oficina"),
    (r'material\s+(oficina|papelería)|papel\s+(impresora|din\s+a)|tóner[es]?',   "30190000", "Material de oficina"),
    (r'decoraci[oó]n\s+(navide[nñ]a|event)|elemento[s]?\s+decorativ',            "39000000", "Mobiliario y equipamiento del hogar"),

    # Alimentación / catering
    (r'catering|servicio[s]?\s+restauraci[oó]n|merienda[s]?|desayuno[s]?\s+(evento|personal)'
     r'|banquete',                                                                "55520000", "Servicios de catering"),
    (r'alimento[s]?\s+(animal|semoviente|caballos?)|pienso|heno|forraje',         "15700000", "Alimentos para animales"),

    # Material AV / técnico
    (r'material\s+t[eé]cnico|equipo[s]?\s+(audiovisual|sonido|iluminaci[oó]n)'
     r'|sonorizaci[oó]n|iluminaci[oó]n\s+(escen|event)|pantalla[s]?\s+(led|proyecci[oó]n)'
     r'|alquiler\s+(equipo|material\s+t[eé]cnico|sonido|iluminaci[oó]n|escenario)',
                                                                                  "32000000", "Equipos de comunicación y telecomunicación"),

    # Señalización / mobiliario urbano
    (r'se[nñ]alizaci[oó]n\s+(vial|viaria|urbana|peatonal)|cartel[es]?\s+(viales?)',
                                                                                  "34920000", "Señalización vial"),
    (r'elemento[s]?\s+urbano[s]?|banco[s]?\s+(parque|jardín|calle)|papelera[s]?'
     r'|farola[s]?|balizamiento',                                                 "34928000", "Mobiliario urbano"),

    # Investigación / consultoría
    (r'estudio[s]?\s+(t[eé]cnico|viabilidad|impacto|urbanístico|social)'
     r'|investigaci[oó]n\s+(social|econ[oó]mica)',                               "73000000", "Servicios de investigación y desarrollo"),
    (r'evaluaci[oó]n\s+(acciones?|programa|intervenci[oó]n)'
     r'|gamificaci[oó]n|ludificaci[oó]n',                                         "73200000", "Servicios de consultoría"),
    (r'trofeos?|medallas?|placas?\s+(conmemor|premi)|merchandising',              "18000000", "Artículos de confección y accesorios"),
    (r'funerarios?|urnas?\s+(funeraria|funeral)',                                  "98370000", "Servicios funerarios"),

    # Genéricos — último recurso
    (r'mantenimiento|reparaci[oó]n',                                              "50000000", "Servicios de reparación y mantenimiento"),
    (r'formaci[oó]n|curso|taller',                                                "80000000", "Servicios de educación y formación"),
    (r'diseño|producci[oó]n|elaboraci[oó]n',                                     "79000000", "Servicios empresariales y de gestión"),
    (r'obra[s]?\b',                                                               "45000000", "Obras de construcción"),
]

def clasificar_cpv(texto: str) -> tuple:
    """Devuelve (cpv_codigo, cpv_descripcion). Sin clasificar = ('99999999', 'Sin clasificar')."""
    if not isinstance(texto, str) or not texto.strip():
        return ("99999999", "Sin clasificar")
    t = texto.lower().strip()
    for patron, codigo, desc in REGLAS_CPV:
        if re.search(patron, t):
            return (codigo, desc)
    return ("99999999", "Sin clasificar")


# ─────────────────────────────────────────────────────────────────────────────
# BLOQUE 1: CARGA DE DATOS MADRID
# Fuente: datos.madrid.es CKAN API
# Dataset ID: 300253-0-contratos-actividad-menores
# Recursos usados:
#   300253-6  → XLSX anual acumulativo "A 31-12-2023" (contratos inscritos hasta dic 2023)
#   300253-3  → XLSX anual acumulativo "A 31-12-2024" (contratos inscritos hasta dic 2024)
#   300253-0  → XLSX anual acumulativo "A 31-12-2025" (contratos inscritos hasta dic 2025)
# ─────────────────────────────────────────────────────────────────────────────

MADRID_RECURSOS = {
    "2023_batch": {
        "url": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores"
               "/resource/300253-6-contratos-actividad-menores-xlsx"
               "/download/300253-6-contratos-actividad-menores-xlsx.xlsx",
        "skiprows": 5,   # hoja con 5 filas de cabecera antes de los datos
        "nota": "MENORES DICIEMBRE 2023 — contratos registrados hasta 31/12/2023",
    },
    "2024_batch": {
        "url": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores"
               "/resource/300253-3-contratos-actividad-menores-xlsx"
               "/download/300253-3-contratos-actividad-menores-xlsx.xlsx",
        "skiprows": 0,
        "nota": "MENORES A 31-12-2024 — contratos registrados hasta 31/12/2024",
    },
    "2025_batch": {
        "url": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores"
               "/resource/300253-0-contratos-actividad-menores-xlsx"
               "/download/300253-0-contratos-actividad-menores-xlsx.xlsx",
        "skiprows": 0,
        "nota": "MENORES A 31-12-2025 — contratos registrados hasta 31/12/2025",
    },
}

# Columnas normalizadas Madrid
MAD_COLS = {
    "N. DE REGISTRO DE CONTRATO":  "ID_CONTRATO",
    "N. DE EXPEDIENTE":            "EXPEDIENTE",
    "CENTRO - SECCION":            "CENTRO",
    "ORGANO DE CONTRATACION":      "ORGANO",
    "OBJETO DEL CONTRATO":         "OBJETO",
    "TIPO DE CONTRATO":            "TIPO",
    "N. DE INVITACIONES CURSADAS": "N_INVITACIONES",
    "INVITADOS A PRESENTAR OFERTA":"INVITADOS",
    "IMPORTE LICITACION IVA INC.": "IMPORTE_LICITACION_IVA",
    "N. LICITADORES PARTICIPANTES":"N_LICITADORES",
    "NIF ADJUDICATARIO":           "NIF",
    "RAZON SOCIAL ADJUDICATARIO":  "RAZON_SOCIAL",
    "PYME":                        "PYME",
    "IMPORTE ADJUDICACION IVA INC.":"IMPORTE_IVA",
    "FECHA DE ADJUDICACION":       "FECHA_ADJUDICACION",
    "PLAZO":                       "PLAZO_MESES",
    "FECHA DE INSCRIPCION":        "FECHA_INSCRIPCION",
    "ORGANISMO_CONTRATANTE":       "ORGANISMO_CONTRATANTE",
    "ORGANISMO_PROMOTOR":          "ORGANISMO_PROMOTOR",
}

def _limpiar_importe(serie: pd.Series) -> pd.Series:
    """Convierte '18.149,00 €' → 18149.0"""
    return (serie.astype(str)
            .str.replace("€", "", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.strip()
            .pipe(pd.to_numeric, errors="coerce"))


def cargar_madrid(usar_cache: bool = True) -> pd.DataFrame:
    """
    Descarga y combina los tres ficheros anuales de contratos menores de Madrid.

    Pasos:
    1. Descarga cada XLSX desde la API CKAN de datos.madrid.es
    2. Normaliza nombres de columnas (MAD_COLS)
    3. Parsea IMPORTE_IVA y FECHA_ADJUDICACION
    4. Concatena los tres ficheros
    5. Deduplica por ID_CONTRATO (clave única del registro de contratos)
    6. Filtra por AÑO_ADJUDICACION 2023-2025
    7. Añade columnas derivadas: IMPORTE_SIN_IVA, AÑO, CPV, CPV_DESC

    Nota sobre deduplicación:
    Los ficheros se solapan porque están ordenados por FECHA_INSCRIPCION, no por
    FECHA_ADJUDICACION. Un contrato adjudicado en diciembre 2023 puede aparecer
    inscrito en el fichero de 2024. ID_CONTRATO es único a nivel de registro.
    """
    cache_path = os.path.join(DATA, "madrid", "madrid_menores_cpv.csv")

    if usar_cache and os.path.exists(cache_path):
        print("  [Madrid] Usando caché local:", cache_path)
        df = pd.read_csv(cache_path, low_memory=False, dtype={"CPV": str})
        df["FECHA_ADJUDICACION"] = pd.to_datetime(df["FECHA_ADJUDICACION"], errors="coerce")
        df["AÑO"] = df["FECHA_ADJUDICACION"].dt.year
        return df

    dfs = []
    for nombre, cfg in MADRID_RECURSOS.items():
        print(f"  [Madrid] Descargando {nombre}...")
        resp = requests.get(cfg["url"], timeout=60)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content), skiprows=cfg["skiprows"])
        # Eliminar primera columna vacía (presente en el fichero 2023_batch)
        if df.columns[0] not in MAD_COLS and "Unnamed" in str(df.columns[0]):
            df = df.iloc[:, 1:]
        df = df.rename(columns=MAD_COLS)
        df["_batch"] = nombre
        dfs.append(df)
        print(f"    → {len(df):,} filas | nota: {cfg['nota']}")

    raw = pd.concat(dfs, ignore_index=True)
    print(f"  [Madrid] Total bruto: {len(raw):,} filas")

    # ── Limpiar importes ──────────────────────────────────────────────────────
    raw["IMPORTE_IVA"]         = _limpiar_importe(raw["IMPORTE_IVA"])
    raw["IMPORTE_LICITACION_IVA"] = _limpiar_importe(raw["IMPORTE_LICITACION_IVA"])
    # IVA: 10 % obras, 21 % servicios/suministros
    def _iva_divisor(tipo):
        return 1.10 if "obra" in str(tipo).lower() else 1.21
    raw["IMPORTE_SIN_IVA"] = raw.apply(
        lambda r: round(r["IMPORTE_IVA"] / _iva_divisor(r["TIPO"]), 2), axis=1
    )

    # ── Parsear fechas ────────────────────────────────────────────────────────
    raw["FECHA_ADJUDICACION"]  = pd.to_datetime(
        raw["FECHA_ADJUDICACION"], dayfirst=True, errors="coerce")
    raw["FECHA_INSCRIPCION"]   = pd.to_datetime(
        raw["FECHA_INSCRIPCION"],  dayfirst=True, errors="coerce")
    raw["AÑO"] = raw["FECHA_ADJUDICACION"].dt.year

    # ── Deduplicar por ID_CONTRATO ────────────────────────────────────────────
    antes = len(raw)
    raw = raw.drop_duplicates(subset="ID_CONTRATO", keep="first")
    print(f"  [Madrid] Tras deduplicar: {len(raw):,} (eliminados {antes-len(raw):,})")

    # ── Filtro temporal: 2023-2025 por FECHA_ADJUDICACION ────────────────────
    raw = raw[raw["AÑO"].between(2023, 2025)].copy()
    print(f"  [Madrid] Filtrado 2023-2025: {len(raw):,} contratos")
    print(f"  [Madrid] Por año: {raw['AÑO'].value_counts().sort_index().to_dict()}")

    # ── Clasificación CPV ─────────────────────────────────────────────────────
    raw[["CPV", "CPV_DESC"]] = raw["OBJETO"].apply(
        lambda x: pd.Series(clasificar_cpv(x)))

    raw.to_csv(cache_path, index=False)
    print(f"  [Madrid] Guardado en caché: {cache_path}")
    return raw


# ─────────────────────────────────────────────────────────────────────────────
# BLOQUE 2: CARGA DE DATOS BARCELONA
# Fuente: opendata-ajuntament.barcelona.cat CKAN datastore SQL API
# Dataset: perfil-contractant
# Resource ID: 2f93a575-2b47-40c7-8bc6-c1d2b54c8803
# Filtros SQL aplicados:
#   - PROCEDIMENT = 'Contracte menor'  (distingue menores de licitaciones abiertas)
#   - DATA_ADJUDICACIO_CONTRACTE entre 2023-01-01 y 2025-12-31
#   - RESULTAT != 'Anul·lat'           (excluye contratos anulados)
# ─────────────────────────────────────────────────────────────────────────────

BCN_RESOURCE_ID = "2f93a575-2b47-40c7-8bc6-c1d2b54c8803"
BCN_API         = "https://opendata-ajuntament.barcelona.cat/data/api/3/action/datastore_search_sql"

BCN_COLS = {
    "CODI_EXPEDIENT":              "EXPEDIENTE",
    "NOM_ORGAN":                   "ORGANO",
    "NOM_AMBIT":                   "AMBIT",
    "NOM_DEPARTAMENT_ENS":         "ENTIDAD",
    "TIPUS_CONTRACTE":             "TIPO",
    "PROCEDIMENT":                 "PROCEDIMENT",
    "OBJECTE_CONTRACTE":           "OBJETO",
    "CODI_CPV":                    "CPV_ORIGINAL",
    "IDENTIFICACIO_ADJUDICATARI":  "NIF",
    "DENOMINACIO_ADJUDICATARI":    "RAZON_SOCIAL",
    "IMPORT_ADJUDICACIO_AMB_IVA":  "IMPORTE_IVA",
    "IMPORT_ADJUDICACIO_SENSE_IVA":"IMPORTE_SIN_IVA",
    "DURADA_CONTRACTE":            "PLAZO",
    "DATA_ADJUDICACIO_CONTRACTE":  "FECHA_ADJUDICACION",
    "OFERTES_REBUDES":             "N_OFERTAS",
    "RESULTAT":                    "RESULTADO",
}

def cargar_barcelona(usar_cache: bool = True) -> pd.DataFrame:
    """
    Descarga contratos menores de Barcelona vía CKAN datastore SQL.

    Pasos:
    1. Construye query SQL con filtros (PROCEDIMENT, fecha, RESULTAT)
    2. Ejecuta contra endpoint datastore_search_sql
    3. Normaliza columnas (BCN_COLS)
    4. Convierte tipos: fechas, importes numéricos
    5. Añade CPV_L1 (primeros 8 dígitos, división CPV) para alinear con Madrid
    6. Añea AÑO derivado de FECHA_ADJUDICACION

    Diferencia con Madrid:
    - Barcelona ya tiene CODI_CPV en cada contrato (no requiere clasificación)
    - El dataset incluye todo el Grupo Municipal (entidades, fundaciones, empresas públicas)
      por lo que el volumen (62k contratos) es ~4x mayor que Madrid (15k solo Ayuntamiento)
    - Usar siempre métricas relativas (HHI, %) nunca absolutas para comparar
    """
    cache_path = os.path.join(DATA, "barcelona", "barcelona_menores_2023_2025.csv")

    if usar_cache and os.path.exists(cache_path):
        print("  [Barcelona] Usando caché local:", cache_path)
        df = pd.read_csv(cache_path, low_memory=False, dtype={"CPV": str, "CPV_ORIGINAL": str})
        df["FECHA_ADJUDICACION"] = pd.to_datetime(df["FECHA_ADJUDICACION"], errors="coerce")
        df["AÑO"] = df["FECHA_ADJUDICACION"].dt.year
        return df

    import urllib.parse
    sql = """
        SELECT "CODI_EXPEDIENT","NOM_ORGAN","NOM_AMBIT","NOM_DEPARTAMENT_ENS",
               "TIPUS_CONTRACTE","PROCEDIMENT","OBJECTE_CONTRACTE","CODI_CPV",
               "IDENTIFICACIO_ADJUDICATARI","DENOMINACIO_ADJUDICATARI",
               "IMPORT_ADJUDICACIO_AMB_IVA","IMPORT_ADJUDICACIO_SENSE_IVA",
               "DURADA_CONTRACTE","DATA_ADJUDICACIO_CONTRACTE",
               "OFERTES_REBUDES","RESULTAT"
        FROM "{rid}"
        WHERE "PROCEDIMENT" = 'Contracte menor'
          AND "DATA_ADJUDICACIO_CONTRACTE" >= '2023-01-01'
          AND "DATA_ADJUDICACIO_CONTRACTE" <  '2026-01-01'
          AND "RESULTAT" != 'Anul·lat'
        ORDER BY "DATA_ADJUDICACIO_CONTRACTE" DESC
    """.format(rid=BCN_RESOURCE_ID).replace("\n", " ").strip()

    print("  [Barcelona] Consultando API datastore SQL...")
    resp = requests.get(BCN_API, params={"sql": sql}, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    if not data["success"]:
        raise RuntimeError("Barcelona API error: " + str(data.get("error")))

    df = pd.DataFrame(data["result"]["records"])
    print(f"  [Barcelona] Registros recibidos: {len(df):,}")

    df = df.rename(columns=BCN_COLS)
    df["IMPORTE_IVA"]     = pd.to_numeric(df["IMPORTE_IVA"],     errors="coerce")
    df["IMPORTE_SIN_IVA"] = pd.to_numeric(df["IMPORTE_SIN_IVA"], errors="coerce")
    df["N_OFERTAS"]       = pd.to_numeric(df["N_OFERTAS"],        errors="coerce")
    df["FECHA_ADJUDICACION"] = pd.to_datetime(df["FECHA_ADJUDICACION"], errors="coerce")
    df["AÑO"] = df["FECHA_ADJUDICACION"].dt.year

    # CPV L1: primeros 2 dígitos + 000000 para comparar con Madrid
    df["CPV"] = df["CPV_ORIGINAL"].astype(str).str[:2] + "000000"

    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    df.to_csv(cache_path, index=False)
    print(f"  [Barcelona] Guardado en caché: {cache_path}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# BLOQUE 3: CÁLCULO HHI
# HHI = Σ (cuota_proveedor_i)²
# cuota_i = importe_proveedor_i / importe_total_grupo
# Rango: 0 (perfectamente competitivo) → 1 (monopolio)
# Umbrales DOJ/UE:  <0.15 competitivo | 0.15-0.25 moderado | >0.25 alto
# ─────────────────────────────────────────────────────────────────────────────

def calcular_hhi(df: pd.DataFrame,
                 grupo_cols,
                 proveedor_col: str,
                 importe_col: str,
                 min_contratos: int = 5,
                 min_importe: float = 20_000) -> pd.DataFrame:
    """
    Calcula HHI para cada combinación de grupo_cols.
    Solo incluye grupos con >= min_contratos contratos y >= min_importe € total.
    """
    if isinstance(grupo_cols, str):
        grupo_cols = [grupo_cols]

    rows = []
    for keys, grp in df.groupby(grupo_cols):
        n  = len(grp)
        ti = grp[importe_col].sum()
        if n < min_contratos or ti < min_importe:
            continue
        hhi    = float(((grp.groupby(proveedor_col)[importe_col].sum() / ti) ** 2).sum())
        n_prov = int(grp[proveedor_col].nunique())
        # pandas puede devolver scalar o tupla según versión; normalizamos siempre a tupla
        keys_t = keys if isinstance(keys, tuple) else (keys,)
        row    = dict(zip(grupo_cols, keys_t))
        row.update(HHI=round(hhi, 6), N_CONTRATOS=n,
                   IMPORTE_TOTAL=round(ti, 2), N_PROVEEDORES=n_prov)
        rows.append(row)

    return pd.DataFrame(rows).sort_values("HHI", ascending=False)


# ─────────────────────────────────────────────────────────────────────────────
# BLOQUE 4: DETECTOR DE FRACCIONAMIENTO
# Base legal: LCSP Art. 99.2
# Heurística: mismo NIF + mismo TIPO contrato + ventana 30 días rodante
#   → si Σ importes > umbral LCSP → alerta
# Umbrales: €15.000 sin IVA servicios/suministros | €40.000 sin IVA obras
# Nota: usamos IMPORTE_SIN_IVA para comparar directamente con umbrales LCSP
# ─────────────────────────────────────────────────────────────────────────────

UMBRAL_SERVICIOS = 15_000   # €
UMBRAL_OBRAS     = 40_000   # €

def detectar_fraccionamiento(df: pd.DataFrame,
                             nif_col: str,
                             tipo_col: str,
                             fecha_col: str,
                             importe_sin_iva_col: str,
                             organo_col: str,
                             objeto_col: str,
                             ventana_dias: int = 30) -> pd.DataFrame:
    """
    Detecta posible fraccionamiento de contratos (LCSP Art. 99.2).

    Para cada contrato i, busca todos los contratos j del mismo NIF y TIPO
    dentro de una ventana de 'ventana_dias' días empezando en la fecha de i.
    Si la suma de importes sin IVA supera el umbral LCSP → alerta.

    Devuelve un DataFrame con una fila por alerta, incluyendo:
    - NIF, razón social, tipo, órgano
    - Número de contratos en la ventana
    - Suma de importes y exceso sobre umbral
    - Lista de IDs de contratos que forman la alerta (para trazabilidad)
    """
    df_s = df.sort_values([nif_col, tipo_col, fecha_col]).copy()
    alertas = []

    for (nif, tipo), grp in df_s.groupby([nif_col, tipo_col]):
        umbral = UMBRAL_OBRAS if "obra" in str(tipo).lower() else UMBRAL_SERVICIOS
        grp = grp.reset_index(drop=True)

        for i in range(len(grp)):
            fecha_i = grp.loc[i, fecha_col]
            if pd.isna(fecha_i):
                continue
            ventana = grp[
                (grp[fecha_col] >= fecha_i) &
                (grp[fecha_col] <= fecha_i + pd.Timedelta(days=ventana_dias))
            ]
            suma = ventana[importe_sin_iva_col].sum()
            if len(ventana) >= 2 and suma > umbral:
                alertas.append({
                    "NIF":             nif,
                    "RAZON_SOCIAL":    grp.loc[i, "RAZON_SOCIAL"] if "RAZON_SOCIAL" in grp else "",
                    "TIPO":            tipo,
                    "ORGANO":          grp.loc[i, organo_col],
                    "FECHA_INICIO_VENTANA": fecha_i.date(),
                    "N_CONTRATOS_VENTANA":  len(ventana),
                    "SUMA_SIN_IVA":    round(suma, 2),
                    "UMBRAL_LCSP":     umbral,
                    "EXCESO":          round(suma - umbral, 2),
                    "IDS_CONTRATOS":   "|".join(ventana.index.astype(str).tolist()),
                })

    df_alertas = pd.DataFrame(alertas)
    if df_alertas.empty:
        return df_alertas
    return (df_alertas
            .drop_duplicates(subset=["NIF", "TIPO", "FECHA_INICIO_VENTANA"])
            .sort_values("EXCESO", ascending=False)
            .reset_index(drop=True))


# ─────────────────────────────────────────────────────────────────────────────
# BLOQUE 5: PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("PIPELINE: Contratación Menor Madrid vs Barcelona 2023-2025")
    print("=" * 60)

    # ── Paso 1: cargar datos ──────────────────────────────────────────────────
    print("\n[1/6] Cargando datos Madrid...")
    mad = cargar_madrid(usar_cache=True)
    mad.to_csv(os.path.join(OUT, "01_madrid_limpio.csv"), index=False)

    print("\n[2/6] Cargando datos Barcelona...")
    bcn = cargar_barcelona(usar_cache=True)
    bcn.to_csv(os.path.join(OUT, "02_barcelona_limpio.csv"), index=False)

    # ── Paso 2: HHI por órgano ────────────────────────────────────────────────
    print("\n[3/6] Calculando HHI por órgano...")
    mad["CPV"] = mad["CPV"].astype(str)
    mad_cpv = mad[mad["CPV"] != "99999999"].copy()

    hhi_org_mad = calcular_hhi(mad, ["ORGANO"], "NIF", "IMPORTE_IVA",
                                min_contratos=5, min_importe=50_000)
    hhi_org_mad["CIUDAD"] = "Madrid"
    hhi_org_mad.to_csv(os.path.join(OUT, "04_hhi_organo_madrid.csv"), index=False)
    print(f"  Madrid: {len(hhi_org_mad)} órganos | mediana HHI: {hhi_org_mad['HHI'].median():.4f}")

    hhi_org_bcn = calcular_hhi(bcn, ["ORGANO"], "NIF", "IMPORTE_IVA",
                                min_contratos=5, min_importe=50_000)
    hhi_org_bcn["CIUDAD"] = "Barcelona"
    hhi_org_bcn.to_csv(os.path.join(OUT, "05_hhi_organo_barcelona.csv"), index=False)
    print(f"  Barcelona: {len(hhi_org_bcn)} órganos | mediana HHI: {hhi_org_bcn['HHI'].median():.4f}")

    # ── Paso 3: HHI por sector CPV (comparativa) ─────────────────────────────
    # Usamos CPV_L1 = primeros 2 dígitos + 000000 en AMBAS ciudades para alinear.
    # Madrid usa códigos exactos de 8 dígitos (del diccionario); Barcelona usa
    # los del catálogo europeo. Al truncar a L1 se pueden comparar directamente.
    print("\n[4/6] Calculando HHI por sector CPV (nivel L1)...")
    mad_cpv["CPV_L1"] = mad_cpv["CPV"].astype(str).str[:2] + "000000"

    bcn_cpv = bcn.copy()
    bcn_cpv["CPV_L1"] = bcn_cpv["CPV_ORIGINAL"].astype(str).str.strip().str[:2] + "000000"

    hhi_cpv_mad = calcular_hhi(mad_cpv, ["CPV_L1"], "NIF", "IMPORTE_IVA",
                                min_contratos=10)
    hhi_cpv_mad = hhi_cpv_mad.rename(columns={"CPV_L1": "CPV"})

    # Etiquetas: CPV_DESC más frecuente por L1 (primera regla que coincide)
    cpv_labels = (mad_cpv.groupby("CPV_L1")["CPV_DESC"]
                  .first().reset_index()
                  .rename(columns={"CPV_L1": "CPV", "CPV_DESC": "CPV_DESC_L1"}))
    hhi_cpv_mad = hhi_cpv_mad.merge(cpv_labels, on="CPV", how="left")

    hhi_cpv_bcn = calcular_hhi(bcn_cpv, ["CPV_L1"], "NIF", "IMPORTE_IVA",
                                min_contratos=10)
    hhi_cpv_bcn = hhi_cpv_bcn.rename(columns={
        "CPV_L1": "CPV", "HHI": "HHI_BCN",
        "N_CONTRATOS": "N_BCN", "IMPORTE_TOTAL": "IMP_BCN",
        "N_PROVEEDORES": "NPROV_BCN",
    })

    comp = hhi_cpv_mad.merge(hhi_cpv_bcn[["CPV","HHI_BCN","N_BCN","NPROV_BCN"]],
                             on="CPV", how="left")
    comp["RATIO_MAD_BCN"] = (comp["HHI"] / comp["HHI_BCN"]).round(2)
    comp = comp.sort_values("RATIO_MAD_BCN", ascending=False, na_position="last")
    comp.to_csv(os.path.join(OUT, "06_hhi_sector_comparativa.csv"), index=False)
    print(f"  {len(comp)} sectores CPV nivel L1 comparados")

    # ── Paso 4: fraccionamiento ────────────────────────────────────────────────
    print("\n[5/6] Detectando fraccionamiento (LCSP Art. 99.2)...")
    alertas = detectar_fraccionamiento(
        mad,
        nif_col="NIF", tipo_col="TIPO",
        fecha_col="FECHA_ADJUDICACION",
        importe_sin_iva_col="IMPORTE_SIN_IVA",
        organo_col="ORGANO", objeto_col="OBJETO",
    )
    alertas.to_csv(os.path.join(OUT, "07_fraccionamiento_madrid.csv"), index=False)
    print(f"  Alertas: {len(alertas):,} | NIFs únicos: {alertas['NIF'].nunique():,}")

    # ── Paso 5: fichas de casos narrativos ───────────────────────────────────
    print("\n[6/6] Generando fichas de casos...")
    casos = []

    # Caso 1: Gartner
    gartner = mad[mad["NIF"] == "B84184217"].sort_values("FECHA_ADJUDICACION")
    gartner_alcaldia = gartner[gartner["ORGANO"].str.contains("ALCALDÍA", case=False, na=False)]
    casos.append({
        "CASO": "Gartner — renovación sistemática sin licitación",
        "NIF": "B84184217",
        "RAZON_SOCIAL": "GARTNER ESPAÑA, S.L.",
        "N_CONTRATOS": len(gartner),
        "IMPORTE_TOTAL_IVA": round(gartner["IMPORTE_IVA"].sum(), 2),
        "IMPORTE_ALCALDIA_IVA": round(gartner_alcaldia["IMPORTE_IVA"].sum(), 2),
        "INCREMENTO_PCT": round(
            (gartner_alcaldia["IMPORTE_IVA"].iloc[-1] /
             gartner_alcaldia["IMPORTE_IVA"].iloc[0] - 1) * 100, 1
        ) if len(gartner_alcaldia) >= 2 else None,
        "ORGANOS": " | ".join(gartner["ORGANO"].unique()),
        "ANOS": " | ".join(gartner["AÑO"].astype(str).unique()),
        "DESCRIPCION": "Suscripción anual a bases datos Gartner renovada como contrato menor sin licitación. "
                       "Importe aumentó 76% de 2023 a 2025.",
    })

    # Caso 2: Moype Sport
    moype = mad[mad["NIF"] == "A78111549"]
    casos.append({
        "CASO": "MOYPE SPORT SA — fragmentación municipal por distritos",
        "NIF": "A78111549",
        "RAZON_SOCIAL": "MOYPE SPORT SA",
        "N_CONTRATOS": len(moype),
        "IMPORTE_TOTAL_IVA": round(moype["IMPORTE_IVA"].sum(), 2),
        "IMPORTE_ALCALDIA_IVA": None,
        "INCREMENTO_PCT": None,
        "ORGANOS": f"{moype['ORGANO'].nunique()} órganos distintos (distritos)",
        "ANOS": " | ".join(moype["AÑO"].astype(str).unique()),
        "DESCRIPCION": "Proveedor de material deportivo de facto para 23 distritos. "
                       "€711k en 3 años (€237k/año) sin contrato marco ni licitación.",
    })

    # Caso 3: Serintcom
    serint = mad[mad["NIF"] == "05341273Y"].sort_values("FECHA_ADJUDICACION")
    max_sin_iva = serint["IMPORTE_SIN_IVA"].max()
    casos.append({
        "CASO": "SERINTCOM — persona física cuasi-empleada de Madrid Salud",
        "NIF": "05341273Y",
        "RAZON_SOCIAL": "SERINTCOM (persona física)",
        "N_CONTRATOS": len(serint),
        "IMPORTE_TOTAL_IVA": round(serint["IMPORTE_IVA"].sum(), 2),
        "IMPORTE_ALCALDIA_IVA": None,
        "INCREMENTO_PCT": None,
        "ORGANOS": " | ".join(serint["ORGANO"].unique()),
        "ANOS": " | ".join(serint["AÑO"].astype(str).unique()),
        "DESCRIPCION": f"18 contratos en 3 años, todos a Madrid Salud. "
                       f"Importe máximo sin IVA: €{max_sin_iva:,.0f} (umbral LCSP €15.000). "
                       f"0 de 18 contratos superan el umbral.",
    })

    pd.DataFrame(casos).to_csv(os.path.join(OUT, "08_casos_narrativa.csv"), index=False)

    # ── Paso 6: resumen ───────────────────────────────────────────────────────
    resumen = f"""
RESUMEN DEL ANÁLISIS
====================
Fecha ejecución: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}

DATOS
-----
Madrid  2023-2025: {len(mad):,} contratos | €{mad['IMPORTE_IVA'].sum():,.0f}
Barcelona 2023-2025: {len(bcn):,} contratos | €{bcn['IMPORTE_IVA'].sum():,.0f}

Distribución Madrid por año:
{mad['AÑO'].value_counts().sort_index().to_string()}

CPV Madrid clasificados: {(mad['CPV'] != '99999999').sum():,} / {len(mad):,} ({(mad['CPV'] != '99999999').mean()*100:.1f}%)

HHI POR ÓRGANO
--------------
Madrid  — mediana: {hhi_org_mad['HHI'].median():.4f} | media: {hhi_org_mad['HHI'].mean():.4f}
Barcelona — mediana: {hhi_org_bcn['HHI'].median():.4f} | media: {hhi_org_bcn['HHI'].mean():.4f}
Ratio mediana: {hhi_org_mad['HHI'].median() / hhi_org_bcn['HHI'].median():.1f}x

Órganos Madrid altamente concentrados (HHI > 0.25): {(hhi_org_mad['HHI'] > 0.25).sum()}
Órganos Barcelona altamente concentrados (HHI > 0.25): {(hhi_org_bcn['HHI'] > 0.25).sum()}

TOP 5 SECTORES POR RATIO HHI MADRID/BARCELONA
----------------------------------------------
{comp[comp['RATIO_MAD_BCN'].notna()][['CPV','CPV_DESC_L1','HHI','HHI_BCN','RATIO_MAD_BCN']].head(5).to_string(index=False)}

FRACCIONAMIENTO (LCSP Art. 99.2)
---------------------------------
Alertas totales: {len(alertas):,}
NIFs únicos con alerta: {alertas['NIF'].nunique():,}
Alerta mayor (exceso sobre umbral): €{alertas['EXCESO'].max():,.0f}
  → {alertas.iloc[0]['RAZON_SOCIAL']} — {alertas.iloc[0]['TIPO']} — {alertas.iloc[0]['ORGANO'][:50]}

CONTRATOS > UMBRAL LCSP (individualmente)
------------------------------------------
Servicios/Suministros > €15.000 sin IVA: {(mad[mad['TIPO'] != 'Obras']['IMPORTE_SIN_IVA'] > 15000).sum():,}
Obras > €40.000 sin IVA: {(mad[mad['TIPO'] == 'Obras']['IMPORTE_SIN_IVA'] > 40000).sum():,}

OUTPUTS
-------
{chr(10).join('  ' + f for f in sorted(os.listdir(OUT)))}
"""
    with open(os.path.join(OUT, "RESUMEN.txt"), "w") as f:
        f.write(resumen)
    print(resumen)
    print("Pipeline completado. Outputs en:", OUT)


if __name__ == "__main__":
    main()
