# Vigilante de Contratación Menor

Explorador comparativo de contratos menores de Madrid y Barcelona (2023–2025). Detecta concentración de proveedores (índice HHI) y posible fraccionamiento de contratos respecto a los umbrales de la Ley de Contratos del Sector Público (LCSP).

**Demo en vivo:** [https://gonxo.github.io/vigilante-contratos-menores/web/](https://gonxo.github.io/vigilante-contratos-menores/web/)

---

## Concurso

Proyecto presentado al **[Concurso de Reutilización de Datos Abiertos del Ayuntamiento de Madrid 2026](https://datos.madrid.es/)** — categoría: *Servicios web, aplicaciones y visualizaciones*.

---

## Cómo obtener el código

**Opción A — clonar con Git:**
```bash
git clone https://github.com/gonxo/vigilante-contratos-menores.git
cd vigilante-contratos-menores
```

**Opción B — descargar ZIP** (sin necesidad de Git):
1. Ir a [github.com/gonxo/vigilante-contratos-menores](https://github.com/gonxo/vigilante-contratos-menores)
2. Clic en **Code → Download ZIP**
3. Descomprimir y entrar en la carpeta

---

## Requisitos previos

- **Python 3.9+** (probado con 3.10 y 3.12)
- Conexión a internet (primera ejecución: descarga ~100 MB de las APIs)
- ~500 MB de espacio en disco para la caché y los CSVs intermedios

---

## Regenerar los datos

> **Primera ejecución:** descarga ~100 MB desde las APIs de Madrid y Barcelona; tarda ~5–10 min según la red. Las siguientes ejecuciones usan caché local en `data/` y tardan ~1–2 min.

```bash
# 1. Crear entorno virtual e instalar dependencias
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r analysis/requirements.txt

# 2. Ejecutar el pipeline ETL completo
python analysis/pipeline.py
# Crea data/ (caché) y output/ con 8 CSVs + RESUMEN.txt

# 3. Verificar que funcionó
cat output/RESUMEN.txt
# Debe mostrar ~15 000 contratos Madrid, ~62 000 Barcelona, ~866 alertas

# 4. Regenerar datos para la web
python analysis/extract_web_data.py
# Actualiza web/data.json

# 5. Abrir la visualización
open web/index.html          # macOS
xdg-open web/index.html      # Linux
start web/index.html         # Windows
# o arrastrar web/index.html a cualquier navegador moderno
```

> `data/` y `output/` están excluidos del repositorio (`.gitignore`). Solo `web/data.json` se incluye para que la visualización funcione sin ejecutar el pipeline.

---

## Solución de problemas

| Síntoma | Causa probable | Solución |
|---|---|---|
| `ConnectionError` o timeout en el paso 2 | API de Madrid/Barcelona caída o lenta | Volver a ejecutar; el script reanuda desde caché parcial |
| `ModuleNotFoundError: openpyxl` | Dependencia no instalada | `pip install -r analysis/requirements.txt` |
| `KeyError` en `extract_web_data.py` | `output/` incompleto | Volver a ejecutar `pipeline.py` desde cero |
| `web/data.json` con datos de fecha anterior | No se ejecutó el paso 4 | Ejecutar `python analysis/extract_web_data.py` |

---

## Datos utilizados

| Dataset | Ciudad | URL |
|---|---|---|
| Contratos actividad menores | Madrid | [datos.madrid.es](https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores) |
| Contractes menors | Barcelona | [opendata-ajuntament.barcelona.cat](https://opendata-ajuntament.barcelona.cat/data/es/dataset/contractes-menors) |

---

## Metodología

Se calcula el **Índice Herfindahl-Hirschman (HHI)** por órgano contratante y sector CPV para medir concentración de proveedores. Se aplica una heurística de **fraccionamiento** comparando el importe acumulado de contratos al mismo proveedor en ventanas de 30 días con los umbrales de la LCSP (€15.000 para servicios/suministros, €40.000 para obras, sin IVA).

---

## Estructura del repositorio

```
vigilante-contratos-menores/
├── analysis/
│   ├── pipeline.py           # ETL completo: descarga, limpieza, HHI, fraccionamiento → output/*.csv
│   ├── extract_web_data.py   # Transforma output/*.csv → web/data.json
│   └── requirements.txt      # Dependencias Python
├── memoria/
│   └── Memoria_Vigilante_Contratacion_Menor.docx
├── web/
│   ├── data.json             # Datos preprocesados (generado por extract_web_data.py)
│   └── index.html            # Visualización interactiva (autocontenida, sin servidor)
├── .gitignore
├── LICENSE
└── README.md
```

---

## Aviso legal

Este proyecto presenta **indicadores de riesgo** basados en métricas públicas (HHI, umbrales LCSP). No constituye una acusación de fraude. Todos los datos proceden de portales de datos abiertos oficiales. Los indicadores deben interpretarse como señales estadísticas que requieren análisis adicional por parte de las autoridades competentes.

---

## Licencia

[CC-BY-4.0](LICENSE) — Gonzalo Martín Roldán, 2026
