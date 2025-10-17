# -*- coding: utf-8 -*-
"""
ANEXO II (Grado B: Certificados de competencia)
- Por familia, detecta bloques de Certificados de Competencia incluidos en los Certificados Profesionales (C padres).
- Dentro de cada C padre (p.ej. ADG_C_002_3B. ...), parsea la tabla:
    'Formación a cursar' | 'Certificado de Competencia en' | 'Duración ... MEFD'
- Extrae y normaliza:
    * CERT_PADRE_COD / CERT_PADRE_DENOM / CERT_PADRE_COMPLETO (el C padre)
    * FORMACION_CODIGO / FORMACION_TITULO (3001, 3002, ...)
    * COD_CERT_COMP / NOM_CERT_COMP (p.ej. ADG_B_3001. Denominación)
    * DURACION_MEFD_H (número)
- Heurísticas de rescate para rellenar códigos/títulos.

Salida:
- Un Excel por BOE (hojas por familia).
- Un consolidado: RDs_GradoB_Consolidado_por_familia.xlsx
"""

import re
import sys
import html
import logging
import chardet
import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from collections import defaultdict

# ========= CONFIG =========
BOE_URLS = [
    "https://www.boe.es/diario_boe/txt.php?id=BOE-A-2025-7040",
    "https://www.boe.es/eli/es/rd/2025/03/18/209",
    "https://www.boe.es/eli/es/rd/2025/03/18/210",
    "https://www.boe.es/diario_boe/txt.php?id=BOE-A-2025-7096",
    "https://www.boe.es/eli/es/rd/2025/03/18/212",
    "https://www.boe.es/eli/es/rd/2025/03/18/213",
    "https://www.boe.es/diario_boe/txt.php?id=BOE-A-2025-6797",
]

SALIDA_CONSOLIDADO = "RDs_GradoB_Consolidado_por_familia.xlsx"
CARPETA_POR_BOE = Path("salidas_por_boe_B"); CARPETA_POR_BOE.mkdir(exist_ok=True)
DEBUG_DIR = Path("debug_b_grado"); DEBUG_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# ========= AUX/UTIL =========
RE_COD_B = re.compile(r"\b([A-Z]{3}_B_\d{4})\b")
RE_COD_C = re.compile(r"\b([A-Z]{3}_C_\d{3,4}_[0-9]B)\b")
RE_FORM  = re.compile(r"^\s*(\d{3,4})[.\-: ]+\s*(.+?)\s*$")

def norm(s: str) -> str:
    if s is None:
        return ""
    s = html.unescape(s)
    s = (s.replace("\xa0", " ").replace("\u2002", " ").replace("\u2003", " ").replace(" ", " ")
           .replace("–", "-").replace("—", "-"))
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"[ \t]*\n[ \t]*", "\n", s)
    return s.strip()

def to_int_or_blank(x: str):
    if x is None or str(x).strip() == "":
        return ""
    m = re.search(r"\d+", str(x))
    return int(m.group(0)) if m else ""

def split_code_title_from_dot(txt: str, kind="B"):
    """Divide 'COD. Título' -> (COD, Título) para B o C."""
    if not txt: return "", ""
    txt = txt.strip().strip(".")
    if kind == "B":
        m = re.match(r"^\s*([A-Z]{3}_B_\d{4})\s*\.?\s*(.*)$", txt)
    else:
        m = re.match(r"^\s*([A-Z]{3}_C_\d{3,4}_[0-9]B)\s*\.?\s*(.*)$", txt)
    if m:
        return m.group(1).strip(), m.group(2).strip().strip(".")
    return "", ""

def extract_b_from_text(txt: str):
    """Devuelve (cod_b, nom_b) desde 'ADG_B_3002. Denominación' o solo código si no hay título."""
    c_b, n_b = split_code_title_from_dot(txt, kind="B")
    if c_b: return c_b, n_b
    m = RE_COD_B.search(txt or "")
    return (m.group(1), "") if m else ("","")

def parse_formacion_line(txt: str):
    """Devuelve (FORMACION_CODIGO, FORMACION_TITULO) para '3001. Título...'."""
    if not txt: return "", ""
    m = RE_FORM.match(txt)
    if m: return m.group(1).strip(), m.group(2).strip().strip(".")
    return "", ""

def sanitize_row_B(row: dict) -> dict:
    """Rellenos heurísticos para B."""
    # COD/NOM B desde "Certificado de Competencia en"
    if (not row.get("COD_CERT_COMP")) or (not row.get("NOM_CERT_COMP")):
        c_b, n_b = extract_b_from_text(row.get("Certificado de Competencia en",""))
        row["COD_CERT_COMP"] = row.get("COD_CERT_COMP") or c_b
        row["NOM_CERT_COMP"] = row.get("NOM_CERT_COMP") or n_b

    # Formación a cursar -> código/título
    if (not row.get("FORMACION_CODIGO")) or (not row.get("FORMACION_TITULO")):
        f_cod, f_nom = parse_formacion_line(row.get("Formación a cursar",""))
        row["FORMACION_CODIGO"] = row.get("FORMACION_CODIGO") or f_cod
        row["FORMACION_TITULO"] = row.get("FORMACION_TITULO") or f_nom

    # Duración MEFD -> número
    if isinstance(row.get("DURACION_MEFD_H"), str):
        m = re.search(r"\d+", row["DURACION_MEFD_H"])
        row["DURACION_MEFD_H"] = int(m.group(0)) if m else ""

    # C padre desde CERT_PADRE_COMPLETO si faltara
    if (not row.get("CERT_PADRE_COD")) or (not row.get("CERT_PADRE_DENOM")):
        c_c, n_c = split_code_title_from_dot(row.get("CERT_PADRE_COMPLETO",""), kind="C")
        if not c_c:
            m = RE_COD_C.search(row.get("CERT_PADRE_COMPLETO",""))
            c_c = m.group(1) if m else ""
        row["CERT_PADRE_COD"] = row.get("CERT_PADRE_COD") or c_c
        row["CERT_PADRE_DENOM"] = row.get("CERT_PADRE_DENOM") or n_c
    return row

def get_html(url: str) -> str:
    logging.info(f"Descargando {url}")
    hdrs = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Python scraper"}
    r = requests.get(url, headers=hdrs, timeout=60); r.raise_for_status()
    raw = r.content
    enc = r.encoding or chardet.detect(raw)["encoding"] or "utf-8"
    try:    return raw.decode(enc, errors="replace")
    except: return raw.decode("utf-8", errors="replace")

def extract_main_text(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "lxml")
    main = soup.find(id="texto") or soup.find("div", {"class":"texto"}) or soup.body or soup
    for tag in main.find_all(["script","style"]): tag.decompose()
    txt = norm(main.get_text("\n"))
    try: (DEBUG_DIR/"boe_texto_principal.txt").write_text(txt, encoding="utf-8")
    except: pass
    return txt

def parse_rd_header(full_text: str):
    m_num = re.search(r"Real\s+Decreto\s+(\d+/\d{4})", full_text, re.I)
    rd_num = m_num.group(1) if m_num else ""
    meses = {"enero":"01","febrero":"02","marzo":"03","abril":"04","mayo":"05","junio":"06",
             "julio":"07","agosto":"08","septiembre":"09","setiembre":"09","octubre":"10",
             "noviembre":"11","diciembre":"12"}
    m_fecha = re.search(r"de\s+(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})", full_text, re.I)
    fecha = ""
    if m_fecha:
        d = int(m_fecha.group(1)); m = meses.get(m_fecha.group(2).lower(),"01"); y=int(m_fecha.group(3))
        fecha = f"{y:04d}-{m}-{d:02d}"
    return rd_num, fecha

def get_boe_id_from_url(url: str) -> str:
    m = re.search(r"id=(BOE-[A-Z]-\d{4}-\d+)", url)
    return m.group(1) if m else ""

# ========= RECORTE ANEXO II =========
def slice_annex_II_gradeB(full_text: str) -> str:
    # Muy tolerante a variantes (con/sin "Oferta de")
    m_start = re.search(
        r"ANEXO\s*II\s*\n+.*?Oferta\s+(?:de\s+)?Grado\s*B\s*:\s*Certificados\s+de\s+competencia",
        full_text, re.I | re.DOTALL
    )
    if not m_start:
        m_start = re.search(
            r"Oferta\s+(?:de\s+)?Grado\s*B\s*:\s*Certificados\s+de\s+competencia",
            full_text, re.I
        )
    if not m_start:
        raise RuntimeError("No se localiza el encabezado del ANEXO II (Grado B).")
    start = m_start.start()
    m_end = re.search(r"^\s*ANEXO\s*[IVXLC]+\b", full_text[m_start.end():], re.I|re.M)
    end = m_start.end()+m_end.start() if m_end else len(full_text)
    annex = full_text[start:end].strip()
    try: (DEBUG_DIR/"anexo_II_gradeB.txt").write_text(annex, encoding="utf-8")
    except: pass
    return annex

# ========= FAMILIAS =========
def split_by_family(annex_text: str) -> dict:
    pat = re.compile(
        r"(?:^|\n)\s*(?:\d+\.\s*)?CERTIFICADOS?\s+DE\s+COMPETENCIA\s+DE\s+LA\s+FAMILIA\s+PROFESIONAL\s+([^\n:]+?)\s*:?\s*$",
        re.I | re.M
    )
    matches = list(pat.finditer(annex_text))
    if not matches:
        (DEBUG_DIR/"no_family_headers.txt").write_text(annex_text[:20000], encoding="utf-8")
        raise RuntimeError("No se han encontrado encabezados de familia en el ANEXO II (Grado B).")
    blocks={}
    for i,m in enumerate(matches):
        fam = norm(m.group(1)).strip(" .:")
        start=m.end(); end=matches[i+1].start() if i+1<len(matches) else len(annex_text)
        chunk = annex_text[start:end].strip()
        blocks[fam]=chunk
        try: (DEBUG_DIR/f"familia_{fam.replace('/','-')}.txt").write_text(chunk, encoding="utf-8")
        except: pass
    return blocks

# ========= BLOQUES POR C PADRE =========
def split_blocks_by_parentC(family_block: str):
    """
    Tras el texto 'CERTIFICADOS DE COMPETENCIA INCLUIDOS EN LOS CERTIFICADOS PROFESIONALES',
    aparecen entradas tipo:
      a) ADG_C_002_3B. Denominación...
    Cortamos desde cada una hasta la siguiente.
    """
    # posicionar tras la frase introductoria (si existe)
    intro = re.search(
        r"CERTIFICADOS?\s+DE\s+COMPETENCIA\s+INCLUIDOS?\s+EN\s+LOS\s+CERTIFICADOS?\s+PROFESIONALES",
        family_block, re.I
    )
    start_from = intro.end() if intro else 0
    block_text = family_block[start_from:]

    pat_head = re.compile(r"(?:^|\n)\s*(?:[a-z]\)\s*)?([A-Z]{3}_C_\d{3,4}_[0-9]B)\s*\.\s*(.+)", re.I)
    heads = list(pat_head.finditer(block_text))
    out=[]
    for i,h in enumerate(heads):
        codC = h.group(1).strip()
        nomC = norm(h.group(2)).split("\n",1)[0].strip().rstrip(" .")
        s = h.end()
        e = heads[i+1].start() if i+1 < len(heads) else len(block_text)
        sub = block_text[s:e].strip()
        try: (DEBUG_DIR/f"C_padre_{codC}.txt").write_text(sub, encoding="utf-8")
        except: pass
        out.append((codC, nomC, sub))
    return out

# ========= PATRONES FILAS TABLA B =========
FORM_LINE   = re.compile(r"^\s*(\d{3,4})[.\-: ]+\s*(.+?)\s*$")
CERTB_LINE1 = re.compile(r"^\s*Certificado\s+de\s+Competencia\s+en\s*:?\s*$", re.I)
CERTB_LINE2 = re.compile(r"^\s*([A-Z]{3}_B_\d{4})\s*\.?\s*(.+?)\s*$")
DUR_LINE    = re.compile(r"^\s*(\d+)\s*(?:h|horas?)?\s*$", re.I)

def parse_table_B(subblock: str):
    """
    Extrae filas de 3 columnas (pueden venir en líneas separadas):
      - Formación a cursar (3001. Título)
      - Certificado de Competencia en (ADG_B_3001. Denominación)
      - Duración ... (número)
    """
    lines = [norm(l) for l in subblock.split("\n")]

    def is_header(l):
        return (re.search(r"Formaci[oó]n\s+a\s+cursar", l, re.I) or
                re.search(r"Certificado\s+de\s+Competencia\s+en", l, re.I) or
                re.search(r"Duraci[oó]n.+MEFD", l, re.I))

    rows=[]
    state="FORM"  # FORM -> CERTB_WAIT or CERTB -> DUR
    form_code=form_title=cert_b_code=cert_b_title=dur=""

    i=0
    while i < len(lines):
        line = lines[i].strip(); i += 1
        if not line or is_header(line):
            continue

        if state == "FORM":
            m = FORM_LINE.match(line)
            if m:
                form_code, form_title = m.group(1), m.group(2).strip().rstrip(".")
                state = "CERTB_WAIT"
                continue
            # ruido
            continue

        if state == "CERTB_WAIT":
            # puede venir primero la línea 'Certificado de Competencia en'
            if CERTB_LINE1.match(line):
                # siguiente línea debería tener el código y título
                if i < len(lines) and CERTB_LINE2.match(lines[i]):
                    m2 = CERTB_LINE2.match(lines[i]); i += 1
                    cert_b_code, cert_b_title = m2.group(1), m2.group(2).strip().rstrip(".")
                    state = "DUR"
                    continue
                else:
                    # si no, intenta extraer en la misma línea (por si viniera todo junto)
                    m2 = CERTB_LINE2.match(line)
                    if m2:
                        cert_b_code, cert_b_title = m2.group(1), m2.group(2).strip().rstrip(".")
                        state = "DUR"
                        continue
                    else:
                        # queda a la espera hasta que aparezca la línea con el B
                        continue
            # o puede venir directamente 'ADG_B_3001. ...'
            m2 = CERTB_LINE2.match(line)
            if m2:
                cert_b_code, cert_b_title = m2.group(1), m2.group(2).strip().rstrip(".")
                state = "DUR"; continue
            # ruido
            continue

        if state == "DUR":
            m = DUR_LINE.match(line)
            if m:
                dur = m.group(1)
                rows.append((form_code, form_title, cert_b_code, cert_b_title, dur))
                # reset
                form_code=form_title=cert_b_code=cert_b_title=dur=""
                state="FORM"
                continue
            # Si aparece otra formación sin haber visto duración, cerramos la fila con dur vacío
            if FORM_LINE.match(line):
                rows.append((form_code, form_title, cert_b_code, cert_b_title, ""))
                form_code, form_title = FORM_LINE.match(line).groups()
                form_title = form_title.strip().rstrip(".")
                cert_b_code=cert_b_title=dur=""
                state="CERTB_WAIT"
                continue
            # si aparece otra cabecera o B, ignoramos y seguimos esperando duración
            m2 = CERTB_LINE2.match(line)
            if m2:
                # fila previa sin duración
                rows.append((form_code, form_title, cert_b_code, cert_b_title, ""))
                cert_b_code, cert_b_title = m2.group(1), m2.group(2).strip().rstrip(".")
                state="DUR"
                continue
            continue

    # flush
    if form_code or form_title or cert_b_code or cert_b_title or dur:
        rows.append((form_code, form_title, cert_b_code, cert_b_title, dur))
    return rows

# ========= PROCESO POR BOE =========
def procesar_boe(url: str):
    try:
        html_text = get_html(url)
        full_text = extract_main_text(html_text)
        rd_num, fecha_rd = parse_rd_header(full_text)
        rd_id = get_boe_id_from_url(url) or ""
        fecha_rd = fecha_rd or ""

        annex_II = slice_annex_II_gradeB(full_text)
        fam_blocks = split_by_family(annex_II)

        out_by_family={}
        for fam_name, fam_block in fam_blocks.items():
            parent_blocks = split_blocks_by_parentC(fam_block)  # (codC, nomC, sub)
            rows=[]
            for codC, nomC, sub in parent_blocks:
                table_rows = parse_table_B(sub)
                if not table_rows:
                    table_rows=[("","","","", "")]
                for f_code, f_title, b_code, b_title, dur in table_rows:
                    row = {
                        "Familia": fam_name.title(),
                        "CERT_PADRE_COMPLETO": f"{codC}. {nomC}",
                        "CERT_PADRE_COD": codC,
                        "CERT_PADRE_DENOM": nomC,
                        "Formación a cursar": (f"{f_code}. {f_title}".strip(". ").strip() if f_code or f_title else ""),
                        "FORMACION_CODIGO": f_code,
                        "FORMACION_TITULO": f_title,
                        "Certificado de Competencia en": (f"{b_code}. {b_title}".strip(". ").strip() if b_code or b_title else ""),
                        "COD_CERT_COMP": b_code,
                        "NOM_CERT_COMP": b_title,
                        "DURACION_MEFD_H": to_int_or_blank(dur),
                        # compatibilidad con columnas antiguas si las usabas
                        "CERT_COMP_COD": b_code,
                        "CERT_COMP_TITULO": b_title,
                        "FUENTE_URL": url, "FECHA_RD": fecha_rd, "RD_ID": rd_id, "RD_NUM": rd_num
                    }
                    row = sanitize_row_B(row)
                    rows.append(row)

            cols = [
                "Familia",
                "CERT_PADRE_COD","CERT_PADRE_DENOM","CERT_PADRE_COMPLETO",
                "Formación a cursar","FORMACION_CODIGO","FORMACION_TITULO",
                "Certificado de Competencia en","COD_CERT_COMP","NOM_CERT_COMP",
                "DURACION_MEFD_H",
                "CERT_COMP_COD","CERT_COMP_TITULO",
                "FUENTE_URL","FECHA_RD","RD_ID","RD_NUM"
            ]
            df = pd.DataFrame(rows)
            for c in cols:
                if c not in df.columns: df[c]=""
            df = df[cols]
            out_by_family[fam_name.title()[:31]] = df

        raw_name = (rd_id or rd_num or "BOE")
        safe_name = re.sub(r"[^A-Za-z0-9._-]+","-", raw_name)
        out_path = CARPETA_POR_BOE / f"{safe_name}_GradoB_AnexoII_por_familia.xlsx"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            for sheet, df in out_by_family.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
        logging.info(f"OK: generado {out_path}")
        return out_by_family

    except Exception:
        logging.exception(f"Fallo procesando %s", url)
        return {}

def main():
    acumulado = defaultdict(list)
    for url in BOE_URLS:
        fam_to_df = procesar_boe(url)
        for fam_sheet, df in fam_to_df.items():
            acumulado[fam_sheet].append(df)
    if not acumulado:
        logging.error("No se han generado datos. Revisa las URLs o los mensajes de error.")
        sys.exit(1)

    with pd.ExcelWriter(SALIDA_CONSOLIDADO, engine="openpyxl") as writer:
        for fam_sheet, dflist in sorted(acumulado.items()):
            dfcat = pd.concat(dflist, ignore_index=True)
            dfcat = dfcat.drop_duplicates(
                subset=["FUENTE_URL","CERT_PADRE_COD","FORMACION_CODIGO","COD_CERT_COMP"],
                keep="first"
            )
            dfcat.to_excel(writer, sheet_name=fam_sheet[:31], index=False)
    logging.info("Consolidado OK: %s", SALIDA_CONSOLIDADO)
    print(f"\nListo:\n- Consolidado: {SALIDA_CONSOLIDADO}\n- Individuales por BOE en: {CARPETA_POR_BOE.resolve()}\n")

if __name__ == "__main__":
    # pip install requests beautifulsoup4 lxml chardet pandas openpyxl
    main()
