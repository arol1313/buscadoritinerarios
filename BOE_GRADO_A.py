# -*- coding: utf-8 -*-
"""
ANEXO III (Grado A: Acreditaciones parciales de competencia)
- Una fila por acreditación parcial (A) vinculada a su certificado de competencia (B).
- Columnas EXACTAS de la tabla y metadatos de RD.
- Heurísticas de rescate para rellenar COD/NOM cuando el BOE viene con saltos/variantes.

Salida:
- Un Excel por BOE (hojas por familia).
- Un consolidado: RDs_GradoA_Consolidado_por_familia.xlsx
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

SALIDA_CONSOLIDADO = "RDs_GradoA_Consolidado_por_familia.xlsx"
CARPETA_POR_BOE = Path("salidas_por_boe_A"); CARPETA_POR_BOE.mkdir(exist_ok=True)
DEBUG_DIR = Path("debug_a_grado"); DEBUG_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# ========= AUX/UTIL =========
RE_COD_A = re.compile(r"\b([A-Z]{3}_A_\d{4}_[0-9]{2})\b")
RE_COD_B = re.compile(r"\b([A-Z]{3}_B_\d{4})\b")

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

def split_code_title_from_dot(txt: str):
    """Divide 'COD. Título' en (COD, Título). Si no hay punto, intenta por regex."""
    if not txt: return "", ""
    txt = txt.strip().strip(".")
    m = re.match(r"^\s*([A-Z]{3}_[AB]_\d{4}(?:_[0-9]{2})?)\s*\.?\s*(.*)$", txt)
    if m:
        return m.group(1).strip(), m.group(2).strip().strip(".")
    return "", ""

def sanitize_row_A(row: dict) -> dict:
    """Rellenos heurísticos para A."""
    # B (COD/NOM) desde CERT_B_COMPLETO
    if (not row.get("COD_CERT_B")) or (not row.get("NOM_CERT_B")):
        c_b, n_b = split_code_title_from_dot(row.get("CERT_B_COMPLETO",""))
        if not c_b:
            m = RE_COD_B.search(row.get("CERT_B_COMPLETO",""))
            c_b = m.group(1) if m else ""
        row["COD_CERT_B"] = row.get("COD_CERT_B") or c_b
        row["NOM_CERT_B"] = row.get("NOM_CERT_B") or n_b

    # A (COD/NOM) desde “Acreditación parcial de competencia”
    if (not row.get("COD_ACRED_PARC")) or (not row.get("NOM_ACRED_PARCIAL")):
        c_a, n_a = split_code_title_from_dot(row.get("Acreditación parcial de competencia",""))
        if not c_a:
            m = RE_COD_A.search(row.get("Acreditación parcial de competencia",""))
            c_a = m.group(1) if m else ""
        row["COD_ACRED_PARC"]    = row.get("COD_ACRED_PARC")    or c_a
        row["NOM_ACRED_PARCIAL"] = row.get("NOM_ACRED_PARCIAL") or (n_a or row.get("NOM_ACRED",""))

    # Legacy alineadas
    row["COD_ACRED"] = row.get("COD_ACRED") or row.get("COD_ACRED_PARC","")
    row["NOM_ACRED"] = row.get("NOM_ACRED") or row.get("NOM_ACRED_PARCIAL","")

    # Duración numérica
    col_d = "Duración en el ámbito de gestión del MEFD en horas"
    if isinstance(row.get(col_d), str):
        m = re.search(r"\d+", row[col_d])
        row[col_d] = int(m.group(0)) if m else ""
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

# ========= RECORTE ANEXO III =========
def slice_annex_III_gradeA(full_text: str) -> str:
    m_start = re.search(
        r"ANEXO\s*III\s*\n+.*?Oferta\s+(?:de\s+)?Grado\s*A\s*:\s*Acreditaciones\s+parciales\s+de\s+competencia",
        full_text, re.I|re.DOTALL
    )
    if not m_start:
        m_start = re.search(
            r"Oferta\s+(?:de\s+)?Grado\s*A\s*:\s*Acreditaciones\s+parciales\s+de\s+competencia",
            full_text, re.I
        )
    if not m_start:
        raise RuntimeError("No se localiza ANEXO III / Oferta Grado A.")
    start = m_start.start()
    m_end = re.search(r"^\s*ANEXO\s*[IVXLC]+\b", full_text[m_start.end():], re.I|re.M)
    end = m_start.end()+m_end.start() if m_end else len(full_text)
    annex = full_text[start:end].strip()
    try: (DEBUG_DIR/"anexo_III_gradeA.txt").write_text(annex, encoding="utf-8")
    except: pass
    return annex

# ========= FAMILIAS =========
def split_by_family(annex_text: str) -> dict:
    pat = re.compile(
        r"(?:^|\n)\s*(?:\d+\.\s*)?ACREDITACIONES\s+PARCIALES\s+DE\s+COMPETENCIA\s+DE\s+LA\s+FAMILIA\s+PROFESIONAL\s+([^\n]+)",
        re.I
    )
    matches = list(pat.finditer(annex_text))
    if not matches:
        (DEBUG_DIR/"no_family_headers.txt").write_text(annex_text[:20000], encoding="utf-8")
        raise RuntimeError("No se han encontrado encabezados de familia en ANEXO III (Grado A).")
    blocks={}
    for i,m in enumerate(matches):
        fam = norm(m.group(1)).strip(" .:")
        start=m.end(); end=matches[i+1].start() if i+1<len(matches) else len(annex_text)
        chunk = annex_text[start:end].strip()
        blocks[fam]=chunk
        try: (DEBUG_DIR/f"familia_{fam.replace('/','-')}.txt").write_text(chunk, encoding="utf-8")
        except: pass
    return blocks

# ========= BLOQUES POR CERTIFICADO B =========
def split_blocks_by_certB(family_block: str):
    intro = re.search(
        r"Acreditaciones\s+parciales\s+de\s+competencia\s+que\s+configuran\s+el\s+Certificado\s+de\s+Competencia\s+en\s*:",
        family_block, re.I
    )
    start_from = intro.end() if intro else 0
    block_text = family_block[start_from:]

    pat_head = re.compile(r"(?:^|\n)\s*(?:[a-z]\)\s*)?([A-Z]{3}_B_\d{4})\s*\.\s*(.+)", re.I)
    heads = list(pat_head.finditer(block_text))
    out=[]
    for i,h in enumerate(heads):
        codB = h.group(1).strip()
        nomB = norm(h.group(2)).split("\n",1)[0].strip().rstrip(" .")
        s = h.end()
        e = heads[i+1].start() if i+1 < len(heads) else len(block_text)
        sub = block_text[s:e].strip()
        try: (DEBUG_DIR/f"certB_{codB}.txt").write_text(sub, encoding="utf-8")
        except: pass
        out.append((codB, nomB, sub))
    return out

# ========= PATRONES DE FILA (A, RA, DUR) =========
ACRED_ONE_LINE  = re.compile(r"^([A-Z]{3}_A_\d{4}_[0-9]{2})\.\s*(.+?)\s*$", re.UNICODE)
ACRED_CODE_ONLY = re.compile(r"^([A-Z]{3}_A_\d{4}_[0-9]{2})\.\s*$", re.UNICODE)
RA_LINE         = re.compile(r"^(RA\d+\.\s*.+?)\s*$", re.UNICODE | re.IGNORECASE)
DUR_LINE        = re.compile(r"^\s*(\d+)\s*(?:h|horas?)?\s*$", re.IGNORECASE)

def parse_table_blocks(subblock: str):
    """Extrae filas A (código+nombre) -> RA (formación a cursar) -> DUR (horas)."""
    lines = [norm(l) for l in subblock.split("\n")]

    def is_header(l):
        return bool(re.search(r"Acreditaci[oó]n\s+parcial\s+de\s+competencia", l, re.I)) \
            or bool(re.search(r"Formaci[oó]n\s+a\s+cursar", l, re.I)) \
            or bool(re.search(r"Duraci[oó]n.+MEFD", l, re.I))

    rows = []
    state = "A"
    codA = nomA = ra_txt = dur = ""

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        i += 1
        if not line or is_header(line):
            continue

        if state == "A":
            m1 = ACRED_ONE_LINE.match(line)
            if m1:
                codA, nomA = m1.group(1), m1.group(2).strip().rstrip(".")
                state = "RA"; continue
            m2 = ACRED_CODE_ONLY.match(line)
            if m2:
                codA = m2.group(1); nomA = ""; state = "A_TITLE"; continue
            continue

        if state == "A_TITLE":
            if ACRED_ONE_LINE.match(line) or ACRED_CODE_ONLY.match(line):
                rows.append((codA, nomA, "", ""))
                codA, nomA = (ACRED_ONE_LINE.match(line).groups() if ACRED_ONE_LINE.match(line)
                              else (ACRED_CODE_ONLY.match(line).group(1), ""))
                state = "RA" if ACRED_ONE_LINE.match(line) else "A_TITLE"
                continue
            if RA_LINE.match(line) or DUR_LINE.match(line):
                nomA = ""
            else:
                nomA = line.strip().rstrip("."); state = "RA"; continue

        if state == "RA":
            if RA_LINE.match(line):
                ra_txt = RA_LINE.match(line).group(1).strip()
                state = "DUR"; continue
            if ACRED_ONE_LINE.match(line) or ACRED_CODE_ONLY.match(line):
                rows.append((codA, nomA, "", ""))
                codA, nomA = (ACRED_ONE_LINE.match(line).groups() if ACRED_ONE_LINE.match(line)
                              else (ACRED_CODE_ONLY.match(line).group(1), ""))
                ra_txt = dur = ""
                state = "RA" if ACRED_ONE_LINE.match(line) else "A_TITLE"
                continue
            if ra_txt: ra_txt += " " + line
            else: ra_txt = line
            continue

        if state == "DUR":
            if DUR_LINE.match(line):
                dur = DUR_LINE.match(line).group(1)
                rows.append((codA, nomA, ra_txt, dur))
                codA = nomA = ra_txt = dur = ""; state = "A"; continue
            if ACRED_ONE_LINE.match(line) or ACRED_CODE_ONLY.match(line):
                rows.append((codA, nomA, ra_txt, ""))
                codA, nomA = (ACRED_ONE_LINE.match(line).groups() if ACRED_ONE_LINE.match(line)
                              else (ACRED_CODE_ONLY.match(line).group(1), ""))
                ra_txt = dur = ""
                state = "RA" if ACRED_ONE_LINE.match(line) else "A_TITLE"
                continue
            if RA_LINE.match(line):
                ra_txt += " " + RA_LINE.match(line).group(1).strip()
                continue
            continue

    if codA or nomA or ra_txt or dur:
        rows.append((codA, nomA, ra_txt, dur))

    return rows  # [(codA, nomA, RA, DUR)]

# ========= PROCESO POR BOE =========
def procesar_boe(url: str):
    try:
        html_text = get_html(url)
        full_text = extract_main_text(html_text)
        rd_num, fecha_rd = parse_rd_header(full_text)
        rd_id = get_boe_id_from_url(url) or ""
        fecha_rd = fecha_rd or ""

        annex_III = slice_annex_III_gradeA(full_text)
        fam_blocks = split_by_family(annex_III)

        out_by_family={}
        for fam_name, fam_block in fam_blocks.items():
            certB_blocks = split_blocks_by_certB(fam_block)
            rows=[]
            for codB, nomB, sub in certB_blocks:
                tuples = parse_table_blocks(sub)
                if not tuples:
                    tuples=[("","", "", "")]
                for codA, nomA, ra, dur in tuples:
                    acredit_full = (f"{codA}. {nomA}" if codA else nomA).strip().strip(".")
                    row = {
                        "Familia": fam_name.title(),
                        "CERT_B_COMPLETO": f"{codB}. {nomB}",
                        "COD_CERT_B": codB,
                        "NOM_CERT_B": nomB,
                        "Acreditación parcial de competencia": acredit_full,
                        "Formación a cursar": ra,
                        "Duración en el ámbito de gestión del MEFD en horas": to_int_or_blank(dur),
                        "COD_ACRED_PARC": codA,
                        "NOM_ACRED_PARCIAL": nomA,
                        "COD_ACRED": codA,
                        "NOM_ACRED": nomA,
                        "FUENTE_URL": url, "FECHA_RD": fecha_rd, "RD_ID": rd_id, "RD_NUM": rd_num
                    }
                    row = sanitize_row_A(row)
                    rows.append(row)

            cols = [
                "Familia",
                "CERT_B_COMPLETO","COD_CERT_B","NOM_CERT_B",
                "Acreditación parcial de competencia","COD_ACRED_PARC","NOM_ACRED_PARCIAL",
                "Formación a cursar","Duración en el ámbito de gestión del MEFD en horas",
                "COD_ACRED","NOM_ACRED",
                "FUENTE_URL","FECHA_RD","RD_ID","RD_NUM"
            ]
            df = pd.DataFrame(rows)
            for c in cols:
                if c not in df.columns: df[c]=""
            df = df[cols]
            out_by_family[fam_name.title()[:31]] = df

        raw_name = (rd_id or rd_num or "BOE")
        safe_name = re.sub(r"[^A-Za-z0-9._-]+","-", raw_name)
        out_path = CARPETA_POR_BOE / f"{safe_name}_GradoA_AnexoIII_por_familia.xlsx"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            for sheet, df in out_by_family.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
        logging.info(f"OK: generado %s", out_path)
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
                subset=[
                    "FUENTE_URL","COD_CERT_B",
                    "Acreditación parcial de competencia",
                    "Formación a cursar",
                    "Duración en el ámbito de gestión del MEFD en horas"
                ],
                keep="first"
            )
            dfcat.to_excel(writer, sheet_name=fam_sheet[:31], index=False)
    logging.info("Consolidado OK: %s", SALIDA_CONSOLIDADO)
    print(f"\nListo:\n- Consolidado: {SALIDA_CONSOLIDADO}\n- Individuales por BOE en: {CARPETA_POR_BOE.resolve()}\n")

if __name__ == "__main__":
    # pip install requests beautifulsoup4 lxml chardet pandas openpyxl
    main()
