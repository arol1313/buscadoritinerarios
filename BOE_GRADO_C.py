# -*- coding: utf-8 -*-
"""
ANEXO I (Grado C: Certificados profesionales)
- Recorre todas las familias y certificados del ANEXO I.
- Saca una fila por cada UC (apartado 4) y replica datos de identificación.
- Añade módulos (apartado 7) en la columna MODULOS_PROF (una fila por UC x módulo).
- Metadatos: FUENTE_URL, FECHA_RD, RD_ID, RD_NUM.

Columnas:
FAMILIA, DENOMINACION, CODIGO, TITULO_FP_ASOCIADO, NIVEL, DURACION,
REFERENTE_CINE, REFERENTE_MEC, COMP_GENERAL, COMP_PROF,
CODIGO_COMP, ESTANDAR_COMP, OCUPACIONES, MODULOS_PROF,
FUENTE_URL, FECHA_RD, RD_ID, RD_NUM
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
    "https://www.boe.es/diario_boe/txt.php?id=BOE-A-2025-6797",
    "https://www.boe.es/diario_boe/txt.php?id=BOE-A-2025-7040",
    "https://www.boe.es/diario_boe/txt.php?id=BOE-A-2025-7096",
    "https://www.boe.es/eli/es/rd/2025/03/18/209",
    "https://www.boe.es/eli/es/rd/2025/03/18/210",
    "https://www.boe.es/eli/es/rd/2025/03/18/212",
    "https://www.boe.es/eli/es/rd/2025/03/18/213",
]

SALIDA_CONSOLIDADO = "RDs_GradoC_Consolidado_por_familia.xlsx"
CARPETA_POR_BOE = Path("salidas_por_boe"); CARPETA_POR_BOE.mkdir(exist_ok=True)
DEBUG_DIR = Path("debug_c_grado"); DEBUG_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# ========= UTIL =========
def norm(s: str) -> str:
    if s is None: return ""
    s = html.unescape(s)
    s = (s.replace("\xa0", " ").replace("\u2002", " ").replace("\u2003", " ").replace(" ", " ")
           .replace("–", "-").replace("—", "-"))
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"[ \t]*\n[ \t]*", "\n", s)
    return s.strip()

def to_int_or_blank(x: str):
    if x is None or str(x).strip()=="":
        return ""
    m = re.search(r"\d+", str(x))
    return int(m.group(0)) if m else ""

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

# ========= RECORTE ANEXO I =========
def slice_annex_I_gradeC(full_text: str) -> str:
    m_start = re.search(
        r"ANEXO\s*I\s*\n+.*?Oferta\s+de\s+Grado\s*C\s*:\s*Certificados\s+profesionales",
        full_text, re.I|re.DOTALL
    )
    if not m_start:
        m_start = re.search(
            r"Oferta\s+de\s+Grado\s*C\s*:\s*Certificados\s+profesionales",
            full_text, re.I
        )
    if not m_start:
        raise RuntimeError("No se localiza ANEXO I / Oferta Grado C.")
    start = m_start.start()
    m_end = re.search(r"^\s*ANEXO\s*[IVXLC]+\b", full_text[m_start.end():], re.I|re.M)
    end = m_start.end()+m_end.start() if m_end else len(full_text)
    annex = full_text[start:end].strip()
    try: (DEBUG_DIR/"anexo_I.txt").write_text(annex, encoding="utf-8")
    except: pass
    return annex

# ========= FAMILIAS (robusto) =========
def split_by_family(annex_text: str) -> dict:
    """
    Acepta encabezados tipo:
      '1. CERTIFICADOS PROFESIONALES DE/PARA LA FAMILIA PROFESIONAL <NOMBRE>:'
    con numeración opcional, saltos de línea y dos puntos.
    """
    patterns = [
        r"(?:^|\n)\s*(?:\d+\.\s*)?CERTIFICADOS?\s+PROFESIONALES?\s+(?:DE|PARA)\s+LA\s+FAMILIA\s+PROFESIONAL\s+([^\n:]+?)\s*:?\s*$",
        r"(?:^|\n)\s*(?:\d+\.\s*)?CERTIFICADOS?\s+PROFESIONALES?\s+(?:DE|PARA)\s+LA\s+FAMILIA\s+([^\n:]+?)\s*:?\s*$",
        r"(?:^|\n)\s*(?:\d+\.\s*)?CERTIFICADOS?\s+PROFESIONALES?\s+(?:DE|PARA)\s+LA\s+FAMILIA\s*(?:\n|\s)+PROFESIONAL\s+([^\n:]+?)\s*:?\s*$",
    ]
    heads = []
    for rx in patterns:
        for m in re.finditer(rx, annex_text, flags=re.I | re.M):
            heads.append((m.start(), m.end(), m.group(1)))
    heads.sort(key=lambda t: t[0])

    if not heads:
        try: (DEBUG_DIR / "no_family_headers.txt").write_text(annex_text[:20000], encoding="utf-8")
        except: pass
        raise RuntimeError("No se han encontrado encabezados de familias en el ANEXO I.")

    blocks = {}
    for i, (s, e, fam_raw) in enumerate(heads):
        fam = norm(fam_raw).strip(" .:")
        end = heads[i + 1][0] if i + 1 < len(heads) else len(annex_text)
        chunk = annex_text[e:end].strip()
        blocks[fam] = chunk
        try: (DEBUG_DIR / f"familia_{fam.replace('/', '-')}.txt").write_text(chunk, encoding="utf-8")
        except: pass
    return blocks

# ========= CERTIFICADOS C =========
HEAD_CERT = re.compile(r"(?:^|\n)\s*(?:[a-z]\)\s*)?CERTIFICADO\s+PROFESIONAL\s*:\s*(.+?)\s*(?:\n|$)", re.I)

def split_certificates_C(family_block: str):
    marks = list(HEAD_CERT.finditer(family_block))
    certs=[]
    for i,m in enumerate(marks):
        denom = norm(m.group(1)).strip().rstrip(" .")
        start=m.end()
        end = marks[i+1].start() if i+1<len(marks) else len(family_block)
        sub = family_block[start:end].strip()
        code = find_in_identification(sub, key="Código")
        fam_from_id = find_in_identification(sub, key="Familia Profesional") or ""
        certs.append((code, denom, fam_from_id, sub))
        try: (DEBUG_DIR/f"cert_{(code or denom)[:40]}.txt").write_text(sub, encoding="utf-8")
        except: pass
    return certs

# ========= SECCIONES =========
def section_slice(block: str, title_regex: str, next_titles_regex_list):
    m = re.search(title_regex, block, re.I|re.M)
    if not m: return ""
    start = m.end()
    ends=[]
    for rx in next_titles_regex_list:
        n = re.search(rx, block[start:], re.I|re.M)
        if n: ends.append(start + n.start())
    end = min(ends) if ends else len(block)
    return norm(block[start:end])

def find_in_identification(block: str, key: str):
    ident = section_slice(
        block,
        r"^\s*1\.\s*Identificaci[oó]n\s*$",
        [
            r"^\s*2\.\s*Competencia\s+general",
            r"^\s*3\.\s*Competencias\s+profesionales",
            r"^\s*4\.\s*Relaci[oó]n\s+de\s+Est[aá]ndares",
            r"^\s*5\.\s*Entorno\s+profesional",
            r"^\s*6\.\s*Orientaciones",
            r"^\s*7\.\s*M[oó]dulos\s+profesionales",
        ]
    )
    if not ident: return ""
    pat = re.compile(rf"[-–]\s*{re.escape(key)}\s*:\s*(.+)", re.I)
    m = pat.search(ident)
    if not m: return ""
    return norm(m.group(1)).strip().rstrip(" .")

def get_ident_dict(block: str):
    keys = {
        "Denominación": "DENOMINACION",
        "Código": "CODIGO",
        "Título de Formación Profesional asociado": "TITULO_FP_ASOCIADO",
        "Nivel": "NIVEL",
        "Duración orientativa": "DURACION",
        "Familia Profesional": "FAMILIA",
        "Referente en la Clasificación Internacional Normalizada de la Educación": "REFERENTE_CINE",
        "Referencia del Marco Español de Cualificaciones para el aprendizaje permanente": "REFERENTE_MEC",
    }
    out = {v:"" for v in keys.values()}
    ident = section_slice(
        block,
        r"^\s*1\.\s*Identificaci[oó]n\s*$",
        [
            r"^\s*2\.\s*Competencia\s+general",
            r"^\s*3\.\s*Competencias\s+profesionales",
            r"^\s*4\.\s*Relaci[oó]n\s+de\s+Est[aá]ndares",
            r"^\s*5\.\s*Entorno\s+profesional",
            r"^\s*6\.\s*Orientaciones",
            r"^\s*7\.\s*M[oó]dulos\s+profesionales",
        ]
    )
    if ident:
        for key,label in keys.items():
            pat = re.compile(rf"[-–]\s*{re.escape(key)}\s*:\s*(.+)", re.I)
            m = pat.search(ident)
            if m:
                out[label] = norm(m.group(1)).strip().rstrip(" .")
        out["DURACION"] = to_int_or_blank(out["DURACION"])
    return out

def get_comp_general(block: str):
    return section_slice(
        block,
        r"^\s*2\.\s*Competencia\s+general\s*$",
        [
            r"^\s*3\.\s*Competencias\s+profesionales.*$",
            r"^\s*4\.\s*Relaci[oó]n\s+de\s+Est[aá]ndares",
            r"^\s*5\.\s*Entorno\s+profesional",
            r"^\s*6\.\s*Orientaciones",
            r"^\s*7\.\s*M[oó]dulos\s+profesionales",
        ]
    )

def get_comp_prof(block: str):
    txt = section_slice(
        block,
        r"^\s*3\.\s*Competencias\s+profesionales.*$",
        [
            r"^\s*4\.\s*Relaci[oó]n\s+de\s+Est[aá]ndares",
            r"^\s*5\.\s*Entorno\s+profesional",
            r"^\s*6\.\s*Orientaciones",
            r"^\s*7\.\s*M[oó]dulos\s+profesionales",
        ]
    )
    if not txt: return ""
    lines = []
    for line in txt.split("\n"):
        L = norm(line).lstrip("-–• ").strip()
        if not L: continue
        L = re.sub(r"^[a-z]\)\s*", "", L)
        lines.append(L)
    return " | ".join(lines)

def get_estandares(block: str):
    txt = section_slice(
        block,
        r"^\s*4\.\s*Relaci[oó]n\s+de\s+Est[aá]ndares.*$",
        [
            r"^\s*5\.\s*Entorno\s+profesional",
            r"^\s*6\.\s*Orientaciones",
            r"^\s*7\.\s*M[oó]dulos\s+profesionales",
        ]
    )
    if not txt: return []
    ucs=[]
    for raw in txt.split("\n"):
        line = norm(raw).strip().lstrip("-–• ")
        if not line: continue
        m = re.match(r"^(UC\d{4}_[1-3])\s*:\s*(.+)$", line)
        if m:
            ucs.append((m.group(1).strip(), m.group(2).strip().rstrip(" .")))
    return ucs

def get_ocupaciones(block: str):
    txt = section_slice(
        block,
        r"^\s*5\.\s*Entorno\s+profesional\s*$",
        [
            r"^\s*6\.\s*Orientaciones",
            r"^\s*7\.\s*M[oó]dulos\s+profesionales",
        ]
    )
    if not txt: return ""
    items=[]
    for raw in txt.split("\n"):
        line = norm(raw).strip()
        if not line: continue
        line = line.lstrip("-–• ").strip().rstrip(" .")
        if line: items.append(line)
    return " | ".join(items)

def get_modulos(block: str):
    txt = section_slice(
        block,
        r"^\s*7\.\s*M[oó]dulos\s+profesionales.*$",
        [
            r"^\s*8\.\s*Correspondencia",
            r"^\s*8\.\s*Itinerarios",
            r"^\s*ANEXO\s*[IVXLC]+",
        ]
    )
    if not txt: return []
    mods=[]
    for raw in txt.split("\n"):
        line = norm(raw).strip().lstrip("-–• ").strip()
        if not line: continue
        m = re.match(r"^(\d{3,4})[.\-: ]+\s*(.+?)\s*$", line)
        if m:
            cod = m.group(1).strip()
            nom = m.group(2).strip().rstrip(" .")
            mods.append((cod, nom))
    return mods

# ========= PROCESO POR BOE =========
def procesar_boe(url: str):
    try:
        html_text = get_html(url)
        full_text = extract_main_text(html_text)
        rd_num, fecha_rd = parse_rd_header(full_text)
        rd_id = get_boe_id_from_url(url) or ""
        fecha_rd = fecha_rd or ""

        annex_I = slice_annex_I_gradeC(full_text)
        fam_blocks = split_by_family(annex_I)

        out_by_family={}
        for fam_name, fam_block in fam_blocks.items():
            certs = split_certificates_C(fam_block)
            rows=[]
            for codigo, denom, fam_from_id, sub in certs:
                ident = get_ident_dict(sub)
                fam = ident.get("FAMILIA") or fam_name
                comp_gen = get_comp_general(sub)
                comp_prof = get_comp_prof(sub)
                ucs = get_estandares(sub) or [("", "")]
                mods = get_modulos(sub) or [("", "")]
                ocup = get_ocupaciones(sub)

                for uc_cod, uc_desc in ucs:
                    for mod_cod, mod_nom in mods:
                        rows.append({
                            "FAMILIA": fam,
                            "DENOMINACION": ident.get("DENOMINACION") or denom,
                            "CODIGO": ident.get("CODIGO") or codigo,
                            "TITULO_FP_ASOCIADO": ident.get("TITULO_FP_ASOCIADO"),
                            "NIVEL": ident.get("NIVEL"),
                            "DURACION": ident.get("DURACION"),
                            "REFERENTE_CINE": ident.get("REFERENTE_CINE"),
                            "REFERENTE_MEC": ident.get("REFERENTE_MEC"),
                            "COMP_GENERAL": comp_gen,
                            "COMP_PROF": comp_prof,
                            "CODIGO_COMP": uc_cod,
                            "ESTANDAR_COMP": uc_desc,
                            "OCUPACIONES": ocup,
                            "MODULOS_PROF": f"{mod_cod}. {mod_nom}".strip(". ").strip() if mod_cod or mod_nom else "",
                            "FUENTE_URL": url,
                            "FECHA_RD": fecha_rd,
                            "RD_ID": rd_id,
                            "RD_NUM": rd_num,
                        })

            cols = [
                "FAMILIA","DENOMINACION","CODIGO","TITULO_FP_ASOCIADO","NIVEL","DURACION",
                "REFERENTE_CINE","REFERENTE_MEC","COMP_GENERAL","COMP_PROF",
                "CODIGO_COMP","ESTANDAR_COMP","OCUPACIONES","MODULOS_PROF",
                "FUENTE_URL","FECHA_RD","RD_ID","RD_NUM"
            ]
            df = pd.DataFrame(rows)
            for c in cols:
                if c not in df.columns: df[c]=""
            df = df[cols]
            out_by_family[fam_name[:31] or "Familia"] = df

        # Excel por BOE
        raw_name = (rd_id or rd_num or "BOE")
        safe_name = re.sub(r"[^A-Za-z0-9._-]+","-", raw_name)
        out_path = CARPETA_POR_BOE / f"{safe_name}_GradoC_por_familia.xlsx"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            for sheet, df in out_by_family.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
        logging.info(f"OK: generado {out_path}")
        return out_by_family

    except Exception:
        logging.exception(f"Fallo procesando {url}")
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
                subset=["FUENTE_URL","CODIGO","CODIGO_COMP","MODULOS_PROF"],
                keep="first"
            )
            dfcat.to_excel(writer, sheet_name=fam_sheet[:31] or "Familia", index=False)

    logging.info(f"Consolidado OK: {SALIDA_CONSOLIDADO}")
    print(f"\nListo:\n- Consolidado: {SALIDA_CONSOLIDADO}\n- Individuales por BOE en: {CARPETA_POR_BOE.resolve()}\n")

if __name__ == "__main__":
    # Requisitos: pip install requests beautifulsoup4 lxml chardet pandas openpyxl
    main()
