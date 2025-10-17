# -*- coding: utf-8 -*-
"""
RELACIONADOR_ABC.py (versión mejorada)
- Carga los consolidados (o sus versiones fix_* si existen).
- Normaliza y construye relaciones:
    map_b_a: B -> conjunto de A
    map_c_b: C -> conjunto de B
- Valida campos clave y reporta huérfanos / faltantes.
- Exporta REL_ABC_MASTER.xlsx con:
    grado_a, grado_b, grado_c, map_b_a, map_c_b, ref_b, ref_c, resumen, errores_*

Funciones de consulta:
    consulta_desde_A(...)
    consulta_desde_B(...)
    consulta_desde_C(...)

Requisitos: pandas, openpyxl
"""

import re
import pandas as pd
from pathlib import Path
from collections import defaultdict

# ======= CONFIG =======
F_A = "RDs_GradoA_Consolidado_por_familia.xlsx"
F_B = "RDs_GradoB_Consolidado_por_familia.xlsx"
F_C = "RDs_GradoC_Consolidado_por_familia.xlsx"
F_OUT = "REL_ABC_MASTER.xlsx"

# Usar versiones reparadas si existen
def prefer_fix(path: str) -> str:
    p = Path(path)
    p_fix = p.with_name("fix_" + p.name)
    return str(p_fix if p_fix.exists() else p)

# ======= HELPERS =======
def norm(s):
    if pd.isna(s): return ""
    s = str(s).strip()
    s = re.sub(r"\s*\.\s*$", "", s)
    return s

def code_or_blank(s):
    s = norm(s)
    return s if s else ""

def to_int_or_blank(x):
    try:
        if pd.isna(x): return ""
        m = re.search(r"\d+", str(x))
        return int(m.group(0)) if m else ""
    except:
        return ""

def load_all_sheets(xlsx_path):
    xlsx_path = Path(xlsx_path)
    if not xlsx_path.exists():
        raise FileNotFoundError(f"No encontrado: {xlsx_path}")
    xl = pd.ExcelFile(xlsx_path)
    frames = []
    for sh in xl.sheet_names:
        df = xl.parse(sh)
        df["_sheet"] = sh
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ======= CARGA =======
def cargar_grado_A():
    dfA = load_all_sheets(prefer_fix(F_A))
    # Columnas esperadas tras los scrapers y reparador:
    # Familia, CERT_B_COMPLETO, COD_CERT_B, NOM_CERT_B,
    # Acreditación parcial de competencia, COD_ACRED_PARC, NOM_ACRED_PARCIAL,
    # Formación a cursar, Duración en el ámbito de gestión del MEFD en horas,
    # COD_ACRED, NOM_ACRED, FUENTE_URL, FECHA_RD, RD_ID, RD_NUM
    rename = {
        "Familia":"familia",
        "CERT_B_COMPLETO":"cert_b_completo",
        "COD_CERT_B":"cod_b",
        "NOM_CERT_B":"nom_b",
        "Acreditación parcial de competencia":"acred_parcial",
        "COD_ACRED_PARC":"cod_a",
        "NOM_ACRED_PARCIAL":"nom_a",
        "Formación a cursar":"ra_texto",
        "Duración en el ámbito de gestión del MEFD en horas":"horas_a",
    }
    for k in rename:
        if k not in dfA.columns: dfA[k] = ""
    dfA = dfA.rename(columns=rename)
    dfA["cod_a"] = dfA["cod_a"].map(code_or_blank)
    dfA["cod_b"] = dfA["cod_b"].map(code_or_blank)
    dfA["horas_a"] = dfA["horas_a"].map(to_int_or_blank)
    return dfA

def cargar_grado_B():
    dfB = load_all_sheets(prefer_fix(F_B))
    # Columnas esperadas:
    # Familia, CERT_PADRE_COD, CERT_PADRE_DENOM, CERT_PADRE_COMPLETO,
    # Formación a cursar, FORMACION_CODIGO, FORMACION_TITULO,
    # Certificado de Competencia en, COD_CERT_COMP, NOM_CERT_COMP, DURACION_MEFD_H,
    # FUENTE_URL, FECHA_RD, RD_ID, RD_NUM
    rename = {
        "Familia":"familia",
        "CERT_PADRE_COD":"cod_c",
        "CERT_PADRE_DENOM":"nom_c",
        "CERT_PADRE_COMPLETO":"cert_c_completo",
        "COD_CERT_COMP":"cod_b",
        "NOM_CERT_COMP":"nom_b",
        "DURACION_MEFD_H":"horas_b",
        "Formación a cursar":"form_b_texto",
        "FORMACION_CODIGO":"form_b_codigo",
        "FORMACION_TITULO":"form_b_titulo",
    }
    for k in rename:
        if k not in dfB.columns: dfB[k] = ""
    dfB = dfB.rename(columns=rename)
    dfB["cod_b"] = dfB["cod_b"].map(code_or_blank)
    dfB["cod_c"] = dfB["cod_c"].map(code_or_blank)
    dfB["horas_b"] = dfB["horas_b"].map(to_int_or_blank)
    return dfB

def cargar_grado_C():
    dfC = load_all_sheets(prefer_fix(F_C))
    rename = {
        "FAMILIA":"familia",
        "DENOMINACION":"nom_c",
        "CODIGO":"cod_c",
        "DURACION":"horas_c",
    }
    for k in rename:
        if k not in dfC.columns: dfC[k] = ""
    dfC = dfC.rename(columns=rename)
    dfC["cod_c"] = dfC["cod_c"].map(code_or_blank)
    dfC["horas_c"] = dfC["horas_c"].map(to_int_or_blank)
    # C únicos
    dfC_min = dfC[["familia","cod_c","nom_c","horas_c"]].drop_duplicates()
    return dfC_min

# ======= MAPAS =======
def construir_mapas(dfA, dfB, dfC):
    # B -> set(A)
    map_b_a = (
        dfA[["cod_b","cod_a","nom_a","ra_texto","horas_a","familia"]]
        .dropna(subset=["cod_b","cod_a"])
        .query("cod_b != '' and cod_a != ''")
        .drop_duplicates()
    )
    # C -> set(B)
    map_c_b = (
        dfB[["cod_c","cod_b","familia"]]
        .dropna(subset=["cod_c","cod_b"])
        .query("cod_c != '' and cod_b != ''")
        .drop_duplicates()
    )
    # Referencias
    ref_b = (
        dfB[["cod_b","nom_b","horas_b","familia"]]
        .query("cod_b != ''")
        .drop_duplicates(subset=["cod_b"])
    )
    ref_c = dfC.drop_duplicates(subset=["cod_c"])
    return map_b_a, map_c_b, ref_b, ref_c

# ======= VALIDACIONES =======
def validar(dfA, dfB, dfC, map_b_a, map_c_b):
    errores = {}

    missA = dfA[(dfA["cod_b"]=="") | (dfA["cod_a"]=="")]
    if not missA.empty: errores["errores_grado_a_faltan_codigos"] = missA

    missB = dfB[(dfB["cod_b"]=="") | (dfB["cod_c"]=="")]
    if not missB.empty: errores["errores_grado_b_faltan_codigos"] = missB

    # Huérfanos: A que apuntan a B inexistente
    b_existentes = set(map_b_a["cod_b"].unique())
    b_ref = set(map_c_b["cod_b"].unique())  # Bs que al menos aparecen en algún C
    b_huerfanos = sorted(b_existentes - b_ref)
    if b_huerfanos:
        err = map_b_a[map_b_a["cod_b"].isin(b_huerfanos)]
        if not err.empty: errores["errores_b_sin_c"] = err

    # Huérfanos: B que apuntan a C inexistente (por si el C no apareció en consolidado C)
    c_existentes = set(map_c_b["cod_c"].unique())
    # Si quieres, podrías compararlo con dfC['cod_c'] por si dfC_min se quedó corto
    # de momento, no reportamos más si están en map_c_b.
    return errores

# ======= CONSULTAS =======
def consulta_desde_A(dfA, dfB, map_b_a, map_c_b, ref_b, ref_c, cod_a):
    cod_a = code_or_blank(cod_a)
    b_rows = map_b_a[map_b_a["cod_a"]==cod_a]
    if b_rows.empty:
        return {"cod_a": cod_a, "acceso_B": [], "acceso_C": []}

    resultado_B = []
    for cod_b in sorted(b_rows["cod_b"].unique()):
        req_A = map_b_a[map_b_a["cod_b"]==cod_b][["cod_a","nom_a","horas_a"]].drop_duplicates()
        ya_tengo = {cod_a}
        faltan = [r for r in req_A.to_dict("records") if r["cod_a"] not in ya_tengo]
        horas_pend = sum([int(x["horas_a"]) for x in faltan if str(x["horas_a"]).isdigit()])
        info_b = ref_b[ref_b["cod_b"]==cod_b].to_dict("records")
        resultado_B.append({
            "cod_b": cod_b,
            "nom_b": info_b[0]["nom_b"] if info_b else "",
            "a_requeridas": req_A.to_dict("records"),
            "a_faltan": faltan,
            "horas_pendientes": horas_pend,
        })

    cs = map_c_b[map_c_b["cod_b"].isin(b_rows["cod_b"].unique())]["cod_c"].unique()
    resultado_C = []
    for cod_c in sorted(cs):
        req_B = map_c_b[map_c_b["cod_c"]==cod_c]["cod_b"].unique().tolist()
        tengo_B = []  # con una A suelta no completas B; dejamos vacío
        faltan_B = [b for b in req_B if b not in tengo_B]
        info_c = ref_c[ref_c["cod_c"]==cod_c].to_dict("records")
        detalle_B = []
        for b in faltan_B:
            reqA = map_b_a[map_b_a["cod_b"]==b][["cod_a","nom_a","horas_a"]].drop_duplicates()
            horas_b = ref_b.loc[ref_b["cod_b"]==b, "horas_b"]
            horas_b = int(horas_b.iloc[0]) if not horas_b.empty and str(horas_b.iloc[0]).isdigit() else ""
            detalle_B.append({
                "cod_b": b,
                "nom_b": ref_b.loc[ref_b["cod_b"]==b, "nom_b"].iloc[0] if not ref_b[ref_b["cod_b"]==b].empty else "",
                "a_requeridas": reqA.to_dict("records"),
                "horas_b": horas_b,
            })
        resultado_C.append({
            "cod_c": cod_c,
            "nom_c": info_c[0]["nom_c"] if info_c else "",
            "b_requeridos": req_B,
            "b_faltan": faltan_B,
            "detalle_b_faltan": detalle_B,
        })

    return {"cod_a": cod_a, "acceso_B": resultado_B, "acceso_C": resultado_C}

def consulta_desde_B(map_b_a, map_c_b, ref_b, ref_c, cod_b):
    cod_b = code_or_blank(cod_b)
    req_A = map_b_a[map_b_a["cod_b"]==cod_b][["cod_a","nom_a","horas_a"]].drop_duplicates()
    cs = map_c_b[map_c_b["cod_b"]==cod_b]["cod_c"].unique()
    resultado_C = []
    for cod_c in sorted(cs):
        req_B = map_c_b[map_c_b["cod_c"]==cod_c]["cod_b"].unique().tolist()
        faltan = [b for b in req_B if b != cod_b]
        detalle = []
        for b in faltan:
            _A = map_b_a[map_b_a["cod_b"]==b][["cod_a","nom_a","horas_a"]].drop_duplicates()
            horas_b = ref_b.loc[ref_b["cod_b"]==b, "horas_b"]
            horas_b = int(horas_b.iloc[0]) if not horas_b.empty and str(horas_b.iloc[0]).isdigit() else ""
            detalle.append({
                "cod_b": b,
                "nom_b": ref_b.loc[ref_b["cod_b"]==b, "nom_b"].iloc[0] if not ref_b[ref_b["cod_b"]==b].empty else "",
                "a_requeridas": _A.to_dict("records"),
                "horas_b": horas_b,
            })
        info_c = ref_c[ref_c["cod_c"]==cod_c].to_dict("records")
        resultado_C.append({
            "cod_c": cod_c,
            "nom_c": info_c[0]["nom_c"] if info_c else "",
            "b_requeridos": req_B,
            "b_faltan_si_solo_tengo_este_b": faltan,
            "detalle_b_faltan": detalle,
        })

    info_b = ref_b[ref_b["cod_b"]==cod_b].to_dict("records")
    return {
        "cod_b": cod_b,
        "nom_b": info_b[0]["nom_b"] if info_b else "",
        "a_requeridas": req_A.to_dict("records"),
        "acceso_C": resultado_C
    }

def consulta_desde_C(map_b_a, map_c_b, ref_b, ref_c, cod_c):
    cod_c = code_or_blank(cod_c)
    req_B = map_c_b[map_c_b["cod_c"]==cod_c]["cod_b"].unique().tolist()
    detalle = []
    for b in sorted(req_B):
        reqA = map_b_a[map_b_a["cod_b"]==b][["cod_a","nom_a","horas_a"]].drop_duplicates()
        horas_b = ref_b.loc[ref_b["cod_b"]==b, "horas_b"]
        horas_b = int(horas_b.iloc[0]) if not horas_b.empty and str(horas_b.iloc[0]).isdigit() else ""
        detalle.append({
            "cod_b": b,
            "nom_b": ref_b.loc[ref_b["cod_b"]==b, "nom_b"].iloc[0] if not ref_b[ref_b["cod_b"]==b].empty else "",
            "a_requeridas": reqA.to_dict("records"),
            "horas_b": horas_b,
        })
    info_c = ref_c[ref_c["cod_c"]==cod_c].to_dict("records")
    return {
        "cod_c": cod_c,
        "nom_c": info_c[0]["nom_c"] if info_c else "",
        "b_requeridos": req_B,
        "detalle_b": detalle
    }

# ======= PIPELINE PRINCIPAL =======
def main():
    # Carga
    dfA = cargar_grado_A()
    dfB = cargar_grado_B()
    dfC = cargar_grado_C()

    # Construcción
    map_b_a, map_c_b, ref_b, ref_c = construir_mapas(dfA, dfB, dfC)

    # Validaciones
    errores = validar(dfA, dfB, dfC, map_b_a, map_c_b)

    # Resumen
    resumen = pd.DataFrame({
        "tabla": ["grado_a","grado_b","grado_c","map_b_a","map_c_b","ref_b","ref_c"],
        "filas": [len(dfA), len(dfB), len(dfC), len(map_b_a), len(map_c_b), len(ref_b), len(ref_c)]
    })

    # Guardado
    with pd.ExcelWriter(F_OUT, engine="openpyxl") as w:
        dfA.to_excel(w, sheet_name="grado_a", index=False)
        dfB.to_excel(w, sheet_name="grado_b", index=False)
        dfC.to_excel(w, sheet_name="grado_c", index=False)
        map_b_a.to_excel(w, sheet_name="map_b_a", index=False)
        map_c_b.to_excel(w, sheet_name="map_c_b", index=False)
        ref_b.to_excel(w, sheet_name="ref_b", index=False)
        ref_c.to_excel(w, sheet_name="ref_c", index=False)
        resumen.to_excel(w, sheet_name="resumen", index=False)
        for name, dferr in errores.items():
            dferr.to_excel(w, sheet_name=name[:31], index=False)

    print(f"OK -> {F_OUT}")
    print("Funciones de consulta (interactivo):")
    print(" - consulta_desde_A(dfA, dfB, map_b_a, map_c_b, ref_b, ref_c, 'ADG_A_0156_02')")
    print(" - consulta_desde_B(map_b_a, map_c_b, ref_b, ref_c, 'AFD_B_3003')")
    print(" - consulta_desde_C(map_b_a, map_c_b, ref_b, ref_c, 'ADG_C_002_3B')")

if __name__ == "__main__":
    main()
