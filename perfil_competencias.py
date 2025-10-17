# -*- coding: utf-8 -*-
import pandas as pd

# --------- Helpers internos ---------
def _try_cols(df, cols):
    """Devuelve sub-dataframe con columnas que existan (ignora las que no)."""
    keep = [c for c in cols if c in df.columns]
    return df[keep].copy() if keep else pd.DataFrame(columns=cols)

def _nom_cols_uc(dfC):
    """
    Intenta detectar columnas para UCs (grado C).
    Devuelve: {'cod_uc': <col_cod>, 'desc_uc': <col_desc>} o {} si no encuentra.
    """
    # candidatos por nombre
    cand_cod = [c for c in dfC.columns if c.upper().startswith("UC") or "CODIGO_UC" in c.upper() or "COD_UC" in c.upper()]
    cand_desc = [c for c in dfC.columns if "DESCRIP" in c.upper() or "ESTANDAR_COMP" in c.upper() or "NOMBRE_UC" in c.upper()]

    cod_uc = None
    desc_uc = None
    for c in ["CODIGO_UC","COD_UC","UC_CODIGO","UC","CODIGO_COMP","CODIGO_ESTANDAR","COD_ESTANDAR"]:
        if c in dfC.columns:
            cod_uc = c; break
    if not cod_uc and cand_cod:
        cod_uc = cand_cod[0]

    for c in ["DESCRIPCION_UC","UC_DESCRIPCION","NOMBRE_UC","ESTANDAR_COMP","DESCRIPCION_ESTANDAR","DESC_UC"]:
        if c in dfC.columns:
            desc_uc = c; break
    if not desc_uc and cand_desc:
        desc_uc = cand_desc[0]

    if cod_uc and desc_uc:
        return {"cod_uc": cod_uc, "desc_uc": desc_uc}
    return {}

# --------- API principal ---------
def perfil_competencias(as_hechas, bs_hechos, cs_hechos,
                        dfA, dfB, dfC, map_b_a):
    """
    Genera el perfil de competencias de una persona:
      - as_hechas: iterable de códigos A (p.ej. {"ADG_A_0156_01", ...})
      - bs_hechos: iterable de códigos B (p.ej. {"AFD_B_3003", ...})
      - cs_hechos: iterable de códigos C (p.ej. {"ADG_C_001_3B", ...})  (opcional)

    Requiere:
      - dfA (con al menos columnas: cod_a, nom_a, ra_texto, cod_b, nom_b, familia)
      - dfB (para nombres/horas de B: cod_b, nom_b, horas_b, familias si la tienes)
      - dfC (si trae UCs, mejor)
      - map_b_a (relación B->A: columnas cod_b, cod_a, nom_a, ra_texto, horas_a, familia)

    Devuelve tres DataFrames:
      competencias_A, competencias_B, competencias_C
    """
    as_hechas = {str(x).strip() for x in (as_hechas or []) if str(x).strip()}
    bs_hechos = {str(x).strip() for x in (bs_hechos or []) if str(x).strip()}
    cs_hechos = {str(x).strip() for x in (cs_hechos or []) if str(x).strip()}

    # ---- A: tomamos la RA como competencia textual alcanzada
    dfA_min = _try_cols(dfA, ["familia","cod_a","nom_a","ra_texto","cod_b","nom_b"])
    comp_A = (
        dfA_min[dfA_min["cod_a"].isin(as_hechas)]
        .drop_duplicates(subset=["cod_a","nom_a","ra_texto"])
        .rename(columns={
            "cod_a":"COD_A",
            "nom_a":"NOM_A",
            "ra_texto":"COMPETENCIA_TEXTUAL_RA",
            "cod_b":"B_RELACIONADO",
            "nom_b":"NOM_B_RELACIONADO",
            "familia":"FAMILIA_A"
        })
        .sort_values(["COD_A"])
        .reset_index(drop=True)
    )

    # ---- B: un B implica poseer las A que lo componen (según map_b_a)
    m = map_b_a.copy()
    m["cod_b"] = m["cod_b"].astype(str).str.strip()
    m["cod_a"] = m["cod_a"].astype(str).str.strip()
    m = m[(m["cod_b"]!="") & (m["cod_a"]!="")]

    dfA_pick = _try_cols(dfA, ["cod_a","nom_a","ra_texto"]).drop_duplicates("cod_a")
    as_por_b = (
        m[m["cod_b"].isin(bs_hechos)]
        .merge(dfA_pick, on="cod_a", how="left")
        .rename(columns={"cod_b":"COD_B","cod_a":"COD_A","nom_a":"NOM_A","ra_texto":"COMPETENCIA_TEXTUAL_RA"})
    )

    # referencia de B (si tus dfB la traen)
    ref_b_cols = [c for c in ["cod_b","nom_b","horas_b","familias"] if c in dfB.columns]
    if ref_b_cols:
        ref_b_df = dfB[ref_b_cols].drop_duplicates(subset=["cod_b"]).rename(columns={"cod_b":"COD_B"})
    else:
        ref_b_df = pd.DataFrame(columns=["COD_B","NOM_B","HORAS_B","FAMILIAS_B"])

    comp_B = (
        as_por_b.merge(ref_b_df, on="COD_B", how="left")
        .rename(columns={"nom_b":"NOM_B","horas_b":"HORAS_B","familias":"FAMILIAS_B"})
        .sort_values(["COD_B","COD_A"])
        .reset_index(drop=True)
    )

    # ---- C: listar UCs si existen en dfC
    uc_map = _nom_cols_uc(dfC)
    if uc_map:
        baseC = _try_cols(dfC, ["familia","cod_c","nom_c","horas_c", uc_map["cod_uc"], uc_map["desc_uc"]])
        comp_C = (
            baseC[baseC["cod_c"].astype(str).str.strip().isin(cs_hechos)]
            .rename(columns={
                "familia":"FAMILIA_C",
                "cod_c":"COD_C",
                "nom_c":"NOM_C",
                "horas_c":"HORAS_C",
                uc_map["cod_uc"]:"COD_UC",
                uc_map["desc_uc"]:"DESC_UC"
            })
            .dropna(subset=["COD_UC"])
            .drop_duplicates(subset=["COD_C","COD_UC","DESC_UC"])
            .sort_values(["COD_C","COD_UC"])
            .reset_index(drop=True)
        )
    else:
        comp_C = pd.DataFrame(columns=["COD_C","NOM_C","HORAS_C","FAMILIA_C","COD_UC","DESC_UC"])

    return comp_A, comp_B, comp_C


def exportar_perfil_competencias(as_hechas, bs_hechos, cs_hechos,
                                 dfA, dfB, dfC, map_b_a,
                                 out_xlsx="PERFIL_COMPETENCIAS.xlsx"):
    """
    Exporta a Excel el perfil de competencias en 3 pestañas:
      - Competencias_desde_A
      - Competencias_desde_B
      - Competencias_desde_C
    """
    comp_A, comp_B, comp_C = perfil_competencias(as_hechas, bs_hechos, cs_hechos, dfA, dfB, dfC, map_b_a)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        comp_A.to_excel(w, sheet_name="Competencias_desde_A", index=False)
        comp_B.to_excel(w, sheet_name="Competencias_desde_B", index=False)
        comp_C.to_excel(w, sheet_name="Competencias_desde_C", index=False)
    return out_xlsx
