# app_planificador_abc.py
# -*- coding: utf-8 -*-
import re, io, traceback, unicodedata, html
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Iterable
import pandas as pd
import streamlit as st

# === Integración ESCO PRO ===
try:
    from esco_mapper_pro import map_competencias_a_esco_pro
except Exception:
    map_competencias_a_esco_pro = None

# (Opcional) módulo externo; si no existe, devolvemos vacíos
try:
    from perfil_competencias import perfil_competencias
except Exception:
    def perfil_competencias(*args, **kwargs):
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

# ========= Config =========
F_A = "RDs_GradoA_Consolidado_por_familia.xlsx"
F_B = "RDs_GradoB_Consolidado_por_familia.xlsx"
F_C = "RDs_GradoC_Consolidado_por_familia.xlsx"

def prefer_fix(path: str) -> str:
    p = Path(path); p_fix = p.with_name("fix_" + p.name)
    return str(p_fix if p_fix.exists() else p)

# ========= Helpers =========
def nz(x, default=""):
    try:
        if not pd.api.types.is_scalar(x):
            return default
    except Exception:
        pass
    try:
        if pd.isna(x): return default
    except Exception:
        pass
    s = str(x).strip()
    return s if s else default

def to_int_or_blank(x):
    try:
        if pd.isna(x): return ""
        m = re.search(r"\d+", str(x))
        return int(m.group(0)) if m else ""
    except Exception:
        return ""

def dedup_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()]

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\s+", " ", c).strip() for c in df.columns]
    df.columns = [c.lower() for c in df.columns]
    return df

def coalesce_cols(df, cols):
    out = pd.Series([""]*len(df), index=df.index, dtype=object)
    for c in cols:
        if c in df.columns:
            cand = df[c].astype(str).str.strip()
            out = out.where(out != "", cand.where(cand != "", out))
    return out

def first_nonempty(series):
    for x in series:
        sx = str(x).strip()
        if sx and sx.lower() not in ("nan", "none"):
            return sx
    return ""

def parse_nom_from_completo(s):
    s = nz(s)
    if not s: return ""
    m = re.match(r"^[A-Z]{3}_[A-Z]_\d{4}(?:_[0-9A-Z]+)?\.\s*(.+)$", s)
    return m.group(1).strip() if m else ""

def extract_cod_b(s):
    s = nz(s)
    if not s: return ""
    m = re.search(r"\b([A-Z]{3}_B_\d{4}(?:_[0-9A-Z]+)?)\b", s)
    return m.group(1) if m else ""

def ensure_cod_b(df: pd.DataFrame, col_candidates) -> pd.Series:
    cod = df.get("cod_b", pd.Series([""]*len(df)))
    cod = cod.astype(str).str.strip()
    if (cod == "").all():
        assembled = pd.Series([""]*len(df), index=df.index, dtype=object)
        for c in col_candidates:
            if c in df.columns:
                extra = df[c].astype(str).map(extract_cod_b)
                assembled = assembled.where(assembled != "", extra.where(extra != "", assembled))
        cod = assembled
    return cod.astype(str).str.strip()

def download_excel_button(dfs_dict: Dict[str, pd.DataFrame], filename: str, label: str):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        for sheet, df in dfs_dict.items():
            (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_excel(
                w, sheet_name=sheet[:31], index=False
            )
    st.download_button(
        label=label,
        data=bio.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

def export_master_with_timestamp(dfs_dict: Dict[str, pd.DataFrame], base_name: str = "PLAN_MAESTRO"):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{base_name}_{ts}.xlsx"
    download_excel_button(dfs_dict, filename, f"💾 Exportar maestro ({filename})")

def extract_codes(df: pd.DataFrame, col: str) -> str:
    if df is None or len(df) == 0 or col not in df.columns:
        return ""
    vals = sorted({nz(x) for x in df[col].tolist() if nz(x)})
    return " ".join(vals)

def copy_to_clipboard_button(label: str, text: str, key: str):
    import streamlit.components.v1 as components
    import html as _html
    safe_text = _html.escape(text, quote=True)
    btn_html = f"""
    <button id="copybtn_{key}" style="
        padding:0.5rem 0.75rem;border-radius:8px;border:1px solid #e0e0e0;
        cursor:pointer;background:#f7f7f7;">{_html.escape(label)}</button>
    <span id="copystate_{key}" style="margin-left:8px;color:#666;"></span>
    <script>
    const btn_{key} = document.getElementById("copybtn_{key}");
    const st_{key} = document.getElementById("copystate_{key}");
    if (btn_{key}) {{
        btn_{key}.onclick = async () => {{
            try {{
                await navigator.clipboard.writeText("{safe_text}");
                st_{key}.textContent = "copiado ✓";
                setTimeout(()=>{{ st_{key}.textContent=""; }}, 1800);
            }} catch(e) {{
                st_{key}.textContent = "no se pudo copiar";
                setTimeout(()=>{{ st_{key}.textContent=""; }}, 2500);
            }}
        }}
    }}
    </script>
    """
    components.html(btn_html, height=40)

# ======== Normalización de familias ========
def norm_txt(s: str) -> str:
    s = nz(s)
    if not s:
        return ""
    s = " ".join(s.split())
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.casefold()

def canonicalize_families(series_iterables: Iterable[pd.Series]):
    by_norm = {}
    for ser in series_iterables:
        for raw in ser:
            f = nz(raw)
            if not f:
                continue
            k = norm_txt(f)
            by_norm.setdefault(k, []).append(f)
    canon = {}
    for k, vals in by_norm.items():
        freq = {}
        for v in vals:
            freq[v] = freq.get(v, 0) + 1
        best = sorted(freq.items(), key=lambda x: (x[1], len(x[0])), reverse=True)[0][0]
        canon[k] = best
    etiquetas = sorted(set(canon.values()))
    return etiquetas, canon

# ========= Estado global =========
if "export_pack" not in st.session_state:
    st.session_state["export_pack"] = {}
if "esco_queue" not in st.session_state:
    st.session_state["esco_queue"] = []
if "last_frames" not in st.session_state:
    st.session_state["last_frames"] = {}

def set_export_sheet(name: str, df: pd.DataFrame):
    if df is None: return
    st.session_state["export_pack"][name] = df.copy()

def get_export_pack() -> Dict[str, pd.DataFrame]:
    return dict(st.session_state.get("export_pack", {}))

def set_last(name: str, df: pd.DataFrame):
    st.session_state["last_frames"][name] = df.copy() if df is not None else None

def get_last(name: str) -> pd.DataFrame:
    return st.session_state["last_frames"].get(name)

def add_to_esco_queue(texts: List[str], source_tag: str):
    seen = set([t.strip() for t in st.session_state["esco_queue"]])
    added = 0
    for t in texts:
        s = nz(t)
        if not s: continue
        if s not in seen:
            st.session_state["esco_queue"].append(s)
            seen.add(s)
            added += 1
    if added > 0:
        st.success(f"Se añadieron {added} elementos a la bandeja ESCO ({source_tag}).")
    else:
        st.info("Nada nuevo que añadir a la bandeja ESCO.")

def get_esco_queue_df() -> pd.DataFrame:
    items = st.session_state.get("esco_queue", [])
    if not items:
        return pd.DataFrame()
    return pd.DataFrame({"texto": [str(x).strip() for x in items if str(x).strip()]})

# ========= Carga & normalización =========
@st.cache_data(show_spinner=False)
def cargar_grado_A():
    dfA = pd.DataFrame()
    xl = pd.ExcelFile(prefer_fix(F_A))
    parts = [xl.parse(sh).assign(_sheet=sh) for sh in xl.sheet_names]
    if parts: dfA = pd.concat(parts, ignore_index=True)
    dfA = dedup_columns(normalize_columns(dfA))
    alias = {
        "familia":"familia",
        "cert_b_completo":"cert_b_completo",
        "cod_cert_b":"cod_b",
        "nom_cert_b":"nom_b",
        "acreditación parcial de competencia":"acred_parcial",
        "acreditacion parcial de competencia":"acred_parcial",
        "cod_acred_parc":"cod_a",
        "nom_acred_parcial":"nom_a",
        "formación a cursar":"ra_texto",
        "formacion a cursar":"ra_texto",
        "duración en el ámbito de gestión del mefd en horas":"horas_a",
        "duracion en el ambito de gestion del mefd en horas":"horas_a",
    }
    for k,v in alias.items():
        if k in dfA.columns: dfA.rename(columns={k:v}, inplace=True)
    for need in ["familia","cert_b_completo","cod_b","nom_b","acred_parcial","cod_a","nom_a","ra_texto","horas_a"]:
        if need not in dfA.columns: dfA[need] = ""
    dfA["cod_b"] = dfA["cod_b"].astype(str).str.strip()
    mask = dfA["cod_b"] == ""
    dfA.loc[mask, "cod_b"] = dfA.loc[mask, "cert_b_completo"].map(parse_nom_from_completo).map(lambda _: extract_cod_b(_))
    dfA["cod_a"] = dfA["cod_a"].astype(str).str.strip()
    dfA["horas_a"] = dfA["horas_a"].map(to_int_or_blank)
    return dfA

@st.cache_data(show_spinner=False)
def cargar_grado_B():
    dfB = pd.DataFrame()
    xl = pd.ExcelFile(prefer_fix(F_B))
    parts = [xl.parse(sh).assign(_sheet=sh) for sh in xl.sheet_names]
    if parts: dfB = pd.concat(parts, ignore_index=True)
    dfB = dedup_columns(normalize_columns(dfB))
    rename_min = {
        "familia":"familia",
        "cert_padre_cod":"cod_c",
        "cert_padre_denom":"nom_c",
        "cert_padre_completo":"cert_c_completo",
        "cod_cert_comp":"cod_b",
        "nom_cert_comp":"nom_b",
        "duracion_mefd_h":"horas_b",
        "formación a cursar":"form_b_texto",
        "formacion a cursar":"form_b_texto",
        "formacion_codigo":"form_b_codigo",
        "formacion_titulo":"form_b_titulo",
    }
    for k,v in rename_min.items():
        if k in dfB.columns: dfB.rename(columns={k:v}, inplace=True)
    dfB["cod_b"] = ensure_cod_b(dfB, ["cod_b","cert_c_completo","nom_b","form_b_texto","form_b_titulo"])
    posibles = []
    for c in ["cert_comp_titulo","nom_b","form_b_titulo","nom_cert_b","denominacion_b","denominación_b","título_b","titulo_b","nombre_b","desc_b"]:
        if c in dfB.columns: posibles.append(c)
    import re as _re
    rg = _re.compile(r"(nom|denomin|t[ií]tulo|titulo|desc)", _re.I)
    for c in dfB.columns:
        if c not in posibles and rg.search(c): posibles.append(c)
    dfB["nom_b"] = coalesce_cols(dfB, posibles).astype(str).str.strip()
    dfB.loc[dfB["nom_b"].str.lower().isin(["", "nan", "none"]), "nom_b"] = ""
    dfB["horas_b"] = dfB.get("horas_b", pd.Series([""]*len(dfB))).map(to_int_or_blank)
    dfB["cod_c"] = dfB.get("cod_c", pd.Series([""]*len(dfB))).astype(str).str.strip()
    dfB["cod_b"] = dfB["cod_b"].astype(str).str.strip()
    return dedup_columns(dfB)

@st.cache_data(show_spinner=False)
def cargar_grado_C():
    dfC = pd.DataFrame()
    xl = pd.ExcelFile(prefer_fix(F_C))
    parts = [xl.parse(sh).assign(_sheet=sh) for sh in xl.sheet_names]
    if parts: dfC = pd.concat(parts, ignore_index=True)
    dfC = dedup_columns(normalize_columns(dfC))
    alias = {"familia":"familia","denominacion":"nom_c","denominación":"nom_c",
             "codigo":"cod_c","código":"cod_c","duracion":"horas_c","duración":"horas_c"}
    for k,v in alias.items():
        if k in dfC.columns: dfC.rename(columns={k:v}, inplace=True)
    nivel_col = None
    for cand in ["nivel","nivel_c","level","nivel (c)","nivel c"]:
        if cand in dfC.columns: nivel_col = cand; break
    if nivel_col is None: dfC["nivel_c"] = ""
    else: dfC["nivel_c"] = dfC[nivel_col]
    dfC["cod_c"] = dfC.get("cod_c", pd.Series([""]*len(dfC))).astype(str).str.trim()
    dfC["horas_c"] = dfC.get("horas_c", pd.Series([""]*len(dfC))).map(to_int_or_blank)
    def norm_level(x):
        s = nz(x).lower(); m = re.search(r"\d+", s)
        return m.group(0) if m else ("Desconocido" if s=="" else s)
    dfC["nivel_c"] = dfC["nivel_c"].map(norm_level)
    return dfC[["familia","cod_c","nom_c","horas_c","nivel_c"]].drop_duplicates()

# ========= Relaciones y refs + Niveles inferidos =========
@st.cache_data(show_spinner=False)
def construir_mapas(dfA, dfB, dfC):
    map_b_a = (
        dfA[["cod_b","cod_a","nom_a","ra_texto","horas_a","familia"]]
        .dropna(subset=["cod_b","cod_a"])
        .query("cod_b != '' and cod_a != ''")
        .drop_duplicates()
    )
    map_c_b = (
        dfB[["cod_c","cod_b","familia"]]
        .dropna(subset=["cod_c","cod_b"])
        .query("cod_c != '' and cod_b != ''")
        .drop_duplicates()
    )
    ref_c = (
        dfC.groupby("cod_c", as_index=False)
           .agg(nom_c=("nom_c","first"),
                horas_c=("horas_c","first"),
                familias=("familia", lambda s: ", ".join(sorted(set(nz(x) for x in s if nz(x))))),
                nivel_c=("nivel_c", first_nonempty))
    )
    b2niveles = (map_c_b.merge(dfC[["cod_c","nivel_c"]], on="cod_c", how="left")
                      .groupby("cod_b")["nivel_c"]
                      .apply(lambda s: sorted(set([nz(x) for x in s if nz(x)])))
                      .to_dict())
    def pick_b_level(levels):
        if not levels: return "Desconocido"
        uniq = [x for x in levels if x]
        return "/".join(uniq) if len(uniq)>1 else uniq[0]
    ref_b_B = (
        dfB.groupby("cod_b", as_index=False)
           .agg(nom_b=("nom_b", first_nonempty),
                horas_b=("horas_b","first"),
                familias=("familia", lambda s: ", ".join(sorted(set(nz(x) for x in s if nz(x))))))
    )
    dfA_bnames = dfA[["cod_b","cert_b_completo"]].dropna().copy()
    dfA_bnames["fallback_nom_b"] = dfA_bnames["cert_b_completo"].map(parse_nom_from_completo)
    dfA_bnames = dfA_bnames[dfA_bnames["fallback_nom_b"]!=""].drop_duplicates(subset=["cod_b"])[["cod_b","fallback_nom_b"]]
    ref_b = ref_b_B.merge(dfA_bnames, on="cod_b", how="left")
    ref_b["nom_b"] = ref_b.apply(lambda r: nz(r["nom_b"]) or nz(r["fallback_nom_b"]), axis=1)
    ref_b.drop(columns=["fallback_nom_b"], inplace=True, errors="ignore")
    ref_b["nivel_b"] = ref_b["cod_b"].map(lambda cb: pick_b_level(b2niveles.get(cb, [])))
    a2niveles = (map_b_a.merge(ref_b[["cod_b","nivel_b"]], on="cod_b", how="left")
                      .groupby("cod_a")["nivel_b"]
                      .apply(lambda s: sorted(set([nz(x) for x in s if nz(x)])))
                      .to_dict())
    return map_b_a, map_c_b, ref_b, ref_c, a2niveles

# ========= UI =========
st.set_page_config(page_title="Planificador A ↔ B ↔ C + ESCO (PRO)", layout="wide")
st.title("Planificador de itinerarios: A → B → C (multi-familia) + Niveles + Perfil + ESCO (PRO)")

with st.sidebar:
    st.header("Fuentes de datos")
    st.caption("Se usan automáticamente ficheros fix_* si existen.")
    st.write(f"- A: `{prefer_fix(F_A)}`")
    st.write(f"- B: `{prefer_fix(F_B)}`")
    st.write(f"- C: `{prefer_fix(F_C)}`")

    misma_familia = not st.checkbox("Ignorar familia en las vinculaciones", value=True)
    exigir_mismo_nivel = st.checkbox("Exigir mismo nivel (A→B y B→C)", value=True)
    debug_mode = st.toggle("Mostrar trazas de error (debug)", value=False)

# Carga + familias/niveles
try:
    dfA = cargar_grado_A(); dfB = cargar_grado_B(); dfC = cargar_grado_C()
    map_b_a, map_c_b, ref_b, ref_c, a2niveles = construir_mapas(dfA, dfB, dfC)
    familias_all, fam_canon_map = canonicalize_families([
        dfA.get("familia", pd.Series()).astype(str),
        dfB.get("familia", pd.Series()).astype(str),
        dfC.get("familia", pd.Series()).astype(str),
    ])
    niveles_all = sorted({nz(x) for x in ref_c.get("nivel_c", pd.Series())} - {""}) or ["Desconocido"]
    with st.sidebar:
        st.header("Filtros previos")
        fam_sel = st.multiselect("Familias (opcional)", familias_all, default=[])
        niv_sel = st.multiselect("Niveles (opcional)", niveles_all, default=[])
    norm_sel = {norm_txt(x) for x in (fam_sel or [])}
except Exception as e:
    if debug_mode:
        st.exception(e); st.code(traceback.format_exc())
    else:
        st.error(f"Error cargando datos: {e}")
    st.stop()

# === Export Master + Bandeja ESCO ===
if "export_pack" not in st.session_state: st.session_state["export_pack"] = {}
with st.sidebar:
    st.header("Exportación Maestro")
    if st.button("💾 Exportar todo lo calculado (con timestamp)"):
        pack = dict(st.session_state["export_pack"])
        esco_df = get_esco_queue_df()
        if not esco_df.empty:
            pack["ESCO_bandeja"] = esco_df
        if not pack:
            st.warning("Aún no hay resultados calculados para exportar.")
        else:
            export_master_with_timestamp(pack, base_name="PLAN_MAESTRO")
    st.caption("Incluye lo último calculado en cada pestaña y la bandeja ESCO.")
    st.divider()
    st.header("Bandeja ESCO")
    st.caption("Acumula textos enviados con “Mandar a ESCO”.")
    esco_items = st.session_state.get("esco_queue", [])
    st.write(f"Ítems en bandeja: **{len(esco_items)}**")
    if esco_items:
        with st.expander("👁️ Vista previa de la bandeja ESCO (sin mapear)"):
            st.dataframe(pd.DataFrame({"texto": esco_items}), use_container_width=True, hide_index=True)
        if st.button("🧹 Vaciar bandeja ESCO"):
            st.session_state["esco_queue"] = []
            st.success("Bandeja ESCO vaciada.")

tabs = st.tabs([
    "🔎 Desde A → B",
    "🔀 Desde B → C",
    "🎯 Perfil de Competencias",
    "🧭 Mapeo a ESCO (PRO)"
])

# ---------------------- (pestañas 1 y 2 y 3: igual que tu versión funcional previa) ----------------------
# Para abreviar: usa aquí tus bloques que ya te funcionan (plan_desde_As, plan_desde_Bs, perfil, etc.)
# Mantengo solo la pestaña 4 completa (ES PRO). Si necesitas que re-ponga 1/2/3 enteras, pídemelo y las incluyo.

# --- Tab 4: Mapeo a ESCO (PRO) — CORREGIDO ---
with tabs[3]:
    st.subheader("Mapeo de competencias → ESCO (skills & occupations) — PRO")
    st.caption("Usa API ESCO + fuzzy (opcional: embeddings / LLM).")

    queue_items = [t.strip() for t in st.session_state.get("esco_queue", []) if str(t).strip()]
    n_queue = len(queue_items)

    fuente = st.radio(
        "Fuente de las competencias a mapear",
        [f"Cuadro de texto", f"Bandeja ESCO ({n_queue})"],
        horizontal=True,
        key="esco_source_radio"
    )

    if fuente.startswith("Cuadro"):
        if "esco_textarea" not in st.session_state:
            st.session_state["esco_textarea"] = "\n".join(queue_items)
        if st.button("⤵️ Rellenar cuadro con bandeja ESCO"):
            st.session_state["esco_textarea"] = "\n".join(queue_items)
        comps_txt = st.text_area(
            "Competencias a mapear (una por línea)",
            st.session_state["esco_textarea"],
            height=220,
            key="esco_textarea"
        )
        to_map = [t.strip() for t in comps_txt.splitlines() if t.strip()]
    else:
        st.write(f"Ítems en bandeja: **{n_queue}**")
        if n_queue:
            with st.expander("Vista previa de la bandeja ESCO"):
                st.dataframe(pd.DataFrame({"texto": queue_items}), use_container_width=True, hide_index=True)
        to_map = list(queue_items)

    colK1, colK2 = st.columns(2)
    with colK1:
        top_k_skills = st.slider("Top-K skills ESCO", 1, 50, 8, 1, key="k_skills")
        alpha = st.number_input("Peso fuzzy (α)", 0.0, 1.0, 0.60, 0.05, key="alpha")
    with colK2:
        top_k_occs   = st.slider("Top-K ocupaciones por skill", 1, 50, 8, 1, key="k_occs")
        beta = st.number_input("Peso embeddings (β)", 0.0, 1.0, 0.40, 0.05, key="beta")
    use_llm = st.checkbox("Rerank con LLM (OpenAI)", value=False, key="use_llm")

    lanzar = st.button("▶️ Ejecutar mapeo", type="primary", use_container_width=True)
    st.markdown(f"**Diagnóstico:** entradas recibidas para mapear: `{len(to_map)}`")

    cont_info = st.container()
    cont_sk   = st.container()
    cont_occ  = st.container()
    cont_uni  = st.container()
    cont_dl   = st.container()

    df_s = df_o = None
    if lanzar:
        if not to_map:
            st.warning("No hay competencias para mapear. Escribe en el cuadro o usa la bandeja ESCO.")
        elif map_competencias_a_esco_pro is None:
            st.error("No se encontró `esco_mapper_pro.py` o faltan dependencias.")
        else:
            try:
                df_s, df_o = map_competencias_a_esco_pro(
                    to_map,
                    top_k_skills=top_k_skills,
                    top_k_occs=top_k_occs,
                    alpha_fuzzy=alpha, beta_emb=beta,
                    use_llm_rerank=use_llm
                )
            except Exception as e:
                st.error(f"Error llamando a esco_mapper_pro: {e}")
                st.code(traceback.format_exc())

    with cont_info:
        n_inputs = len(set([str(x).strip() for x in to_map])) if to_map else 0
        n_s = 0 if df_s is None else len(df_s)
        n_o = 0 if df_o is None else len(df_o)
        st.info(f"Resultados crudos — inputs: {n_inputs} · filas skills: {n_s} · filas ocupaciones: {n_o}")

    min_score = st.slider("Puntaje mínimo (score para mostrar)", 0.0, 1.0, 0.30, 0.05, key="min_score")
    top_k_por_input = st.number_input("Máximo de matches por input (Top-K para mostrar)", 1, 50, 8, 1, key="k_por_input")

    def vista_muchos_a_muchos(df: pd.DataFrame, nombre: str) -> pd.DataFrame:
        if df is None or df.empty:
            st.warning(f"No hay filas en {nombre}."); return pd.DataFrame()
        for need in ["input_text", "score"]:
            if need not in df.columns:
                st.error(f"{nombre}: falta columna `{need}` en la salida."); return pd.DataFrame()
        view = df[df["score"] >= min_score].copy()
        view.sort_values(["input_text", "score"], ascending=[True, False], inplace=True)
        view["rank_in_input"] = view.groupby("input_text").cumcount() + 1
        view = view[view["rank_in_input"] <= top_k_por_input]
        if view.empty and len(df) > 0:
            st.warning(f"{nombre}: sin filas tras filtros. Baja el score mínimo o sube el Top-K.")
        return view

    df_s_view = vista_muchos_a_muchos(df_s, "Skills ESCO (crudo)") if (df_s is not None) else pd.DataFrame()
    df_o_view = vista_muchos_a_muchos(df_o, "Ocupaciones ESCO (crudo)") if (df_o is not None) else pd.DataFrame()

    with cont_sk:
        st.markdown("### Resultados: **Skills ESCO (muchos-a-muchos)**")
        st.dataframe(df_s_view, use_container_width=True, hide_index=True)
    with cont_occ:
        st.markdown("### Resultados: **Ocupaciones relacionadas (muchos-a-muchos)**")
        st.dataframe(df_o_view, use_container_width=True, hide_index=True)

    with cont_uni:
        st.markdown("## Tabla unificada: Competencias origen → Competencias ESCO")
        def _safe(df, cols):
            if df is None or df.empty: return pd.DataFrame(columns=cols)
            for c in cols:
                if c not in df.columns: df[c] = ""
            return df[cols].copy()
        cols_s = ["input_text", "esco_skill_label", "esco_skill_uri", "score", "method"]
        df_map = _safe(df_s_view, cols_s)
        df_map.rename(columns={
            "input_text": "competencia_origen",
            "esco_skill_label": "esco_skill",
            "esco_skill_uri": "esco_skill_uri",
            "score": "score",
            "method": "metodo"
        }, inplace=True)
        if not df_o_view.empty:
            occ_cols = ["input_text", "esco_skill_uri", "esco_occ_label", "esco_occ_uri", "score"]
            dfo = _safe(df_o_view, occ_cols).copy()
            dfo.rename(columns={
                "input_text": "competencia_origen",
                "esco_occ_label": "esco_occupation",
                "esco_occ_uri": "esco_occupation_uri",
                "score": "occ_score"
            }, inplace=True)
            dfo_grp = (dfo
                .assign(pair=lambda r: r["esco_occupation"] + " ⟨" + r["esco_occupation_uri"] + "⟩")
                .groupby(["competencia_origen","esco_skill_uri"], as_index=False)
                .agg(ocupaciones=("pair", lambda s: " | ".join(list(dict.fromkeys([str(x) for x in s]))))))
            df_map = df_map.merge(dfo_grp, on=["competencia_origen","esco_skill_uri"], how="left")
        else:
            if not df_map.empty: df_map["ocupaciones"] = ""
        df_map.sort_values(["competencia_origen","score"], ascending=[True, False], inplace=True)
        st.dataframe(df_map, use_container_width=True, hide_index=True)

    with cont_dl:
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        download_excel_button({"ESCO_mapeo_unificado": df_map},
                              f"ESCO_mapeo_unificado_{ts}.xlsx",
                              "💾 Descargar Excel (tabla unificada)")
        def extract_uris(df, col):
            if df is None or df.empty or col not in df.columns: return ""
            vals = sorted({str(x).strip() for x in df[col].tolist() if str(x).strip()})
            return " ".join(vals)
        codes_to_copy = extract_uris(df_map, "esco_skill_uri")
        copy_to_clipboard_button("📋 Copiar URIs de skills (visibles)", codes_to_copy, key="copy_unificado_skills")
        # Deja la tabla lista para exportación maestro
        st.session_state["export_pack"]["ESCO_mapeo_unificado"] = df_map.copy()
