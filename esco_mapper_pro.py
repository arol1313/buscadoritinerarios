# esco_mapper_pro.py
# -*- coding: utf-8 -*-
"""
Mapeo PRO de competencias de texto → ESCO (skills y ocupaciones)
Usa la API oficial de ESCO (quick mode) + fuzzy. Embeddings/LLM opcionales.
Requisitos: requests, rapidfuzz, pandas
Opcionales: sentence-transformers, openai (si activas rerank con LLM)
"""

from __future__ import annotations
import requests, math
import pandas as pd

ESCO_BASE = "https://ec.europa.eu/esco/api"

# ---------- Utilidades ----------
def _nz(x, d=""):
    s = "" if x is None else str(x).strip()
    return s if s else d

def _safe_get_label(item, lang="es"):
    # ESCO quick devuelve preferredLabel por idioma o title
    pl = item.get("preferredLabel") or {}
    return _nz(pl.get(lang) or item.get("title") or "")

def _safe_score(item):
    try:
        v = item.get("score", None)
        if v is None: return None
        return float(v)
    except Exception:
        return None

# Fuzzy (rápido y sin dependencias fuertes)
try:
    from rapidfuzz import fuzz
    def fuzzy_sim(a, b):
        if not a or not b: return 0.0
        return fuzz.WRatio(a, b) / 100.0
except Exception:
    def fuzzy_sim(a, b):
        return 0.0

# ---------- Búsquedas ESCO ----------
def _esco_search(text, types=("skill",), lang="es", limit=25, offset=0):
    """
    /search en quick mode (full=false). 'types' debe contener 'skill' y/o 'occupation'
    """
    params = {
        "text": text,
        "language": lang,
        "full": "false",
        "limit": int(limit),
        "offset": int(offset),
        "selectedVersion": "latest",
        "viewObsolete": "false",
    }
    # ESCO espera 'type' repetido; requests admite lista y lo repite
    params["type"] = list(types)

    r = requests.get(f"{ESCO_BASE}/search", params=params, headers={"Accept-Language": lang}, timeout=30)
    r.raise_for_status()
    return r.json()

def esco_search_skills(query: str, lang="es", limit=25, offset=0):
    data = _esco_search(query, types=("skill",), lang=lang, limit=limit, offset=offset)
    out = []
    for it in data.get("_embedded", {}).get("results", []):
        out.append({
            "esco_skill_uri": _nz(it.get("uri")),
            "esco_skill_label": _safe_get_label(it, lang=lang),
            "score": _safe_score(it),
        })
    return out

def esco_search_occupations(query: str, lang="es", limit=25, offset=0):
    data = _esco_search(query, types=("occupation",), lang=lang, limit=limit, offset=offset)
    out = []
    for it in data.get("_embedded", {}).get("results", []):
        out.append({
            "esco_occ_uri": _nz(it.get("uri")),
            "esco_occ_label": _safe_get_label(it, lang=lang),
            "score": _safe_score(it),
        })
    return out

# ---------- Núcleo PRO ----------
def map_competencias_a_esco_pro(
    textos: list[str],
    *,
    language: str = "es",
    top_k_skills: int = 8,
    top_k_occs: int = 8,
    alpha_fuzzy: float = 0.60,      # peso fuzzy
    beta_emb: float = 0.40,         # placeholder si añades embeddings
    use_llm_rerank: bool = False    # placeholder si añades LLM
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Devuelve dos DataFrames:
      - df_skills: columnas => input_text, esco_skill_label, esco_skill_uri, score, method
      - df_occs:   columnas => input_text, esco_occ_label, esco_occ_uri, score, method
    score = combinación de 'score' ESCO (si viene) + fuzzy con el texto original.
    """
    textos = [t.strip() for t in textos if _nz(t)]
    if not textos:
        return pd.DataFrame(), pd.DataFrame()

    rows_s, rows_o = [], []

    for q in textos:
        # SKILLS
        try:
            sk = esco_search_skills(q, lang=language, limit=50)
        except Exception:
            sk = []
        # rescoring: combino score ESCO (0..1 aprox) con fuzzy
        rescored = []
        for it in sk:
            es = it.get("score", 0.0) or 0.0
            fz = fuzzy_sim(q, it.get("esco_skill_label", ""))
            score = alpha_fuzzy * fz + (1.0 - alpha_fuzzy) * (es if es is not None else 0.0)
            rescored.append((score, it))
        rescored.sort(key=lambda x: x[0], reverse=True)
        rescored = rescored[:max(1, int(top_k_skills))]
        for sc, it in rescored:
            rows_s.append({
                "input_text": q,
                "esco_skill_label": _nz(it.get("esco_skill_label")),
                "esco_skill_uri": _nz(it.get("esco_skill_uri")),
                "score": float(sc),
                "method": "search+fuzzy"
            })

        # OCCUPATIONS (por texto original, opcionalmente podrías buscar por cada skill)
        try:
            oc = esco_search_occupations(q, lang=language, limit=50)
        except Exception:
            oc = []
        rescored_o = []
        for it in oc:
            es = it.get("score", 0.0) or 0.0
            fz = fuzzy_sim(q, it.get("esco_occ_label", ""))
            score = alpha_fuzzy * fz + (1.0 - alpha_fuzzy) * (es if es is not None else 0.0)
            rescored_o.append((score, it))
        rescored_o.sort(key=lambda x: x[0], reverse=True)
        rescored_o = rescored_o[:max(1, int(top_k_occs))]
        for sc, it in rescored_o:
            rows_o.append({
                "input_text": q,
                "esco_occ_label": _nz(it.get("esco_occ_label")),
                "esco_occ_uri": _nz(it.get("esco_occ_uri")),
                "score": float(sc),
                "method": "search+fuzzy"
            })

    df_s = pd.DataFrame(rows_s, columns=["input_text","esco_skill_label","esco_skill_uri","score","method"])
    df_o = pd.DataFrame(rows_o, columns=["input_text","esco_occ_label","esco_occ_uri","score","method"])
    return df_s, df_o
