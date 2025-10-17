# esco_mapper_pro.py
# -*- coding: utf-8 -*-
"""
Mapeo PRO de competencias de texto → ESCO (skills y ocupaciones)
- API ESCO en quick mode (full=false)
- Fuzzy con rapidfuzz
- Reintentos y headers robustos
Requisitos: requests, rapidfuzz, pandas
"""

from __future__ import annotations
import time
import math
import requests
import pandas as pd

ESCO_BASE = "https://ec.europa.eu/esco/api"

HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "es",
    "User-Agent": "Planificador-ABC/1.0 (+https://example.local)"
}

def _nz(x, d=""):
    s = "" if x is None else str(x).strip()
    return s if s else d

def _safe_get_label(item, lang="es"):
    pl = item.get("preferredLabel") or {}
    return _nz(pl.get(lang) or item.get("title") or "")

def _safe_score(item):
    try:
        v = item.get("score", None)
        if v is None: return None
        return float(v)
    except Exception:
        return None

# Fuzzy (rápido)
try:
    from rapidfuzz import fuzz
    def fuzzy_sim(a, b):
        if not a or not b: return 0.0
        return fuzz.WRatio(a, b) / 100.0
except Exception:
    def fuzzy_sim(a, b):
        return 0.0

# -------- core HTTP with retries --------
def _get_json_with_retries(url, params, headers=HEADERS, max_retries=3, timeout=30):
    last_err = None
    for i in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json(), None
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (i + 1))
    return None, last_err

def _esco_search(text, types=("skill",), lang="es", limit=25, offset=0):
    params = {
        "text": text,
        "language": lang,
        "full": "false",
        "limit": int(limit),
        "offset": int(offset),
        "selectedVersion": "latest",
        "viewObsolete": "false",
        "type": list(types),  # requests generará type=skill&type=occupation si hay más de uno
    }
    data, err = _get_json_with_retries(f"{ESCO_BASE}/search", params)
    if err:
        raise RuntimeError(f"Fallo /search ESCO: {err}")
    return data

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

def map_competencias_a_esco_pro(
    textos: list[str],
    *,
    language: str = "es",
    top_k_skills: int = 8,
    top_k_occs: int = 8,
    alpha_fuzzy: float = 0.60,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Devuelve dos DataFrames (skills, occupations) y un dict de métricas:
      - df_skills: input_text, esco_skill_label, esco_skill_uri, score, method
      - df_occs:   input_text, esco_occ_label, esco_occ_uri, score, method
      - metrics:   {'n_inputs':..., 'calls_skills':..., 'calls_occs':..., 'elapsed_s':...}
    """
    textos = [t.strip() for t in textos if _nz(t)]
    metrics = {"n_inputs": len(textos), "calls_skills": 0, "calls_occs": 0, "elapsed_s": 0.0}
    if not textos:
        return pd.DataFrame(), pd.DataFrame(), metrics

    t0 = time.time()
    rows_s, rows_o = [], []

    for q in textos:
        # SKILLS
        sk = esco_search_skills(q, lang=language, limit=50)
        metrics["calls_skills"] += 1
        rescored = []
        for it in sk:
            es = it.get("score", 0.0) or 0.0
            fz = fuzzy_sim(q, it.get("esco_skill_label", ""))
            score = alpha_fuzzy * fz + (1.0 - alpha_fuzzy) * es
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

        # OCCUPATIONS por texto original
        oc = esco_search_occupations(q, lang=language, limit=50)
        metrics["calls_occs"] += 1
        rescored_o = []
        for it in oc:
            es = it.get("score", 0.0) or 0.0
            fz = fuzzy_sim(q, it.get("esco_occ_label", ""))
            score = alpha_fuzzy * fz + (1.0 - alpha_fuzzy) * es
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

    metrics["elapsed_s"] = round(time.time() - t0, 3)
    df_s = pd.DataFrame(rows_s, columns=["input_text","esco_skill_label","esco_skill_uri","score","method"])
    df_o = pd.DataFrame(rows_o, columns=["input_text","esco_occ_label","esco_occ_uri","score","method"])
    return df_s, df_o, metrics
