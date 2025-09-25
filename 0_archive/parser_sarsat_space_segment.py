# parser_sarsat_space_segment.py
# ============================================================================
# Script Name: parser_sarsat_space_segment.py
# Last Updated (UTC): 2025-09-22
# Update Summary:
# - v2 robust parser: pdfplumber tables + stream fallback + aggressive text harvest
# - Fuzzy header normalization, auto name-column detection
# - Accepts CLI args for input PDF and output directory; writes catalog + raw rows + log
# Description:
#   Parses the Cospas–SARSAT "Current Space Segment Status and SAR Payloads" PDF
#   to produce a pipeline-ready CSV catalog of SAR-capable satellites (LEO/GEO/MEO).
# External Data Sources:
#   - The PDF itself (downloaded by user)
# Internal Variables:
#   - Uses pdfplumber if available for table extraction
#   - Falls back to PyMuPDF (fitz) or PyPDF to get text and salvage names
# Produced DataFrames:
#   - df_catalog: columns = [
#       name, operator, sar_status, orbit_type, constellation,
#       launch_date, notes, source_primary, record_asof
#     ]
# Data Handling Notes:
#   - Best-effort parsing; unknown/missing -> NaN (to store as NULL in SQL)
#   - Saves a raw rows dump for troubleshooting (before normalization)
# ============================================================================

from __future__ import annotations

import sys
import re
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np

# ---------- Optional backends ----------
_have_pdfplumber = False
_have_fitz = False
_have_pypdf = False
try:
    import pdfplumber  # type: ignore
    _have_pdfplumber = True
except Exception:
    pass

if not _have_pdfplumber:
    try:
        import fitz  # PyMuPDF
        _have_fitz = True
    except Exception:
        try:
            from pypdf import PdfReader
            _have_pypdf = True
        except Exception:
            pass

# ---------- CLI / Paths ----------
# ----- Robust path handling (replaces DEFAULT_INPUT/OUTDIR block) -----
import argparse
from pathlib import Path

def resolve_paths() -> tuple[Path, Path]:
    parser = argparse.ArgumentParser(description="Parse COSPAS–SARSAT Space Segment PDF to CSV.")
    parser.add_argument("-i", "--input", type=str, help="Path to the Space Segment PDF")
    parser.add_argument("-o", "--outdir", type=str, help="Output directory (defaults to input's folder)")
    args = parser.parse_args()

    default_pdf = Path(r"C:\Users\gehig\Projects\RescueDecisionSystems\data\reference\satellites\SARSAT space segment.pdf")
    if args.input:
        input_pdf = Path(args.input).expanduser().resolve()
    else:
        input_pdf = default_pdf

    if not input_pdf.exists():
        raise FileNotFoundError(f"Input PDF not found: {input_pdf}")

    out_dir = Path(args.outdir).expanduser().resolve() if args.outdir else input_pdf.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[paths] INPUT_PDF = {input_pdf}")
    print(f"[paths] OUT_DIR   = {out_dir}")
    return input_pdf, out_dir

INPUT_PDF, OUT_DIR = resolve_paths()
RAW_ROWS_CSV = OUT_DIR / "sarsat_space_segment_rows_raw.csv"
CATALOG_CSV  = OUT_DIR / "sarsat_satellites.csv"
LOG_PATH     = OUT_DIR / "sarsat_space_segment_parse.log"

# ---------- Heuristics / Regex ----------
# Broad, non-capturing satellite name regex (no capture groups to avoid pandas warning)
SAT_NAME_RE = re.compile(
    r'(?:\b(?:'
    r'GOES[-\s]?\d{1,2}|HIMAWARI[-\s]?\d+|INSAT[-\s]?\d+[DR]?|'
    r'METEOSAT[-\s]?\d+|MSG[-\s]?\d+|MTG[-\s]?(?:I|S)?\d*|'
    r'(?:ELEKTRO|ELECTRO)[-\s]?L?\d*|GK[-\s]?2A|GEO[-\s]?KOMPSAT[-\s]?2A|'
    r'NOAA[-\s]?\d+|METOP(?:-[ABC])?|COSPAS|SARSAT|'
    r'GALILEO|GSAT[-\s]?\d+|NAVSTAR|GPS(?:[-\s]?[IVX]+)?|GLONASS(?:-K2?)?|'
    r'BEIDOU|BDS|BD-\d+'
    r')\b)',
    re.IGNORECASE
)

# Short LEOSAR identifiers (e.g., S11, C5). Use only when the section = LEOSAR
LEO_SHORT_RE = re.compile(r'\b(?:S|C)-?\d{1,2}\b', re.IGNORECASE)

# ---------- Utilities ----------
def _clean_cell(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _guess_section_title(text: str) -> str:
    t = text.upper()
    if "LEOSAR" in t:
        return "LEOSAR"
    if "GEOSAR" in t:
        return "GEOSAR"
    if "MEOSAR" in t:
        return "MEOSAR"
    return ""

def _normalize_header_fuzzy(h: str) -> str:
    s = _clean_cell(h).lower().replace("/", " ").replace("-", " ")
    toks = set(s.split())
    if {"satellite","name"} & toks or "spacecraft" in toks or "vehicle" in toks or "satellites" in toks:
        return "name"
    if "operator" in toks or "owner" in toks:
        return "operator"
    if "sar" in toks and ({"status","payload","transponder"} & toks):
        return "sar_status"
    if "orbit" in toks or "orbital" in toks:
        return "orbit"
    if "constellation" in toks or "gnss" in toks:
        return "constellation"
    if "launch" in toks and "date" in toks:
        return "launch_date"
    if "notes" in toks or "remarks" in toks:
        return "notes"
    return _clean_cell(h).lower()

def _derive_orbit_type(section_hint: str, row: Dict[str, Any]) -> str:
    val = str(row.get("orbit") or "").upper()
    if "LEO" in val:
        return "LEO"
    if "GEO" in val:
        return "GEO"
    if "MEO" in val:
        return "MEO"
    return {"LEOSAR": "LEO", "GEOSAR": "GEO", "MEOSAR": "MEO"}.get(section_hint, "")

def _derive_constellation(row: Dict[str, Any]) -> str:
    name = str(row.get("name") or "").upper()
    const = str(row.get("constellation") or "").upper()
    if any(k in name for k in ["GALILEO","GSAT","FOC"]):
        return "Galileo"
    if name.startswith("GPS") or "NAVSTAR" in name:
        return "GPS"
    if "GLONASS" in name or re.search(r"\bK-?\d", name):
        return "GLONASS"
    if "BEIDOU" in name or "BDS" in name or "BD-" in name:
        return "BeiDou"
    if any(k in name for k in ["GOES","MSG","MTG","INSAT","HIMAWARI","ELECTRO","ELEKTRO","GEO-KOMPSAT"]):
        return "GEO"
    return const.title() if const else ""

def _post_normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows with empty name
    df = df[df["name"].astype(str).str.strip().ne("")]
    # Deduplicate on ['name', 'orbit_type']
    df = df.drop_duplicates(subset=["name", "orbit_type"])
    # Parse launch_date with multiple formats
    if "launch_date" in df.columns:
        def parse_date(val):
            for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y", "%d.%m.%Y"):
                try:
                    return pd.to_datetime(val, format=fmt)
                except Exception:
                    continue
            try:
                return pd.to_datetime(val)
            except Exception:
                return pd.NaT
        df["launch_date"] = df["launch_date"].apply(parse_date)
    # Fill source_primary and record_asof
    if "source_primary" not in df.columns:
        df["source_primary"] = np.nan
    if "record_asof" not in df.columns:
        df["record_asof"] = pd.Timestamp.now("UTC")
    return df

def _pick_name_column(df: pd.DataFrame) -> pd.DataFrame:
    if "name" in df.columns and df["name"].astype(str).str.strip().ne("").any():
        return df
    scores: dict[str,int] = {}
    for c in df.columns:
        vals = df[c].astype(str)
        hits = vals.str.contains(SAT_NAME_RE, regex=True, na=False).sum()
        scores[c] = int(hits)
    if scores:
        best = max(scores, key=scores.get)
        if scores[best] > 0:
            return df.rename(columns={best: "name"})
    for c in df.columns:
        vals = df[c].astype(str).str.strip()
        if (vals.ne("").sum() >= max(5, len(df)//10)):
            return df.rename(columns={c: "name"})
    if df.columns.size:
        return df.rename(columns={df.columns[0]: "name"})
    return df

# ---------- pdfplumber parsing ----------
def _parse_tables_pdfplumber(pdf: "pdfplumber.PDF") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pidx, page in enumerate(pdf.pages):
        text = page.extract_text() or ""
        section_hint = _guess_section_title(text)

        tables = page.extract_tables({
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "intersection_x_tolerance": 5,
            "intersection_y_tolerance": 5,
            "snap_tolerance": 3,
        }) or []
        if not tables:
            tables = page.extract_tables({
                "vertical_strategy": "text",
                "horizontal_strategy": "text",
            }) or []

        for tbl in tables:
            if not tbl or len(tbl) < 2:
                continue
            headers = [_normalize_header_fuzzy(_clean_cell(h)) for h in tbl[0]]
            for r in tbl[1:]:
                cells = [_clean_cell(c) for c in r]
                rec = dict(zip(headers, cells))
                rec["__page"] = pidx + 1
                rec["__section_hint"] = section_hint
                rows.append(rec)
    return rows

def _parse_text_aggressive_pdfplumber(pdf: "pdfplumber.PDF") -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for pidx, page in enumerate(pdf.pages):
        text = page.extract_text() or ""
        section_hint = _guess_section_title(text)
        for raw in (text.splitlines() if text else []):
            line = raw.strip()
            if not line:
                continue
            if line.upper() in ("LEOSAR","GEOSAR","MEOSAR"):
                continue
            if SAT_NAME_RE.search(line):
                rows.append({
                    "name": _clean_cell(line),
                    "orbit_type": {"LEOSAR":"LEO","GEOSAR":"GEO","MEOSAR":"MEO"}.get(section_hint,""),
                    "__page": pidx+1,
                    "__section_hint": section_hint
                })
                continue
            if section_hint == "LEOSAR" and LEO_SHORT_RE.search(line):
                rows.append({
                    "name": LEO_SHORT_RE.search(line).group(0).upper(),
                    "orbit_type": "LEO",
                    "__page": pidx+1,
                    "__section_hint": section_hint
                })
    return rows

# ---------- text-extraction fallbacks (no tables) ----------
def _extract_text_fallback(pdf_path: Path) -> str:
    if _have_fitz:
        # PyMuPDF
        doc = fitz.open(str(pdf_path))
        parts = []
        for i, page in enumerate(doc):
            try:
                parts.append(page.get_text("text"))
            except Exception:
                pass
        return "\n".join(parts)
    if _have_pypdf:
        # PyPDF
        reader = PdfReader(str(pdf_path))
        pages = []
        for i, page in enumerate(reader.pages):
            try:
                pages.append(page.extract_text() or "")
            except Exception:
                pass
        return "\n".join(pages)
    # nothing available
    return ""

def _parse_text_only(text: str) -> List[Dict[str, Any]]:
    """Text-only parsing when pdfplumber isn't available."""
    rows: List[Dict[str, Any]] = []
    current_section = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        up = line.upper()
        if up.startswith("LEOSAR"):
            current_section = "LEOSAR"; continue
        if up.startswith("GEOSAR"):
            current_section = "GEOSAR"; continue
        if up.startswith("MEOSAR"):
            current_section = "MEOSAR"; continue

        if SAT_NAME_RE.search(line):
            rows.append({
                "name": _clean_cell(line),
                "orbit_type": {"LEOSAR":"LEO","GEOSAR":"GEO","MEOSAR":"MEO"}.get(current_section,""),
                "__page": np.nan,
                "__section_hint": current_section
            })
            continue
        if current_section == "LEOSAR" and LEO_SHORT_RE.search(line):
            rows.append({
                "name": LEO_SHORT_RE.search(line).group(0).upper(),
                "orbit_type": "LEO",
                "__page": np.nan,
                "__section_hint": current_section
            })
    return rows

# ---------- Main ----------
def main() -> None:
    if not INPUT_PDF.exists():
        raise FileNotFoundError(f"Input PDF not found: {INPUT_PDF}")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, Any]] = []
    df_raw: pd.DataFrame

    if _have_pdfplumber:
        with pdfplumber.open(str(INPUT_PDF)) as pdf:
            # tables phase
            rows = _parse_tables_pdfplumber(pdf)
            df_raw = pd.DataFrame(rows)
            # persist raw early
            df_raw.to_csv(RAW_ROWS_CSV, index=False)

            # enrich with derived fields if we have anything
            if not df_raw.empty:
                if "orbit_type" not in df_raw.columns:
                    df_raw["orbit_type"] = df_raw.apply(
                        lambda r: _derive_orbit_type(r.get("__section_hint",""), r.to_dict()),
                        axis=1
                    )
                df_raw = _pick_name_column(df_raw)
                if "constellation" not in df_raw.columns:
                    df_raw["constellation"] = np.nan
                df_raw["constellation"] = df_raw.apply(
                    lambda r: _derive_constellation(r.to_dict())
                    if pd.isna(r.get("constellation")) or not r.get("constellation")
                    else r.get("constellation"), axis=1
                )

            # Dump raw rows before salvage
            df_raw.to_csv(RAW_ROWS_CSV, index=False, encoding="utf-8")

            need_salvage = df_raw.empty or ("name" not in df_raw.columns) or (len(df_raw) < 20)
            if need_salvage:
                text_rows = _parse_text_aggressive_pdfplumber(pdf)
                df_raw = pd.concat([df_raw, pd.DataFrame(text_rows)], ignore_index=True, sort=False) if not df_raw.empty else pd.DataFrame(text_rows)

            # Dump raw rows after salvage
            df_raw.to_csv(RAW_ROWS_CSV, index=False, encoding="utf-8")

            with LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(f"Raw columns: {list(df_raw.columns)}\n")
                if "orbit_type" in df_raw.columns:
                    counts = df_raw["orbit_type"].value_counts(dropna=False).to_dict()
                    f.write(f"Raw orbit_type counts: {counts}\n")
                sample = df_raw.get("name").dropna().astype(str).head(15).tolist() if "name" in df_raw.columns else []
                f.write(f"Sample names: {sample}\n")

    else:
        # No pdfplumber: fallback to text-only
        txt = _extract_text_fallback(INPUT_PDF)
        rows = _parse_text_only(txt)
        df_raw = pd.DataFrame(rows)
        df_raw.to_csv(RAW_ROWS_CSV, index=False)

    # Final normalize -> catalog
    if df_raw is None or df_raw.empty:
        df_final = pd.DataFrame(columns=[
            "name","operator","sar_status","orbit_type","constellation",
            "launch_date","notes","source_primary","record_asof"
        ])
    else:
        # Clean trailing parenthetical notes in name (e.g., "(operational)")
        if "name" in df_raw.columns:
            df_raw["name"] = (
                df_raw["name"].astype(str)
                .str.replace(r"\s+\(.*?\)$", "", regex=True)
                .str.strip()
            )
        df_raw["source_primary"] = "Cospas-Sarsat: Current Space Segment Status and SAR Payloads (PDF)"
        df_raw["record_asof"] = pd.Timestamp.utcnow().isoformat()
        df_final = _post_normalize(df_raw)

    # Write outputs
    df_final.to_csv(CATALOG_CSV, index=False)

    with LOG_PATH.open("w", encoding="utf-8") as f:
        f.write(f"[{dt.datetime.utcnow().isoformat()}Z] Rows before normalize: {len(df_raw) if df_raw is not None else 0}; Final catalog rows: {len(df_final)}\n")
        f.write(f"Source PDF: {INPUT_PDF}\n")
        f.write(f"Raw rows CSV: {RAW_ROWS_CSV}\n")
        f.write(f"Catalog CSV: {CATALOG_CSV}\n")
        if not _have_pdfplumber:
            f.write("Note: pdfplumber not available; text-only fallback used.\n")

    print(f"OK: wrote catalog {CATALOG_CSV} with {len(df_final)} rows")
    print(f"Raw rows (debug): {RAW_ROWS_CSV}")
    print(f"Log: {LOG_PATH}")

if __name__ == "__main__":
    main()

import re

pattern = re.compile(r"hello", re.IGNORECASE)
print(pattern.match("HELLO"))  # Matches, returns a match object
print(pattern.match("hello"))  # Matches, returns a match object
print(pattern.match("HeLlO"))  # Matches, returns a match object
