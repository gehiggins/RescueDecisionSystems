"""
field_validator.py

RescueDecisionSystems Field Validator
------------------------------------
This module provides robust, field-agnostic validation and extraction utilities for structured data (especially coordinates) from raw text.

Key Features:
- Pure validation logic for latitude/longitude, beacon IDs, timestamps, and more.
- Robust regex and normalization for all observed coordinate formats (Decimal Minutes, DMS, Decimal Degrees).
- Confidence scoring, notes, and error handling for every extraction.
- Pair-first coordinate validation: finds and validates lat/lon pairs in context.
- No side effects; all functions are pure and deterministic.

Usage:
- Use validate_and_extract_coordinate_pair(...) for robust coordinate pair extraction.
- All results include top-level and nested fields for downstream mapping.

Author: gehiggins
Date: 2025-08-30
"""
import logging
def _safe_search(pattern, text):
    """
    Safe regex search utility.
    Returns (match, None) on success, or (None, reason) on failure.
    Used for anchor and pattern matching throughout the validator.
    """
    import re
    try:
        m = pattern.search(text) if hasattr(pattern, "search") else re.search(pattern, text)
        if m is None:
            return None, "no_match"
        return m, None
    except re.error as e:
        return None, f"regex_error:{e}"

def _grp(m, idx=1, default=None):
    """
    Safe regex group accessor.
    Returns group(idx) if available, else group(0), else default.
    """
    if not m:
        return default
    try:
        return m.group(idx)
    except IndexError:
        try:
            return m.group(0)
        except Exception:
            return default
import re
from typing import Any, Dict, Optional, Tuple, List

try:
    from flashtext import KeywordProcessor
    _HAS_FLASHTEXT = True
except ImportError:
    _HAS_FLASHTEXT = False

logger = logging.getLogger(__name__)

"""
FIELD_CONFIG: Dict[str, Dict[str, Any]]
Configuration for supported fields (latitude, longitude, beacon_id, etc).
Defines anchor patterns, regexes, ranges, and other extraction details.
"""
FIELD_CONFIG: Dict[str, Dict[str, Any]] = {
    "latitude": {
        "anchor_patterns": [r"lat", r"latitude", r"\\bN\\b", r"\\bS\\b"],
        "pattern_primary": r"(?P<lat>[+-]?\d{1,2}\.\d{3,6})",
        "pattern_fallbacks": [
            r"(?P<lat>[+-]?\d{1,2}\.\d+)",
            r"(?P<lat>\d{1,2} \d{1,2}\.\d+)",
            r"(?P<lat>\d{1,2}° ?\d{1,2}\.\d+[\'\"]? [NS])"
        ],
        "range": (-90, 90),
        "paired_with": "longitude",
        "window_chars": 30,
    },
    "longitude": {
        "anchor_patterns": [r"lon", r"longitude", r"\\bE\\b", r"\\bW\\b"],
        "pattern_primary": r"(?P<lon>[+-]?\d{1,3}\.\d{3,6})",
        "pattern_fallbacks": [
            r"(?P<lon>[+-]?\d{1,3}\.\d+)",
            r"(?P<lon>\d{1,3} \d{1,2}\.\d+)",
            r"(?P<lon>\d{1,3}° ?\d{1,2}\.\d+[\'\"]? [EW])"
        ],
        "range": (-180, 180),
        "paired_with": "latitude",
        "window_chars": 30,
    },
    "beacon_id": {
        "anchor_patterns": [r"beacon", r"id", r"beacon id", r"hex id"],
        "pattern_primary": r"(?P<beacon_id>[0-9A-Fa-f]{15})",
        "pattern_fallbacks": [r"(?P<beacon_id>[0-9A-Fa-f]{12,15})"],
        "window_chars": 20,
    },
    "detect_time": {
        "anchor_patterns": [r"detect", r"time", r"dtg", r"timestamp"],
        "pattern_primary": r"(?P<detect_time>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})",
        "pattern_fallbacks": [
            r"(?P<detect_time>\d{2}/\d{2}/\d{4} \d{2}:\d{2})",
            r"(?P<detect_time>\d{8}T\d{6}Z)",
            r"(?P<detect_time>\d{4} \d{2} \d{2} \d{2} \d{2} \d{2})"
        ],
        "window_chars": 25,
    },
    "position_method": {
        "anchor_patterns": [r"method", r"pos method", r"position method"],
        "pattern_primary": r"(?P<position_method>GPS|GNSS|Doppler|Manual|AIS)",
        "pattern_fallbacks": [r"(?P<position_method>[A-Za-z]{3,10})"],
        "enums": ["GPS", "GNSS", "Doppler", "Manual", "AIS"],
        "window_chars": 15,
    },
    "position_resolution": {
        "anchor_patterns": [r"resolution", r"pos res", r"position resolution"],
        "pattern_primary": r"(?P<position_resolution>\d+(\.\d+)? ?(m|km|NM))",
        "pattern_fallbacks": [r"(?P<position_resolution>\d+(\.\d+)?)"],
        "window_chars": 15,
    },
}

def validate_and_extract(field_name: str, raw_text: str, config: dict, context: Optional[dict] = None) -> dict:
    """
    Generic field extraction and validation pipeline.
    Used for non-coordinate fields (beacon_id, detect_time, etc).
    Returns dict with value, validity, confidence, notes, etc.
    """
    result = {
        "value": None,
        "raw_span": None,
        "valid": False,
        "confidence": 0.0,
        "checks_passed": [],
        "checks_failed": [],
        "fallback_used": False,
        "notes": [],
    }
    checks = []
    notes = []
    anchor_found = False
    anchor_span = None
    window_text = raw_text

    # 1. Anchor search
    anchors = config.get("anchor_patterns", [])
    if _HAS_FLASHTEXT and anchors:
        kp = KeywordProcessor()
        for a in anchors:
            kp.add_keyword(a)
        found = kp.extract_keywords(raw_text, span_info=True)
        if found:
            anchor_found = True
            anchor_span = found[0][1:]
            window_start = max(0, anchor_span[0] - config.get("window_chars", 20))
            window_end = min(len(raw_text), anchor_span[1] + config.get("window_chars", 20))
            window_text = raw_text[window_start:window_end]
            notes.append("Anchor found via FlashText: %s" % found[0][0])
        else:
            notes.append("No anchor found via FlashText.")
    else:
        for a in anchors:
            m, err = _safe_search(a, raw_text)
            if m:
                anchor_found = True
                anchor_span = m.span()
                window_start = max(0, anchor_span[0] - config.get("window_chars", 20))
                window_end = min(len(raw_text), anchor_span[1] + config.get("window_chars", 20))
                window_text = raw_text[window_start:window_end]
                notes.append(f"Anchor found via regex: {a}")
                break
        if not anchor_found:
            notes.append("No anchor found via regex.")

    # 2. Pattern family (primary + fallbacks)
    patterns = [config.get("pattern_primary")] + config.get("pattern_fallbacks", [])
    match = None
    for idx, pat in enumerate(patterns):
        m, err = _safe_search(pat, window_text)
        if m:
            if m.lastindex and m.group(1) is not None:
                value = m.group(1)
            else:
                value = m.group(0)
            span = m.span()
            match = m
            if idx > 0:
                result["fallback_used"] = True
                notes.append(f"Used fallback pattern {idx}: {pat}")
            else:
                notes.append(f"Used primary pattern: {pat}")
            break
        else:
            notes.append(f"Pattern {pat} did not match: {err}")
            result["value"] = None
            result["raw_span"] = None
            result["valid"] = False
            result["confidence"] = 0.0
            result["notes"] = notes + ["no regex match in this stage"]
            result["checks_failed"].append("pattern_no_match")
            return result

    # Minutes-only longitude repair logic
    if not match and field_name == "longitude":
        # Search for minutes-only token: e.g., '30.200W'
        min_only_pat = re.compile(r"\b(\d{1,2}\.\d{2,})\s*([EW])\b")
        min_match, min_err = _safe_search(min_only_pat, window_text)
        if min_match:
            if min_match.lastindex and min_match.group(1) is not None:
                min_val = min_match.group(1)
            else:
                min_val = min_match.group(0)
            cardinal = min_match.group(2) if min_match.lastindex and min_match.group(2) is not None else None
            left_window_start = max(0, min_match.start() - 8)
            left_window = window_text[left_window_start:min_match.start()]
            deg_pat = re.compile(r"\b(\d{1,3})\b\s?$")
            deg_match, deg_err = _safe_search(deg_pat, left_window)
            if deg_match:
                if deg_match.lastindex and deg_match.group(1) is not None:
                    deg_val = deg_match.group(1)
                else:
                    deg_val = deg_match.group(0)
                try:
                    deg_int = int(deg_val)
                except Exception:
                    notes.append(f"degree_parse_error:{deg_val}")
                    result["notes"] = notes + ["No pattern matched."]
                    result["valid"] = False
                    result["value"] = None
                    result["raw_span"] = None
                    result["confidence"] = 0.0
                    result["checks_failed"].append("pattern_no_match")
                    notes.append("no regex match in this stage")
                    return result
                if 0 <= deg_int <= 180:
                    canonical = f"{deg_int:03d} {float(min_val):06.3f}{cardinal}"
                    result["value"] = canonical
                    result["raw_span"] = (left_window_start + deg_match.start(), min_match.end())
                    result["fallback_used"] = True
                    result["checks_passed"].append("minutes_only_repair")
                    notes.append(f"Longitude minutes-only repaired: {canonical}")
                    result["confidence"] = max(0.0, result.get("confidence", 0.5) - 0.15)
                    match = None
                else:
                    notes.append(f"Degrees out of range for longitude repair: {deg_val}")
                    result["notes"] = notes + ["No pattern matched."]
                    result["valid"] = False
                    result["value"] = None
                    result["raw_span"] = None
                    result["confidence"] = 0.0
                    result["checks_failed"].append("pattern_no_match")
                    notes.append("no regex match in this stage")
                    return result
            else:
                notes.append(f"no_degrees_found_left_of_minutes:{deg_err}")
                result["notes"] = notes + ["No pattern matched."]
                result["valid"] = False
                result["value"] = None
                result["raw_span"] = None
                result["confidence"] = 0.0
                result["checks_failed"].append("pattern_no_match")
                notes.append("no regex match in this stage")
                return result
        else:
            notes.append(f"minutes_only_no_match:{min_err}")
            result["notes"] = notes + ["No pattern matched."]
            result["valid"] = False
            result["value"] = None
            result["raw_span"] = None
            result["confidence"] = 0.0
            result["checks_failed"].append("pattern_no_match")
            notes.append("no regex match in this stage")
            return result

    # 3. Structure check (components/order)
    if match:
        value = _grp(match, 1)
        span = match.span()
        result["value"] = value
        result["raw_span"] = span
        checks.append("structure")

    # 4. Range check (numeric bounds/enums/units)
    valid = True
    if "range" in config:
        try:
            num = float(value) if value is not None else None
            minv, maxv = config["range"]
            if num is None or not (minv <= num <= maxv):
                valid = False
                result["checks_failed"].append("range")
                notes.append(f"Value {num} out of range {minv}-{maxv}")
            else:
                result["checks_passed"].append("range")
        except Exception as e:
            valid = False
            result["checks_failed"].append("range")
            notes.append(f"Range check error: {e}")
    if "enums" in config:
        if value not in config["enums"]:
            valid = False
            result["checks_failed"].append("enums")
            notes.append(f"Value '{value}' not in enums {config['enums']}")
        else:
            result["checks_passed"].append("enums")

    # 5. Dependency/pairing (e.g., lat↔lon)
    if config.get("paired_with") and context:
        paired_field = config["paired_with"]
        paired_value = context.get(paired_field)
        if paired_value is not None:
            # Example: lat/lon proximity check
            try:
                lat = float(value) if field_name == "latitude" else float(paired_value)
                lon = float(value) if field_name == "longitude" else float(paired_value)
                if abs(lat) > 90 or abs(lon) > 180:
                    valid = False
                    result["checks_failed"].append("pairing")
                    notes.append("Lat/lon pairing out of bounds.")
                else:
                    result["checks_passed"].append("pairing")
            except Exception as e:
                valid = False
                result["checks_failed"].append("pairing")
                notes.append(f"Pairing check error: {e}")

    # 6. Sliding window fallback (±N chars)
    # Already handled above by window_text

    # 7. Normalize (canonical value)
    # Example: strip units, standardize format
    if field_name in ("latitude", "longitude"):
        try:
            result["value"] = f"{float(value):.6f}"
            result["checks_passed"].append("normalize")
        except Exception:
            pass
    elif field_name == "position_resolution":
        # Remove units, keep float
        v = re.sub(r"[^\d\.]+", "", value)
        try:
            result["value"] = f"{float(v):.2f}"
            result["checks_passed"].append("normalize")
        except Exception:
            pass
    else:
        result["value"] = str(value)
        result["checks_passed"].append("normalize")

    # 8. Confidence score
    # TODO: enrich confidence scoring
    score = 0.5
    if anchor_found:
        score += 0.2
    if valid:
        score += 0.2
    if result["fallback_used"]:
        score -= 0.1
    result["confidence"] = min(score, 1.0)
    result["valid"] = valid
    result["notes"] = notes
    logger.debug(f"validate_and_extract: {field_name} valid={valid} confidence={result['confidence']} notes={notes}")
    return result

# TODO: Add more field configs and enrich confidence scoring

"""
Example usage:
res = validate_and_extract("latitude", "lat: 45.123456 N", FIELD_CONFIG["latitude"])
print(res)
"""

# --- Coordinate validators (append) ---
from typing import Dict, Any, Optional
import re
from app.utils_coordinates import clean_and_standardize_coordinate, parse_any_coordinate

def _record(checks_passed, checks_failed, name, ok, note=None):
    """
    Utility to record check results and notes.
    Appends check name to passed/failed, returns note if provided.
    """
    (checks_passed if ok else checks_failed).append(name)
    if note:
        return [note]
    return []


def validate_and_extract_coordinate_token(field_name: str, raw_text: str, config: dict, context: Optional[dict]=None) -> Dict[str, Any]:
    """
    Validate a single coordinate token (latitude or longitude).
    Supports decimal-minutes, DMS, and decimal degrees formats.
    Rejects minutes-only fragments and preserves leading zeros.
    Returns dict with value, validity, confidence, notes, etc.
    """
    text = raw_text or ""
    std = clean_and_standardize_coordinate(text)
    checks_passed, checks_failed, notes = [], [], []

    # Decimal-minutes regex (robust, preserves leading zeros)
    dec_min_lat = r"^(?P<deg>\d{1,2})\s+(?P<min>[0-5]?\d(?:\.\d+)?)\s*[NS]$"
    dec_min_lon = r"^(?P<deg>\d{1,3})\s+(?P<min>[0-5]?\d(?:\.\d+)?)\s*[EW]$"
    minutes_only = r"^(?P<min>[0-5]?\d(?:\.\d+)?)\s*[NSEW]$"

    # Reject minutes-only fragments
    if re.match(minutes_only, std):
        notes.append("Rejected: minutes-only fragment (no degree component)")
        return {
            "value": None,
            "raw_span": (context or {}).get("span"),
            "valid": False,
            "confidence": 0.0,
            "checks_passed": [],
            "checks_failed": ["structure"],
            "fallback_used": False,
            "notes": notes,
        }

    # Try decimal-minutes first
    structure_ok = False
    value = None
    parse_ok = False
    m = None
    if field_name == "latitude":
        m = re.match(dec_min_lat, std)
    elif field_name == "longitude":
        m = re.match(dec_min_lon, std)
    if m:
        structure_ok = True
        deg = m.group("deg")
        min_ = m.group("min")
        hemi = std[-1].upper()
        try:
            deg_val = int(deg)
            min_val = float(min_)
            value = deg_val + min_val / 60.0
            if hemi in ["S", "W"]:
                value = -value
            parse_ok = True
            checks_passed.append("parse")
        except Exception as e:
            checks_failed.append("parse")
            notes.append(f"parse_error: {e}")
        notes.append("Matched decimal-minutes format")
    else:
        # Try DMS or DD fallback
        # DMS: 37°45.600'N or 075°30.200'W
        dms_pat = r"^(?P<deg>\d{1,3})[°\s]+(?P<min>\d{1,2}(?:\.\d+)?)[\'\s]*(?P<sec>\d{1,2}(?:\.\d+)?)?[\"\s]*[NSEW]$"
        dd_pat = r"^(?P<dd>[+-]?\d{1,3}\.\d+)[NSEW]$"
        m = re.match(dms_pat, std)
        if m:
            structure_ok = True
            deg = m.group("deg")
            min_ = m.group("min")
            sec = m.group("sec") or "0"
            hemi = std[-1].upper()
            try:
                deg_val = int(deg)
                min_val = float(min_)
                sec_val = float(sec)
                value = deg_val + min_val / 60.0 + sec_val / 3600.0
                if hemi in ["S", "W"]:
                    value = -value
                parse_ok = True
                checks_passed.append("parse")
            except Exception as e:
                checks_failed.append("parse")
                notes.append(f"parse_error: {e}")
            notes.append("Matched DMS format")
        else:
            m = re.match(dd_pat, std)
            if m:
                structure_ok = True
                dd = m.group("dd")
                hemi = std[-1].upper()
                try:
                    value = float(dd)
                    if hemi in ["S", "W"]:
                        value = -value
                    parse_ok = True
                    checks_passed.append("parse")
                except Exception as e:
                    checks_failed.append("parse")
                    notes.append(f"parse_error: {e}")
                notes.append("Matched decimal-degree format")
    # Range check after parse
    if parse_ok:
        is_lat = field_name == "latitude"
        in_range = (-90.0 <= value <= 90.0) if is_lat else (-180.0 <= value <= 180.0)
        notes += _record(checks_passed, checks_failed, "range", in_range,
                         None if in_range else "range: out of bounds")

    valid = structure_ok and parse_ok and ("range" not in checks_failed)
    # Confidence scoring
    if structure_ok and parse_ok:
        if "Matched decimal-minutes format" in notes or "Matched DMS format" in notes:
            confidence = 0.92
        elif "Matched decimal-degree format" in notes:
            confidence = 0.85
        else:
            confidence = 0.7
    else:
        confidence = 0.2
    span = (context or {}).get("span")

    if std != text:
        notes.append(f"normalized='{std}'")

    return {
        "value": value,
        "raw_span": span,
        "valid": valid,
        "confidence": confidence,
        "checks_passed": checks_passed,
        "checks_failed": checks_failed,
        "fallback_used": False,
        "notes": notes,
    }


def validate_and_extract_coordinate_pair(field_name: str, raw_text: str, config: dict, context: Optional[dict]=None) -> Dict[str, Any]:
    """
    Validate and extract a latitude/longitude pair from raw text.
    - Tries decimal-minutes pair regex first (any order).
    - Falls back to proximity DM, then DMS/DD tokens.
    - Exposes lat_token/lon_token, format_type, confidence, and all top-level fields.
    - Returns dict with all extraction metadata for downstream mapping.
    """

    std = clean_and_standardize_coordinate(raw_text or "")
    checks_passed, checks_failed, notes = [], [], []
    span = (context or {}).get("span")
    lat_token = None
    lon_token = None
    format_type = None
    lat_dd = lon_dd = None
    start_pos = end_pos = None

    # Pair-first: decimal-minutes regex for lat+lon (any order, preserves leading zeros)
    pair_dec_min = re.compile(r"(?P<lat>\d{1,2}\s+[0-5]?\d(?:\.\d+)?\s*[NS])\s+(?P<lon>\d{1,3}\s+[0-5]?\d(?:\.\d+)?\s*[EW])|(?P<lon2>\d{1,3}\s+[0-5]?\d(?:\.\d+)?\s*[EW])\s+(?P<lat2>\d{1,2}\s+[0-5]?\d(?:\.\d+)?\s*[NS])")
    m = pair_dec_min.search(std)
    if m:
        # Support both lat-lon and lon-lat order
        lat_token = m.group("lat") or m.group("lat2")
        lon_token = m.group("lon") or m.group("lon2")
        start_pos = m.start()
        end_pos = m.end()
        format_type = "Decimal Minutes"
        # Reject minutes-only fragments
        if re.match(r"^[0-5]?\d(?:\.\d+)?[NSEW]$", lat_token) or re.match(r"^[0-5]?\d(?:\.\d+)?[NSEW]$", lon_token):
            checks_failed.append("pair_structure"); notes.append("pair: minutes-only fragment detected")
        else:
            r_lat = validate_and_extract_coordinate_token("latitude", lat_token, config, context)
            r_lon = validate_and_extract_coordinate_token("longitude", lon_token, config, context)
            lat_dd = r_lat["value"]
            lon_dd = r_lon["value"]
            notes.extend([f"lat: {n}" for n in (r_lat["notes"] or [])])
            notes.extend([f"lon: {n}" for n in (r_lon["notes"] or [])])
            (checks_passed if r_lat["valid"] else checks_failed).append("lat_valid")
            (checks_passed if r_lon["valid"] else checks_failed).append("lon_valid")
            notes.append("Matched pair decimal-minutes format")
            # Return immediately on first valid DM pair
            return {
                "value": {"lat_dd": lat_dd, "lon_dd": lon_dd},
                "lat_dd": lat_dd,
                "lon_dd": lon_dd,
                "lat_token": lat_token,
                "lon_token": lon_token,
                "is_valid": True,
                "format_type": format_type,
                "confidence": 0.92,
                "raw_span": span,
                "valid": True,
                "start_pos": start_pos,
                "end_pos": end_pos,
                "checks_passed": checks_passed,
                "checks_failed": checks_failed,
                "fallback_used": False,
                "notes": notes,
            }

    # Proximity fallback: find individual DM lat & lon tokens within 120 chars
    dm_lat_pat = r"\d{1,2}\s+[0-5]?\d(?:\.\d+)?\s*[NS]"
    dm_lon_pat = r"\d{1,3}\s+[0-5]?\d(?:\.\d+)?\s*[EW]"
    lat_matches = [m for m in re.finditer(dm_lat_pat, std)]
    lon_matches = [m for m in re.finditer(dm_lon_pat, std)]
    for lat_m in lat_matches:
        for lon_m in lon_matches:
            # Reject minutes-only fragments
            if re.match(r"^[0-5]?\d(?:\.\d+)?[NSEW]$", lat_m.group(0)) or re.match(r"^[0-5]?\d(?:\.\d+)?[NSEW]$", lon_m.group(0)):
                continue
            # Proximity check
            if abs(lat_m.start() - lon_m.start()) <= 120:
                lat_token = lat_m.group(0)
                lon_token = lon_m.group(0)
                start_pos = min(lat_m.start(), lon_m.start())
                end_pos = max(lat_m.end(), lon_m.end())
                format_type = "Decimal Minutes"
                r_lat = validate_and_extract_coordinate_token("latitude", lat_token, config, context)
                r_lon = validate_and_extract_coordinate_token("longitude", lon_token, config, context)
                lat_dd = r_lat["value"]
                lon_dd = r_lon["value"]
                notes.extend([f"lat: {n}" for n in (r_lat["notes"] or [])])
                notes.extend([f"lon: {n}" for n in (r_lon["notes"] or [])])
                (checks_passed if r_lat["valid"] else checks_failed).append("lat_valid")
                (checks_passed if r_lon["valid"] else checks_failed).append("lon_valid")
                notes.append("Matched DM lat/lon pair by proximity")
                # Return immediately on first valid proximity DM pair
                return {
                    "value": {"lat_dd": lat_dd, "lon_dd": lon_dd},
                    "lat_dd": lat_dd,
                    "lon_dd": lon_dd,
                    "lat_token": lat_token,
                    "lon_token": lon_token,
                    "is_valid": True,
                    "format_type": format_type,
                    "confidence": 0.91,
                    "raw_span": span,
                    "valid": True,
                    "start_pos": start_pos,
                    "end_pos": end_pos,
                    "checks_passed": checks_passed,
                    "checks_failed": checks_failed,
                    "fallback_used": False,
                    "notes": notes,
                }

    # Fallback: try to find two tokens (DMS or DD)
    token_pat = r"\d{1,3}[°\s]+\d{1,2}(?:\.\d+)?[\'\s]*(?:\d{1,2}(?:\.\d+)?)?[\"\s]*[NSEW]|[+-]?\d{1,3}\.\d+[NSEW]"
    tokens = re.findall(token_pat, std)
    if len(tokens) >= 2:
        lat_token = next((t for t in tokens if t.strip().upper().endswith(("N","S"))), None)
        lon_token = next((t for t in tokens if t.strip().upper().endswith(("E","W"))), None)
        # Determine format type for each token
        dms_pat = r"^\d{1,3}[°\s]+\d{1,2}(?:\.\d+)?[\'\s]*(?:\d{1,2}(?:\.\d+)?)?[\"\s]*[NSEW]$"
        dd_pat = r"^[+-]?\d{1,3}\.\d+[NSEW]$"
        lat_is_dms = bool(lat_token and re.match(dms_pat, lat_token))
        lon_is_dms = bool(lon_token and re.match(dms_pat, lon_token))
        lat_is_dd = bool(lat_token and re.match(dd_pat, lat_token))
        lon_is_dd = bool(lon_token and re.match(dd_pat, lon_token))
        if lat_is_dms and lon_is_dms:
            format_type = "DMS"
        elif lat_is_dd and lon_is_dd:
            format_type = "Decimal Degrees"
        elif (lat_is_dms and lon_is_dd) or (lat_is_dd and lon_is_dms):
            format_type = "Mixed"
            notes.append("mixed token formats")
        else:
            format_type = "Unknown"
        if lat_token:
            r_lat = validate_and_extract_coordinate_token("latitude", lat_token, config, context)
            lat_dd = r_lat["value"]
            notes.extend([f"lat: {n}" for n in (r_lat["notes"] or [])])
            (checks_passed if r_lat["valid"] else checks_failed).append("lat_valid")
        else:
            checks_failed.append("lat_missing"); notes.append("no latitude token found")
        if lon_token:
            r_lon = validate_and_extract_coordinate_token("longitude", lon_token, config, context)
            lon_dd = r_lon["value"]
            notes.extend([f"lon: {n}" for n in (r_lon["notes"] or [])])
            (checks_passed if r_lon["valid"] else checks_failed).append("lon_valid")
        else:
            checks_failed.append("lon_missing"); notes.append("no longitude token found")
    else:
        checks_failed.append("pair_structure"); notes.append("pair: expected two tokens")

    valid = ("lat_valid" in checks_passed) and ("lon_valid" in checks_passed)
    # Confidence scoring
    if valid:
        if format_type == "DMS":
            confidence = 0.92
        elif format_type == "Decimal Degrees":
            confidence = 0.85
        elif format_type == "Mixed":
            confidence = 0.7
        elif format_type == "Decimal Minutes":
            confidence = 0.91
        else:
            confidence = 0.7
    else:
        confidence = 0.2
    if std != (raw_text or ""):
        notes.append(f"normalized='{std}'")

    return {
        "value": {"lat_dd": lat_dd, "lon_dd": lon_dd},
        "lat_dd": lat_dd,
        "lon_dd": lon_dd,
        "lat_token": lat_token,
        "lon_token": lon_token,
        "is_valid": valid,
        "format_type": format_type,
        "confidence": confidence,
        "raw_span": span,
        "valid": valid,
        "start_pos": start_pos,
        "end_pos": end_pos,
        "checks_passed": checks_passed,
        "checks_failed": checks_failed,
        "fallback_used": False,
        "notes": notes,
    }
if __name__ == "__main__":
    import sys
    from pathlib import Path
    sample_path = "sample_sarsat_message.txt"
    if Path(sample_path).exists():
        t = Path(sample_path).read_text(encoding="utf-8", errors="ignore")
        r = validate_and_extract_coordinate_pair("coord_pair", t, {}, {})
        print(f"format_type={r.get('format_type')}, is_valid={r.get('is_valid')}, lat_dd={r.get('lat_dd')}, lon_dd={r.get('lon_dd')}")
