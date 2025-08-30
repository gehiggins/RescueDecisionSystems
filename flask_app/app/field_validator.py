"""
field_validator.py

Self-contained field validation and extraction utility for RescueDecisionSystems.

Provides a field-agnostic pipeline for extracting and validating structured data from raw text.
"""
import logging
import re
from typing import Any, Dict, Optional, Tuple, List

try:
    from flashtext import KeywordProcessor
    _HAS_FLASHTEXT = True
except ImportError:
    _HAS_FLASHTEXT = False

logger = logging.getLogger(__name__)

# Field configuration for supported fields
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
    Extract and validate a field from raw text using a configurable pipeline.

    Args:
        field_name: Name of the field to extract.
        raw_text: Raw input text.
        config: Field configuration dict.
        context: Optional context for dependency checks.

    Returns:
        dict with keys: value, raw_span, valid, confidence, checks_passed, checks_failed, fallback_used, notes
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
            m = re.search(a, raw_text, re.IGNORECASE)
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
        m = re.search(pat, window_text)
        if m:
            match = m
            if idx > 0:
                result["fallback_used"] = True
                notes.append(f"Used fallback pattern {idx}: {pat}")
            else:
                notes.append(f"Used primary pattern: {pat}")
            break
    if not match:
        result["notes"] = notes + ["No pattern matched."]
        return result

    # 3. Structure check (components/order)
    value = match.group(1) if match.lastindex else match.group(0)
    span = match.span()
    result["value"] = value
    result["raw_span"] = span
    checks.append("structure")

    # 4. Range check (numeric bounds/enums/units)
    valid = True
    if "range" in config:
        try:
            num = float(value)
            minv, maxv = config["range"]
            if not (minv <= num <= maxv):
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
    return result

# TODO: Add more field configs and enrich confidence scoring

"""
Example usage:
res = validate_and_extract("latitude", "lat: 45.123456 N", FIELD_CONFIG["latitude"])
print(res)
"""
