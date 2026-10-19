# RescueDecisionSystems – Dev Map

## How to run
```bash
conda activate unified_env
python -m flask_app.app.pipeline_controller

# Project Map (High-Signal Files)

- **flask_app/app/preparse_coordinate_mapper.py**  
  Pre-scan of SARSAT messages; emits `data/debugging/debug_preparsed_coordinates.csv`.  
  🔗 Validator integration point: where coordinate rows are appended.

- **flask_app/app/parser_sarsat_msg.py**  
  Main structured parser; sets A/B positions + metadata.  
  🔗 Validator integration point: inside `parse_sarsat_message(...)`, at the PROB EE SOL LATITUDE/LONGITUDE section.

- **flask_app/app/utils_coordinates.py**  
  Coordinate utilities (regex / clean / convert).

- **flask_app/app/gis_mapping.py**  
  Folium map rendering.

- **flask_app/app/pipeline_controller.py**  
  Orchestrates: parse → map → DB.

- **data/debugging/debug_preparsed_coordinates.csv**  
  Output of preparse scan for QA.

## Data Paths
- Debug CSV: `data/debugging/debug_preparsed_coordinates.csv`
- Shapefiles: `data/shapefiles/...`
- Map output: `data/maps/`

## Integration Plan for Validator
- **Pre-scan**: swap ad-hoc validity checks → `validate_and_extract()` so invalid-but-detected pairs still get logged.  
  File: `flask_app/app/preparse_coordinate_mapper.py`
- **Main parse**: wrap PROB EE SOL LAT/LON logic with `validate_and_extract()`.  
  File: `flask_app/app/parser_sarsat_msg.py`

## Roadmap Context (for Continue)

- Phase 1 – Parsing Core: ✅ Complete and stable (as of Sep 2025).
- Phase 2 – Data Integrations: 🟡 In progress. Weather fetchers modularized (shore + buoy). Selection logic under active work. GIS shows A/B positions + range rings; satellite overlay not yet complete.
- Phase 3 – Operator Products: 🔜 SQL schema + Flask web UI for operator interaction.
- Phase 4 – Advanced Analysis: 🔮 Doppler footprint validity checks, time-slider GIS, ML reliability scoring, and training/test harness.

Continue should treat these phases as the high-level build sequence. Current priority is finishing Phase 2 (weather selection + GIS + satellite overlay).


## New Module
- `flask_app/app/field_validator.py`  
  API: `validate_and_extract(field_name, raw_text, config, context=None)`  
  Returns: `value, raw_span, valid, confidence, checks_passed, checks_failed, fallback_used, notes`.

## update test to confirm continue is reading this update
- the code word is peanutbutterSandwich

Top-of-file preambles & docstrings (nuanced rule)

If a module already has a module-level docstring or preamble at the top, insert any new summary/preamble immediately after it (never after imports).

If the module lacks a top-level docstring/preamble, place the new preamble at line 1, before imports.

Never place documentation blocks after imports or code unless I explicitly ask you to.

Anchors for deterministic insertions

Respect these anchors if present (create them if missing, as comments only; no functional changes):

# [RDS-ANCHOR: PREAMBLE_END] (end of preamble/docstring)

# [RDS-ANCHOR: IMPORTS_END] (end of imports block)

# [RDS-ANCHOR: FINAL_MAP_STEP] (pipeline_controller’s last mapping step)

# [RDS-ANCHOR: GIS_EXPORTS] (gis_mapping’s export helpers, non-functional)

New docs/comments go right after PREAMBLE_END; new imports (only if required and approved by the prompt) go right after IMPORTS_END; controller map logic changes only happen at FINAL_MAP_STEP; helper summaries only at GIS_EXPORTS.

Scope & safety rails (apply our permanent rules)

Start from the current file content I provided. Do not remove existing logic or doc blocks. You may insert or surgically modify within the scope I name (e.g., “final map step only”).

If a change would conflict with existing code, produce a plan + dry-run diff and stop. Do not apply until the plan matches anchors/positions.

No duplicate preambles. If a preamble exists, merge/append rather than create a second one.

Placement algorithm (deterministic)

Step A: Detect module-level docstring/preamble at the very top (triple-quoted or commented). If found, set insertion point after it; else set insertion point line 1.

Step B: Detect the imports block; mark IMPORTS_END at the last contiguous import line.

Step C: For any code change, locate the named anchor for that change (e.g., FINAL_MAP_STEP). If the anchor is missing, create the anchor as a comment in the correct region, then insert beneath it.

If any step is ambiguous, halt and show me a plan (10 lines of context before/after the proposed insertion).

Approval keyword & provenance

Include the word “peanutbutterSandwich” in your plan output to confirm you’ve read the latest DEV_NOTES.

Echo back the exact file path you will modify (must match our map) and the line/anchor target before making edits:

flask_app/app/gis_mapping.py (Folium/GIS; mapping module)

flask_app/app/pipeline_controller.py (orchestrates parse → map → DB)

Phase & output alignment

Keep changes small and at the end of the pipeline (mapping/output only) — Phase 2 priority. GIS is A/B positions + range rings; no satellite overlay yet.

Output paths must remain under data/maps/ (see Data Paths).

Apply/abort rules

If anchors or patterns don’t match, do not guess. Stop and show the plan with context and proposed anchors.

After edits, print the acceptance lines exactly as specified in the prompt (mapping step only).

### EDIT→VERIFY→CONFIRM Protocol (MANDATORY)

When I ask for a code change:
1) EDIT: Propose a unified diff (no prose mixed in). Then apply it.
2) VERIFY: Re-open the edited file(s) from disk and show:
   - The exact line numbers changed
   - A 15–20 line excerpt around each change
   - A quick grep proving the key token(s) exist (see commands below)
3) CONFIRM: Only after VERIFY passes, state: "CONFIRMED: Change present on disk."

NEVER say a change exists until you’ve shown the VERIFY evidence.

Verification commands (Windows PowerShell examples):
- Show matching lines with context:
  Select-String -Path "<file>" -Pattern "<needle>" -Context 3,3
- Count matches (should be > 0):
  (Select-String -Path "<file>" -Pattern "<needle>").Count
- Git diff preview of what changed:
  git status
  git diff -- <file>

