# RDS operational visualization, satellite intelligence, and SRR coordination

Status: proposed future capability; not implemented

Recorded: 2026-10-19

Planning baseline: `main` commit `7ac84f11a35a7e104636035ba52294906f794aa1`

Source: project-owner concept supplied 2026-10-19

## Sequencing decision

This document records the intended direction without representing these
capabilities as current RDS behavior.

Implementation is deliberately deferred until the current RDS scripts and
integrations are working and tested.

The proposed frontend technologies and phases below are candidates, not final
architecture decisions. A focused integration design and proof of concept must
precede any major rewrite or production commitment.

## Purpose

The long-term goal is to turn a text-based COSPAS–SARSAT alert into a dynamic,
understandable, and evidence-based operational picture. A watchstander should
be able to understand:

1. what happened, including detections, reported positions, reception paths,
   and location solutions;
2. what is happening, including current alert status, constellation position,
   and relevant operational conditions; and
3. what may happen next, including predicted satellite passes and potential
   opportunities for additional information.

## User-experience goals

### Continuous map-to-globe interaction

Provide one continuous geographic environment:

- a detailed operational map at local scales;
- a progressive transition to a three-dimensional Earth as the operator zooms
  outward;
- a complete-Earth view at global scale;
- uninterrupted zooming back to the incident;
- preservation of the selected location, overlays, alert context, and
  simulation time through the transition; and
- north-up, predictable camera behavior by default.

The implementation may use one renderer or synchronized renderers, but that
technical distinction should be invisible to the operator.

### Map-first and scale-dependent display

The default view should be a professional cartographic map rather than
satellite photography. Imagery, bathymetry, terrain, nautical information, and
weather should be optional layers.

Information should change with scale:

- local: alert positions, uncertainty, SRR boundaries, environmental context,
  and nearby operational features;
- regional: multiple detections, broader coverage, satellite ground tracks,
  and predicted passes; and
- global: constellations, orbital patterns, receiving stations, and the
  relationship between coverage and the incident.

## Satellite and ground-system intelligence

Support LEOSAR, GEOSAR, and MEOSAR assets when authoritative data is available.
Candidate information includes:

- satellite identity and constellation;
- position at the current or selected time;
- historical and predicted positions;
- ground track and visibility or coverage footprint;
- relationship to the alert location; and
- whether the satellite actually contributed to a detection.

Display applicable LEOLUTs, GEOLUTs, MEOLUTs, Mission Control Centers, and
Rescue Coordination Centers as reliable data permits. Known reception paths
must be visually distinguishable from modeled, theoretical, or predicted
connectivity.

Future-pass displays may include the next relevant pass, visibility interval,
coverage geometry, time to a potentially useful observation, and the possible
availability of additional location information. An overhead satellite must
not be presented as proof of successful detection or a new usable position.

## Common timeline

Provide one timeline independent of geographic zoom. It should support:

- historical replay of detections, receptions, reported locations,
  uncertainty changes, and relevant case events;
- a current-time constellation and operational view; and
- future animation of predicted positions and possible observation windows.

Confirmed observations, present state, calculated data, and predictions must
be visually and semantically distinct.

## SRR boundary and split-position coordination

The focused requirements, verified current-architecture review, implementation
gates, and minimal delivery plan for this capability are maintained in
[`INTER_RCC_MESSAGING_AND_COORDINATION.md`](INTER_RCC_MESSAGING_AND_COORDINATION.md).
The summary below remains part of the broader visualization concept.

RDS should include chat and sharing between SRR command centers. The chat may
be useful for any alert, and RDS should specifically prompt coordination when:

- an alert position or its uncertainty area intersects an SRR boundary;
- the A and B reported positions fall in different SRRs;
- one position is inside an SRR while the other or its uncertainty area is
  outside it;
- more than two SRRs overlap the alert or uncertainty area; or
- available position evidence is split across or close to multiple SRRs.

The prompt should explain why it appeared, for example:

- `Position A: SRR Alpha`
- `Position B: SRR Bravo`
- `A uncertainty area intersects Alpha and Bravo`
- `Alert is within the configured boundary-proximity threshold`

For these cases, the operational view should show:

- relevant SRR boundaries and labels;
- A and B positions with their independent SRR associations;
- error circles or other uncertainty geometry crossing boundaries;
- all SRRs intersecting the operational area;
- a clear boundary/split alert notice;
- a prompt to start or join chat with the affected command centers; and
- sharing of the alert, A/B positions, uncertainty, map, and later updates in
  that chat.

## Alert intelligence and case learning

The visualization should help explain the evidence used by RDS, including
beacon information, message contents, satellite and reception characteristics,
GNSS versus Doppler positions, repeated detections, geographic consistency,
environmental context, historical patterns, and validated prior outcomes.

Future automated ingestion may replace manual copy-and-paste while preserving
the downstream processing model.

Case closure and feedback should capture validated outcomes, confirmed
locations, activation type, investigation findings, response decisions, and
operational results. Models must learn from validated outcomes, not from their
own earlier predictions. Administrative reporting controls must never prevent
access to urgent decision support.

## Proposed layers

### Alert and location

- current and historical positions;
- detection events;
- position uncertainty;
- movement or drift estimates;
- SRR boundaries and boundary-intersection state; and
- coordination status.

### Satellite and ground systems

- LEOSAR, GEOSAR, and MEOSAR assets;
- identities, ground tracks, visibility or coverage footprints;
- historical detections and predicted passes;
- LUTs and applicable receiving infrastructure; and
- confirmed alert paths.

### Environmental and operational context

- cartographic basemap and optional imagery;
- bathymetry and terrain;
- weather and day/night boundary;
- maritime and aviation information; and
- RCC/MCC locations where operationally appropriate.

## Preliminary architecture candidates

Retain Python as the analytical core. Do not rewrite working backend components
solely to add visualization.

| Function | Candidate technology |
|---|---|
| Interactive map and globe | MapLibre GL JS |
| User interface | React and TypeScript |
| Browser-side satellite propagation | satellite.js |
| Advanced overlays, if demonstrated necessary | deck.gl |
| Analytical API | Existing Python stack; evaluate FastAPI |
| Geographic persistence | PostgreSQL and PostGIS |
| Advanced orbital 3D, if demonstrated necessary | CesiumJS |

MapLibre should be evaluated first as a unified renderer. CesiumJS or a
two-renderer design should be introduced only if a proof of concept shows that
the simpler option cannot meet orbital, transition, or performance needs.

The common data model must distinguish authoritative observations, calculated
data, predictions, and analytical assessments.

## Deferred implementation plan

### Foundation gate — complete the existing system first

Do not begin these phases until the current RDS scripts and integrations are
working and tested.

### Phase 1 — integration and data design

Document the existing architecture, authoritative data sources, shared
geographic/time model, SRR data source, and incremental integration approach.

### Phase 2 — map-to-globe proof of concept

Demonstrate continuous local-to-global zoom, a cartographic basemap, north-up
behavior, preserved incident context, scale-dependent display, and optional
imagery.

### Phase 3 — satellite prototype

Add representative orbital information, propagation, constellation-specific
markers, tracks, motion, and appropriately qualified coverage footprints.

### Phase 4 — timeline and prediction

Add synchronized historical, current, and future views while clearly
distinguishing observed and predicted information.

### Phase 5 — alert integration

Populate the map automatically from the stable RDS alert pipeline and connect
locations, uncertainty, satellite events, and explanations.

### Phase 6 — SRR boundary intelligence and coordination prototype

Load SRR geometry, detect boundary and A/B split scenarios, show the relevant
SRRs on the map, and prompt chat and sharing between the affected command
centers.

### Phase 7 — advanced context

Add selected LUT paths, environmental information, maritime/aviation context,
and more advanced detection predictions according to operational value and
data reliability.

### Phase 8 — validation and operational usability

Use representative historical scenarios to test geographic accuracy, orbital
accuracy, time synchronization, SRR boundary behavior, coordination prompts,
interface responsiveness, and operator comprehension. Validate analytical
results independently of presentation.

## Governing principles

1. Operational value and safety come first.
2. Keep the watchstander experience simple and explainable.
3. Preserve working RDS components and introduce complexity incrementally.
4. Use one source of truth for geographic, temporal, and case information.
5. Clearly distinguish facts, calculations, predictions, and human decisions.
6. Use real, authoritative data where available and label demonstrations.
7. Design manual alert entry so it can later be replaced by automated intake.
8. Close the learning loop with validated case outcomes.
