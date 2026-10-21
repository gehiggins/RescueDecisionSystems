# RDS product assurance framework

Status: adopted project framework

Effective: 2026-10-21

Baseline at adoption: `main` commit `657339f759732469a179084331518a4b1d1883d0`

This document governs how Rescue Decision Systems (RDS) capabilities,
requirements, development work, and verification are organized. It begins
with the operational purpose of RDS and works downward toward individual
acceptance criteria and technical tests.

It is intentionally broader than the present implementation. Current code,
available data, and tests are evidence about the product; they do not define
the full limits of the product.

## Guiding principle

> Begin with the operational outcome, divide it into capabilities and
> situations, derive requirements from those, and only then define individual
> tests. Tests provide evidence of the mission; they do not define the mission.

RDS work must remain traceable in both directions:

- every detailed requirement or test should support a broader capability and
  operational outcome; and
- every adopted operational outcome should eventually be supported by defined
  capabilities, requirements, and evidence.

This prevents convenient technical checks from being mistaken for proof that
the product works operationally.

## 1. Mission and operational outcome

RDS supports search-and-rescue operations by turning alert information and
relevant environmental, geographic, and satellite data into a timely,
understandable, dependable, and shareable operational picture.

Product evaluation begins by asking whether RDS supports that outcome. It does
not begin with a particular script, screen, field, provider, or test.

## 2. Operational lifecycle

The top-level RDS workflow is:

1. receive an alert;
2. interpret and validate it;
3. establish possible locations and uncertainty;
4. gather relevant weather, satellite, geographic, and other supporting
   information;
5. present a coherent operational picture;
6. help operators assess and coordinate the response;
7. support continuing updates as the incident and conditions change; and
8. preserve the operational record for review and improvement.

This lifecycle is the organizing spine for product work. A component can work
in isolation while the complete lifecycle still fails.

## 3. Capability areas

The initial capability map is deliberately broad and non-exhaustive:

- alert intake and interpretation;
- location calculation and uncertainty;
- mapping and operational visualization;
- weather and environmental intelligence;
- satellite intelligence;
- SRR boundary and cross-RCC coordination;
- case information, sharing, and communication;
- data history and auditability;
- reliability and degraded-operation support;
- operator experience;
- security, access, and system administration; and
- deployment and sustainment.

Capabilities may be refined or divided as operational understanding improves.
Adding detail must not silently narrow the mission or discard situations that
do not fit the current implementation.

## 4. Operational situations

Each capability must be considered across realistic situations, including:

- normal alerts with complete data;
- incomplete, malformed, delayed, duplicated, or conflicting alerts;
- stale, partial, conflicting, or unavailable supporting data;
- external-provider or network failure;
- degraded or disconnected operation;
- positions close to an SRR boundary;
- A/B solutions split between SRRs;
- multiple RCCs coordinating on the same incident;
- continuing updates to an existing incident; and
- high-volume or time-critical operations.

This list is a starting structure, not a closed set. New operational evidence
can add or change situations before detailed criteria are written.

## 5. Capability requirements

Requirements are derived only after the relevant operational outcome,
capability, and situations are understood. They should state what operators
need the product to accomplish without prematurely constraining the solution
to current code or one provider.

For example, weather and environmental intelligence should be considered in
terms such as:

- where and when an observation or forecast applies;
- the source and nature of the data;
- its age and relevance to the alert or incident;
- the atmospheric and marine conditions available;
- whether values are observed, forecast, estimated, missing, or stale; and
- how RDS behaves when one or more weather sources fail.

The currently displayed fields are therefore evidence of partial capability,
not the definition of the complete weather requirement.

## 6. Acceptance criteria and evidence

Measurable acceptance criteria are written after the broader requirement is
defined. Evidence can include:

- unit and component tests;
- integration and end-to-end tests;
- complete browser-rendering and interaction tests;
- controlled-data scenarios;
- live-provider checks;
- degraded-operation and recovery tests;
- operator workflow exercises;
- screenshots, logs, and test reports; and
- recorded uncertainties and unverified areas.

Evidence must be described precisely. A generated file does not prove that a
browser rendered it correctly. Embedded data does not prove that an external
provider supplied current data. A controlled example does not prove that a
live integration works.

Use these evidence states consistently:

- **Passed:** the stated criterion was directly exercised and the expected
  evidence was observed.
- **Failed:** the criterion was exercised and the expected result was not
  achieved.
- **Unverified:** the criterion was not exercised or the required evidence
  could not be observed.
- **Not applicable:** the criterion does not apply to the stated situation,
  with the reason recorded.

An overall workflow or capability must not be reported as passed when a
required part is failed or unverified. Partial success should be reported at
the level actually demonstrated.

## 7. Traceability and work records

Planning documents, issues, pull requests, and test reports should identify,
at the appropriate level:

| Level | Question answered |
|---|---|
| Operational outcome | What real-world result are we supporting? |
| Lifecycle stage | Where in the end-to-end workflow does it occur? |
| Capability | What broad product ability is involved? |
| Situation | Under what operating conditions must it work? |
| Requirement | What must RDS accomplish? |
| Acceptance criterion | What observable result demonstrates it? |
| Evidence | What was actually exercised and observed? |
| Status and uncertainty | What passed, failed, or remains unverified? |

Not every small repair needs a large new document, but its purpose and evidence
should still be traceable upward. Conversely, broad product goals should not be
declared complete based only on isolated low-level tests.

## 8. Applying the framework to the current map finding

The 2026-10-21 generated-example test demonstrated that RDS created an HTML
map and embedded alert and weather information. A user-observed browser render
then showed that the OpenStreetMap background tiles returned HTTP 403 errors.

The finding is recorded at the broader levels as follows:

- **Operational outcome:** provide an understandable operational picture.
- **Lifecycle stage:** present a coherent operational picture.
- **Capability:** mapping and operational visualization.
- **Situation:** an external map service rejects or cannot serve tile requests.
- **Operational effect:** alert positions can exist in the generated output
  while geographic context is unavailable to the operator.
- **Evidence status:** map generation and overlays partially passed; complete
  browser rendering failed.
- **Required follow-up:** define and verify suitable basemap service behavior,
  failure detection, and an operationally appropriate fallback.

This example illustrates why validation begins above the individual test. The
HTML file was successfully generated, but the required operational outcome was
not achieved.

## 9. Maintaining this framework

Update this document when the project adopts a material change to its mission,
operational lifecycle, capability structure, evidence vocabulary, or assurance
approach. More detailed capability and test documents may be added beneath it
without rewriting the framework for every repair.

Do not mark an aspirational capability as implemented merely because it appears
here. Implementation status belongs in the codebase status and capability-
specific records, supported by the evidence defined through this framework.
