# RDS inter-RCC messaging and coordination

Status: proposed enhancement; not implemented

Recorded: 2026-10-21

Architecture review date: 2026-10-21

Planning baseline: `main` commit `fa117887365918c93a31859223039365f8bd55b4`

Source: project-owner requirements supplied 2026-10-21

## Objective and scope

Add a simple, reliable messaging capability inside RDS so watchstanders can
communicate with one or more participating RCCs. RDS should also recommend
communication when deterministic analysis identifies an alert that may involve
multiple SRRs.

Messaging may be linked to an alert or case, but a case is not required to
start a conversation. Recommendations are advisory and must not obstruct the
existing alert workflow. RDS may recommend coordination; it does not transfer
SAR responsibility or make an operational decision for an RCC.

This proposal is limited to:

- direct and group RCC conversations;
- message history, timestamps, unread state, and new-message notification;
- optional alert/case association;
- SRR-boundary and related deterministic coordination recommendations; and
- entry points from the existing alert analysis and map experience.

It does not propose a separate communications product, a general incident-
management platform, or changes to existing alert, weather, satellite, and GIS
analysis except where integration is required.

## Requested watchstander experience

A watchstander can open RDS, select one or more participating RCCs, and start a
conversation without first opening a case. Messages show the sender, sending
RCC, and UTC timestamp, remain available after reconnecting, and identify new
or unread activity.

When a conversation is linked to an alert, RDS shows the alert or case
identifier and makes the conversation available from that alert's history.
The participants can discuss positions, updates, and operational actions in
the same simple conversation view.

When a deterministic rule finds a possible multi-SRR situation, the alert
analysis shows the relevant RCC and a short reason, with one action to open or
join a conversation. Examples include:

- an alert position near an SRR boundary;
- an uncertainty area intersecting more than one SRR;
- A and B positions associated with different SRRs;
- a later position crossing an SRR boundary; or
- a related alert already associated with another RCC.

## Verified current architecture

The following findings were checked against the planning baseline. They are
current implementation facts, not assumptions about a future deployment.

| Area | Verified state | Reusable part | Gap for this feature |
|---|---|---|---|
| Web application | Flask application factory, blueprint routes, Jinja templates, and browser JavaScript | Existing application, JSON conventions, templates, and response safeguards | No messaging routes, background delivery channel, or chat UI |
| Frontend | Server-rendered pages with embedded JavaScript; the alert map is generated Folium HTML displayed in an iframe | `sarsat_analysis.html` can host a coordination panel beside the current results | No React or other client framework is present; a frontend rewrite is unnecessary for the first version |
| Authentication | No login, user account, session identity, or RCC membership implementation was found | None currently | A verified watchstander and RCC identity is required before messages can be attributed or access-controlled |
| Database | SQLAlchemy models and PostgreSQL driver are declared; setup is lazy through `DATABASE_URL` | Existing SQLAlchemy foundation and alert/weather model patterns | Alert persistence is disabled in the active pipeline; no migration framework or messaging tables exist |
| Alert identity | Parser and pipeline retain `site_id` and `alert_sequence_number`; `SARSATAlert.id` exists only after persistence | These fields can provide a display/reference key | RDS does not yet have a persistent case entity or a guaranteed durable alert key in the active web workflow |
| GIS | Pandas, GeoPandas, Shapely, Folium, and validated point/ring/track geometry are active | Existing A/B positions and uncertainty rings can feed deterministic SRR rules | Only coastline shapefiles are committed; no authoritative SRR polygons, RCC directory, or adjacency data were found |
| Real-time delivery | Ordinary HTTP JSON requests are used | Existing Flask/JavaScript request pattern | No WebSocket, Socket.IO, server-sent-event, queue, or pub/sub dependency is present |
| Deployment | Gunicorn is an optional server dependency; no production deployment was performed | Existing application entry point and CI configuration checks | Production worker count, instance count, proxy behavior, and connection-lifetime support are not established |

## Architecture constraints and decisions still required

Implementation should not start by guessing the following project decisions:

1. **Identity source:** choose how watchstanders authenticate and how an
   account is assigned to an RCC. There is no existing authentication system
   to reuse. This may be a small RDS login or an approved upstream identity
   provider, but the choice affects every message and access check.
2. **Authoritative SRR data:** select a maintained source, version, licence,
   update procedure, and RCC-to-SRR ownership mapping. Coastline data cannot be
   substituted for SRR boundaries.
3. **Durable alert/case key:** decide whether a conversation links to a
   persisted `SARSATAlert`, a new case record, or another stable operational
   identifier. `site_id` alone may recur and is not a database relationship in
   the active workflow.
4. **Persistence activation:** decide when the existing PostgreSQL path may be
   enabled and introduce a repeatable migration mechanism before adding
   messaging tables. This proposal does not enable database writes.
5. **Retention and access:** set message-retention, correction, export, and
   authorized-participant rules appropriate to RCC use. This plan does not
   invent those policies.
6. **Deployment topology:** establish whether one or multiple application
   instances will run. That determines whether database polling remains
   sufficient or a shared event service is later justified.

These are implementation gates because they materially affect identity,
operational records, and deployment. They are not resolved by this document.

## Minimal integration design

### Application structure

Keep the feature in the existing Flask application. Add a dedicated
coordination blueprint and service layer rather than expanding the alert route
with message handling. Reuse the current Jinja and browser-JavaScript approach
for the first implementation; do not introduce React or replace the Folium map
to build chat.

The alert-analysis page should gain a coordination panel outside the map iframe
so it remains usable when the map is unavailable. The panel should provide:

- conversation list and unread indicators;
- participating RCC names;
- message history and composer;
- an optional alert/case link; and
- a concise recommendation card with an **Open conversation** action.

SRR boundaries and highlighted regions can remain map layers. The message
composer and recommendation action should remain normal RDS page controls,
which avoids coupling message behavior to generated map HTML.

### Initial delivery mechanism

Start with database-backed JSON endpoints and bounded, configurable short
polling for messages newer than the last message identifier. Sending remains a
normal authenticated `POST`; receiving uses a lightweight authenticated `GET`.
This provides near-real-time behavior without adding a socket server, broker,
or new browser library, and it works with the current Flask request model.

Suspend polling when the page is not active and resume with the last seen
identifier. Reconnection therefore reads durable history rather than relying
on an in-memory event. If operational testing later shows that polling does not
meet latency or load requirements, server-sent events are the next incremental
option. WebSockets and shared pub/sub should be added only when the verified
deployment topology requires them.

### Proposed persistent model

The exact migration is deferred, but the minimal relational model is:

| Record | Essential fields |
|---|---|
| `rcc` | stable ID, display name, short code, active state |
| `watchstander` | stable ID, login identity reference, display name, home RCC, active state |
| `conversation` | stable ID, optional title, created time, creator, optional alert/case link |
| `conversation_rcc` | conversation, RCC, joined time |
| `conversation_participant` | conversation, watchstander, joined/left time |
| `message` | monotonic ID, conversation, sender, sender RCC snapshot, UTC created time, body |
| `conversation_read_state` | conversation, watchstander, last-read message/time |

Keep the sender RCC on each message as a historical snapshot even if the user's
later assignment changes. Store timestamps in UTC. Use database constraints
and indexes for conversation/time and unread lookups. Do not store the message
history only in process memory.

An alert/case association should be a proper foreign key or link table after
the durable case-key decision. Do not encode the relationship only in message
text.

### Proposed API surface

All endpoints require the future authenticated watchstander identity and must
derive RCC membership on the server rather than trust a browser-supplied RCC.

| Endpoint responsibility | Behavior |
|---|---|
| List RCCs | Return active directory entries available to the signed-in watchstander |
| List/create conversations | Support direct and group conversations, with an optional alert/case link |
| Read/send messages | Page older history and return messages after a monotonic cursor; create one validated message |
| Update read state | Record the last message read by the current watchstander |
| Retrieve case conversations | Return conversations explicitly linked to the durable alert/case key |
| Retrieve recommendations | Return current deterministic recommendation records and their evidence |

Request-size limits, stable JSON errors, server-side logging, and no-store
responses should follow the patterns already used by `/process_alert`.

### Geographic recommendation service

Add a pure service that accepts normalized alert geometry and a versioned SRR
dataset. It should return structured recommendations, not directly create
conversations or send messages.

The first rules should be deterministic:

1. identify the SRR containing each valid A/B position;
2. calculate distance from each position to the nearest SRR boundary;
3. intersect each uncertainty geometry with SRR polygons;
4. compare A and B SRR memberships;
5. compare a later normalized position with prior persisted positions; and
6. when stable correlation is available, identify related alerts already
   associated with another RCC.

Every recommendation should contain a rule code, relevant RCC/SRR identifiers,
a short operator-facing explanation, and the evidence used. Boundary-distance
thresholds must be configurable and approved from operational requirements;
this plan does not invent a distance.

The service should not guess when coordinates, uncertainty, SRR geometry, or
RCC mappings are unavailable. It should return an explicit unavailable or
insufficient-data result and leave the existing alert analysis usable.

## Incremental implementation plan

### Phase 1 — foundation decisions and fixtures

- approve the identity source and RCC membership model;
- select and document the authoritative SRR/RCC data source;
- choose the durable alert/case key and database migration mechanism;
- add versioned non-sensitive test fixtures for RCCs and synthetic SRR
  boundaries; and
- confirm the intended deployment topology.

Acceptance: the four core inputs—actor identity, RCC directory, durable alert
key, and versioned SRR geometry—are available in tests without enabling a
production deployment.

### Phase 2 — basic messaging

- add the approved database migration and models;
- add authenticated directory, conversation, message, history, and read-state
  endpoints;
- add the integrated conversation panel and configurable short polling; and
- test two RCC identities, group conversations, reconnect, ordering, paging,
  unread state, and authorization boundaries.

Acceptance: two watchstanders assigned to different RCCs exchange and later
retrieve messages inside RDS without leaving the application.

### Phase 3 — alert/case linkage

- link conversations to the approved durable alert/case key;
- add **Open conversation** from the alert analysis;
- show linked conversations with the case history; and
- share a concise alert reference without copying uncontrolled raw data into
  every message.

Acceptance: a watchstander opens a conversation from an active alert and later
retrieves it through that alert/case.

### Phase 4 — SRR recommendations

- load the approved, versioned SRR geometry and RCC mapping;
- implement and unit-test containment, proximity, overlap, A/B split, and
  boundary-crossing rules;
- display SRR boundaries and highlighted relevant regions;
- show concise, evidence-backed recommendation cards; and
- connect each recommendation to the existing conversation flow.

Acceptance: a qualifying synthetic or approved historical scenario identifies
the relevant RCC, explains the deterministic trigger, and offers to open a
conversation without sending a message automatically.

### Phase 5 — operational refinement

- exercise simultaneous users, reconnects, message ordering, and database
  recovery;
- test group membership and unread behavior;
- measure delivery latency and polling load before considering another event
  transport;
- test SRR edges, holes, antimeridian geometry, overlapping uncertainty, and
  false or unnecessary recommendations; and
- verify that messaging and SRR failures cannot stop alert processing.

Acceptance: the feature meets approved latency, data, and usability criteria
without regressing the existing offline alert suite.

## Verification strategy

Required automated coverage includes:

- model constraints, migration upgrade/downgrade, and UTC timestamps;
- authenticated participant and RCC access boundaries;
- direct and group conversation creation;
- message validation, ordering, pagination, unread counts, and reconnection;
- durable alert/case association;
- deterministic SRR containment, proximity, overlap, split-position, and
  relocation fixtures;
- missing or malformed geometry and unavailable SRR data;
- stable recommendation explanations and direct conversation actions; and
- existing alert processing when the coordination service is unavailable.

Browser-level checks should cover keyboard use, focus, readable new-message
status, and a complete alert-to-conversation workflow. Load testing should use
the approved concurrent-watchstander and message-rate targets rather than
inventing production capacity requirements.

## Dependency policy

The first version should add no frontend framework, WebSocket package,
message broker, or PostGIS requirement. It can reuse Flask, browser JavaScript,
SQLAlchemy, PostgreSQL, GeoPandas, and Shapely already declared by RDS.

A migration tool and authentication integration will be required after their
approaches are approved. PostGIS may be evaluated later if measured SRR query
volume or multi-instance operation makes in-process versioned geometry
insufficient. Any additional dependency must solve a demonstrated requirement.

## Current readiness conclusion

The existing Flask page, JSON patterns, SQLAlchemy foundation, normalized
alert positions, uncertainty rings, and GIS libraries are reusable. A separate
platform or frontend rewrite is not required.

Implementation is not yet ready to begin because authenticated RCC identity,
active durable persistence, a stable case link, and authoritative SRR/RCC data
are absent. Those gaps should be resolved explicitly before Phase 2 or Phase 4
code is introduced. Until then, this document records the requested feature
and a minimal integration path without representing it as current behavior.
