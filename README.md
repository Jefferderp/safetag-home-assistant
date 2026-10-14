# Safetag GPS Tracking for Home Assistant

Unofficial, read-only integration using the **official Safetag API** and live
WebSocket. Includes a reusable asynchronous Python client and an optional
Bearer-authenticated REST gateway. No dashboard cookies or browser scraping
are needed at runtime. Not affiliated with Safetag.

## Install with HACS

Requires Home Assistant **2025.3.3 or newer**, HACS, and a Safetag API key.

**1. Open/add the repository in HACS:**

[![Open/add in HACS](https://img.shields.io/badge/HACS-Open%2Fadd_repository-41BDF5?style=for-the-badge)](https://my.home-assistant.io/redirect/hacs_repository/?owner=Jefferderp&repository=safetag-home-assistant&category=integration)

If necessary, add `https://github.com/Jefferderp/safetag-home-assistant` as a
HACS custom repository, category **Integration**.

**2. Download Safetag GPS Tracking in HACS, then restart Home Assistant Core.**
A browser refresh is not a Core restart. HACS cannot install and load a custom
component in literally one click.

**3. After the restart, add the integration:**

[![Add integration after restart](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=safetag)

Enter your Safetag account email and API key. The form validates authentication
and the tracker response before saving. Credentials stay in Home Assistant's
local configuration; nothing is uploaded to this repository.

For manual installation, copy `custom_components/safetag/` into your Home
Assistant `config/custom_components/` directory, restart Core, and add Safetag.

## Entities

Each tracker receives a GPS `device_tracker` and the following telemetry:

| Entity | Units and meaning |
| --- | --- |
| GPS position | Latitude/longitude; Home Assistant zones and map support |
| Speed | mph, converted from Safetag's km/h using `km/h / 1.609344` |
| Heading | Degrees, as reported |
| External voltage | V; vehicle/external supply voltage, not the internal battery percentage |
| Internal battery level | Safetag's raw **0–6** scale, not a percentage; undocumented values are unknown |
| Cellular signal | %; do not interpret as dBm or GPS accuracy |
| Location updated | Timestamp of the last location report |
| Status updated | Timestamp of the last tracker communication |
| Charging | Whether the wired tracker reports external charging/power |
| Engine cut | Read-only relay state: true = engine cut; no relay control is provided |
| Model/product code | Diagnostic numeric Safetag codes, not guessed model names |

Missing, invalid, and null values remain unknown; missing data is never silently
changed to zero. Your tracker model may not expose every field. Safetag's
[tracker documentation](https://docs.safetagtracking.com/reference/get-trackers)
defines speed, battery, signal, and timestamps. The live dashboard renders
`status.external_voltage` with a `V` suffix, confirming its unit.

## Updates and reliability

A paginated REST snapshot bootstraps each account, followed by one WebSocket
connection for live updates. The client merges partial status updates rather
than dropping existing telemetry. Disconnections use bounded exponential
reconnect backoff; authentication failure starts Home Assistant reauthentication.
Home Assistant reconciles tracker membership on reconnect, at most once per
minute. The optional gateway obtains a fresh authoritative snapshot before each
connection attempt, including reconnects, and honors server retry delays. Healthy
connections do not poll. The client limits its REST calls to 20 per minute;
Safetag enforces the total per-account limit across independent clients.
There is no location-history polling. Unknown trackers can be added dynamically.

A working connection does **not** mean the tracker has a fresh GPS fix. Check
**Location updated** and **Status updated** before using position data for an
automation. A parked or sleeping tracker may report infrequently. Connection
failures make telemetry unavailable; location timestamps remain authoritative.

Safetag currently documents **20 requests/minute**, **5 WebSocket connections
per account**, and a minimum of **10 trackers for free API usage**. A key may work
with fewer devices; availability and pricing remain Safetag's decision. Both
Home Assistant and an independently running gateway count toward the connection
limit. Create/revoke keys in your Safetag account's Developer tab.

## Optional REST API gateway

The Home Assistant integration connects directly to Safetag and **does not
require this gateway**. The gateway serves other applications from a cached
snapshot updated by the official WebSocket.

Run from this repository with Python 3.13+:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install .
# Set these in your service's protected environment, not source control:
export SAFETAG_USERNAME='your-account@example.invalid'
export SAFETAG_API_KEY='<your-private-api-key>'
export SAFETAG_GATEWAY_TOKEN='<a-separate-random-token-of-at-least-32-characters>'
.venv/bin/python -m gateway
```

Never paste real secrets into a shell command stored in history. A restricted
service environment file is preferable. See [SECURITY.md](SECURITY.md).

The gateway defaults to loopback; use a TLS reverse proxy for remote access.
All data routes and OpenAPI require a Bearer authorization header using your
separate gateway token.
`/health` reveals only service liveness. GPS responses contain private location
data even though tracker IDs are hashed. No account email, IMEI, billing details,
or upstream tracker names are included in public gateway responses.

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Minimal liveness check |
| `GET /v1/status` | Connection/cache freshness |
| `GET /v1/trackers` | All cached normalized tracker telemetry |
| `GET /v1/trackers/{id}` | One tracker using its hashed API ID |
| `GET /openapi.json` | Authenticated machine-readable API specification |

The API is read-only: it cannot cut the engine, change alerts, delete location
history, modify subscriptions, or activate trackers. Gateway credentials and
cache are in memory; no location history is persisted. API access logs are off.

## Privacy

- No bundled credentials, real tracker identifiers, real coordinates, personal
  labels, or production logs. All test fixtures are synthetic.
- Stable entity/device IDs use account-scoped SHA-256 digests, not raw IMEIs or
  email addresses. Deleting and re-adding the same account preserves IDs.
- Tracker display names are generic. Rename devices/entities locally if desired.
- Diagnostics exclude credentials, raw identifiers, coordinates, and telemetry
  values. Do not post full Home Assistant logs publicly.
- Your account email/API key must still be stored locally for authentication.
  Home Assistant Recorder may store GPS and sensor history; protect backups or
  exclude these entities from Recorder if you do not want history.

## Version 1.0.1

- Fix reconnect task failure when Home Assistant provides `retry_after=None`
  after a failed reconnect snapshot. Numeric server retry delays remain honored.
- Allow unload/reload to finish even if the background task already failed,
  without logging upstream exception details.
- Use the public `TrackerEntity` import ahead of HA Core 2027.6.
- Entity and device identifiers are unchanged.

If upgrading from 1.0.0 after a stuck connection, download the update in HACS
(or use Redownload if necessary), then **restart Home Assistant Core**. Reloading
the old integration can fail and does not load the updated Python modules.
Do not delete the integration or rotate a working API key.

## Troubleshooting

- **Invalid authentication:** verify the email/key, expiry and API entitlement;
  rotate the key in Safetag and use the integration's reauthentication form.
- **Cannot connect:** allow outbound HTTPS/WSS to `api.safetagtracking.com`.
  Check DNS, firewall, TLS certificates and Safetag's service status.
- **UI configuration unsupported:** verify HACS downloaded
  `custom_components/safetag/`, restart **Core**, and inspect local logs for
  config-flow import errors. Do not publish logs containing private information.
- **Old position:** check tracker power, cellular reception and location timestamp.
  Restarting Home Assistant cannot manufacture a newer tracker report.
- **Rate limited / too many sockets:** stop duplicate gateway/HA instances and
  honor upstream limits. Never use history polling as a reconnect workaround.

## Development

```bash
python3.13 -m venv .venv
.venv/bin/python -m pip install -e '.[test]' -c tests/constraints.txt
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/python -m compileall -q custom_components gateway
```

Tests run against the declared minimum Home Assistant release. Tests never
contact a real account. Runtime dependencies for the Home Assistant component
are provided by Home Assistant; FastAPI is only needed for the optional gateway.

License: MIT.
