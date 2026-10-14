# Security and privacy

Never include credentials, coordinates, tracker identifiers, names, or complete
Home Assistant logs in a public issue. Rotate an exposed Safetag API key in the
Safetag account Developer tab. Report vulnerabilities privately via GitHub's
private vulnerability reporting if available; otherwise contact the maintainer
without attaching secrets or exploit data to a public issue.

The integration sends your account email and API key only to the fixed HTTPS/WSS
Safetag API endpoints. Home Assistant stores configuration credentials in its
local `.storage` directory; protect that directory and your backups. GPS states
are personal data and may be recorded by Home Assistant Recorder/history.
Diagnostics intentionally omit credentials, account and device identifiers,
friendly names, coordinates, and telemetry values.

The optional gateway uses a separate local Bearer token and hashed device IDs.
Treat location responses as private even with hashed identifiers. Keep the
service on loopback, or use a TLS reverse proxy and scoped firewall rules.
Do not expose the unauthenticated upstream tracker stream or put credentials
in URLs. No driving/engine control endpoints are implemented.
