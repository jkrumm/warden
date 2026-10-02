"""clients — the transport half of the old bash dispatch actuator, ported to
Python (Wave 5.1). Each module owns one outbound edge (sideclaw, GitHub,
Slack, the one-arm deploy allowlist); the
lifecycle/policy half that used to glue them together in the bash script
comes in a later wave and imports these as plain functions.
"""
