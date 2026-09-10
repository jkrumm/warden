"""Shim: the Slack client moved to scripts/clients/slack.py (Wave 5.1). Loaded by path from triage.py, dispatch-sweep.py and tests."""
import sys
from pathlib import Path
_SCRIPTS = str(Path(__file__).resolve().parent)
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)
from clients.slack import *  # noqa: F401,F403
from clients.slack import resolve_slack_token, slack_post_message  # explicit re-export
