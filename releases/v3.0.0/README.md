# Local-agent bridge 3.0

See AGENT_PROMPT.md for the API, recovery policy and limits.

Modules: agent_bridge.py (coordinator/worker), workspace_ops.py (file/search/Git), bridge_core.py (tested transport/capture helpers).

Run test_v3.py from this directory. Tests create only disposable directories and user systemd test jobs.

Deployment uses a versioned release, atomic launcher/config swap, preserved backups and an independent rollback supervisor.
