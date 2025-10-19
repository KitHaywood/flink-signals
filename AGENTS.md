## Agent Execution Notes

- Always activate the bundled virtual environment before invoking tooling that depends on Python dependencies. Use `. .venv/bin/activate && pytest` for the test suite.
- Maintain the existing virtualenv path in future scripts or docs; updates should reflect `.venv` usage unless the environment layout changes.
