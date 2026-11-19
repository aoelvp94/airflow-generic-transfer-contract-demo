"""Local object-store emulators and the in-process fake HTTP API.

Runtime entry point: ``emulators/emulators.sh``. Connection settings:
``emulators/emulators.env``. Compose file: ``emulators/docker-compose.yml``.
The fake API used by the pagination bench and parity tests is
:mod:`emulators.fake_api`.
"""
