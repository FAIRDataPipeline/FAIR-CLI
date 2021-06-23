import pytest
import tempfile

import fair.common as fdp_com


@pytest.fixture(scope="session")
def set_constant_paths(monkeypatch):
    monkeypatch.setattr(fdp_com, "REGISTRY", tempfile.mkdtemp())
