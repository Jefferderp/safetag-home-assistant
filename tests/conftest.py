"""Home Assistant integration fixtures."""

import pytest


@pytest.fixture(autouse=True)
def custom_integrations(enable_custom_integrations):
    """Allow loading this custom integration in HA's flow manager."""
    from pathlib import Path

    import custom_components

    path = str(Path(__file__).resolve().parents[1] / "custom_components")
    original = list(custom_components.__path__)
    custom_components.__path__ = [path, *original]
    yield
    custom_components.__path__ = original
