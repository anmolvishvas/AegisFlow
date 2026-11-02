"""Smoke test to verify project environment and package imports."""

import aegisflow


def test_package_import() -> None:
    """Verify that aegisflow can be imported and has a valid version."""
    assert aegisflow.__version__ == "0.1.0"
