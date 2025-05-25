"""
Unit tests for the HSM Simulator module.

These tests validate that the HsmSimulator class correctly retrieves cryptographic keys,
and raises appropriate errors when invalid key IDs are requested.
"""

import pytest
from spark_catalyst_udf_comparison.hsm_simulator import HsmSimulator


def test_get_key_valid():
    """
    Test retrieving a valid key from the HSM simulator.
    """
    key = HsmSimulator.get_key("key_generic")
    assert key == b"generic_key_1234"


def test_get_key_invalid():
    """
    Test that an invalid key ID raises a ValueError.
    """
    with pytest.raises(ValueError) as exc_info:
        HsmSimulator.get_key("invalid_key")

    assert "Unknown key" in str(exc_info.value)
    assert "invalid_key" in str(exc_info.value)


def test_get_key_case_sensitive():
    """
    Test that key retrieval is case-sensitive.
    """
    with pytest.raises(ValueError):
        HsmSimulator.get_key("KEY_GENERIC")