"""
Unit tests for crypto_config.py

This module tests the cryptographic configuration values used in encryption/decryption.
"""

import pytest
from spark_catalyst_udf_comparison.crypto_config import AES_KEY, AES_GCM, IV_SIZE, ENCONDING


def test_aes_key_retrieved():
    """
    Test that AES_KEY is retrieved correctly from HsmSimulator.
    """
    assert isinstance(AES_KEY, bytes)
    assert len(AES_KEY) > 0


def test_aes_gcm_instance_created():
    """
    Test that AES_GCM instance was successfully created with the key.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    assert isinstance(AES_GCM, AESGCM)


def test_iv_size():
    """
    Test that IV_SIZE is set to the recommended value (12 bytes for AES-GCM).
    """
    assert IV_SIZE == 12


def test_encoding():
    """
    Test that encoding is set to UTF-8.
    """
    assert ENCONDING == "utf-8"


def test_invalid_key_raises_error():
    """
    Test that invalid keys raise appropriate exceptions when used with AESGCM.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    with pytest.raises(ValueError) as exc_info:
        invalid_key = b"short-key"
        AESGCM(invalid_key)

    assert "AESGCM key must be 128, 192, or 256 bits." in str(exc_info.value)