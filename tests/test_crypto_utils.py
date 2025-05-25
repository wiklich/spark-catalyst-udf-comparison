"""
Unit tests for crypto_utils.py

This module validates the correctness and failure handling of AES-GCM encryption/decryption functions,
without relying on Spark or HSM hardware.
"""

import pytest
import os
import base64
from typing import Optional
from spark_catalyst_udf_comparison.crypto_utils import encrypt_with_key, decrypt_with_key

IV_SIZE = 12
ENCONDING = "utf-8"


def test_encrypt_with_key_valid_input():
    text = "Hello World"
    encrypted = encrypt_with_key(text)
    assert isinstance(encrypted, str)
    assert len(encrypted) > 0


def test_decrypt_with_key_valid_input():
    text = "Secret Message"
    encrypted = encrypt_with_key(text)
    decrypted = decrypt_with_key(encrypted)
    assert decrypted == text


def test_decrypt_with_key_invalid_types():
    invalid_inputs = [None, 123, True, b'bytes', object(), "not_base64", "base64_but_too_short"]
    for data in invalid_inputs:
        result = decrypt_with_key(data)
        assert result is None


def test_decrypt_with_key_invalid_base64():
    encoded = "invalid_base64_string_@#$"
    result = decrypt_with_key(encoded)
    assert result is None


def test_decrypt_with_key_corrupted_data():
    text = "Data to corrupt"
    encrypted = encrypt_with_key(text)
    broken = encrypted[:5] + "X" + encrypted[6:]
    result = decrypt_with_key(broken)
    assert result is None


def test_encrypt_with_key_returns_valid_base64():
    text = "TestBase64Output"
    encrypted = encrypt_with_key(text)
    decoded = base64.b64decode(encrypted)
    assert len(decoded) > IV_SIZE


def test_encrypt_decrypt_roundtrip():
    text = "Top Secret Message"
    encrypted = encrypt_with_key(text)
    decrypted = decrypt_with_key(encrypted)
    assert decrypted == text


def test_decrypt_with_key_invalid_iv():
    fake_iv = base64.b64encode(b"shortivdata").decode("utf-8")
    result = decrypt_with_key(fake_iv)
    assert result is None


def test_encrypt_decrypt_with_special_characters():
    text = "!@#$%^&*()_+{}[]|:\"<>?,./"
    encrypted = encrypt_with_key(text)
    decrypted = decrypt_with_key(encrypted)
    assert decrypted == text


def test_encrypt_decrypt_with_unicode():
    text = "áéíóúãõçñàè"
    encrypted = encrypt_with_key(text)
    decrypted = decrypt_with_key(encrypted)
    assert decrypted == text


def test_decrypt_with_key_missing_iv():
    fake_data = base64.b64encode(b"too_short_for_iv").decode("utf-8")
    result = decrypt_with_key(fake_data)
    assert result is None