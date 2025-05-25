"""
Cryptographic Utility Module

Provides functions for AES-GCM encryption and decryption using a key retrieved from an HSM simulator.
These functions are intended for use in PySpark Python UDFs.
"""

import os
from typing import Optional
import base64
from spark_catalyst_udf_comparison.crypto_config import AES_GCM, IV_SIZE, ENCONDING


def encrypt_with_key(text: str) -> Optional[str]:
    """
    Encrypts a string using AES-GCM with a preloaded key.

    Args:
        text (str): The plaintext string to encrypt.

    Returns:
        Optional[str]: Base64-encoded encrypted string containing IV + ciphertext,
                       or None if input is invalid.
    """
    if text is None:
        return None

    iv = os.urandom(IV_SIZE)
    data = text.encode(ENCONDING)
    encrypted = AES_GCM.encrypt(iv, data, associated_data=None)

    full_encrypted = iv + encrypted
    return base64.b64encode(full_encrypted).decode(ENCONDING)


def decrypt_with_key(encoded: str) -> Optional[str]:
    """
    Decrypts a Base64-encoded string using AES-GCM.

    Args:
        encoded (str): Encoded string containing IV + ciphertext.

    Returns:
        Optional[str]: Decrypted plaintext string, or None if decoding fails or input is invalid.
    """
    if encoded is None or not isinstance(encoded, str):
        return None

    try:
        full_data = base64.b64decode(encoded)
        if len(full_data) < IV_SIZE:
            return None

        iv = full_data[:IV_SIZE]
        cipher_text = full_data[IV_SIZE:]

        decrypted = AES_GCM.decrypt(iv, cipher_text, associated_data=None)
        return decrypted.decode(ENCONDING) if decrypted else None
    except Exception:
        return None