"""
Crypto Configuration Module

This module centralizes cryptographic configuration used across the project,
including key retrieval, cipher mode, and IV size. This helps avoid duplication
and improves maintainability.
"""

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from spark_catalyst_udf_comparison.hsm_simulator import HsmSimulator


# Retrieve encryption key from HSM simulator
AES_KEY = HsmSimulator.get_key("key_generic")

# Initialize AES-GCM cipher instance
AES_GCM = AESGCM(AES_KEY)

# Recommended IV size for AES-GCM
IV_SIZE = 12  # bytes

# Encoding used for crypto operations
ENCONDING = "utf-8"