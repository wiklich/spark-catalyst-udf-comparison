"""
HSM Simulator Module

This module provides a mock implementation of a Hardware Security Module (HSM)
used to simulate secure key retrieval for cryptographic operations.
"""

from typing import Dict

KEYS: Dict[str, bytes] = {
    "key_generic": b"generic_key_1234"
}


class HsmSimulator:
    """
    Simulates a Hardware Security Module (HSM) that securely stores and retrieves cryptographic keys.

    This class provides static methods to access predefined keys used in encryption/decryption processes.
    """

    @staticmethod
    def get_key(key_id: str) -> bytes:
        """
        Retrieves a cryptographic key from the simulated HSM.

        Args:
            key_id (str): The identifier of the key to retrieve.

        Returns:
            bytes: The cryptographic key in bytes.

        Raises:
            ValueError: If the requested key ID does not exist.
        """
        if key_id not in KEYS:
            raise ValueError(f"Unknown key: {key_id}")
        return KEYS[key_id]