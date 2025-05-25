"""
Pandas Cryptographic Utilities

Provides vectorized encryption and decryption functions compatible with Pandas Series,
suitable for use in PySpark Pandas UDFs.
"""

import os
from typing import Optional
import base64
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from spark_catalyst_udf_comparison.crypto_config import AES_GCM, IV_SIZE, ENCONDING


def encrypt_with_pandas(text_series: pd.Series) -> pd.Series:
    """
    Encrypts each string in a Pandas Series using AES-GCM.

    Args:
        text_series (pd.Series): A series of strings to encrypt.

    Returns:
        pd.Series: A series of base64-encoded encrypted strings.
    """

    def _encrypt(text: Optional[str]) -> Optional[str]:
        if text is None:
            return None
        iv = os.urandom(IV_SIZE)
        data = text.encode(ENCONDING)
        encrypted = AES_GCM.encrypt(iv, data, associated_data=None)
        return base64.b64encode(iv + encrypted).decode(ENCONDING)

    return text_series.apply(_encrypt)


def decrypt_with_pandas(encoded_series: pd.Series) -> pd.Series:
    """
    Decrypts each string in a Pandas Series using AES-GCM.

    Args:
        encoded_series (pd.Series): A series of base64-encoded strings containing IV + ciphertext.

    Returns:
        pd.Series: A series of decrypted plaintext strings.
    """

    def _decrypt(encoded: Optional[str]) -> Optional[str]:
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

    return encoded_series.apply(_decrypt)

encrypt_with_key_udf = pandas_udf(encrypt_with_pandas, returnType=StringType())
decrypt_with_key_udf = pandas_udf(decrypt_with_pandas, returnType=StringType())