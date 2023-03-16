import hashlib
from typing import Optional


def string_hash_63(string: Optional[str]) -> int:
    if string is None:
        return 1
    bytes_digest = hashlib.sha3_256(string.encode('utf-8')).digest()
    return int.from_bytes(bytes_digest[:8], byteorder='big') >> 1
