import base64
import hashlib

XOR_KEY = "DetectiveConan2024"


def xor_encrypt(data: str, key: str = XOR_KEY) -> str:
    """XOR-encrypt a string and return base64-encoded result."""
    key_bytes = [ord(c) for c in key]
    encrypted = bytes(
        ord(c) ^ key_bytes[i % len(key_bytes)]
        for i, c in enumerate(data)
    )
    return base64.b64encode(encrypted).decode()


def xor_decrypt(enc: str, key: str = XOR_KEY) -> str:
    """Decrypt a base64 XOR-encrypted string."""
    key_bytes = [ord(c) for c in key]
    raw = base64.b64decode(enc)
    return "".join(
        chr(b ^ key_bytes[i % len(key_bytes)]) for i, b in enumerate(raw)
    )


def hash_password(password: str, key: str = "ConanEncryptKey2024") -> str:
    """SHA-256 hash a password then XOR-encrypt it."""
    sha = hashlib.sha256(password.encode()).hexdigest()
    return xor_encrypt(sha, key)
