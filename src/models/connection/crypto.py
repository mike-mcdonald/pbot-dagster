import base64

from Crypto.Cipher import AES
from Crypto import Random
from Crypto.Util.Padding import pad, unpad


def encrypt(key, data: str):
    key = key.encode("utf8")
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return (
        base64.b64encode(iv + cipher.encrypt(pad(data.encode("utf8"), AES.block_size)))
    ).decode("utf8")


def decrypt(key, encoded: str):
    key = key.encode("utf8")
    data = base64.b64decode(encoded)
    iv = data[:16]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return (unpad(cipher.decrypt(data[16:]), AES.block_size)).decode("utf8")
