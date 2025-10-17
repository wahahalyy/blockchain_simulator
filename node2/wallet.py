import os
import hashlib
import json
import time
from ecdsa import SigningKey, VerifyingKey, SECP256k1

class Wallet:
    def __init__(self):
        # 创建一个随机私钥
        self.private_key = SigningKey.generate(curve=SECP256k1)
        self.public_key = self.private_key.get_verifying_key()
        self.nonce = 0  # 交易计数器，防止重放攻击

    def sign(self, data):
        """
        使用私钥对数据（字符串）签名
        """
        data_hash = hashlib.sha256(data.encode()).digest()
        signature = self.private_key.sign(data_hash)
        return signature.hex()

    def verify(self, data, signature, public_key_hex):
        """
        验证签名，调用静态方法
        """
        return Wallet.verify_static(data, signature, public_key_hex)

    @staticmethod
    def verify_static(data, signature, public_key_hex):
        """
        静态方法验证签名，不依赖实例，输入数据（字符串）、签名和公钥十六进制字符
        """
        try:
            public_key = VerifyingKey.from_string(bytes.fromhex(public_key_hex), curve=SECP256k1)
            data_hash = hashlib.sha256(data.encode()).digest()
            return public_key.verify(bytes.fromhex(signature), data_hash)
        except Exception:
            return False

    def get_address(self):
        """
        返回公钥的十六进制作为钱包地址
        """
        return self.public_key.to_string().hex()

    def increment_nonce(self):
        """增加nonce值"""
        self.nonce += 1
        return self.nonce

    def get_current_nonce(self):
        """获取当前nonce值"""
        return self.nonce

    def to_dict(self):
        """
        序列化钱包（注意：实际使用中私钥应妥善保管，此处仅为示例）
        """
        return {
            "private_key": self.private_key.to_string().hex(),
            "nonce": self.nonce
        }

    @classmethod
    def from_dict(cls, data):
        """
        根据存储的数据恢复钱包实例
        """
        wallet = cls.__new__(cls)
        wallet.private_key = SigningKey.from_string(bytes.fromhex(data["private_key"]), curve=SECP256k1)
        wallet.public_key = wallet.private_key.get_verifying_key()
        wallet.nonce = data.get("nonce", 0)
        return wallet

if __name__ == '__main__':
    wallet = Wallet()
    data = "hello blockchain"
    signature = wallet.sign(data)
    print("Address:", wallet.get_address())
    print("Signature:", signature)
    print("Verify:", wallet.verify(data, signature, wallet.get_address()))