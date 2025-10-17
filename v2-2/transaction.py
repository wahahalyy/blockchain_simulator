import json
import hashlib
import time
from wallet import Wallet

class Transaction:
    def __init__(self, sender, recipient, amount, nonce=0, signature="", txid=None):
        self.sender = sender
        self.recipient = recipient
        self.amount = amount
        self.nonce = nonce
        self.timestamp = time.time()
        self.signature = signature
        
        # 关键修复：先设置txid为None，然后根据情况计算
        self.txid = None
        
        if txid:
            self.txid = txid
        else:
            # 使用专门的方法计算初始哈希，不依赖txid字段
            self.txid = self._compute_initial_hash()
    
    def _compute_initial_hash(self):
        """计算初始交易哈希，不依赖任何实例属性"""
        transaction_data = {
            'sender': self.sender,
            'recipient': self.recipient,
            'amount': self.amount,
            'nonce': self.nonce,
            'timestamp': self.timestamp
        }
        transaction_string = json.dumps(transaction_data, sort_keys=True)
        return hashlib.sha256(transaction_string.encode()).hexdigest()

    def to_dict(self, include_signature=True):
        """转换为字典"""
        # 确保txid存在
        if self.txid is None:
            self.txid = self.compute_hash()
            
        data = {
            'txid': self.txid,
            'sender': self.sender,
            'recipient': self.recipient,
            'amount': self.amount,
            'nonce': self.nonce,
            'timestamp': self.timestamp
        }
        if include_signature:
            data['signature'] = self.signature
        return data

    def compute_hash(self):
        """
        计算交易数据的哈希，用于签名和数据完整性验证
        不包含签名字段，确保交易ID一致性
        """
        transaction_data = {
            'sender': self.sender,
            'recipient': self.recipient,
            'amount': self.amount,
            'nonce': self.nonce,
            'timestamp': self.timestamp
        }
        transaction_string = json.dumps(transaction_data, sort_keys=True)
        return hashlib.sha256(transaction_string.encode()).hexdigest()

    def sign_transaction(self, wallet: Wallet):
        """
        使用钱包签名交易，确保交易由对应钱包发起
        """
        if wallet.get_address() != self.sender:
            raise Exception("无效的签名钱包")
        
        # 设置nonce
        if self.nonce == 0:
            self.nonce = wallet.get_current_nonce()
        
        # 重新计算交易ID（使用正确的哈希方法）
        self.txid = self.compute_hash()
        
        # 签名
        self.signature = wallet.sign(self.txid)
        
        # 增加钱包nonce
        wallet.increment_nonce()

    def is_valid(self):
        """
        验证交易有效性。对于 coinbase 交易（sender 为空）直接返回 True，
        否则调用 Wallet.verify_static 检查签名
        """
        if self.sender == "":
            return True
        
        # 验证基本字段
        if not all([self.sender, self.recipient, self.amount > 0]):
            return False
            
        # 验证签名
        if not self.txid or not self.signature:
            return False
            
        return Wallet.verify_static(self.txid, self.signature, self.sender)

    def __str__(self):
        txid_display = self.txid[:16] + "..." if self.txid else "None"
        return f"TX({txid_display}): {self.sender[:8]}... -> {self.recipient[:8]}... ({self.amount})"

if __name__ == '__main__':
    # 示例用法：产生交易并签名验证
    from wallet import Wallet
    sender_wallet = Wallet()
    recipient_wallet = Wallet()
    tx = Transaction(
        sender=sender_wallet.get_address(), 
        recipient=recipient_wallet.get_address(), 
        amount=10
    )
    tx.sign_transaction(sender_wallet)
    print("Transaction:", tx.to_dict())
    print("Transaction valid:", tx.is_valid())