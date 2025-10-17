import hashlib
import json
from threading import Lock

class UTXOSet:
    def __init__(self, blockchain=None):
        self.utxos = {}  # {txid: {"address": addr, "amount": amount, "spent": False}}
        self.lock = Lock()
        if blockchain:
            self.rebuild_from_blockchain(blockchain)
    
    def compute_txid(self, tx_dict):
        """
        计算交易ID。为确保一致性，移除签名字段后计算交易数据的哈希值。
        """
        tx_copy = tx_dict.copy()
        tx_copy.pop("signature", None)
        tx_string = json.dumps(tx_copy, sort_keys=True)
        return hashlib.sha256(tx_string.encode()).hexdigest()

    def rebuild_from_blockchain(self, blockchain):
        """从区块链重新构建UTXO集合"""
        with self.lock:
            self.utxos = {}
            for block in blockchain.chain:
                for tx_dict in block.transactions:
                    txid = tx_dict.get("txid") or self.compute_txid(tx_dict)
                    sender = tx_dict.get("sender")
                    recipient = tx_dict.get("recipient")
                    amount = tx_dict.get("amount", 0)
                    
                    # 如果是普通交易，标记发送者的UTXO为已花费（简化处理）
                    # 实际应该根据交易输入来精确标记
                    
                    # 添加接收者的UTXO
                    if recipient and amount > 0:
                        self.utxos[txid] = {
                            "address": recipient,
                            "amount": amount,
                            "spent": False
                        }
                    
                    # 如果是coinbase交易
                    if sender == "" and recipient:
                        self.utxos[txid] = {
                            "address": recipient,
                            "amount": amount,
                            "spent": False
                        }

    def add_utxo(self, txid, address, amount):
        """添加UTXO"""
        with self.lock:
            self.utxos[txid] = {
                "address": address,
                "amount": amount,
                "spent": False
            }

    def spend_utxo(self, txid):
        """标记UTXO为已花费"""
        with self.lock:
            if txid in self.utxos:
                self.utxos[txid]["spent"] = True
                return True
        return False

    def get_balance(self, address):
        """获取地址余额"""
        with self.lock:
            balance = 0
            for utxo in self.utxos.values():
                if utxo["address"] == address and not utxo["spent"]:
                    balance += utxo["amount"]
            return balance

    def get_utxos_for_address(self, address, min_amount=0):
        """获取地址的所有未花费UTXO"""
        with self.lock:
            utxos = []
            for txid, utxo in self.utxos.items():
                if (utxo["address"] == address and 
                    not utxo["spent"] and 
                    utxo["amount"] >= min_amount):
                    utxos.append({
                        "txid": txid,
                        "amount": utxo["amount"]
                    })
            return utxos

    def get_total_supply(self):
        """获取总货币供应量"""
        with self.lock:
            total = 0
            for utxo in self.utxos.values():
                if not utxo["spent"]:
                    total += utxo["amount"]
            return total

    def to_dict(self):
        """序列化UTXO集"""
        with self.lock:
            return self.utxos.copy()

    def from_dict(self, data):
        """从字典恢复UTXO集"""
        with self.lock:
            self.utxos = data

    def __len__(self):
        """获取UTXO数量"""
        with self.lock:
            return len(self.utxos)

    def get_stats(self):
        """获取统计信息"""
        with self.lock:
            total_utxos = len(self.utxos)
            spent_utxos = sum(1 for u in self.utxos.values() if u["spent"])
            unique_addresses = len(set(u["address"] for u in self.utxos.values()))
            
            return {
                "total_utxos": total_utxos,
                "spent_utxos": spent_utxos,
                "unspent_utxos": total_utxos - spent_utxos,
                "unique_addresses": unique_addresses,
                "total_supply": self.get_total_supply()
            }