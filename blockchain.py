import hashlib
import json
import time
import os
from threading import Lock
from transaction import Transaction

DIFFICULTY_ADJUSTMENT_INTERVAL = 5
TARGET_BLOCK_TIME = 10
COINBASE_REWARD = 50
MAX_BLOCK_SIZE = 1024 * 1024
MAX_TXS_PER_BLOCK = 1000
DATA_FILE = "blockchain_data.json"

class Block:
    def __init__(self, index, timestamp, transactions, previous_hash, nonce=0, hash_value=None):
        self.index = index
        self.timestamp = timestamp
        self.transactions = transactions
        self.previous_hash = previous_hash
        self.nonce = nonce
        self.hash = hash_value if hash_value else self.compute_hash()

    def compute_hash(self):
        block_string = json.dumps({
            'index': self.index,
            'timestamp': self.timestamp,
            'transactions': self.transactions,
            'previous_hash': self.previous_hash,
            'nonce': self.nonce
        }, sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()

    def to_dict(self):
        return {
            'index': self.index,
            'timestamp': self.timestamp,
            'transactions': self.transactions,
            'previous_hash': self.previous_hash,
            'nonce': self.nonce,
            'hash': self.hash
        }

    def get_size(self):
        return len(json.dumps(self.to_dict()))

class UTXOSet:
    def __init__(self):
        self.utxos = {}
        self.lock = Lock()
    
    def add_utxo(self, txid, address, amount):
        with self.lock:
            self.utxos[txid] = {
                "address": address,
                "amount": amount,
                "spent": False
            }
    
    def spend_utxo(self, txid):
        with self.lock:
            if txid in self.utxos:
                self.utxos[txid]["spent"] = True
                return True
        return False
    
    def get_balance(self, address):
        with self.lock:
            balance = 0
            for utxo in self.utxos.values():
                if utxo["address"] == address and not utxo["spent"]:
                    balance += utxo["amount"]
            return balance
    
    def get_utxos_for_address(self, address):
        with self.lock:
            utxos = []
            for txid, utxo in self.utxos.items():
                if utxo["address"] == address and not utxo["spent"]:
                    utxos.append({
                        "txid": txid,
                        "amount": utxo["amount"]
                    })
            return utxos
    
    def to_dict(self):
        with self.lock:
            return self.utxos.copy()
    
    def from_dict(self, data):
        with self.lock:
            self.utxos = data

class Mempool:
    def __init__(self):
        self.transactions = {}
        self.lock = Lock()
        self.max_size = 10000
    
    def add_transaction(self, transaction_dict):
        with self.lock:
            txid = transaction_dict.get("txid")
            if not txid:
                return False, "无效的交易ID"
            
            if txid in self.transactions:
                return False, "交易已存在"
            
            if len(self.transactions) >= self.max_size:
                return False, "内存池已满"
            
            self.transactions[txid] = transaction_dict
            return True, "交易添加成功"
    
    def get_transactions_for_block(self, max_count=MAX_TXS_PER_BLOCK):
        with self.lock:
            # 按交易时间排序，先进先出
            sorted_txs = sorted(
                self.transactions.values(),
                key=lambda x: x.get("timestamp", 0)
            )[:max_count]
            
            for tx in sorted_txs:
                if tx.get("txid") in self.transactions:
                    del self.transactions[tx["txid"]]
            
            return sorted_txs
    
    def remove_transaction(self, txid):
        with self.lock:
            if txid in self.transactions:
                del self.transactions[txid]
                return True
        return False
    
    def get_transaction(self, txid):
        with self.lock:
            return self.transactions.get(txid)
    
    def size(self):
        with self.lock:
            return len(self.transactions)

class Blockchain:
    difficulty = 2

    def __init__(self):
        self.chain = []
        self.utxo_set = UTXOSet()
        self.mempool = Mempool()
        self.chain_lock = Lock()
        self.load_chain()
        self.rebuild_utxo_set()

    def validate_chain(self, chain):
        if not chain:
            return False
        
        if chain[0].index != 0 or chain[0].previous_hash != "0":
            return False
        
        for i in range(1, len(chain)):
            current = chain[i]
            previous = chain[i - 1]
            
            if current.previous_hash != previous.hash:
                return False
            
            if not current.hash.startswith('0' * Blockchain.difficulty):
                return False
            
            if current.hash != current.compute_hash():
                return False
            
            if not self.validate_block_transactions(current.transactions):
                return False
        
        return True

    def load_chain(self):
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, 'r') as f:
                    chain_data = json.load(f)
                self.chain = []
                for block_data in chain_data.get("chain", []):
                    block = Block(
                        block_data["index"],
                        block_data["timestamp"],
                        block_data["transactions"],
                        block_data["previous_hash"],
                        block_data["nonce"],
                        block_data["hash"]
                    )
                    self.chain.append(block)
                
                if "utxo_set" in chain_data:
                    self.utxo_set.from_dict(chain_data["utxo_set"])
                
                print(f"成功加载区块链，区块数：{len(self.chain)}")
            except Exception as e:
                print(f"加载区块链数据失败：{e}")
                self.chain = []
        else:
            self.chain = []

    def save_chain(self):
        try:
            data = {
                "chain": [block.to_dict() for block in self.chain],
                "utxo_set": self.utxo_set.to_dict(),
                "timestamp": time.time()
            }
            with open(DATA_FILE, 'w') as f:
                json.dump(data, f, indent=4, sort_keys=True)
        except Exception as e:
            print(f"保存区块链数据失败：{e}")

    def create_genesis_block(self):
        with self.chain_lock:
            if self.chain:
                raise Exception("区块链已存在，不能重复创建创世区块")
            
            genesis_block = Block(0, time.time(), [], "0")
            self.chain.append(genesis_block)
            self.save_chain()
            print("创世区块创建成功")

    @property
    def last_block(self):
        return self.chain[-1] if self.chain else None

    def proof_of_work(self, block):
        block.nonce = 0
        computed_hash = block.compute_hash()
        while not computed_hash.startswith('0' * Blockchain.difficulty):
            block.nonce += 1
            computed_hash = block.compute_hash()
        return computed_hash

    def adjust_difficulty(self):
        if len(self.chain) % DIFFICULTY_ADJUSTMENT_INTERVAL != 0 or len(self.chain) < DIFFICULTY_ADJUSTMENT_INTERVAL:
            return
        
        last_adjustment_block = self.chain[-DIFFICULTY_ADJUSTMENT_INTERVAL]
        expected_time = TARGET_BLOCK_TIME * DIFFICULTY_ADJUSTMENT_INTERVAL
        actual_time = self.last_block.timestamp - last_adjustment_block.timestamp

        print(f"\n=== 难度调整检查 ===")
        print(f"检查区间: 区块 {last_adjustment_block.index} 到 {self.last_block.index}")
        print(f"期望时间: {expected_time} 秒")
        print(f"实际时间: {actual_time:.2f} 秒")
        print(f"当前难度: {Blockchain.difficulty}")

        if actual_time < expected_time / 2:
            Blockchain.difficulty += 1
            print(f"🚀 挖矿太快！提高难度到 {Blockchain.difficulty}")
        elif actual_time > expected_time * 2 and Blockchain.difficulty > 1:
            Blockchain.difficulty -= 1
            print(f"🐢 挖矿太慢！降低难度到 {Blockchain.difficulty}")
        else:
            print(f"✅ 挖矿速度正常，保持难度 {Blockchain.difficulty}")
        print("==================\n")

    def add_block(self, block, proof):
        with self.chain_lock:
            if self.last_block and block.previous_hash != self.last_block.hash:
                return False, "前一个区块哈希不匹配"
            
            if not self.is_valid_proof(block, proof):
                return False, "无效的工作量证明"
            
            if block.get_size() > MAX_BLOCK_SIZE:
                return False, "区块大小超过限制"
            
            if not self.validate_block_transactions(block.transactions):
                return False, "区块包含无效交易"
            
            block.hash = proof
            self.chain.append(block)
            
            self.update_utxo_set(block)
            
            self.save_chain()
            self.adjust_difficulty()
            return True, "区块添加成功"

    def is_valid_proof(self, block, block_hash):
        return (block_hash.startswith('0' * Blockchain.difficulty) and 
                block_hash == block.compute_hash())

    def rebuild_utxo_set(self):
        self.utxo_set = UTXOSet()
        for block in self.chain:
            self.update_utxo_set(block)

    def select_utxos_for_payment(self, address, amount):
        """选择用于支付的UTXO集合"""
        utxos = self.utxo_set.get_utxos_for_address(address)
        utxos.sort(key=lambda x: x["amount"])  # 最小面额优先

        selected = []
        total = 0

        for utxo in utxos:
            if total >= amount:
                break
            selected.append(utxo)
            total += utxo["amount"]

        if total < amount:
            return None, total  # 金额不足

        return selected, total

    def update_utxo_set(self, block):
    #更新UTXO集以包含新区块的交易 - 修复版本
        for tx_dict in block.transactions:
            txid = tx_dict.get("txid")
            sender = tx_dict.get("sender")
            recipient = tx_dict.get("recipient")
            amount = tx_dict.get("amount", 0)

            # 处理coinbase交易（矿工奖励）
            if sender == "":
                if recipient and amount > 0:
                    self.utxo_set.add_utxo(txid, recipient, amount)
                continue

            # 处理普通交易
            # 1. 收集发送者的UTXO并按金额排序
            sender_utxos = self.utxo_set.get_utxos_for_address(sender)
            sender_utxos.sort(key=lambda x: x["amount"])

            # 2. 选择足够的UTXO来支付（最小UTXO优先）
            selected_utxos = []
            total_selected = 0

            for utxo in sender_utxos:
                if total_selected >= amount:
                    break
                selected_utxos.append(utxo)
                total_selected += utxo["amount"]

            # 3. 如果金额不足，跳过此交易（理论上不应该发生，因为验证时已检查）
            if total_selected < amount:
                print(f"警告: 交易 {txid} 金额不足，跳过")
                continue

            # 4. 标记所有选中的UTXO为已花费
            for utxo in selected_utxos:
                self.utxo_set.spend_utxo(utxo["txid"])

            # 5. 创建接收者的UTXO
            self.utxo_set.add_utxo(txid, recipient, amount)

            # 6. 计算并创建找零UTXO
            change = total_selected - amount
            if change > 0:
                change_txid = f"{txid}_change"
                self.utxo_set.add_utxo(change_txid, sender, change)

    def remove_confirmed_transactions(self, block):
        """从内存池中移除已经被区块包含的交易"""
        removed_count = 0
        for tx_dict in block.transactions:
            # 跳过coinbase交易
            if tx_dict.get("sender") == "":
                continue
                
            txid = tx_dict.get("txid")
            if txid and self.mempool.remove_transaction(txid):
                removed_count += 1
        
        if removed_count > 0:
            print(f"从内存池中移除了 {removed_count} 笔已确认的交易")
        
        return removed_count

    def get_balance(self, address):
        return self.utxo_set.get_balance(address)


    def validate_transaction(self, transaction_dict):
        try:
            required_fields = ["txid", "sender", "recipient", "amount", "nonce", "signature"]
            if not all(field in transaction_dict for field in required_fields):
                return False, "缺少必要字段"

            # coinbase交易特殊处理
            if transaction_dict["sender"] == "":
                return True, "Coinbase交易有效"

            sender = transaction_dict["sender"]
            amount = transaction_dict["amount"]

            # 验证签名
            tx = Transaction(
                transaction_dict["sender"],
                transaction_dict["recipient"],
                transaction_dict["amount"],
                transaction_dict["nonce"]
            )
            tx.txid = transaction_dict["txid"]
            tx.signature = transaction_dict["signature"]

            if not tx.is_valid():
                return False, "交易签名无效"

            # 验证是否有足够的UTXO（不仅仅是总余额）
            sender_utxos = self.utxo_set.get_utxos_for_address(sender)
            total_available = sum(utxo["amount"] for utxo in sender_utxos)

            if total_available < amount:
                return False, f"余额不足，需要{amount}，可用余额{total_available}"

            return True, "交易有效"
        except Exception as e:
            return False, f"交易验证异常: {str(e)}"

    def validate_block_transactions(self, transactions):
        total_size = 0
        coinbase_count = 0
        
        for tx_dict in transactions:
            # 检查区块大小
            total_size += len(json.dumps(tx_dict))
            if total_size > MAX_BLOCK_SIZE:
                return False
            
            # 检查coinbase交易数量
            if tx_dict.get("sender") == "":
                coinbase_count += 1
                if coinbase_count > 1:
                    return False
            
            # 验证交易
            is_valid, message = self.validate_transaction(tx_dict)
            if not is_valid:
                print(f"无效交易: {message}")
                return False
        
        return coinbase_count == 1  # 必须有且只有一个coinbase交易

    def add_new_transaction(self, transaction_dict):
        is_valid, message = self.validate_transaction(transaction_dict)
        if not is_valid:
            raise Exception(f"交易无效: {message}")
        
        success, msg = self.mempool.add_transaction(transaction_dict)
        if not success:
            raise Exception(f"添加到内存池失败: {msg}")
        
        return True

    def mine(self, miner_address=None):
        if not miner_address:
            raise Exception("必须提供矿工地址")
        
        # 从内存池获取交易
        transactions = self.mempool.get_transactions_for_block()
        
        print(f"挖矿开始，处理 {len(transactions)} 笔交易")
        
        # 添加coinbase交易
        coinbase_tx = {
            "txid": hashlib.sha256(f"coinbase_{time.time()}".encode()).hexdigest(),
            "sender": "",
            "recipient": miner_address,
            "amount": COINBASE_REWARD,
            "nonce": 0,
            "timestamp": time.time(),
            "signature": ""
        }
        transactions.insert(0, coinbase_tx)
        
        # 验证交易集合
        if not self.validate_block_transactions(transactions):
            # 如果有无效交易，把有效交易放回内存池
            for tx in transactions[1:]:  # 跳过coinbase
                if tx.get("sender"):  # 只放回普通交易
                    self.mempool.add_transaction(tx)
            raise Exception("交易集合验证失败")
        
        previous_hash = self.last_block.hash if self.last_block else "0"
        new_block = Block(
            index=(self.last_block.index + 1) if self.last_block else 0,
            timestamp=time.time(),
            transactions=transactions,
            previous_hash=previous_hash
        )
        
        proof = self.proof_of_work(new_block)
        added, message = self.add_block(new_block, proof)
        
        if not added:
            # 挖矿失败，把交易放回内存池
            for tx in transactions[1:]:  # 跳过coinbase
                if tx.get("sender"):  # 只放回普通交易
                    self.mempool.add_transaction(tx)
            return None
        
        print(f"新区块挖出！索引: {new_block.index}, 哈希: {new_block.hash[:16]}...")
        print(f"矿工 {miner_address[:16]}... 获得挖矿奖励: {COINBASE_REWARD}")
        
        # 从内存池中移除已确认的交易
        removed_count = self.remove_confirmed_transactions(new_block)
        if removed_count > 0:
            print(f"挖矿后清理了 {removed_count} 笔已确认的交易")
        
        return new_block

    def create_block_from_dict(self, block_data):
        return Block(
            block_data["index"],
            block_data["timestamp"],
            block_data["transactions"],
            block_data["previous_hash"],
            block_data["nonce"],
            block_data["hash"]
        )

    def get_chain_info(self):
        return {
            "length": len(self.chain),
            "difficulty": Blockchain.difficulty,
            "mempool_size": self.mempool.size(),
            "last_block": self.last_block.to_dict() if self.last_block else None
        }

if __name__ == '__main__':
    bc = Blockchain()
    if not bc.chain:
        print("区块链为空")
    else:
        print(f"区块链已加载，区块数：{len(bc.chain)}")
        print(f"内存池交易数：{bc.mempool.size()}")