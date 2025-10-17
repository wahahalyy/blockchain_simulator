import logging
import sys
import threading
import time
import os
import json
import requests
import hashlib
from flask import Flask, request, jsonify
from argparse import ArgumentParser
from blockchain import Blockchain, Block, Mempool
from wallet import Wallet

# 配置日志
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(levelname)s [%(threadName)s]: %(message)s',
    handlers=[
        logging.FileHandler("node.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Flask应用用于REST端点
app = Flask(__name__)

# 全局变量
blockchain = Blockchain()
nodes = set()  # 已知节点集合
NODES_FILE = "nodes_list.json"
DEFAULT_WALLET_FILE = "default_wallet.json"
WALLETS_FILE = "wallets.json"

default_wallet = None
wallets = {}
my_node_address = ""
auto_mining_enabled = False
auto_mining_started = False
network_lock = threading.Lock()

class NetworkManager:
    def __init__(self):
        self.known_peers = set()
        self.bootstrap_nodes = set()
        self.healthy_peers = set()
        self.peer_status = {}
        self.max_retries = 3
        self.broadcast_history = {}
        self.broadcast_ttl = 10
        self.broadcast_stats = {
            'last_broadcast': 0,
            'broadcast_count': 0,
            'last_reset': time.time()
        }
        self.max_broadcasts_per_minute = 10
    
    def can_broadcast(self, message_id, source_addr):
        """检查是否可以广播，防止循环"""
        current_time = time.time()
        
        # 清理过期的广播记录
        expired_keys = []
        for key, record in self.broadcast_history.items():
            if current_time - record['timestamp'] > self.broadcast_ttl:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.broadcast_history[key]
        
        # 检查是否已经处理过这个广播
        if message_id in self.broadcast_history:
            return False
        
        # 记录新的广播
        self.broadcast_history[message_id] = {
            'timestamp': current_time,
            'source': source_addr
        }
        
        return True
    
    def can_broadcast_now(self):
        """检查当前是否可以广播（频率限制）"""
        current_time = time.time()
        
        # 每分钟重置计数
        if current_time - self.broadcast_stats['last_reset'] > 60:
            self.broadcast_stats['broadcast_count'] = 0
            self.broadcast_stats['last_reset'] = current_time
        
        # 检查广播频率
        if self.broadcast_stats['broadcast_count'] >= self.max_broadcasts_per_minute:
            return False
        
        # 检查广播间隔（至少1秒）
        if current_time - self.broadcast_stats['last_broadcast'] < 1:
            return False
        
        self.broadcast_stats['broadcast_count'] += 1
        self.broadcast_stats['last_broadcast'] = current_time
        return True
    
    def health_check(self):
        """定期检查节点健康状态，改进版本"""
        while True:
            current_time = time.time()
            dead_peers = set()
            newly_healthy_peers = set()
            
            for peer in list(self.known_peers):
                # 初始化节点状态
                if peer not in self.peer_status:
                    self.peer_status[peer] = {
                        'retries': 0,
                        'last_check': 0,
                        'last_seen': 0
                    }
                
                # 如果上次检查时间太近，跳过（避免频繁检查）
                if current_time - self.peer_status[peer]['last_check'] < 10:
                    continue
                
                self.peer_status[peer]['last_check'] = current_time
                
                try:
                    response = requests.get(f"http://{peer}/health", timeout=3)
                    if response.status_code == 200:
                        # 节点健康
                        self.peer_status[peer]['retries'] = 0
                        self.peer_status[peer]['last_seen'] = current_time
                        
                        if peer not in self.healthy_peers:
                            self.healthy_peers.add(peer)
                            newly_healthy_peers.add(peer)
                            logger.info(f"节点 {peer} 恢复健康")
                    else:
                        # 节点响应但不是200
                        self.peer_status[peer]['retries'] += 1
                        if self.peer_status[peer]['retries'] >= self.max_retries:
                            dead_peers.add(peer)
                except Exception as e:
                    # 节点不可达
                    self.peer_status[peer]['retries'] += 1
                    if self.peer_status[peer]['retries'] >= self.max_retries:
                        dead_peers.add(peer)
            
            # 处理新恢复健康的节点
            if newly_healthy_peers:
                logger.info(f"{len(newly_healthy_peers)} 个节点恢复健康: {list(newly_healthy_peers)}")
            
            # 移除不健康的节点
            for peer in dead_peers:
                if peer in self.healthy_peers:
                    self.healthy_peers.remove(peer)
                    logger.info(f"节点 {peer} 被标记为不健康，重试次数已用完")
            
            time.sleep(30)  # 每30秒检查一次
    
    def force_check_peer(self, peer_addr):
        """强制立即检查特定节点"""
        try:
            response = requests.get(f"http://{peer_addr}/health", timeout=3)
            if response.status_code == 200:
                if peer_addr not in self.healthy_peers:
                    self.healthy_peers.add(peer_addr)
                    logger.info(f"强制检查: 节点 {peer_addr} 健康")
                return True
            else:
                if peer_addr in self.healthy_peers:
                    self.healthy_peers.remove(peer_addr)
                    logger.info(f"强制检查: 节点 {peer_addr} 不健康")
                return False
        except Exception as e:
            if peer_addr in self.healthy_peers:
                self.healthy_peers.remove(peer_addr)
                logger.info(f"强制检查: 节点 {peer_addr} 不可达")
            return False
    
    def get_peer_status(self, peer_addr):
        """获取节点的详细状态"""
        if peer_addr not in self.peer_status:
            return {
                'status': 'unknown',
                'retries': 0,
                'last_seen': 0,
                'last_check': 0
            }
        
        status_info = self.peer_status[peer_addr].copy()
        status_info['status'] = 'healthy' if peer_addr in self.healthy_peers else 'unhealthy'
        status_info['last_seen_str'] = time.ctime(status_info['last_seen']) if status_info['last_seen'] > 0 else '从未'
        status_info['last_check_str'] = time.ctime(status_info['last_check']) if status_info['last_check'] > 0 else '从未'
        
        return status_info

# 初始化网络管理器
network_manager = NetworkManager()

#############################################
# 健康检查端点
#############################################
@app.route('/health', methods=['GET'])
def health_check():
    """健康检查端点"""
    return jsonify({
        "status": "healthy",
        "block_height": len(blockchain.chain),
        "mempool_size": blockchain.mempool.size(),
        "peers_count": len(nodes),
        "auto_mining": auto_mining_enabled
    }), 200

#############################################
# 钱包功能
#############################################
def load_wallets():
    global wallets
    if os.path.exists(WALLETS_FILE):
        try:
            with open(WALLETS_FILE, "r") as f:
                wallets_data = json.load(f)
            wallets = {}
            for addr, data in wallets_data.items():
                wallets[addr] = Wallet.from_dict(data)
            logger.info(f"成功加载 {len(wallets)} 个钱包")
        except Exception as e:
            logger.error(f"加载钱包失败: {e}")
            wallets = {}
    else:
        wallets = {}
        save_wallets()

def save_wallets():
    try:
        wallets_data = {addr: wallet.to_dict() for addr, wallet in wallets.items()}
        with open(WALLETS_FILE, "w") as f:
            json.dump(wallets_data, f, indent=4, sort_keys=True)
    except Exception as e:
        logger.error(f"保存钱包失败: {e}")

def create_new_wallet():
    wallet = Wallet()
    addr = wallet.get_address()
    wallets[addr] = wallet
    save_wallets()
    logger.info(f"新钱包创建成功，地址: {addr}")
    return wallet

def load_default_wallet():
    global default_wallet
    if os.path.exists(DEFAULT_WALLET_FILE):
        try:
            with open(DEFAULT_WALLET_FILE, "r") as f:
                data = json.load(f)
            default_wallet = Wallet.from_dict(data)
            logger.info(f"默认钱包加载成功，地址: {default_wallet.get_address()}")
        except Exception as e:
            logger.error(f"加载默认钱包失败: {e}")
            create_default_wallet()
    else:
        create_default_wallet()
    
    # 同时保存到钱包文件
    wallets[default_wallet.get_address()] = default_wallet
    save_wallets()

def create_default_wallet():
    global default_wallet
    default_wallet = Wallet()
    with open(DEFAULT_WALLET_FILE, "w") as f:
        json.dump(default_wallet.to_dict(), f, indent=4, sort_keys=True)
    logger.info(f"默认钱包创建成功，地址: {default_wallet.get_address()}")

#############################################
# 自动挖矿
#############################################
def start_auto_mining_if_needed():
    """根据设置启动自动挖矿"""
    global auto_mining_started
    if not auto_mining_started and blockchain.chain and default_wallet is not None and auto_mining_enabled:
        auto_mining_started = True
        threading.Thread(
            target=auto_mining, 
            args=(default_wallet.get_address(),), 
            daemon=True,
            name="AutoMining"
        ).start()
        logger.info("自动挖矿线程已启动")

def auto_mining(miner_wallet_address):
    """自动挖矿主循环 - 支持连续挖矿（包括空块）"""
    logger.info("自动挖矿线程已启动 - 连续挖矿模式")
    empty_block_count = 0
    last_block_time = time.time()
    
    while True:
        if not auto_mining_enabled:
            # 如果自动挖矿被禁用，等待一段时间再检查
            time.sleep(10)
            continue
            
        try:
            mempool_size = blockchain.mempool.size()
            current_time = time.time()
            
            # 计算距离上次出块的时间
            time_since_last_block = current_time - last_block_time
            
            # 如果有交易，立即挖矿
            if mempool_size > 0:
                logger.info(f"开始挖矿，内存池中有 {mempool_size} 笔交易")
                block = blockchain.mine(miner_wallet_address)
                if block:
                    last_block_time = current_time
                    empty_block_count = 0
                    logger.info(f"挖矿成功，新区块: {block.index}, 包含 {len(block.transactions)-1} 笔交易")
                    broadcast_new_block(block)
                time.sleep(5)  # 有交易时稍作等待
            
            # 如果没有交易，但距离上次出块时间较长，也挖空块
            elif time_since_last_block > 30:  # 30秒没有新区块就挖空块
                logger.info("内存池为空，但距离上次出块时间较长，开始挖空块")
                block = blockchain.mine(miner_wallet_address)
                if block:
                    last_block_time = current_time
                    empty_block_count += 1
                    logger.info(f"空块挖矿成功，新区块: {block.index} (连续空块: {empty_block_count})")
                    broadcast_new_block(block)
                time.sleep(10)  # 挖空块后等待稍长时间
            
            else:
                # 等待下一次检查
                wait_time = max(5, 30 - time_since_last_block)  # 动态等待时间
                time.sleep(wait_time)
                
        except Exception as e:
            logger.error(f"自动挖矿错误: {e}")
            time.sleep(10)
            
def set_mining_mode(continuous=False):
    """设置挖矿模式"""
    global auto_mining_enabled
    auto_mining_enabled = continuous
    
    if auto_mining_enabled and not auto_mining_started:
        start_auto_mining_if_needed()
    
    mode = "连续挖矿" if continuous else "交易驱动挖矿"
    logger.info(f"挖矿模式已设置为: {mode}")
    return auto_mining_enabled

def toggle_auto_mining():
    """切换自动挖矿状态"""
    global auto_mining_enabled, auto_mining_started
    auto_mining_enabled = not auto_mining_enabled
    
    if auto_mining_enabled:
        logger.info("自动挖矿已启用")
        if not auto_mining_started:
            start_auto_mining_if_needed()
    else:
        logger.info("自动挖矿已禁用")
    
    return auto_mining_enabled

#############################################
# 节点管理
#############################################
def load_node_addresses():
    global nodes
    if os.path.exists(NODES_FILE):
        try:
            with open(NODES_FILE, 'r') as f:
                data = json.load(f)
                nodes = set(data.get("nodes", []))
                network_manager.known_peers = nodes.copy()
            logger.info(f"成功加载 {len(nodes)} 个节点地址")
        except Exception as e:
            logger.error(f"加载节点地址失败: {e}")
            nodes = set()
    else:
        nodes = set()
        save_node_addresses()

def save_node_addresses():
    try:
        with open(NODES_FILE, 'w') as f:
            json.dump({"nodes": list(nodes)}, f, indent=4, sort_keys=True)
    except Exception as e:
        logger.error(f"保存节点地址失败: {e}")

def add_node(node_addr):
    """安全地添加节点"""
    with network_lock:
        if node_addr not in nodes and node_addr != my_node_address:
            nodes.add(node_addr)
            network_manager.known_peers.add(node_addr)
            save_node_addresses()
            return True
    return False

#############################################
# 网络广播功能 - 安全版本
#############################################
def safe_broadcast_nodes_list(source_addr=None):
    """安全的节点列表广播（防风暴版本）"""
    if not network_manager.can_broadcast_now():
        logger.warning("广播频率限制，跳过本次节点列表广播")
        return
    
    nodes_list = list(nodes)
    
    # 生成唯一的广播消息ID
    message_id = hashlib.md5(f"nodes_sync_{time.time()}_{my_node_address}".encode()).hexdigest()
    
    successful_broadcasts = 0
    skipped_broadcasts = 0
    
    for node_addr in list(network_manager.healthy_peers):
        # 避免向源节点广播（如果指定了源节点）
        if source_addr:
            # 注意：这里我们比较IP和端口，但有时可能只比较IP？根据实际情况调整
            if node_addr == source_addr:
                skipped_broadcasts += 1
                continue
            
        try:
            url = f"http://{node_addr}/nodes/sync"
            payload = {
                "nodes": nodes_list,
                "message_id": message_id,
                "source_node": my_node_address,
                "timestamp": time.time()
            }
            response = requests.post(url, json=payload, timeout=5)
            if response.status_code == 200:
                successful_broadcasts += 1
                logger.debug(f"节点列表已安全同步到 {node_addr}")
        except Exception as e:
            logger.debug(f"向节点 {node_addr} 同步节点列表时出错: {e}")
    
    logger.info(f"安全广播完成，成功: {successful_broadcasts}, 跳过: {skipped_broadcasts}, 总计: {len(network_manager.healthy_peers)}")

def broadcast_new_block(new_block):
    """广播新区块到所有健康节点"""
    block_data = new_block.to_dict()
    successful_broadcasts = 0
    
    for node_addr in list(network_manager.healthy_peers):
        try:
            url = f"http://{node_addr}/block/receive"
            response = requests.post(url, json=block_data, timeout=5)
            if response.status_code == 200:
                successful_broadcasts += 1
                logger.debug(f"新区块已广播到节点 {node_addr}")
            else:
                logger.warning(f"向节点 {node_addr} 广播新区块失败，状态码: {response.status_code}")
        except Exception as e:
            logger.debug(f"向节点 {node_addr} 广播新区块时出错: {e}")
    
    logger.info(f"新区块广播完成，成功: {successful_broadcasts}/{len(network_manager.healthy_peers)}")

def broadcast_transaction(transaction_dict):
    """广播交易到所有健康节点"""
    successful_broadcasts = 0
    
    for node_addr in list(network_manager.healthy_peers):
        try:
            url = f"http://{node_addr}/transaction/broadcast"
            response = requests.post(url, json=transaction_dict, timeout=3)
            if response.status_code == 200:
                successful_broadcasts += 1
            else:
                logger.debug(f"向节点 {node_addr} 广播交易失败，状态码: {response.status_code}")
        except Exception as e:
            logger.debug(f"向节点 {node_addr} 广播交易时出错: {e}")
    
    logger.info(f"交易广播完成，成功: {successful_broadcasts}/{len(network_manager.healthy_peers)}")

def broadcast_to_others(transaction_dict, source_addr):
    """向除源节点外的其他节点广播"""
    source_ip = source_addr.split(':')[0] if ':' in source_addr else source_addr
    successful_broadcasts = 0
    
    for node_addr in list(network_manager.healthy_peers):
        # 避免向源节点广播
        node_ip = node_addr.split(':')[0] if ':' in node_addr else node_addr
        if node_ip == source_ip:
            continue
            
        try:
            url = f"http://{node_addr}/transaction/broadcast"
            response = requests.post(url, json=transaction_dict, timeout=3)
            if response.status_code == 200:
                successful_broadcasts += 1
        except:
            continue
    
    if successful_broadcasts > 0:
        logger.debug(f"交易已转发到 {successful_broadcasts} 个其他节点")

def broadcast_block_to_others(block, source_addr):
    """向除源节点外的其他节点广播区块"""
    source_ip = source_addr.split(':')[0] if ':' in source_addr else source_addr
    successful_broadcasts = 0
    
    for node_addr in list(network_manager.healthy_peers):
        # 避免向源节点广播
        node_ip = node_addr.split(':')[0] if ':' in node_addr else node_addr
        if node_ip == source_ip:
            continue
            
        try:
            url = f"http://{node_addr}/block/receive"
            response = requests.post(url, json=block.to_dict(), timeout=5)
            if response.status_code == 200:
                successful_broadcasts += 1
        except:
            continue
    
    if successful_broadcasts > 0:
        logger.debug(f"区块已转发到 {successful_broadcasts} 个其他节点")

#############################################
# Flask端点 - 安全版本
#############################################
@app.route('/chain', methods=['GET'])
def full_chain():
    chain_data = [block.to_dict() for block in blockchain.chain]
    response = {
        "chain": chain_data, 
        "length": len(chain_data),
        "difficulty": Blockchain.difficulty
    }
    return jsonify(response), 200

@app.route('/block/<int:index>', methods=['GET'])
def get_block(index):
    if index < 0 or index >= len(blockchain.chain):
        return jsonify({"error": "区块索引无效"}), 404
    
    block = blockchain.chain[index]
    return jsonify(block.to_dict()), 200

@app.route('/transaction/<txid>', methods=['GET'])
def get_transaction_by_id(txid):
    # 在区块链中查找交易
    for block in blockchain.chain:
        for tx in block.transactions:
            if tx.get("txid") == txid:
                return jsonify(tx), 200
    
    # 在内存池中查找
    tx = blockchain.mempool.get_transaction(txid)
    if tx:
        return jsonify(tx), 200
    
    return jsonify({"error": "交易未找到"}), 404

@app.route('/balance/<address>', methods=['GET'])
def get_balance(address):
    balance = blockchain.get_balance(address)
    return jsonify({"address": address, "balance": balance}), 200

@app.route('/nodes/register', methods=['POST'])
def register_nodes():
    values = request.get_json()
    nodes_list = values.get("nodes", [])
    
    if not nodes_list:
        return jsonify({"error": "请提供节点列表"}), 400
    
    newly_added = []
    for node in nodes_list:
        if add_node(node):
            newly_added.append(node)
    
    # 只有当注册了新节点时才广播，并且延迟广播
    if newly_added:
        # 立即向所有已知节点（除了注册请求的源节点）广播新节点列表
        # 注意：这里我们广播的是完整的节点列表，而不仅仅是新节点，以便同步整个网络
        # 使用安全广播，避免循环
        safe_broadcast_nodes_list(source_addr=request.remote_addr)
    
    response = {
        "message": f"成功添加 {len(newly_added)} 个新节点",
        "newly_added": newly_added,
        "total_nodes": len(nodes),
        "current_nodes": list(nodes)
    }
    return jsonify(response), 201

@app.route('/nodes/sync', methods=['POST'])
def sync_nodes():
    """接收其他节点广播的节点列表（防循环版本）"""
    values = request.get_json()
    nodes_list = values.get("nodes", [])
    message_id = values.get("message_id")  # 广播消息ID
    
    # 检查广播消息ID，防止循环
    if message_id and not network_manager.can_broadcast(message_id, request.remote_addr):
        logger.debug(f"忽略重复的节点同步请求: {message_id}")
        return jsonify({"message": "同步请求已处理"}), 200
    
    if not nodes_list:
        return jsonify({"error": "请提供节点列表"}), 400
    
    newly_added = []
    for node in nodes_list:
        if add_node(node):
            newly_added.append(node)
    
    # 只有当有新节点时才继续广播（避免不必要的传播）
    if newly_added:
        # 延迟广播，避免立即响应造成循环
        threading.Timer(2.0, safe_broadcast_nodes_list).start()
    
    response = {
        "message": f"成功同步 {len(newly_added)} 个新节点",
        "newly_added": newly_added,
        "total_nodes": len(nodes)
    }
    return jsonify(response), 200

@app.route('/nodes/list', methods=['GET'])
def get_nodes_list():
    return jsonify({
        "nodes": list(nodes),
        "healthy_peers": list(network_manager.healthy_peers),
        "total": len(nodes)
    }), 200

@app.route('/nodes/status/<node_addr>', methods=['GET'])
def get_node_status(node_addr):
    """获取特定节点的详细状态"""
    status = network_manager.get_peer_status(node_addr)
    return jsonify(status), 200

@app.route('/nodes/check/<node_addr>', methods=['POST'])
def force_check_node(node_addr):
    """强制检查特定节点"""
    result = network_manager.force_check_peer(node_addr)
    return jsonify({
        "node": node_addr,
        "healthy": result,
        "message": f"节点 {node_addr} 状态: {'健康' if result else '不健康'}"
    }), 200

@app.route('/mempool', methods=['GET'])
def get_mempool():
    return jsonify({
        "size": blockchain.mempool.size(),
        "transactions": list(blockchain.mempool.transactions.values())
    }), 200

@app.route('/transaction/new', methods=['POST'])
def new_transaction():
    values = request.get_json()
    required = ["sender", "recipient", "amount", "signature", "txid", "nonce"]
    
    if not all(k in values for k in required):
        return jsonify({"error": "缺少必要字段"}), 400
    
    try:
        blockchain.add_new_transaction(values)
        # 立即广播交易到网络
        threading.Thread(
            target=broadcast_transaction, 
            args=(values,), 
            daemon=True
        ).start()
        return jsonify({"message": "交易已添加到内存池并广播"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/transaction/broadcast', methods=['POST'])
def receive_broadcasted_transaction():
    """接收其他节点广播的交易"""
    values = request.get_json()
    source_addr = request.remote_addr
    
    try:
        # 验证交易
        is_valid, message = blockchain.validate_transaction(values)
        if not is_valid:
            return jsonify({"error": f"无效交易: {message}"}), 400
        
        # 添加到内存池
        success, msg = blockchain.mempool.add_transaction(values)
        if success:
            logger.info(f"接收并验证了广播交易: {values.get('txid')[:16]}...")
            # 继续广播给其他节点（避免广播循环），传递源地址
            threading.Thread(
                target=broadcast_to_others,
                args=(values, source_addr),
                daemon=True
            ).start()
            return jsonify({"message": "交易接收成功"}), 200
        else:
            return jsonify({"message": f"交易已存在: {msg}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/block/receive', methods=['POST'])
def receive_block():
    """接收其他节点广播的区块"""
    block_data = request.get_json()
    source_addr = request.remote_addr
    
    try:
        new_block = Block(
            block_data["index"],
            block_data["timestamp"],
            block_data["transactions"],
            block_data["previous_hash"],
            block_data["nonce"],
            block_data["hash"]
        )
        
        local_last_index = len(blockchain.chain) - 1
        
        # 处理不同情况的区块
        if new_block.index == local_last_index + 1:
            # 新区块，直接添加
            added, message = blockchain.add_block(new_block, new_block.hash)
            if added:
                logger.info(f"接收到的新区块 #{new_block.index} 已成功追加")
                
                # 从内存池中移除已确认的交易
                removed_count = blockchain.remove_confirmed_transactions(new_block)
                if removed_count > 0:
                    logger.info(f"从内存池中清理了 {removed_count} 笔已确认的交易")
                
                # 继续广播给其他节点，传递源地址
                threading.Thread(
                    target=broadcast_block_to_others,
                    args=(new_block, source_addr),
                    daemon=True
                ).start()
                return jsonify({"message": "区块接收成功"}), 200
            else:
                logger.warning(f"验证新区块失败: {message}")
                return jsonify({"error": message}), 400
                
        elif new_block.index > local_last_index + 1:
            # 接收到的区块超前，触发共识同步
            logger.info(f"接收到的区块 #{new_block.index} 超前本地 #{local_last_index}，触发共识")
            resolve_conflicts()
            return jsonify({"message": "触发链同步"}), 200
            
        else:
            # 接收到的区块不比当前链新，忽略
            logger.info(f"接收到的区块 #{new_block.index} 不比本地链新，忽略")
            return jsonify({"message": "区块已处理"}), 200
            
    except Exception as e:
        logger.error(f"处理接收到的区块时出现异常: {e}")
        return jsonify({"error": "处理区块时出错"}), 500

@app.route('/mine', methods=['GET'])
def mine():
    miner_address = request.args.get("miner_address")
    if not miner_address and default_wallet:
        miner_address = default_wallet.get_address()
    
    if not miner_address:
        return jsonify({"error": "必须提供矿工地址"}), 400
    
    try:
        block = blockchain.mine(miner_address)
        if not block:
            return jsonify({"message": "没有待处理的交易，挖矿已中止"}), 200
        
        response = {
            "message": "已挖出一个新区块",
            "block": block.to_dict()
        }
        # 广播新区块
        threading.Thread(
            target=broadcast_new_block, 
            args=(block,), 
            daemon=True
        ).start()
        return jsonify(response), 200
    except Exception as e:
        logger.error(f"挖矿时出错: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/auto_mining', methods=['POST'])
def toggle_auto_mining_api():
    """API端点：切换自动挖矿状态"""
    values = request.get_json() or {}
    enable = values.get('enable')
    
    if enable is None:
        # 切换状态
        new_state = toggle_auto_mining()
    else:
        # 设置指定状态
        global auto_mining_enabled
        auto_mining_enabled = bool(enable)
        if auto_mining_enabled and not auto_mining_started:
            start_auto_mining_if_needed()
        new_state = auto_mining_enabled
    
    return jsonify({
        "message": f"自动挖矿已{'启用' if new_state else '禁用'}",
        "auto_mining_enabled": new_state
    }), 200

@app.route('/nodes/resolve', methods=['GET'])
def consensus():
    replaced = resolve_conflicts()
    if replaced:
        response = {
            "message": "链已更新为权威链",
            "new_chain": [block.to_dict() for block in blockchain.chain]
        }
    else:
        response = {
            "message": "当前链是权威的",
            "chain": [block.to_dict() for block in blockchain.chain]
        }
    return jsonify(response), 200

def resolve_conflicts():
    """解决链冲突，选择最长有效链"""
    max_length = len(blockchain.chain)
    new_chain = None
    authoritative_node = None
    
    for node_addr in network_manager.healthy_peers:
        try:
            url = f"http://{node_addr}/chain"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                length = data.get("length", 0)
                chain_data = data.get("chain", [])
                
                if length > max_length:
                    new_chain_candidate = []
                    for block_dict in chain_data:
                        block_obj = Block(
                            block_dict["index"],
                            block_dict["timestamp"],
                            block_dict["transactions"],
                            block_dict["previous_hash"],
                            block_dict["nonce"],
                            block_dict["hash"]
                        )
                        new_chain_candidate.append(block_obj)
                    
                    if blockchain.validate_chain(new_chain_candidate):
                        max_length = length
                        new_chain = new_chain_candidate
                        authoritative_node = node_addr
        except Exception as e:
            logger.debug(f"无法从节点 {node_addr} 获取链: {e}")
    
    if new_chain:
        blockchain.chain = new_chain
        blockchain.rebuild_utxo_set()
        blockchain.save_chain()
        logger.info(f"链已更新为来自节点 {authoritative_node} 的最新链，新长度: {max_length}")
        return True
    
    return False

@app.route('/mining/mode', methods=['POST'])
def set_mining_mode_api():
    """API端点：设置挖矿模式"""
    values = request.get_json() or {}
    mode = values.get('mode')  # 'continuous' 或 'transaction_driven'
    
    global auto_mining_enabled
    
    if mode == 'continuous':
        auto_mining_enabled = True
        message = "已启用连续挖矿模式"
    elif mode == 'transaction_driven':
        auto_mining_enabled = True
        message = "已启用交易驱动挖矿模式"
    elif mode == 'disabled':
        auto_mining_enabled = False
        message = "自动挖矿已禁用"
    else:
        return jsonify({"error": "无效的模式参数"}), 400
    
    # 如果需要启动自动挖矿线程
    if auto_mining_enabled and not auto_mining_started:
        start_auto_mining_if_needed()
    
    return jsonify({
        "message": message,
        "auto_mining_enabled": auto_mining_enabled,
        "mode": mode
    }), 200

@app.route('/mining/status', methods=['GET'])
def get_mining_status():
    """获取挖矿状态信息"""
    return jsonify({
        "auto_mining_enabled": auto_mining_enabled,
        "auto_mining_started": auto_mining_started,
        "mempool_size": blockchain.mempool.size(),
        "blockchain_height": len(blockchain.chain),
        "mode": "continuous" if auto_mining_enabled else "disabled"
    }), 200

#############################################
# 网络发现功能
#############################################
def safe_discover_network(max_hops=2):
    """安全的网络发现，限制传播跳数"""
    print("\n--- 安全网络发现 ---")
    discovered_count = 0
    discovery_attempts = 0
    
    # 限制发现的节点数量，避免过度查询
    nodes_to_query = list(network_manager.healthy_peers)[:5]  # 最多查询5个健康节点
    
    for node_addr in nodes_to_query:
        if discovery_attempts >= 10:  # 最大尝试次数
            break
            
        try:
            url = f"http://{node_addr}/nodes/list"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                remote_nodes = data.get("nodes", [])
                
                for remote_node in remote_nodes:
                    if add_node(remote_node):
                        discovered_count += 1
                        print(f"发现新节点: {remote_node}")
                
                discovery_attempts += 1
                time.sleep(0.5)  # 添加延迟，避免过于频繁
        except Exception as e:
            print(f"从节点 {node_addr} 发现网络时出错: {e}")
    
    if discovered_count > 0:
        print(f"安全网络发现完成，新增 {discovered_count} 个节点")
        # 使用安全广播
        safe_broadcast_nodes_list()
    else:
        print("没有发现新的节点")
    
    return discovered_count

def force_check_all_nodes():
    """强制检查所有节点"""
    print("\n--- 强制检查所有节点 ---")
    checked_count = 0
    healthy_count = 0
    
    for node in list(nodes):
        result = network_manager.force_check_peer(node)
        checked_count += 1
        if result:
            healthy_count += 1
        time.sleep(0.5)  # 避免过于频繁的请求
    
    print(f"检查完成: {healthy_count}/{checked_count} 个节点健康")
    input("按回车键继续...")
    view_network_nodes()

#############################################
# CLI菜单功能
#############################################
def print_menu():
    print("\n" + "="*50)
    print("           区块链节点管理系统")
    print("="*50)
    print("1. 注册节点")
    print("2. 创建创世区块")
    print("3. 创建钱包")
    print("4. 发起交易")
    print("5. 手动挖矿")
    print("6. 查看区块链")
    print("7. 查询钱包余额")
    print("8. 查看钱包列表")
    print("9. 查看网络节点")
    print("10. 查看内存池")
    print("11. 解决链冲突")
    print(f"12. {'禁用' if auto_mining_enabled else '启用'}自动挖矿")
    print(f"13. 切换挖矿模式 (当前: {'连续' if auto_mining_enabled else '交易驱动'})")
    print("14. 退出系统")
    print("="*50)

def cli_menu():
    load_wallets()
    while True:
        print_menu()
        choice = input("请选择操作 (1-14): ").strip()
        
        if choice == "1":
            register_node_cli()
        elif choice == "2":
            create_genesis_block_cli()
        elif choice == "3":
            create_wallet_cli()
        elif choice == "4":
            send_transaction_cli()
        elif choice == "5":
            mine_block_cli()
        elif choice == "6":
            view_blockchain()
        elif choice == "7":
            query_wallet_balance_cli()
        elif choice == "8":
            view_wallets_cli()
        elif choice == "9":
            view_network_nodes()
        elif choice == "10":
            view_mempool()
        elif choice == "11":
            resolve_conflicts_cli()
        elif choice == "12":
            toggle_auto_mining_cli()
        elif choice == "13":
            toggle_mining_mode_cli()
        elif choice == "14":
            print("退出系统...")
            os._exit(0)
        else:
            print("无效选项，请重试。")

def toggle_auto_mining_cli():
    """CLI切换自动挖矿"""
    new_state = toggle_auto_mining()
    status = "启用" if new_state else "禁用"
    print(f"自动挖矿已{status}")

def toggle_mining_mode_cli():
    """切换挖矿模式"""
    global auto_mining_enabled
    
    if auto_mining_enabled:
        # 如果当前已启用，询问切换到哪种模式
        print("\n--- 切换挖矿模式 ---")
        print("1. 交易驱动挖矿 (只在有交易时挖矿)")
        print("2. 连续挖矿 (即使没有交易也挖空块)")
        print("3. 禁用自动挖矿")
        
        choice = input("请选择模式 (1-3): ").strip()
        if choice == "1":
            auto_mining_enabled = True
            print("已切换到交易驱动挖矿模式")
        elif choice == "2":
            auto_mining_enabled = True
            print("已切换到连续挖矿模式")
        elif choice == "3":
            auto_mining_enabled = False
            print("自动挖矿已禁用")
        else:
            print("无效选择")
    else:
        # 如果当前禁用，询问启用哪种模式
        print("\n--- 启用挖矿模式 ---")
        print("1. 交易驱动挖矿 (只在有交易时挖矿)")
        print("2. 连续挖矿 (即使没有交易也挖空块)")
        
        choice = input("请选择模式 (1-2): ").strip()
        if choice == "1" or choice == "2":
            auto_mining_enabled = True
            mode = "交易驱动" if choice == "1" else "连续"
            print(f"已启用{mode}挖矿模式")
            
            if not auto_mining_started:
                start_auto_mining_if_needed()
        else:
            print("无效选择")

def register_node_cli():
    print("\n--- 节点注册 ---")
    root = input("输入根节点地址 (例如 127.0.0.1:5000)，或直接回车跳过: ").strip()
    if not root:
        print("注册已跳过。")
        return
    
    global my_node_address
    if not my_node_address:
        my_node = input("输入本节点地址 (例如 127.0.0.1:5000): ").strip()
        if not my_node:
            print("节点地址不能为空！")
            return
        my_node_address = my_node
    
    try:
        url = f"http://{root}/nodes/register"
        payload = {"nodes": [my_node_address]}
        print(f"尝试将节点 {my_node_address} 注册到根节点 {root}...")
        response = requests.post(url, json=payload, timeout=5)
        
        if response.status_code == 201:
            data = response.json()
            print("节点注册成功。")
            
            # 添加根节点
            add_node(root)
            
            # 从响应中获取完整的节点列表并添加
            current_nodes = data.get("current_nodes", [])
            for node in current_nodes:
                if node != my_node_address:  # 不添加自己
                    add_node(node)
            
            print(f"从根节点获取了 {len(current_nodes)} 个节点信息")
            
            # 使用安全的网络发现（而不是立即广播）
            threading.Timer(3.0, safe_discover_network).start()
            
            # 同步链数据
            sync_with_node(root)
            
            # 启动自动挖矿（如果启用）
            if default_wallet is None:
                load_default_wallet()
            start_auto_mining_if_needed()
            
            print("节点注册完成，将在3秒后开始安全网络发现...")
        else:
            print(f"节点注册失败，状态码: {response.status_code}")
    except Exception as e:
        print(f"注册过程中出错: {e}")

def sync_with_node(node_addr):
    """与指定节点同步数据"""
    try:
        # 同步链数据
        chain_resp = requests.get(f"http://{node_addr}/chain", timeout=5)
        if chain_resp.status_code == 200:
            chain_data = chain_resp.json().get("chain", [])
            if chain_data:
                new_chain = []
                for blk in chain_data:
                    block = Block(
                        blk["index"],
                        blk["timestamp"],
                        blk["transactions"],
                        blk["previous_hash"],
                        blk["nonce"],
                        blk["hash"]
                    )
                    new_chain.append(block)
                blockchain.chain = new_chain
                blockchain.rebuild_utxo_set()
                blockchain.save_chain()
                print(f"区块链数据已同步，区块数: {len(chain_data)}")
        
        # 同步节点列表
        nodes_resp = requests.get(f"http://{node_addr}/nodes/list", timeout=5)
        if nodes_resp.status_code == 200:
            remote_nodes = nodes_resp.json().get("nodes", [])
            for remote_node in remote_nodes:
                add_node(remote_node)
            print(f"同步了 {len(remote_nodes)} 个节点信息")
            
    except Exception as e:
        print(f"同步数据失败: {e}")

def create_genesis_block_cli():
    if blockchain.chain:
        print("区块链已存在，不允许创建创世区块。")
    else:
        try:
            blockchain.create_genesis_block()
            print("创世区块创建成功。")
            if default_wallet is None:
                load_default_wallet()
            start_auto_mining_if_needed()
        except Exception as e:
            print(f"创建创世区块失败: {e}")

def create_wallet_cli():
    wallet = create_new_wallet()
    print(f"新钱包创建成功！")
    print(f"地址: {wallet.get_address()}")
    print(f"请妥善保管私钥！")

def send_transaction_cli():
    print("\n--- 发起交易 ---")
    
    # 选择发送者钱包
    if not wallets:
        print("没有可用的钱包，请先创建钱包。")
        return
    
    print("可用钱包:")
    for i, addr in enumerate(wallets.keys(), 1):
        balance = blockchain.get_balance(addr)
        print(f"{i}. {addr[:16]}... (余额: {balance})")
    
    try:
        choice = int(input("选择发送者钱包编号: ")) - 1
        sender_addr = list(wallets.keys())[choice]
    except (ValueError, IndexError):
        print("无效选择")
        return
    
    recipient = input("输入接收者钱包地址: ").strip()
    try:
        amount = float(input("输入转账金额: ").strip())
    except ValueError:
        print("金额必须为数字。")
        return
    
    # 创建并签名交易
    from transaction import Transaction
    wallet = wallets[sender_addr]
    tx = Transaction(sender_addr, recipient, amount)
    tx.sign_transaction(wallet)
    
    try:
        blockchain.add_new_transaction(tx.to_dict())
        print("交易成功添加到内存池！")
        print(f"交易ID: {tx.txid}")
        
        # 立即广播交易
        broadcast_transaction(tx.to_dict())
        print("交易已广播到网络。")
        
    except Exception as e:
        print(f"添加交易失败: {e}")

def mine_block_cli():
    print("\n--- 手动挖矿 ---")
    if not default_wallet:
        load_default_wallet()
    
    miner_addr = input(f"输入矿工地址 (直接回车使用默认钱包 {default_wallet.get_address()[:16]}...): ").strip()
    if not miner_addr:
        miner_addr = default_wallet.get_address()
    
    try:
        block = blockchain.mine(miner_addr)
        if block:
            print(f"挖矿成功！新区块索引: {block.index}")
            print(f"区块哈希: {block.hash[:16]}...")
            print(f"包含交易: {len(block.transactions)} 笔")
            
            # 广播新区块
            broadcast_new_block(block)
            print("新区块已广播到网络。")
        else:
            print("挖矿失败或没有待处理交易")
    except Exception as e:
        print(f"挖矿错误: {e}")

def view_blockchain():
    print("\n--- 当前区块链 ---")
    if not blockchain.chain:
        print("区块链为空。")
        return
    
    print(f"总区块数: {len(blockchain.chain)}")
    print(f"当前难度: {Blockchain.difficulty}")
    print(f"自动挖矿: {'启用' if auto_mining_enabled else '禁用'}")
    print()
    
    for block in blockchain.chain[-10:]:  # 只显示最后10个区块
        print("-" * 60)
        print(f"区块 #{block.index} | 时间: {time.ctime(block.timestamp)}")
        print(f"前一哈希: {block.previous_hash[:16]}...")
        print(f"本块哈希: {block.hash[:16]}...")
        print(f"交易数量: {len(block.transactions)}")
        print(f"Nonce: {block.nonce}")
    
    print("-" * 60)

def query_wallet_balance_cli():
    addr = input("输入要查询余额的钱包地址: ").strip()
    balance = blockchain.get_balance(addr)
    print(f"钱包 {addr[:16]}... 的余额: {balance}")

def view_wallets_cli():
    print("\n--- 已保存的钱包 ---")
    if not wallets:
        print("未找到钱包，请先创建一个。")
    else:
        for i, (addr, wallet) in enumerate(wallets.items(), 1):
            balance = blockchain.get_balance(addr)
            print(f"{i}. 地址: {addr}")
            print(f"   余额: {balance}")
            print(f"   Nonce: {wallet.get_current_nonce()}")
            print()

def view_network_nodes():
    print(f"\n--- 网络节点列表 (共 {len(nodes)} 个节点) ---")
    print(f"健康节点: {len(network_manager.healthy_peers)}")
    
    # 按连接状态排序显示
    healthy_nodes = []
    unhealthy_nodes = []
    
    for node in nodes:
        if node in network_manager.healthy_peers:
            healthy_nodes.append(node)
        else:
            unhealthy_nodes.append(node)
    
    print("\n🟢 健康节点:")
    for i, node in enumerate(healthy_nodes, 1):
        print(f"  {i}. {node}")
    
    print("\n🔴 不健康节点:")
    for i, node in enumerate(unhealthy_nodes, 1):
        print(f"  {i}. {node}")
    
    print("\n命令:")
    print("  - 输入节点编号查看详细状态")
    print("  - 输入 'd' 发现网络")
    print("  - 输入 'c' 强制检查所有节点")
    print("  - 输入 's' 同步节点列表")
    print("  - 输入 'r' 返回主菜单")
    
    choice = input("请选择操作: ").strip()
    
    if choice.isdigit():
        all_nodes = healthy_nodes + unhealthy_nodes
        node_index = int(choice) - 1
        if 0 <= node_index < len(all_nodes):
            node_addr = all_nodes[node_index]
            show_node_details(node_addr)
    elif choice.lower() == 'd':
        safe_discover_network()
        input("按回车键继续...")
        view_network_nodes()
    elif choice.lower() == 'c':
        force_check_all_nodes()
    elif choice.lower() == 's':
        safe_broadcast_nodes_list()
        print("已向网络广播节点列表")
        input("按回车键继续...")
        view_network_nodes()
    elif choice.lower() == 'r':
        return
    else:
        print("无效选择")
        view_network_nodes()

def show_node_details(node_addr):
    """显示节点详细状态"""
    print(f"\n--- 节点 {node_addr} 的详细状态 ---")
    status = network_manager.get_peer_status(node_addr)
    
    print(f"状态: {'健康 ✓' if status['status'] == 'healthy' else '不健康 ✗'}")
    print(f"重试次数: {status['retries']}/{network_manager.max_retries}")
    print(f"最后检查: {status['last_check_str']}")
    print(f"最后在线: {status['last_seen_str']}")
    
    # 尝试获取节点的健康信息
    try:
        response = requests.get(f"http://{node_addr}/health", timeout=3)
        if response.status_code == 200:
            health_data = response.json()
            print(f"区块高度: {health_data.get('block_height', '未知')}")
            print(f"内存池大小: {health_data.get('mempool_size', '未知')}")
            print(f"自动挖矿: {'启用' if health_data.get('auto_mining') else '禁用'}")
        else:
            print("无法获取节点详细信息")
    except:
        print("无法连接到节点获取详细信息")
    
    print("\n操作:")
    print("1. 强制重新检查")
    print("2. 返回节点列表")
    
    choice = input("请选择: ").strip()
    if choice == "1":
        result = network_manager.force_check_peer(node_addr)
        print(f"强制检查结果: 节点 {'健康' if result else '不健康'}")
        input("按回车键继续...")
        show_node_details(node_addr)
    elif choice == "2":
        view_network_nodes()

def view_mempool():
    print("\n--- 内存池状态 ---")
    size = blockchain.mempool.size()
    print(f"交易数量: {size}")
    
    if size > 0:
        print("\n最近交易:")
        transactions = list(blockchain.mempool.transactions.values())[-5:]  # 显示最近5笔
        for tx in transactions:
            print(f"  {tx.get('txid')[:16]}...: {tx.get('sender')[:8]}... -> {tx.get('recipient')[:8]}... ({tx.get('amount')})")

def resolve_conflicts_cli():
    print("\n--- 解决链冲突 ---")
    print("正在寻找最长有效链...")
    replaced = resolve_conflicts()
    if replaced:
        print("链已更新为权威链。")
        print(f"新区块链长度: {len(blockchain.chain)}")
    else:
        print("当前链已经是权威链。")

def start_services():
    """启动所有服务"""
    # 启动网络健康检查
    threading.Thread(
        target=network_manager.health_check, 
        daemon=True,
        name="HealthCheck"
    ).start()

    # 启动定期链同步
    def periodic_sync():
        while True:
            time.sleep(60)  # 每分钟同步一次
            if network_manager.healthy_peers:
                resolve_conflicts()
    
    threading.Thread(
        target=periodic_sync, 
        daemon=True,
        name="PeriodicSync"
    ).start()

    # 启动网络发现（延迟执行，等待服务完全启动）
    def delayed_network_discovery():
        time.sleep(5)  # 等待5秒让服务完全启动
        if nodes:  # 如果有已知节点
            discovered = safe_discover_network()
            if discovered > 0:
                logger.info(f"启动时网络发现: 新增 {discovered} 个节点")
    
    threading.Thread(
        target=delayed_network_discovery,
        daemon=True,
        name="NetworkDiscovery"
    ).start()

#############################################
# 启动服务
#############################################
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-p", "--port", default=5000, type=int, help="Flask监听端口")
    parser.add_argument("--host", default="0.0.0.0", help="Flask监听地址")
    parser.add_argument("--seed-url", type=str, default="", help="用于注册和链同步的种子节点地址")
    parser.add_argument("--my-address", type=str, default="", help="本节点地址 (例如 127.0.0.1:5000)")
    parser.add_argument("--auto-mine", action="store_true", help="启动时启用自动挖矿")
    args = parser.parse_args()
    flask_port = args.port
    flask_host = args.host

    # 设置本节点地址
    if args.my_address:
        my_node_address = args.my_address
    else:
        my_node_address = f"127.0.0.1:{flask_port}"

    # 设置自动挖矿状态
    if args.auto_mine:
        auto_mining_enabled = True

    # 如果有种子节点，添加到引导节点
    if args.seed_url:
        network_manager.bootstrap_nodes.add(args.seed_url)
        add_node(args.seed_url)

    # 加载数据
    load_node_addresses()
    load_wallets()

    # 启动所有服务
    start_services()

    # 如果有区块链数据，启动自动挖矿
    if blockchain.chain and not auto_mining_started:
        if default_wallet is None:
            load_default_wallet()
        start_auto_mining_if_needed()

    # 在后台启动Flask服务
    flask_thread = threading.Thread(
        target=lambda: app.run(
            host=flask_host, 
            port=flask_port, 
            debug=False,
            use_reloader=False
        ), 
        daemon=True,
        name="FlaskServer"
    )
    flask_thread.start()
    
    logger.info(f"Flask服务已启动，监听地址 {flask_host}:{flask_port}")
    logger.info(f"本节点地址: {my_node_address}")
    logger.info(f"已知节点: {len(nodes)} 个")
    logger.info(f"区块链高度: {len(blockchain.chain)}")
    logger.info(f"自动挖矿: {'启用' if auto_mining_enabled else '禁用'}")

    # 启动CLI菜单交互
    cli_menu()