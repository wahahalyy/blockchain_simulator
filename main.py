from node import app, blockchain, my_node_address, auto_mining_enabled
import logging

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    print("="*60)
    print("           区块链节点启动")
    print("="*60)
    print(f"节点地址: {my_node_address}")
    print(f"区块链高度: {len(blockchain.chain)}")
    print(f"内存池交易: {blockchain.mempool.size()}")
    print(f"自动挖矿: {'启用' if auto_mining_enabled else '禁用'}")
    print("="*60)
    
    # 启动节点服务
    app.run(host='0.0.0.0', port=5000, debug=False)