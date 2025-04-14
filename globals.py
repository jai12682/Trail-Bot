from typing import Dict, List
import threading

slave_accounts: List[Dict] = []
current_positions: Dict[str, Dict] = {}
current_config: Dict[str, Dict] = {}
pending_orders: List[Dict] = []
data_lock = threading.Lock()
shutdown_event = threading.Event()
thread_status = {
    "db_updater": True,
    "balance_updater": True,
    "sync_slave_positions": True
}

CONFIG = {
    "db_update_interval": 10,
    "balance_update_interval": 30,
    "slave_sync_interval": 10,
    "max_retries": 3,
    "max_backoff": 30,
}