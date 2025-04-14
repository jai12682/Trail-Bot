import logging
from concurrent_log_handler import ConcurrentRotatingFileHandler
import threading
from binance.client import Client

# Set up a single logger with a lock to prevent concurrent logging
logger = logging.getLogger('trading_app')
logger.setLevel(logging.INFO)
handler = ConcurrentRotatingFileHandler('trading_data.log', maxBytes=10*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(stream_handler)

# Add a lock to the logger to prevent concurrent writes
log_lock = threading.Lock()

def log_message(level, message):
    with log_lock:
        if level == 'INFO':
            logger.info(message)
        elif level == 'ERROR':
            logger.error(message)

def place_order(client: Client, order_data: dict, multiplier: float) -> str:
    """Place an order on Binance Futures."""
    order = client.futures_create_order(
        symbol=order_data['s'],
        side=order_data['S'],
        type=order_data['o'],
        quantity=order_data['q'],
        reduceOnly=order_data.get('R', False),
        price=order_data.get('p'),
        stopPrice=order_data.get('sp')
    )
    return str(order['orderId'])

def close_all_positions(client: Client, user_id: str, sio):
    """Close all open positions for a user."""
    positions = client.futures_position_information()
    for pos in positions:
        if float(pos['positionAmt']) != 0:
            side = 'SELL' if float(pos['positionAmt']) > 0 else 'BUY'
            order = client.futures_create_order(
                symbol=pos['symbol'],
                side=side,
                type='MARKET',
                quantity=abs(float(pos['positionAmt'])),
                reduceOnly=True
            )
    sio.emit('positions', {})