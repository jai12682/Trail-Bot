import sqlite3
import threading
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from flask import Flask, request, jsonify, render_template, flash, redirect, url_for
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user
from werkzeug.security import generate_password_hash, check_password_hash
from binance.client import Client
from binance.exceptions import BinanceAPIException
from concurrent_log_handler import ConcurrentRotatingFileHandler
import logging
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential, retry_if_exception_type
import requests.exceptions
import time
from flask_caching import Cache

# Set up logging
logger = logging.getLogger('trading_app')
logger.setLevel(logging.INFO)

file_handler = ConcurrentRotatingFileHandler('trading_data.log', maxBytes=10*1024*1024, backupCount=5)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(file_formatter)
console_handler.setLevel(logging.INFO)  # Show all INFO logs
logger.addHandler(console_handler)

log_lock = threading.Lock()
db_lock = threading.Lock()

def log_message(level, message, print_to_terminal=False):
    with log_lock:
        if level == 'INFO':
            logger.info(message)
            if print_to_terminal or "Order placed" in message or "TSL triggered" in message or "Set initial SL" in message or "Updated TSL" in message:
                print(f"{datetime.now()} - INFO - {message}")
        elif level == 'ERROR':
            logger.error(message)
            print(f"{datetime.now()} - ERROR - {message}")  # All errors to terminal

def round_quantity(quantity, step_size, precision):
    quantity_decimal = Decimal(str(quantity))
    step_size_decimal = Decimal(str(step_size))
    rounded_quantity = (quantity_decimal // step_size_decimal) * step_size_decimal
    return float(rounded_quantity.quantize(Decimal(f'0.{"0" * precision}'), rounding=ROUND_DOWN))

# Global variables
current_config = {}
pending_orders = []
closed_positions = []
data_lock = threading.Lock()
shutdown_event = threading.Event()

initial_sl_set = {}
last_profit_level = {}

TRAILING_STOP_LEVELS = {
    1.0: 0.2,   # 1.0% profit -> 0.2% TSL offset
    1.3: 0.8,   # 1.3% profit -> 0.8% TSL offset
    1.5: 1.0,   # 1.5% profit -> 1.0% TSL offset
    1.7: 1.3,   # 1.7% profit -> 1.3% TSL offset
    2.0: 1.6,   # 2.0% profit -> 1.6% TSL offset
    2.3: 2.0    # 2.3% profit -> 2.0% TSL offset
}

CONFIG = {
    "db_update_interval": 1,
    "balance_update_interval": 300,
    "trailing_stop_interval": 2,
    "open_positions_interval": 5,
    "initial_stop_offset": 1.0,
    "state_save_interval": 300,
    "max_retries": 3,
    "max_backoff": 30,
    "max_api_weight": 1200,
    "weight_reset_interval": 60
}

app = Flask(__name__)
app.secret_key = 'a-very-secure-and-unique-secret-key-2025'
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

cache = Cache(app, config={'CACHE_TYPE': 'simple'})

class User(UserMixin):
    def __init__(self, id, username=None, password_hash=None):
        self.id = id
        self.username = username
        self.password_hash = password_hash

@login_manager.user_loader
def load_user(user_id):
    with db_lock:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, username, password_hash FROM Users WHERE id = ?", (user_id,))
            user_data = cursor.fetchone()
            if user_data:
                return User(user_data['id'], user_data['username'], user_data['password_hash'])
    return None

def get_db_connection(db_file="trading_data.db"):
    conn = sqlite3.connect(db_file, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def with_db_retry(func):
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        retry=retry_if_exception_type(sqlite3.OperationalError)
    )
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

def initialize_database(db_file="trading_data.db"):
    with db_lock:
        with get_db_connection(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS Config (
                user_id TEXT PRIMARY KEY,
                api_key TEXT NOT NULL,
                api_secret TEXT NOT NULL,
                status INTEGER DEFAULT 1,
                multiplier REAL DEFAULT 1.0,
                leverage INTEGER DEFAULT 1
            )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS Orders (
                order_id TEXT PRIMARY KEY,
                user_id TEXT,
                symbol TEXT,
                side TEXT,
                order_type TEXT,
                price REAL,
                quantity REAL,
                size_usdt REAL DEFAULT 0.0,
                status TEXT,
                time TEXT
            )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS Users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                password_hash TEXT
            )''')
            conn.commit()

def create_default_users():
    with db_lock:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM Users")
            if cursor.fetchone()[0] == 0:
                password_hash_admin = generate_password_hash("admin123")
                cursor.execute("INSERT INTO Users (username, password_hash) VALUES (?, ?)", ("admin", password_hash_admin))
                conn.commit()

@with_db_retry
def db_updater(db_file="trading_data.db"):
    while not shutdown_event.is_set():
        try:
            with db_lock:
                with get_db_connection(db_file) as conn:
                    cursor = conn.cursor()
                    with data_lock:
                        for user_id, config in current_config.items():
                            cursor.execute('''INSERT OR REPLACE INTO Config 
                                (user_id, api_key, api_secret, status, multiplier, leverage)
                                VALUES (?, ?, ?, ?, ?, ?)''',
                                (user_id, config['api_key'], config['api_secret'], config['status'],
                                 config.get('multiplier', 1.0), config.get('leverage', 1)))
                        for order in pending_orders:
                            cursor.execute('''INSERT OR REPLACE INTO Orders 
                                (order_id, user_id, symbol, side, order_type, price, quantity, size_usdt, status, time)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                (order['order_id'], order['user_id'], order['symbol'], order['side'],
                                 order['order_type'], order['price'], order['quantity'], order['size_usdt'], order['status'], order['time']))
                    conn.commit()
        except Exception as e:
            log_message('ERROR', f"Error updating database: {e}")
        shutdown_event.wait(CONFIG["db_update_interval"])

@retry(
    stop=stop_after_attempt(CONFIG["max_retries"]),
    wait=wait_exponential(multiplier=1, min=1, max=CONFIG["max_backoff"]),
    retry=retry_if_exception_type((requests.exceptions.RequestException, BinanceAPIException))
)
def sync_closed_positions():
    with data_lock:
        for user_id, config in current_config.items():
            if not config['status']:
                continue
            try:
                client = Client(config['api_key'], config['api_secret'], requests_params={"timeout": 20})
                trades = client.futures_account_trades()
                current_positions_info = client.futures_position_information()
                position_dict = {pos['symbol']: float(pos['positionAmt']) for pos in current_positions_info}
                
                for trade in trades:
                    symbol = trade['symbol']
                    side = trade['side']
                    quantity = float(trade['qty'])
                    price = float(trade['price'])
                    realized_pnl = float(trade['realizedPnl'])
                    trade_time = datetime.fromtimestamp(trade['time'] / 1000).strftime("%Y-%m-%d %H:%M:%S")
                    
                    matching_order = next((order for order in pending_orders if order['user_id'] == user_id and order['symbol'] == symbol and order['side'] != side and order['status'] == 'FILLED'), None)
                    if matching_order and position_dict.get(symbol, 0.0) == 0 and realized_pnl != 0:
                        entry_price = matching_order['price'] or float(client.get_symbol_ticker(symbol=symbol)['price'])
                        size_usdt = quantity * entry_price
                        with db_lock:
                            with get_db_connection() as conn:
                                cursor = conn.cursor()
                                cursor.execute("UPDATE Orders SET quantity = ?, size_usdt = ?, price = ? WHERE order_id = ?",
                                               (quantity, size_usdt, entry_price, matching_order['order_id']))
                                conn.commit()
                        closed_positions.append({
                            "user_id": user_id,
                            "symbol": symbol,
                            "quantity": quantity,
                            "size_usdt": size_usdt,
                            "entry_price": entry_price,
                            "exit_price": price,
                            "realized_pnl": realized_pnl,
                            "close_time": trade_time
                        })
                        pending_orders.remove(matching_order)

                        open_orders = client.futures_get_open_orders(symbol=symbol)
                        for order in open_orders:
                            if order['type'] == 'STOP_MARKET' and order.get('reduceOnly'):
                                try:
                                    client.futures_cancel_order(symbol=symbol, orderId=order['orderId'])
                                    log_message('INFO', f"User {user_id} - Canceled SL/TSL for closed {symbol} (Order ID: {order['orderId']})")
                                    pending_orders[:] = [o for o in pending_orders if o['order_id'] != str(order['orderId'])]
                                    position_key = (user_id, symbol)
                                    initial_sl_set.pop(position_key, None)
                                    last_profit_level.pop(position_key, None)
                                except BinanceAPIException as e:
                                    log_message('ERROR', f"User {user_id} - Failed to cancel SL/TSL for {symbol}: {str(e)}")
            except Exception as e:
                log_message('ERROR', f"Error syncing closed positions for {user_id}: {str(e)}")

def sync_closed_positions_periodically():
    while not shutdown_event.is_set():
        sync_closed_positions()
        shutdown_event.wait(60)

@retry(
    stop=stop_after_attempt(CONFIG["max_retries"]),
    wait=wait_exponential(multiplier=1, min=1, max=CONFIG["max_backoff"]),
    retry=retry_if_exception_type((requests.exceptions.RequestException, BinanceAPIException))
)
def update_balance():
    with data_lock:
        for user_id, config in current_config.items():
            if not config['status']:
                continue
            try:
                client = Client(config['api_key'], config['api_secret'], requests_params={"timeout": 20})
                account = client.futures_account()
                config['available_fund'] = float(account['availableBalance'])
                config['live_pnl'] = float(account['totalUnrealizedProfit'])
                log_message('INFO', f"Updated balance for {user_id}: Balance={config['available_fund']}, Status={config['status']}")  # Log to file only
            except Exception as e:
                log_message('ERROR', f"Error updating balance for {user_id}: {str(e)}")

def balance_updater():
    while not shutdown_event.is_set():
        update_balance()
        shutdown_event.wait(CONFIG["balance_update_interval"])

@app.route('/webhook', methods=['POST'])
def webhook():
    log_message('INFO', "Webhook endpoint called")
    try:
        data = request.get_json()
        log_message('INFO', f"Webhook data received: {data}")
        if not data:
            log_message('ERROR', "No JSON data received in request body")
            return jsonify({"error": "No JSON data received"}), 400
    except Exception as e:
        log_message('ERROR', f"Failed to parse JSON: {str(e)}")
        return jsonify({"error": f"Invalid JSON: {str(e)}"}), 400

    if data.get('token') != "secret123":
        log_message('ERROR', "Unauthorized webhook request")
        return jsonify({"error": "Unauthorized"}), 401

    action = data.get('action', 'trade').lower()
    symbol = data.get('symbol', '').upper()
    side = data.get('side', '').upper()
    size = float(data.get('size', 0))

    log_message('INFO', f"Parsed webhook data - Action: {action}, Symbol: {symbol}, Side: {side}, Size: {size}")

    if action != "trade" or not symbol or side not in ['BUY', 'SELL'] or size <= 0 or size > 100:
        log_message('ERROR', f"Invalid webhook data: {data}")
        return jsonify({"error": "Invalid data"}), 400

    with data_lock:
        if not current_config:
            log_message('ERROR', "No active users in current_config")
            return jsonify({"message": "No active users"}), 200

        log_message('INFO', f"Current config: {current_config}")
        for user_id, config in current_config.items():
            log_message('INFO', f"Processing user: {user_id}, Status: {config['status']}")
            if not config['status']:
                log_message('INFO', f"Skipping {user_id} - Inactive status")
                continue
            try:
                client = Client(config['api_key'], config['api_secret'], requests_params={"timeout": 20})
                balance = float(client.futures_account()['availableBalance'])
                price = float(client.get_symbol_ticker(symbol=symbol)['price'])
                notional = balance * (size / 100) * config['multiplier'] * config['leverage']
                quantity = notional / price

                log_message('INFO', f"User {user_id} - Balance: {balance}, Price: {price}, Notional: {notional}, Raw Quantity: {quantity}")

                info = client.get_symbol_info(symbol)
                step_size = next(f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE')
                quantity_precision = info.get('quantityPrecision', 0)
                quantity = round_quantity(quantity, float(step_size), quantity_precision)

                log_message('INFO', f"User {user_id} - Rounded Quantity: {quantity}, Step Size: {step_size}, Precision: {quantity_precision}")

                if quantity == 0 or (quantity * price) < 5:
                    log_message('INFO', f"User {user_id} - Skipping order: Quantity {quantity} too small or notional < 5 USDT")
                    continue

                order_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                size_usdt = quantity * price
                log_message('INFO', f"User {user_id} - Preparing order: Symbol={symbol}, Side={side}, Quantity={quantity}, Size_USDT={size_usdt}")

                order = client.futures_create_order(
                    symbol=symbol,
                    side=side,
                    type="MARKET",
                    quantity=quantity
                )

                order_id = str(order['orderId'])
                log_message('INFO', f"User {user_id} - Order placed successfully: Order ID={order_id}, Details={order}", print_to_terminal=True)

                pending_orders.append({
                    "order_id": order_id,
                    "user_id": user_id,
                    "symbol": symbol,
                    "side": side,
                    "order_type": "MARKET",
                    "price": price,
                    "quantity": quantity,
                    "size_usdt": size_usdt,
                    "status": "FILLED",
                    "time": order_time
                })
            except BinanceAPIException as e:
                log_message('ERROR', f"User {user_id} - Binance API error: {str(e)}, Code: {e.code}, Message: {e.message}")
            except Exception as e:
                log_message('ERROR', f"User {user_id} - Unexpected error processing webhook: {str(e)}")
    log_message('INFO', "Webhook processing completed")
    return jsonify({"message": "Webhook processed"}), 200

@app.route('/')
@login_required
def index():
    config_data = fetch_config_data()
    return render_template('dashboard.html', config_data=config_data)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username'].lower()
        password = request.form['password']
        with db_lock:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, username, password_hash FROM Users WHERE LOWER(username) = ?", (username,))
                user = cursor.fetchone()
                if user and check_password_hash(user['password_hash'], password):
                    user_obj = User(user['id'], user['username'], user['password_hash'])
                    login_user(user_obj)
                    flash('Login successful!')
                    return redirect(url_for('index'))
                flash('Invalid username or password')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/save_api', methods=['POST'])
@login_required
def save_api():
    user_id = request.form.get('user_id')
    api_key = request.form.get('api_key')
    api_secret = request.form.get('api_secret')
    multiplier = float(request.form.get('multiplier', 1.0))
    leverage = int(request.form.get('leverage', 1))

    if not api_key or not api_secret or multiplier < 0 or leverage < 1:
        return jsonify({"error": "Invalid input"}), 400

    try:
        client = Client(api_key, api_secret, requests_params={"timeout": 20})
        client.get_account()
    except Exception as e:
        log_message('ERROR', f"Invalid Binance API keys for {user_id}: {e}")
        return jsonify({"error": f"Invalid API keys: {str(e)}"}), 400

    with db_lock:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""INSERT OR REPLACE INTO Config 
                (user_id, api_key, api_secret, status, multiplier, leverage)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (user_id, api_key, api_secret, 1, multiplier, leverage))
            conn.commit()

    with data_lock:
        current_config[user_id] = {
            "api_key": api_key,
            "api_secret": api_secret,
            "status": 1,
            "multiplier": multiplier,
            "leverage": leverage,
            "available_fund": 0.0,
            "live_pnl": 0.0
        }
    return jsonify({"message": "API credentials saved"}), 200

def fetch_config_data():
    config_data = []
    with data_lock:
        for user_id, config in current_config.items():
            config_entry = {
                "user_id": user_id,
                "status": config['status'],
                "multiplier": config['multiplier'],
                "leverage": config['leverage'],
                "available_fund": config.get('available_fund', 0.0),
                "live_pnl": config.get('live_pnl', 0.0)
            }
            if config['status']:
                try:
                    client = Client(config['api_key'], config['api_secret'], requests_params={"timeout": 20})
                    account = client.futures_account()
                    config_entry['available_fund'] = float(account['availableBalance'])
                    config_entry['live_pnl'] = float(account['totalUnrealizedProfit'])
                    config['available_fund'] = config_entry['available_fund']
                    config['live_pnl'] = config_entry['live_pnl']
                except Exception as e:
                    log_message('ERROR', f"User {user_id} - Error fetching config: {str(e)}")
            config_data.append(config_entry)
    return config_data

@app.route('/config', methods=['GET'])
@login_required
@cache.cached(timeout=10)
def get_config():
    return jsonify(fetch_config_data())

@app.route('/orders', methods=['GET'])
@login_required
def get_orders():
    with db_lock:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM Orders")
            orders = [dict(row) for row in cursor.fetchall()]
    return jsonify(orders)

@app.route('/open_positions', methods=['GET'])
@login_required
@cache.cached(timeout=10)
def open_positions():
    try:
        open_positions_data = []
        with data_lock:
            for user_id, config in current_config.items():
                if not config['status']:
                    continue
                client = Client(config['api_key'], config['api_secret'], requests_params={"timeout": 20})
                positions = client.futures_position_information()
                for pos in positions:
                    position_amt = float(pos['positionAmt'])
                    if position_amt != 0:
                        symbol = pos['symbol']
                        entry_price = float(pos['entryPrice'])
                        mark_price = float(client.futures_mark_price(symbol=symbol)['markPrice'])
                        profit_percent = ((mark_price - entry_price) / entry_price * 100) if position_amt > 0 else ((entry_price - mark_price) / entry_price * 100)
                        size_usdt = abs(position_amt) * entry_price

                        orders = client.futures_get_open_orders(symbol=symbol)
                        sl_order = next((o for o in orders if o['type'] == 'STOP_MARKET' and o.get('reduceOnly')), None)
                        sl_price = float(sl_order['stopPrice']) if sl_order else None

                        open_positions_data.append({
                            "user_id": user_id,
                            "symbol": symbol,
                            "size_usdt": f"{size_usdt:.2f}",
                            "entry_price": f"{entry_price:.4f}",
                            "mark_price": f"{mark_price:.4f}",
                            "pnl_percent": f"{profit_percent:.2f}",
                            "sl_tsl_price": f"{sl_price:.4f}" if sl_price else "N/A"
                        })

        total_pnl = sum(float(p['pnl_percent']) for p in open_positions_data) if open_positions_data else 0
        avg_pnl = total_pnl / len(open_positions_data) if open_positions_data else 0
        return jsonify({
            "total_pnl": f"{total_pnl:.2f} ({avg_pnl:.2f}%)",
            "positions": open_positions_data
        })
    except Exception as e:
        log_message('ERROR', f"Error fetching open positions: {str(e)}")
        return jsonify({"error": str(e)}), 500

def fetch_open_positions_periodically():
    while not shutdown_event.is_set():
        try:
            open_positions_data = []
            with data_lock:
                for user_id, config in current_config.items():
                    if not config['status']:
                        continue
                    client = Client(config['api_key'], config['api_secret'], requests_params={"timeout": 20})
                    positions = client.futures_position_information()
                    for pos in positions:
                        position_amt = float(pos['positionAmt'])
                        if position_amt != 0:
                            symbol = pos['symbol']
                            entry_price = float(pos['entryPrice'])
                            mark_price = float(client.futures_mark_price(symbol=symbol)['markPrice'])
                            profit_percent = ((mark_price - entry_price) / entry_price * 100) if position_amt > 0 else ((entry_price - mark_price) / entry_price * 100)
                            size_usdt = abs(position_amt) * entry_price

                            orders = client.futures_get_open_orders(symbol=symbol)
                            sl_order = next((o for o in orders if o['type'] == 'STOP_MARKET' and o.get('reduceOnly')), None)
                            sl_price = float(sl_order['stopPrice']) if sl_order else None

                            open_positions_data.append({
                                "user_id": user_id,
                                "symbol": symbol,
                                "size_usdt": size_usdt,
                                "entry_price": entry_price,
                                "mark_price": mark_price,
                                "pnl_percent": profit_percent,
                                "sl_tsl_price": sl_price if sl_price else None
                            })
        except Exception as e:
            log_message('ERROR', f"Error in periodic open positions fetch: {str(e)}")
        shutdown_event.wait(CONFIG["open_positions_interval"])

@app.route('/update_status', methods=['POST'])
@login_required
def update_status():
    data = request.get_json()
    user_id = data.get('user_id')
    status = data.get('status')
    with data_lock:
        if user_id in current_config:
            current_config[user_id]['status'] = status
            log_message('INFO', f"Updated status for {user_id}: Status={status}", print_to_terminal=True)
            return jsonify({"message": "Status updated"}), 200
    return jsonify({"error": "User not found"}), 404

@app.route('/update_multiplier', methods=['POST'])
@login_required
def update_multiplier():
    data = request.get_json()
    user_id = data.get('user_id')
    multiplier = data.get('multiplier')
    with data_lock:
        if user_id in current_config:
            current_config[user_id]['multiplier'] = multiplier
            return jsonify({"message": "Multiplier updated"}), 200
    return jsonify({"error": "User not found"}), 404

@app.route('/update_leverage', methods=['POST'])
@login_required
def update_leverage():
    data = request.get_json()
    user_id = data.get('user_id')
    leverage = data.get('leverage')
    with data_lock:
        if user_id in current_config:
            current_config[user_id]['leverage'] = leverage
            return jsonify({"message": "Leverage updated"}), 200
    return jsonify({"error": "User not found"}), 404

@app.route('/delete_account', methods=['POST'])
@login_required
def delete_account():
    data = request.get_json()
    user_id = data.get('user_id')
    with data_lock:
        if user_id in current_config:
            del current_config[user_id]
            with db_lock:
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM Config WHERE user_id = ?", (user_id,))
                    conn.commit()
            return jsonify({"message": "Account deleted"}), 200
    return jsonify({"error": "User not found"}), 404

def manage_trailing_stops():
    while not shutdown_event.is_set():
        try:
            with data_lock:
                for user_id, config in current_config.items():
                    if not config['status']:
                        continue  # Skip inactive users silently
                    try:
                        client = Client(config['api_key'], config['api_secret'], requests_params={"timeout": 20})
                        positions = client.futures_position_information()
                        position_symbols = {pos['symbol'] for pos in positions if float(pos['positionAmt']) != 0}
                        open_orders = client.futures_get_open_orders()

                        for order in open_orders:
                            if order['type'] == 'STOP_MARKET' and order.get('reduceOnly') and order['symbol'] not in position_symbols:
                                try:
                                    client.futures_cancel_order(symbol=order['symbol'], orderId=order['orderId'])
                                    log_message('INFO', f"User {user_id} - Canceled orphaned SL/TSL for {order['symbol']} (Order ID: {order['orderId']})", print_to_terminal=True)
                                    pending_orders[:] = [o for o in pending_orders if o['order_id'] != str(order['orderId'])]
                                    position_key = (user_id, order['symbol'])
                                    initial_sl_set.pop(position_key, None)
                                    last_profit_level.pop(position_key, None)
                                except BinanceAPIException as e:
                                    log_message('ERROR', f"User {user_id} - Failed to cancel orphaned order {order['orderId']}: {str(e)}", print_to_terminal=True)

                        for pos in positions:
                            position_amt = float(pos['positionAmt'])
                            symbol = pos['symbol']
                            if position_amt == 0:
                                continue
                            entry_price = float(pos['entryPrice'])
                            current_price = float(client.futures_symbol_ticker(symbol=symbol)['price'])
                            mark_price = float(client.futures_mark_price(symbol=symbol)['markPrice'])

                            exchange_info = client.futures_exchange_info()
                            symbol_info = next((s for s in exchange_info['symbols'] if s['symbol'] == symbol), None)
                            if not symbol_info:
                                continue

                            price_precision = next((f['tickSize'] for f in symbol_info['filters'] 
                                                  if f['filterType'] == 'PRICE_FILTER'), '0.0001')
                            price_precision_decimal = Decimal(price_precision)

                            profit_percent = ((current_price - entry_price) / entry_price * 100) if position_amt > 0 else ((entry_price - current_price) / entry_price * 100)
                            position_key = (user_id, symbol)

                            orders = client.futures_get_open_orders(symbol=symbol)
                            stop_orders = [o for o in orders if o['type'] == 'STOP_MARKET' and o.get('reduceOnly')]
                            current_stop_price = float(stop_orders[0]['stopPrice']) if stop_orders else None

                            if not stop_orders:
                                initial_sl_price = entry_price * (1 - CONFIG['initial_stop_offset'] / 100) if position_amt > 0 else entry_price * (1 + CONFIG['initial_stop_offset'] / 100)
                                initial_sl_price = float(Decimal(str(initial_sl_price)).quantize(price_precision_decimal, rounding=ROUND_DOWN))

                                if (position_amt > 0 and initial_sl_price >= current_price) or (position_amt < 0 and initial_sl_price <= current_price):
                                    continue

                                try:
                                    new_order = client.futures_create_order(
                                        symbol=symbol,
                                        side="SELL" if position_amt > 0 else "BUY",
                                        type="STOP_MARKET",
                                        quantity=abs(position_amt),
                                        stopPrice=str(initial_sl_price),
                                        reduceOnly=True
                                    )
                                except BinanceAPIException as e:
                                    if e.code == -2021:
                                        wider_offset = CONFIG['initial_stop_offset'] * 1.5
                                        initial_sl_price = entry_price * (1 - wider_offset / 100) if position_amt > 0 else entry_price * (1 + wider_offset / 100)
                                        initial_sl_price = float(Decimal(str(initial_sl_price)).quantize(price_precision_decimal, rounding=ROUND_DOWN))
                                        try:
                                            new_order = client.futures_create_order(
                                                symbol=symbol,
                                                side="SELL" if position_amt > 0 else "BUY",
                                                type="STOP_MARKET",
                                                quantity=abs(position_amt),
                                                stopPrice=str(initial_sl_price),
                                                reduceOnly=True
                                            )
                                        except BinanceAPIException as e2:
                                            log_message('ERROR', f"User {user_id} - Failed to set wider initial SL for {symbol}: {str(e2)}", print_to_terminal=True)
                                            continue
                                    else:
                                        log_message('ERROR', f"User {user_id} - Failed to set initial SL for {symbol}: {str(e)}", print_to_terminal=True)
                                        continue

                                order_id = str(new_order['orderId'])
                                order_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                size_usdt = abs(position_amt) * entry_price
                                pending_orders.append({
                                    "order_id": order_id,
                                    "user_id": user_id,
                                    "symbol": symbol,
                                    "side": "SELL" if position_amt > 0 else "BUY",
                                    "order_type": "STOP_MARKET",
                                    "price": initial_sl_price,
                                    "quantity": abs(position_amt),
                                    "size_usdt": size_usdt,
                                    "status": "NEW",
                                    "time": order_time
                                })
                                initial_sl_set[position_key] = initial_sl_price
                                last_profit_level[position_key] = 0.0
                                log_message('INFO', f"User {user_id} - Set initial SL for {symbol} at {initial_sl_price} (Order ID: {order_id})", print_to_terminal=True)
                                time.sleep(1)

                            elif stop_orders:
                                current_stop_order = stop_orders[0]
                                current_stop_price = float(current_stop_order['stopPrice'])

                                new_level = max([level for level in TRAILING_STOP_LEVELS.keys() if profit_percent >= level and level > last_profit_level.get(position_key, 0.0)], default=None)
                                if new_level is not None:
                                    target_offset = TRAILING_STOP_LEVELS[new_level]
                                    new_stop_price = entry_price * (1 + target_offset / 100) if position_amt > 0 else entry_price * (1 - target_offset / 100)
                                    new_stop_price = float(Decimal(str(new_stop_price)).quantize(price_precision_decimal, rounding=ROUND_DOWN))

                                    if (position_amt > 0 and new_stop_price >= current_price) or (position_amt < 0 and new_stop_price <= current_price):
                                        continue

                                    max_attempts = 3
                                    for attempt in range(max_attempts):
                                        try:
                                            new_order = client.futures_create_order(
                                                symbol=symbol,
                                                side="SELL" if position_amt > 0 else "BUY",
                                                type="STOP_MARKET",
                                                quantity=abs(position_amt),
                                                stopPrice=str(new_stop_price),
                                                reduceOnly=True
                                            )
                                            new_order_id = str(new_order['orderId'])
                                            order_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                            size_usdt = abs(position_amt) * entry_price
                                            pending_orders.append({
                                                "order_id": new_order_id,
                                                "user_id": user_id,
                                                "symbol": symbol,
                                                "side": "SELL" if position_amt > 0 else "BUY",
                                                "order_type": "STOP_MARKET",
                                                "price": new_stop_price,
                                                "quantity": abs(position_amt),
                                                "size_usdt": size_usdt,
                                                "status": "NEW",
                                                "time": order_time
                                            })
                                            time.sleep(1)

                                            client.futures_cancel_order(symbol=symbol, orderId=current_stop_order['orderId'])
                                            if current_stop_price == initial_sl_set.get(position_key):
                                                initial_sl_set.pop(position_key)
                                            last_profit_level[position_key] = new_level
                                            log_message('INFO', f"User {user_id} - Updated TSL for {symbol} to {new_stop_price} (Order ID: {new_order_id}) - TSL triggered at {new_level}% profit with {target_offset}% TSL level", print_to_terminal=True)
                                            break
                                        except BinanceAPIException as e:
                                            log_message('ERROR', f"User {user_id} - Failed to update TSL for {symbol} attempt {attempt + 1}: {str(e)}", print_to_terminal=True)
                                            if attempt == max_attempts - 1 and 'new_order_id' in locals():
                                                try:
                                                    client.futures_cancel_order(symbol=symbol, orderId=new_order_id)
                                                except Exception:
                                                    pass
                                            time.sleep(2)
                    except Exception as e:
                        log_message('ERROR', f"User {user_id} - Error in trailing stop manager: {str(e)}", print_to_terminal=True)
        except Exception as e:
            log_message('ERROR', f"Error in trailing stop manager: {str(e)}", print_to_terminal=True)
        shutdown_event.wait(CONFIG['trailing_stop_interval'])

def read_api_keys():
    with db_lock:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, api_key, api_secret, status, multiplier, leverage FROM Config")
            for row in cursor.fetchall():
                user_id = row['user_id']
                current_config[user_id] = {
                    "api_key": row['api_key'],
                    "api_secret": row['api_secret'],
                    "status": row['status'],
                    "multiplier": row['multiplier'],
                    "leverage": row['leverage'],
                    "available_fund": 0.0,
                    "live_pnl": 0.0
                }
    update_balance()  # Initial balance fetch
    with data_lock:
        for user_id, config in current_config.items():
            log_message('INFO', f"Configured account: {user_id}, Balance={config['available_fund']}, Status={config['status']}", print_to_terminal=True)

def update_balance():
    with data_lock:
        for user_id, config in current_config.items():
            if not config['status']:
                continue
            try:
                client = Client(config['api_key'], config['api_secret'], requests_params={"timeout": 20})
                account = client.futures_account()
                config['available_fund'] = float(account['availableBalance'])
                config['live_pnl'] = float(account['totalUnrealizedProfit'])
                log_message('INFO', f"Updated balance for {user_id}: Balance={config['available_fund']}, Status={config['status']}")  # Log to file only
            except Exception as e:
                log_message('ERROR', f"Error updating balance for {user_id}: {str(e)}", print_to_terminal=True)
def main():
    threads = []
    try:
        initialize_database()
        create_default_users()
        read_api_keys()

        threads.append(threading.Thread(target=db_updater, daemon=True))
        threads.append(threading.Thread(target=sync_closed_positions_periodically, daemon=True))
        threads.append(threading.Thread(target=manage_trailing_stops, daemon=True))
        threads.append(threading.Thread(target=fetch_open_positions_periodically, daemon=True))

        for thread in threads:
            thread.start()

        app.run(host='0.0.0.0', port=5000, debug=False)

    except KeyboardInterrupt:
        print("\nShutting down...")
        shutdown_event.set()
        for thread in threads:
            thread.join()
    except Exception as e:
        print(f"Error: {str(e)}")
        shutdown_event.set()
        for thread in threads:
            thread.join()

if __name__ == "__main__":
    main()