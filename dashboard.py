import sqlite3
import threading
from trading_utils import logger, log_message
from globals import (current_positions, current_config, pending_orders, closed_positions, 
                     all_closed_positions, data_lock, shutdown_event, CONFIG)

def get_db_connection(db_file="trading_data.db"):
    """Create a thread-safe database connection."""
    conn = sqlite3.connect(db_file, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # Allows accessing columns by name
    return conn

def initialize_database(db_file="trading_data.db"):
    """Initialize the SQLite database with required tables."""
    with get_db_connection(db_file) as conn:
        cursor = conn.cursor()
        
        # Config table for storing API credentials, account status, and leverage
        cursor.execute('''CREATE TABLE IF NOT EXISTS Config (
            user_id TEXT PRIMARY KEY,
            api_key TEXT NOT NULL,
            api_secret TEXT NOT NULL,
            status INTEGER DEFAULT 1,
            multiplier REAL DEFAULT 1.0,
            leverage INTEGER DEFAULT 1
        )''')

        # Orders table for tracking pending orders (simplified)
        cursor.execute('''CREATE TABLE IF NOT EXISTS Orders (
            order_id TEXT PRIMARY KEY,
            user_id TEXT,
            symbol TEXT,
            side TEXT,
            order_type TEXT,
            price REAL,
            quantity REAL,
            status TEXT,
            time TEXT
        )''')

        conn.commit()
        log_message('INFO', "Database initialized successfully")

def db_updater(db_file="trading_data.db"):
    """Periodically update the database with in-memory data."""
    while not shutdown_event.is_set():
        try:
            with get_db_connection(db_file) as conn:
                cursor = conn.cursor()
                
                # Update Config table
                with data_lock:
                    for user_id, config in current_config.items():
                        cursor.execute('''INSERT OR REPLACE INTO Config 
                            (user_id, api_key, api_secret, status, multiplier, leverage)
                            VALUES (?, ?, ?, ?, ?, ?)''',
                            (user_id, config['api_key'], config['api_secret'], config['status'],
                             config.get('multiplier', 1.0), config.get('leverage', 1)))
                
                # Update Orders table
                with data_lock:
                    for order in pending_orders:
                        cursor.execute('''INSERT OR REPLACE INTO Orders 
                            (order_id, user_id, symbol, side, order_type, price, quantity, status, time)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (order['order_id'], order['user_id'], order['symbol'], order['side'],
                             order['order_type'], order['price'], order['quantity'], order['status'], order['time']))

                conn.commit()
                log_message('INFO', "Database updated successfully")
        except Exception as e:
            log_message('ERROR', f"Error updating database: {e}")
        threading.Event().wait(CONFIG["db_update_interval"])

def calculate_pnl_percentage(current_positions, closed_positions):
    """Calculate the PNL percentage based on current and closed positions."""
    total_value = 0
    total_initial_value = 0
    
    # Calculate from current positions
    for position in current_positions.values():
        if 'entry_price' in position and 'quantity' in position:
            total_initial_value += float(position['entry_price']) * abs(float(position['quantity']))
            total_value += float(position['current_price']) * abs(float(position['quantity']))
    
    # Calculate from closed positions
    for position in closed_positions:
        if 'entry_price' in position and 'exit_price' in position and 'quantity' in position:
            entry_value = float(position['entry_price']) * abs(float(position['quantity']))
            exit_value = float(position['exit_price']) * abs(float(position['quantity']))
            total_initial_value += entry_value
            total_value += exit_value
    
    if total_initial_value == 0:
        return 0
    
    pnl_percentage = ((total_value - total_initial_value) / total_initial_value) * 100
    return round(pnl_percentage, 2)

def get_dashboard_data():
    """Get all data needed for the dashboard."""
    with data_lock:
        pnl_percentage = calculate_pnl_percentage(current_positions, closed_positions)
        
    return {
        'pnl_percentage': pnl_percentage,
        'positions': current_positions,
        'closed_positions': closed_positions
    }