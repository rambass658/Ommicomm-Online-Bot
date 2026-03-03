import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import secrets
import string

DB_FILE = "data/bot_stats.db"
_connection = None
_lock = threading.Lock()

def get_db():
    """Возвращает глобальное соединение с БД, создавая его при первом вызове."""
    global _connection
    if _connection is None:
        _connection = sqlite3.connect(DB_FILE, check_same_thread=False, isolation_level=None)
        # Включаем WAL-режим для лучшей конкурентности
        _connection.execute("PRAGMA journal_mode=WAL")
        _connection.execute("PRAGMA synchronous=NORMAL")
    return _connection

def init_db():
    """Создаёт таблицы для статистики и активации, если их нет."""
    conn = get_db()
    with _lock:
        conn.execute('''CREATE TABLE IF NOT EXISTS commands
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user_id INTEGER,
                      username TEXT,
                      command TEXT,
                      args TEXT,
                      timestamp DATETIME)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS errors
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user_id INTEGER,
                      command TEXT,
                      error TEXT,
                      timestamp DATETIME)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS activation_keys
                     (key TEXT PRIMARY KEY,
                      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                      used BOOLEAN DEFAULT 0,
                      used_by INTEGER DEFAULT NULL,
                      used_at DATETIME DEFAULT NULL)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS users
                     (user_id INTEGER PRIMARY KEY,
                      activated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                      key_used TEXT,
                      FOREIGN KEY(key_used) REFERENCES activation_keys(key))''')

def log_command(user_id: int, username: Optional[str], command: str, args: str = ""):
    conn = get_db()
    with _lock:
        conn.execute("INSERT INTO commands (user_id, username, command, args, timestamp) VALUES (?,?,?,?,?)",
                     (user_id, username or "unknown", command, args, datetime.now()))

def log_error(user_id: int, command: str, error: str):
    conn = get_db()
    with _lock:
        conn.execute("INSERT INTO errors (user_id, command, error, timestamp) VALUES (?,?,?,?)",
                     (user_id, command, str(error)[:200], datetime.now()))

def get_stats() -> dict:
    conn = get_db()
    stats = {}
    with _lock:
        cursor = conn.execute("SELECT COUNT(*) FROM commands")
        stats['total_commands'] = cursor.fetchone()[0]
        cursor = conn.execute("SELECT COUNT(*) FROM errors")
        stats['total_errors'] = cursor.fetchone()[0]
        cursor = conn.execute("SELECT COUNT(DISTINCT user_id) FROM commands")
        stats['unique_users'] = cursor.fetchone()[0]
        today = datetime.now().date()
        cursor = conn.execute("SELECT COUNT(*) FROM commands WHERE date(timestamp) = ?", (today,))
        stats['today_commands'] = cursor.fetchone()[0]
        cursor = conn.execute("SELECT command, COUNT(*) FROM commands GROUP BY command ORDER BY 2 DESC LIMIT 5")
        stats['top_commands'] = cursor.fetchall()
        day_ago = datetime.now() - timedelta(days=1)
        cursor = conn.execute("SELECT strftime('%H', timestamp) as hour, COUNT(*) FROM commands WHERE timestamp > ? GROUP BY hour ORDER BY hour",
                              (day_ago,))
        stats['hourly'] = cursor.fetchall()
    return stats

def generate_key() -> str:
    alphabet = string.ascii_uppercase + string.digits
    part1 = ''.join(secrets.choice(alphabet) for _ in range(4))
    part2 = ''.join(secrets.choice(alphabet) for _ in range(4))
    return f"STNG-{part1}-{part2}"

def add_key(key: str) -> bool:
    conn = get_db()
    try:
        with _lock:
            conn.execute("INSERT INTO activation_keys (key) VALUES (?)", (key,))
        return True
    except sqlite3.IntegrityError:
        return False

def activate_user(user_id: int, key: str) -> bool:
    conn = get_db()
    with _lock:
        cur = conn.execute("SELECT used FROM activation_keys WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row or row[0] == 1:
            return False
        conn.execute("UPDATE activation_keys SET used = 1, used_by = ?, used_at = ? WHERE key = ?",
                     (user_id, datetime.now(), key))
        conn.execute("INSERT OR REPLACE INTO users (user_id, key_used) VALUES (?, ?)",
                     (user_id, key))
    return True

def is_activated(user_id: int) -> bool:
    conn = get_db()
    with _lock:
        cur = conn.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
        return cur.fetchone() is not None

def get_all_keys() -> List[Dict]:
    conn = get_db()
    with _lock:
        cur = conn.execute('''SELECT key, created_at, used, used_by, used_at 
                              FROM activation_keys ORDER BY created_at DESC''')
        rows = cur.fetchall()
    keys = []
    for row in rows:
        keys.append({
            'key': row[0],
            'created_at': row[1],
            'used': bool(row[2]),
            'used_by': row[3],
            'used_at': row[4]
        })
    return keys

def delete_key(key: str) -> bool:
    conn = get_db()
    with _lock:
        cur = conn.execute("DELETE FROM activation_keys WHERE key = ?", (key,))
        return cur.rowcount > 0

def get_users() -> List[Dict]:
    conn = get_db()
    with _lock:
        cur = conn.execute('''SELECT user_id, activated_at, key_used FROM users ORDER BY activated_at DESC''')
        rows = cur.fetchall()
    users = []
    for row in rows:
        users.append({
            'user_id': row[0],
            'activated_at': row[1],
            'key_used': row[2]
        })
    return users