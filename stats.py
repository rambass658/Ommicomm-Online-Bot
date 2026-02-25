import sqlite3
from datetime import datetime, timedelta
from typing import Optional

DB_FILE = "data/bot_stats.db"

def init_db():
    """Создаёт таблицы для статистики, если их нет."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS commands
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  username TEXT,
                  command TEXT,
                  args TEXT,
                  timestamp DATETIME)''')
    c.execute('''CREATE TABLE IF NOT EXISTS errors
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  command TEXT,
                  error TEXT,
                  timestamp DATETIME)''')
    conn.commit()
    conn.close()

def log_command(user_id: int, username: Optional[str], command: str, args: str = ""):
    """Сохраняет информацию о выполненной команде."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT INTO commands (user_id, username, command, args, timestamp) VALUES (?,?,?,?,?)",
              (user_id, username or "unknown", command, args, datetime.now()))
    conn.commit()
    conn.close()

def log_error(user_id: int, command: str, error: str):
    """Сохраняет информацию об ошибке."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT INTO errors (user_id, command, error, timestamp) VALUES (?,?,?,?)",
              (user_id, command, str(error)[:200], datetime.now()))
    conn.commit()
    conn.close()

def get_stats() -> dict:
    """Возвращает словарь со статистикой."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    stats = {}
    
    # Общее количество команд
    c.execute("SELECT COUNT(*) FROM commands")
    stats['total_commands'] = c.fetchone()[0]
    
    # Количество ошибок
    c.execute("SELECT COUNT(*) FROM errors")
    stats['total_errors'] = c.fetchone()[0]
    
    # Уникальные пользователи
    c.execute("SELECT COUNT(DISTINCT user_id) FROM commands")
    stats['unique_users'] = c.fetchone()[0]
    
    # Команды за сегодня
    today = datetime.now().date()
    c.execute("SELECT COUNT(*) FROM commands WHERE date(timestamp) = ?", (today,))
    stats['today_commands'] = c.fetchone()[0]
    
    # Топ-5 команд
    c.execute("SELECT command, COUNT(*) FROM commands GROUP BY command ORDER BY 2 DESC LIMIT 5")
    stats['top_commands'] = c.fetchall()
    
    # Активность по часам (последние 24 часа)
    day_ago = datetime.now() - timedelta(days=1)
    c.execute("SELECT strftime('%H', timestamp) as hour, COUNT(*) FROM commands WHERE timestamp > ? GROUP BY hour ORDER BY hour",
              (day_ago,))
    stats['hourly'] = c.fetchall()
    
    conn.close()
    return stats