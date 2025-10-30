
import mysql.connector
from datetime import datetime
from config import DB_CONFIGS, LOG_TABLES

CONTROL_CONFIG = DB_CONFIGS['CONTROL']
CONF_LOG_TABLE = LOG_TABLES['CONF']

def log_conf_action(action, param_key, old_value=None, new_value=None, message=None):
    conn = None
    try:
        conn = mysql.connector.connect(**CONTROL_CONFIG)
        cursor = conn.cursor()

        log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        query = (
            f"INSERT INTO {CONF_LOG_TABLE} "
            "(log_time, action, param_key, old_value, new_value, message) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )

        values = (
            log_time,
            action,
            param_key,
            str(old_value) if old_value is not None else None,
            str(new_value) if new_value is not None else None,
            message[:255] if message else None
        )

        cursor.execute(query, values)
        conn.commit()

        print(f"[CONF LOG] {action} | Key: {param_key}")
    except Exception as e:
        print(f"CẢNH BÁO: Không thể ghi CONF LOG. Lỗi: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()