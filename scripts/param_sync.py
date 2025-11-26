import mysql.connector
from datetime import datetime
from config import DB_CONFIGS, LOG_TABLES
from log_manager import log_conf_action
from load_config import load_config

config = load_config();



CONTROL_CONFIG = config["DB_CONFIGS"]['CONTROL']
CONF_LOG_TABLE = config['LOG_TABLES']['CONF']
PROCESS_LOG_TABLE = config['LOG_TABLES']['PROCESS']


def get_parameter_value(config_key):
   
    conn = None
    value = None
    
    try:
        conn = mysql.connector.connect(**CONTROL_CONFIG)
        cursor = conn.cursor()
        
        query = f"SELECT config_value FROM config WHERE config_key = %s"
        cursor.execute(query, (config_key,)) 
        result = cursor.fetchone()
        
        if result:
            value = str(result[0])
            log_conf_action("READ", config_key, None, value, "Đọc tham số thành công.")
            return value
        else:
            print(f"Tham số '{config_key}' không tồn tại trong Control DB.")
            log_conf_action("READ_NOT_FOUND", config_key, None, None, "Tham số không tồn tại.")    
    
    except Exception as e:
        print(f"Lỗi khi đọc tham số '{config_key}': {e}")
        log_conf_action("READ_FAIL", config_key, None, None, str(e))

        return None
    finally:
        if conn and conn.is_connected():
            conn.close()


def update_parameter(param_key, new_value):
    conn = None
    
    old_value = get_parameter_value(param_key)
    
    try:
        conn = mysql.connector.connect(**CONTROL_CONFIG)
        cursor = conn.cursor()
        
        update_query = f"""
            UPDATE control_db_parameters 
            SET param_value = %s 
            WHERE param_key = %s
        """
        cursor.execute(update_query, (new_value, param_key))
        conn.commit()
        
        log_conf_action("UPDATE", param_key, old_value, new_value, "Cập nhật tham số thành công.")
        return True
    except Exception as e:
        conn.rollback()
        log_conf_action("UPDATE_FAIL", param_key, old_value, new_value, str(e))
        return False
    finally:
        if conn and conn.is_connected():
            conn.close()

print( get_parameter_value('INSERT_AGGRE_DATA_PROCEDURE'))