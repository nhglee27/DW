# config.py

# Cấu hình Kết nối Database
DB_CONFIGS = {
    'CONTROL': {
        'host': 'mysql',
        'port': '3306',
        'user': 'root',
        'password': 'rootpass',
        'database': 'control_db'
    },
    'STAGING': {
        'host': 'mysql',
        'port': '3306',
        'user': 'root',
        'password': 'rootpass',
        'database': 'agri_staging_db'
    },
    'DW': {
        'host': 'mysql',
        'port': '3306',
        'user': 'root',
        'password': 'rootpass',
        'database': 'data_warehouse'
    }
}
LOG_TABLES = {
    'CONF': 'config_log',    
    'PROCESS': 'process_log'
}