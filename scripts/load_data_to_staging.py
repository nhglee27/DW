import mysql.connector
from mysql.connector import Error
from pathlib import Path

db_config = {
    "host": "mysql",
    "port": 3306,
    "user": "root",
    "password": "rootpass",
    "database": "agri_staging_db",
    "allow_local_infile": True
}

csv_path = Path("./staging/nong_san_01-10-2025_29-10-2025.csv").resolve()
if not csv_path.exists():
    raise FileNotFoundError(f"Không tìm thấy file CSV: {csv_path}")

load_sql = f"""
LOAD DATA LOCAL INFILE '{csv_path}'
INTO TABLE stg_nong_san
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\\n'
IGNORE 1 ROWS
(ten_mat_hang, thi_truong, ngay_raw, gia_raw);
"""

try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    print("⚡ Thực thi LOAD DATA LOCAL INFILE...")
    cursor.execute(load_sql)
    conn.commit()
    
    print("LOAD DATA hoàn tất. Kiểm tra bảng để biết số dòng thực tế.")

except Error as e:
    print("❌ Lỗi:", e)
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
