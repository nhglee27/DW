import mysql.connector
from config import DB_CONFIGS

def test_db_connection(db_name='CONTROL'):
    if db_name not in DB_CONFIGS:
        print(f"Tên DB '{db_name}' không hợp lệ. Chọn một trong: {list(DB_CONFIGS.keys())}")
        return False

    config = DB_CONFIGS[db_name]
    conn = None

    try:
        print(f"Đang kiểm tra kết nối tới DB [{db_name}]...")
        conn = mysql.connector.connect(**config)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SELECT DATABASE();")
            db_name_actual = cursor.fetchone()[0]
            print(f" Kết nối thành công tới database: {db_name_actual}")
            return True
        else:
            print(f" Không thể kết nối tới {db_name}.")
            return False
    except Exception as e:
        print(f"Lỗi khi kết nối tới {db_name}: {e}")
        return False
    finally:
        if conn and conn.is_connected():
            conn.close()
            print(f"Đã đóng kết nối tới {db_name}.")

if __name__ == "__main__":
    test_db_connection('CONTROL')
    test_db_connection('STAGING')
   # test_db_connection('DW')
