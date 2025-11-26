import mysql.connector
import os
import sys
import argparse
from pathlib import Path
from mysql.connector import Error
from datetime import datetime, timedelta

# Import các module tiện ích
from load_config import load_config
from log_manager import log_process_action, log_conf_action, get_process_log_value
from param_sync import get_parameter_value
from send_mail import send_email

# --- CẤU HÌNH PROCESS ---
PROCESS_NAME = "load_to_staging"
PREV_PROCESS = "crawling" # Tên process trước đó để check log
PROCESS_ID = 2 
SEND_TO_EMAIL = get_parameter_value('SEND_TO_EMAIL')

# Load Config DB
config = load_config()
STAGING_CONFIG = config["DB_CONFIGS"]['STAGING']
# Bắt buộc bật local_infile cho client python
STAGING_CONFIG['allow_local_infile'] = True

def execute_load_data(csv_file_path):
    """
    Hàm logic chính: Thực thi kết nối DB và load file
    """
    csv_path_obj = Path(csv_file_path).resolve()
    
    if not csv_path_obj.exists():
        raise FileNotFoundError(f"Không tìm thấy file CSV: {csv_path_obj}")

    conn = None
    cursor = None
    
    try:
        conn = mysql.connector.connect(**STAGING_CONFIG)
        cursor = conn.cursor()
        
        # 1. TRUNCATE bảng staging cũ
        print("   -> Cleaning old data (TRUNCATE stg_products)...")
        cursor.execute("TRUNCATE TABLE stg_products;")
        
        # 2. LOAD DATA INFILE
        # Lưu ý: Đường dẫn file phải là kiểu Unix (/) ngay cả trên Windows
        sql_path = str(csv_path_obj).replace('\\', '/')
        
        print(f"   -> Loading file: {sql_path}")
        
        load_query = f"""
        LOAD DATA LOCAL INFILE '{sql_path}'
        INTO TABLE stg_products
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS
        (name, province, date, price) 
        SET load_date = NOW();
        """
        
        # Cần set global local_infile = 1 (nếu server chưa bật)
        cursor.execute("SET GLOBAL local_infile = 1;")
        cursor.execute(load_query)
        
        records_loaded = cursor.rowcount
        conn.commit()
        
        print(f"   -> Success! Loaded {records_loaded} rows.")
        return records_loaded

    except Error as e:
        print(f"   -> MySQL Error: {e}")
        raise e
    finally:
        if cursor: cursor.close()
        if conn and conn.is_connected(): conn.close()

def run_load_staging(target_date_str=None, force_run=False):
    """
    Hàm điều phối: Kiểm tra log Crawl -> Tính tên file -> Gọi hàm Load
    """
    start_time = datetime.now()
    
    # 1. XÁC ĐỊNH NGÀY MỐC
    file_target_date = None
    if target_date_str:
        allowed_formats = ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']
        for fmt in allowed_formats:
            try:
                file_target_date = datetime.strptime(target_date_str, fmt)
                break 
            except ValueError:
                continue 
        
        if file_target_date is None:
            print(f"❌ Lỗi định dạng ngày '{target_date_str}'! Vui lòng nhập dd/mm/yyyy hoặc YYYY-MM-DD")
            return None
    else:
        file_target_date = datetime.now()

    print(f"--- BẮT ĐẦU LOAD TO STAGING (Target Date: {file_target_date.strftime('%d/%m/%Y')}, Force={force_run}) ---")

    # 2. KIỂM TRA DEPENDENCY (LOG CHECK)
    if not force_run:
        check_log_date = datetime.now().strftime('%Y-%m-%d')
        print(f"🔍 Kiểm tra log bước '{PREV_PROCESS}' ngày chạy {check_log_date}...")
        
        prev_status = get_process_log_value(PREV_PROCESS, check_log_date)
        
        # Backup case: tìm log ngày target
        if prev_status == "Null" or prev_status is None:
            backup_log_date = file_target_date.strftime('%Y-%m-%d')
            if backup_log_date != check_log_date:
                print(f"⚠️ Không thấy log hôm nay. Thử tìm log ngày target: {backup_log_date}...")
                prev_status = get_process_log_value(PREV_PROCESS, backup_log_date)

        # Xử lý trạng thái
        if prev_status == "CND":
            msg = f"⚠️ Bước {PREV_PROCESS} báo 'No Data'. Bỏ qua bước Load."
            print(msg)
            # Log trạng thái Skip
            log_process_action(PROCESS_ID, PROCESS_NAME, start_time, datetime.now(), "LS_SKIP", 0, 0, 0, "Skipped: No Data form Crawler")
            return None

        if prev_status != "CS" and prev_status != "LS": # CS: Completed Success (Crawler)
            msg = f"❌ Không thể chạy Load vì {PREV_PROCESS} chưa thành công (Status: {prev_status}). Dùng --force để bỏ qua."
            print(msg)
            return None
        
        print(f"✅ Bước {PREV_PROCESS} OK (Status: {prev_status}).")
    else:
        print(f"⚠️ FORCE MODE: Bỏ qua kiểm tra log của {PREV_PROCESS}.")

    # 3. TÍNH TOÁN TÊN FILE (Logic: Crawler lưu tên file theo khoảng thời gian)
    # Giả định crawler chạy cho khoảng 7 ngày kết thúc vào target_date
    start_date = file_target_date - timedelta(days=7)
    s_str = start_date.strftime('%d-%m-%Y')
    e_str = file_target_date.strftime('%d-%m-%Y')
    
    # Lấy đường dẫn staging từ DB hoặc mặc định
    staging_dir = get_parameter_value('STAGING_DIR') or "./staging"
    file_name = f"nong_san_{s_str}_{e_str}.csv"
    csv_path = os.path.join(staging_dir, file_name)

    print(f"📂 Tìm file mục tiêu: {csv_path}")

    try:
        if not os.path.exists(csv_path):
            msg = f"Không tìm thấy file {file_name} (Dù log trước đó báo OK hoặc Force Run)"
            print(f"❌ {msg}")
            # Log Fail
            log_process_action(PROCESS_ID, PROCESS_NAME, start_time, datetime.now(), "LF", 0, 0, 0, msg)
            return None

        # 4. Log START
        log_process_action(
            process_config_id=PROCESS_ID,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=None,
            status="LR", # Running
            message=f"Loading {file_name}"
        )

        # 5. THỰC THI LOAD
        records_loaded = execute_load_data(csv_path)

        # 6. Log SUCCESS
        end_time = datetime.now()
        log_process_action(
            process_config_id=PROCESS_ID,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="LS", # Load Success
            records_loaded=records_loaded,
            message=f"Loaded: {file_name}"
        )
        
        if SEND_TO_EMAIL:
             send_email(f"[ETL] LOAD SUCCESS", f"Loaded {records_loaded} rows from {file_name}", [SEND_TO_EMAIL])

        return records_loaded

    except Exception as e:
        end_time = datetime.now()
        error_msg = f"Load Error: {str(e)}"
        print(f"❌ {error_msg}")
        
        log_process_action(
            process_config_id=PROCESS_ID,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="LF", # Load Failed
            message=error_msg
        )
        if SEND_TO_EMAIL:
            send_email(f"[ETL] LOAD FAILED", f"Error: {e}", [SEND_TO_EMAIL])
        
        # Ném lỗi ra ngoài để Pipeline biết là thất bại
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run load staging process manually.")
    parser.add_argument("--date", type=str, default=None, help="Format dd/mm/yyyy or YYYY-MM-DD")
    
    # --- [QUAN TRỌNG] THÊM DÒNG NÀY ĐỂ NHẬN FORCE ---
    parser.add_argument("--force", action="store_true", help="Force run ignoring previous logs")
    
    args = parser.parse_args()

    run_load_staging(target_date_str=args.date, force_run=args.force)
    