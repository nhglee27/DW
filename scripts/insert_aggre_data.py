import mysql.connector
from config import DB_CONFIGS, LOG_TABLES
from log_manager import log_conf_action, get_process_log_value, log_process_action
import os
import sys # <--- [BẮT BUỘC PHẢI CÓ]
from mysql.connector import Error
from param_sync import get_parameter_value
import argparse
from send_mail import send_email
from load_config import load_config
from datetime import datetime 

PROCESS_NAME = "insert_aggre_data"
PREV_PROCESS = "load_to_dw"

# 1. Load config
config = load_config()

# CONTROL_CONFIG = config["DB_CONFIGS"]['CONTROL'] 
DW_CONFIG = config["DB_CONFIGS"]['DW']
# STAGING_CONFIG = config["DB_CONFIGS"]['STAGING']

# Lấy tên Procedure từ bảng Config
procedure_name = get_parameter_value('INSERT_AGGRE_DATA_PROCEDURE')
SEND_TO_EMAIL = get_parameter_value('SEND_TO_EMAIL')


def insert_with_proc(load_date=None, clean=1, force_run=False):
    
    start_time = datetime.now()
    end_time = None 
    
    if load_date is None:
        load_date = datetime.now().strftime('%Y-%m-%d')
   
    conn = None
    cursor = None

    try:
        # --- 1. CHECK DEPENDENCY (Kiểm tra bước trước) ---
        if not force_run:  
            load_status = get_process_log_value(PREV_PROCESS, load_date)
            # Nếu bước trước chưa báo Success (LS)
            if load_status != "LS":
                print(f"❌ Process '{PREV_PROCESS}' not in LS state yet (Status: {load_status}).")
                print(f"   Use --force to skip this check")
                
                # [SỬA LỖI QUAN TRỌNG] Dùng sys.exit(1) thay vì return
                sys.exit(1) 
        else:
            print("⚠️ Force mode: Skipping dependency check")

        # --- 2. CHECK CURRENT STATUS (Kiểm tra chính mình) ---
        current_status = get_process_log_value(PROCESS_NAME, load_date)
    
        if current_status in ("TO", "TR", "TS", "IS") and not force_run:
            print(f"✅ Process '{PROCESS_NAME}' already completed (status={current_status}). Skipping.")
            print(f"   Use --force to run anyway")
            
            # [SỬA] Dùng sys.exit(0) để báo Pipeline là "Xong rồi" (Success/Skip)
            sys.exit(0) 
    
        if force_run:
            print(f"⚠️ Force mode: Running regardless of status (current={current_status})")
        else:
            print(f"🚀 Process '{PROCESS_NAME}' will run (status={current_status})...")

        # --- 3. EXECUTE PROCEDURE ---
        conn = mysql.connector.connect(**DW_CONFIG)
        cursor = conn.cursor()

        print(f"🔄 Đang chạy stored procedure {procedure_name} với load_date={load_date}, clean={clean}...")
        
        # Gọi Procedure: Truyền tham số [ngày load, cờ dọn dẹp]
        cursor.callproc(procedure_name, [load_date, clean])

        # Kiểm tra nhanh kết quả (Đếm số dòng vừa tạo)
        # Lưu ý: Vì Procedure dùng INSERT ... SELECT, nên rowcount của callproc đôi khi không chuẩn
        # Ta query trực tiếp bảng đích để verify
        cursor.execute("""
            SELECT COUNT(*) 
            FROM agg_product_price_weekly 
            WHERE DATE(load_date) = %s
        """, (load_date,))
        
        result_row = cursor.fetchone()
        records_insert = result_row[0] if result_row else 0
        
        conn.commit()
        end_time = datetime.now()

        # --- 4. LOG SUCCESS ---
        log_process_action(
            process_config_id=5,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="IS",  # Insert Success
            records_extract=None,
            records_loaded=records_insert,
            records_transform=None,
            message="Insert completed successfully."
        )

        subject = f"[ETL] Insert Success - {load_date}"
        body = f"""
        Process: {PROCESS_NAME}
        Date: {load_date}
        Status: IS (Success)
        Records inserted: {records_insert}
        Start Time: {start_time}
        End Time: {end_time}
        """
        
        if SEND_TO_EMAIL:
            send_email(subject, body, [SEND_TO_EMAIL])
        print(f"✅ Insert completed successfully. Rows: {records_insert}")

    except mysql.connector.Error as e:
        # --- LOG ERROR (MySQL) ---
        end_time = datetime.now() 
        
        log_process_action(
            process_config_id=5,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="IF", # Insert Failed
            records_extract=None,
            records_loaded=None,
            records_transform=None,
            message=f"MySQL Error: {e}"
        )
        
        subject = f"[ETL] Insert FAILED (MySQL Error) - {load_date}"
        body = f"Error: {e}"
        if SEND_TO_EMAIL:
            send_email(subject, body, [SEND_TO_EMAIL])
        
        print(f"❌ MySQL Error: {e}")
        
        # [SỬA LỖI QUAN TRỌNG] Phải exit(1) để Pipeline biết là lỗi
        sys.exit(1) 

    except Exception as e:
        # --- LOG ERROR (System/Python) ---
        end_time = datetime.now()  
        
        log_process_action(
            process_config_id=5,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="IF",
            records_extract=None,
            records_loaded=None,
            records_transform=None,
            message=f"Other Error: {e}"
        )
        
        subject = f"[ETL] Insert FAILED (Other Error) - {load_date}"
        body = f"Error: {e}"
        if SEND_TO_EMAIL:
            send_email(subject, body, [SEND_TO_EMAIL])
            
        print(f"❌ Other error: {e}")
        
        # [SỬA LỖI QUAN TRỌNG] Phải exit(1) để Pipeline biết là lỗi
        sys.exit(1)

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("Closed MySQL connection.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run insert process manually.")
    parser.add_argument("--date", type=str, default=None, help="Load date (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="Force run even if already completed")
    
    # Logic: 
    # Mặc định (không nhập flag) -> args.no_clean = False -> clean_flag = 1 (Clean ON) -> Chuẩn
    # Nhập --no-clean -> args.no_clean = True -> clean_flag = 0 (Clean OFF) -> Chuẩn
    parser.add_argument("--no-clean", action="store_true", help="Skip cleanup (clean=0)")

    args = parser.parse_args()
    
    clean_flag = 0 if args.no_clean else 1
    
    insert_with_proc(load_date=args.date, clean=clean_flag, force_run=args.force)