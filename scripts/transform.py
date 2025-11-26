import mysql.connector
from config import DB_CONFIGS, LOG_TABLES
from log_manager import log_conf_action, get_process_log_value, log_process_action
import os
from mysql.connector import Error
from param_sync import get_parameter_value
import argparse
from send_mail import send_email
from load_config import load_config
from datetime import datetime
from logger_manager import get_group_logger

PROCESS_NAME = "transform"
PREV_PROCESS = "load_to_staging"

# 1. load config
config = load_config()

CONTROL_CONFIG = config["DB_CONFIGS"]['CONTROL']
STAGING_CONFIG = config["DB_CONFIGS"]['STAGING']
CONF_LOG_TABLE = config['LOG_TABLES']['CONF']
PROCESS_LOG_TABLE = config['LOG_TABLES']['PROCESS']

# 2. load config_param
procedure_name = get_parameter_value('TRANSFORM_PROCEDURE')
SEND_TO_EMAIL = get_parameter_value('SEND_TO_EMAIL')

# 3. setup logger
etl_log = get_group_logger("TRANSFORM")

def transform_with_proc(load_date=None, force_run=False):

    start_time = datetime.now()
    end_time = start_time 

    # --- 1. XỬ LÝ NGÀY DỮ LIỆU (DATA DATE) ---
    # Đây là ngày sẽ gửi vào Procedure để lọc dữ liệu
    target_data_date = None
    
    if load_date:
        # Chuẩn hóa input user nhập
        allowed_formats = ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']
        for fmt in allowed_formats:
            try:
                dt_obj = datetime.strptime(load_date, fmt)
                target_data_date = dt_obj.strftime('%Y-%m-%d')
                break
            except ValueError:
                continue
        
        if target_data_date is None:
            print(f"❌ Lỗi định dạng ngày: {load_date}")
            return
    else:
        # Mặc định lấy ngày hiện tại làm ngày dữ liệu
        target_data_date = datetime.now().strftime('%Y-%m-%d')

    print(f"🚀 Bắt đầu Transform. Data Date: {target_data_date} | Force: {force_run}")
    etl_log.info(f"Start Transform. Data Date: {target_data_date} | Force: {force_run}")

    conn = None
    cursor = None

    try:
        # --- 2. XỬ LÝ NGÀY CHECK LOG (LOG DATE) ---
        # LUÔN DÙNG NGÀY HIỆN TẠI ĐỂ CHECK TIẾN ĐỘ
        current_execution_date = datetime.now().strftime('%Y-%m-%d')
        
        # A. Check bước trước (Load)
        prev_status = get_process_log_value(PREV_PROCESS, current_execution_date)
        etl_log.info(f"Check log '{PREV_PROCESS}' ngày {current_execution_date}: {prev_status}")

        if prev_status != "LS" and not force_run:
            msg = f"❌ Bước trước '{PREV_PROCESS}' chưa hoàn thành hôm nay (Status: {prev_status}). Dùng --force để bỏ qua."
            print(msg)
            return
        elif prev_status != "LS" and force_run:
            print(f"⚠️ Force mode: Bỏ qua check bước trước (Status: {prev_status})")

        # B. Check bước hiện tại (Transform)
        curr_status = get_process_log_value(PROCESS_NAME, current_execution_date)
        
        if curr_status == "TS" and not force_run:
            msg = f"⚠️ Transform đã chạy thành công hôm nay ({current_execution_date}). Skip."
            print(msg)
            return
        
        # --- 3. THỰC THI PROCEDURE ---
        # Kết nối DB
        conn = mysql.connector.connect(**STAGING_CONFIG)
        cursor = conn.cursor()

        # Gọi Procedure với NGÀY DỮ LIỆU (target_data_date)
        print(f"⚡ Đang gọi Procedure: {procedure_name}('{target_data_date}')...")
        etl_log.info(f"Calling {procedure_name} with {target_data_date}")
        
        cursor.callproc(procedure_name, [target_data_date])

        records_transform = 0

        # Lấy kết quả trả về
        for result in cursor.stored_results():
            for row in result.fetchall():
                print(f"   -> Result: {row}")
                # Logic lấy số dòng (tùy chỉnh theo output thực tế của SP)
                if len(row) >= 1:
                     try:
                        records_transform = int(row[-1])
                     except:
                        pass

        conn.commit()
        end_time = datetime.now()
        
        print(f"✅ Transform thành công! Records biến đổi: {records_transform}")

        # --- 4. GHI LOG VÀ GỬI MAIL ---
        log_process_action(
            process_config_id=3,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="TS",
            records_extract=None,
            records_loaded=None,
            records_transform=records_transform,
            message="Transform completed successfully."
        )

        if SEND_TO_EMAIL:
            subject = f"[ETL] Transform Success - {target_data_date}"
            body = f"""
            Process: {PROCESS_NAME}
            Data Date: {target_data_date}
            Execution Date: {current_execution_date}
            Status: TS (Success)
            Records: {records_transform}
            """
            # Đã sửa lỗi thừa tham số etl_log
            send_email(subject, body, [SEND_TO_EMAIL]) 

    except mysql.connector.Error as e:
        end_time = datetime.now()
        print(f"❌ MySQL Error: {e}")
        etl_log.error(f"MySQL Error: {e}")

        log_process_action(
            process_config_id=3,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="TF",
            message=f"MySQL Error: {e}"
        )

        if SEND_TO_EMAIL:
            send_email(f"[ETL] Transform FAILED", f"Error: {e}", [SEND_TO_EMAIL])

    except Exception as e:
        end_time = datetime.now()
        print(f"❌ Other Error: {e}")
        etl_log.error(f"Other Error: {e}")

        log_process_action(
            process_config_id=3,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="TF",
            message=f"Other Error: {e}"
        )
        
        if SEND_TO_EMAIL:
            send_email(f"[ETL] Transform FAILED", f"Error: {e}", [SEND_TO_EMAIL])

    finally:
        if cursor: cursor.close()
        if conn and conn.is_connected():
            conn.close()
            etl_log.info("Closed MySQL connection.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run transform process manually.")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD or dd/mm/yyyy")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    transform_with_proc(load_date=args.date, force_run=args.force)