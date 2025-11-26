import mysql.connector
from config import DB_CONFIGS, LOG_TABLES
from log_manager import get_process_log_value, log_process_action
import os
from mysql.connector import Error
from param_sync import get_parameter_value
import argparse
from send_mail import send_email
from load_config import load_config
from datetime import datetime

# ================== [6.5.X – SETUP CHUNG] ==================
PROCESS_NAME = "load_to_dm"
PREV_PROCESS = "insert_aggre_data"

# [6.5.1] Đọc config.yaml (thông qua load_config)
#  - Lấy cấu hình DB: CONTROL, DW, STAGING, M1D
#  - Lấy tên bảng log từ LOG_TABLES
config = load_config()

CONTROL_CONFIG = config["DB_CONFIGS"]['CONTROL']
MART1_CONFIG = config["DB_CONFIGS"]['M1D']
DW_CONFIG = config["DB_CONFIGS"]['DW']
STAGING_CONFIG = config["DB_CONFIGS"]['STAGING']
CONF_LOG_TABLE = config['LOG_TABLES']['CONF']
PROCESS_LOG_TABLE = config['LOG_TABLES']['PROCESS']

# [6.5.1] Đọc các parameter cần dùng trong bước 6.5.4 & 6.6.6
procedure_name = get_parameter_value('EXPORT_DATA_FORM_DW')
SEND_TO_EMAIL = get_parameter_value('SEND_TO_EMAIL')
load_to_mart1_temp_folder = get_parameter_value('LOAD_TO_D1M_TEMP')


def export_data(load_date, clean=1):
    """
    Export data from DW using stored procedure.
    Returns: (success: bool, records_inserted: int, error_message: str)
    """
    conn = None
    cursor = None
    
    try:
        # [6.5.3] Khởi tạo & Dọn dẹp file CSV cũ (nếu không có --no-clean)
        #  - clean == 1: xóa các file CSV cũ của ngày load_date trong thư mục temp
        if clean == 1 and os.path.exists(load_to_mart1_temp_folder):
            removed_files = 0
            for f in os.listdir(load_to_mart1_temp_folder):
                if load_date in f and f.lower().endswith(".csv"):
                    file_path = os.path.join(load_to_mart1_temp_folder, f)
                    try:
                        os.remove(file_path)
                        removed_files += 1
                    except Exception as e:
                        print(f"Could not delete {file_path}: {e}")
            print(f"Clean mode enabled - deleted {removed_files} CSV file(s).")

        # [6.5.4] EXTRACT DỮ LIỆU
        #  - Kết nối DW
        #  - Gọi SP EXPORT_DATA_FORM_DW để tạo 3 file CSV + ghi vào agg_product_price_weekly
        #  - Đếm số bản ghi theo load_date
        conn = mysql.connector.connect(**DW_CONFIG)
        cursor = conn.cursor()

        print(f"Running stored procedure {procedure_name} with load_date={load_date}, clean={clean}")
        cursor.callproc(procedure_name, [load_date])

        cursor.execute("""
            SELECT COUNT(*) 
            FROM agg_product_price_weekly 
            WHERE DATE(load_date) = %s
        """, (load_date,))
        records_insert = cursor.fetchone()[0]

        conn.commit()
        
        print(f"Export completed: {records_insert} records")
        return True, records_insert, None

    except mysql.connector.Error as e:
        # [6.5.4] Nhánh EXPORT THẤT BẠI (MySQL Error)
        error_msg = f"MySQL Error during export: {e}"
        print(error_msg)
        return False, 0, error_msg

    except Exception as e:
        # [6.5.4] Nhánh EXPORT THẤT BẠI (lỗi khác)
        error_msg = f"Other Error during export: {e}"
        print(error_msg)
        return False, 0, error_msg

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("Closed DW MySQL connection.")


def load_to_mart(load_date):
    """
    Load data to MART1 from CSV files.
    Returns: (success: bool, total_records: int, error_message: str)
    """
    conn = None
    cursor = None
    total_records = 0

    # [6.6.6] Danh sách 3 job tương ứng 3 file CSV cần LOAD
    LOAD_JOBS = [
        {
            "csv": f"export_product_{load_date}.csv",
            "table": "rpt_product_price_summary",
        },
        {
            "csv": f"export_province_{load_date}.csv",
            "table": "rpt_province_price_summary",
        },
        {
            "csv": f"export_weekly_{load_date}.csv",
            "table": "rpt_weekly_price_trend",
        }
    ]

    try:
        # [6.5.5] KẾT NỐI MART1D (allow_local_infile = True)
        mart_config = MART1_CONFIG.copy()
        mart_config['allow_local_infile'] = True
        conn = mysql.connector.connect(**mart_config)
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL local_infile = 1")

        print(f"[MART LOAD] Starting load for date {load_date}")

        # [6.6.6] LOAD 3 FILE CSV
        #  - Với từng job:
        #    + Kiểm tra file CSV tồn tại
        #    + TRUNCATE TABLE
        #    + LOAD DATA LOCAL INFILE
        #    + Đếm số bản ghi theo load_date
        for job in LOAD_JOBS:
            csv_path = os.path.join(load_to_mart1_temp_folder, job["csv"])
            table = job["table"]

            print(f"--> Loading file: {csv_path} → {table}")

            if not os.path.exists(csv_path):
                # Nhánh lỗi: CSV không tồn tại → LOAD THẤT BẠI
                error_msg = f"CSV file not found: {csv_path}"
                print(error_msg)
                return False, 0, error_msg

            try:
                cursor.execute(f"TRUNCATE TABLE {table}")
                print(f"    Table {table} truncated OK.")
            except Exception as te:
                # Nhánh lỗi: TRUNCATE thất bại → LOAD THẤT BẠI
                print(f"    Could not truncate {table}: {te}")
                return False, 0, f"Truncate failed for {table}: {te}"
            
            csv_path_normalized = csv_path.replace("\\", "/")
            
            load_sql = f"""
                LOAD DATA LOCAL INFILE '{csv_path_normalized}'
                INTO TABLE {table}
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
            """

            cursor.execute(load_sql)
            conn.commit()

            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE load_date = %s", (load_date,))
            records_loaded = cursor.fetchone()[0]
            total_records += records_loaded

            print(f"    Loaded OK ({records_loaded} rows)")

        print(f"[MART LOAD] All tables loaded successfully! Total: {total_records} rows")
        return True, total_records, None

    except Exception as e:
        # [6.6.6] Nhánh LOAD THẤT BẠI
        #  - Rollback
        #  - Trả lỗi cho run_full_process xử lý Log IF + gửi mail
        error_msg = f"Load failed: {e}"
        print(error_msg)
        if conn:
            conn.rollback()
        return False, 0, error_msg

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("[MART LOAD] MySQL connection closed.")


def run_full_process(load_date=None, clean=1, force_run=False):
    """
    Main process: Export from DW and Load to MART1.
    Only logs once at the end with combined results.
    """
    # [6.5.3] Khởi tạo: set start_time cho toàn bộ quy trình
    start_time = datetime.now()
    end_time = None

    if load_date is None:
        load_date = datetime.now().strftime('%Y-%m-%d')

    try:
        # [6.5.2] KIỂM TRA ĐIỀU KIỆN CHẠY (phụ thuộc PREV_PROCESS + trạng thái hiện tại)
        if not force_run:
            # Kiểm tra bước trước (insert_aggre_data) đã IS chưa
            load_status = get_process_log_value(PREV_PROCESS, load_date)
            if load_status != "IS":
                # HỦY QUY TRÌNH – In lý do cụ thể & thoát
                print(f"Process '{PREV_PROCESS}' not finished (state={load_status}). Abort.")
                print(f"Use --force to run anyway.")
                return
        else:
            # --force: bỏ qua kiểm tra PREV_PROCESS
            print("Force mode: skipping dependency check")

        # Kiểm tra trạng thái của chính load_to_dm (IF/IR/IS) nếu không có --force
        current_status = get_process_log_value(PROCESS_NAME, load_date)
        if current_status in ("IF", "IR", "IS") and not force_run:
            # HỦY QUY TRÌNH – đã chạy trước đó
            print(f"Process '{PROCESS_NAME}' already done (status={current_status}).")
            print(f"Use --force to run anyway.")
            return

        if force_run:
            print(f"Force run enabled. Current status: {current_status}")
        else:
            print(f"Running process '{PROCESS_NAME}' (current={current_status})...")

        # ================== [6.5.4] EXTRACT DỮ LIỆU ==================
        print("\n=== STEP 1: EXPORT DATA ===")
        export_success, records_exported, export_error = export_data(load_date, clean)
        
        if not export_success:
            # Nhánh EXPORT THẤT BẠI
            #  - Ghi log trạng thái IF
            #  - Gửi mail báo lỗi
            end_time = datetime.now()
            
            log_process_action(
                process_config_id=6,
                process_name=PROCESS_NAME,
                start_time=start_time,
                end_time=end_time,
                status="IF",
                records_extract=0,
                records_loaded=0,
                records_transform=None,
                message=f"Export failed: {export_error}"
            )

            subject = f"[ETL] Process FAILED (Export Error) - {load_date}"
            body = f"""
            Process: {PROCESS_NAME}
            Date: {load_date}
            Status: IF (Export Failed)
            Start Time: {start_time}
            End Time: {end_time}
            Error: {export_error}
            """
            send_email(subject, body, [SEND_TO_EMAIL])
            print(f"Process failed at export stage.")
            return

        # ================== [6.6.6] LOAD 3 FILE CSV ==================
        print("\n=== STEP 2: LOAD TO MART ===")
        load_success, records_loaded, load_error = load_to_mart(load_date)
        
        end_time = datetime.now()

        if not load_success:
            # Nhánh LOAD THẤT BẠI
            #  - Log IF
            #  - Gửi mail báo lỗi
            log_process_action(
                process_config_id=6,
                process_name=PROCESS_NAME,
                start_time=start_time,
                end_time=end_time,
                status="IF",
                records_extract=records_exported,
                records_loaded=0,
                records_transform=None,
                message=f"Export OK but Load failed: {load_error}"
            )

            subject = f"[ETL] Process FAILED (Load Error) - {load_date}"
            body = f"""
            Process: {PROCESS_NAME}
            Date: {load_date}
            Status: IF (Load Failed)
            Start Time: {start_time}
            End Time: {end_time}

            Export: SUCCESS ({records_exported} records)
            Load: FAILED
            Error: {load_error}
            """
            send_email(subject, body, [SEND_TO_EMAIL])
            print(f"Process failed at load stage.")
            return

        # ================== [6.6.7] THÀNH CÔNG – GHI LOG IS ==================
        #  - Log IS vào PROCESS_LOG
        #  - Gửi mail SUCCESS
        #  - In kết quả ra console
        log_process_action(
            process_config_id=6,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="IS",
            records_extract=records_exported,
            records_loaded=records_loaded,
            records_transform=None,
            message="Export and Load completed successfully."
        )

        subject = f"[ETL] Process Success - {load_date}"
        body = f"""
        Process: {PROCESS_NAME}
        Date: {load_date}
        Status: IS (Success)
        Start Time: {start_time}
        End Time: {end_time}

        Export: {records_exported} records
        Load: {records_loaded} records

        All steps completed successfully.
        """
        send_email(subject, body, [SEND_TO_EMAIL])
        print("\n=== PROCESS COMPLETED SUCCESSFULLY ===")

    except Exception as e:
        # [6.x] Nhánh lỗi bất ngờ (Unexpected Error)
        #  - Log IF
        #  - Gửi mail thông báo
        end_time = datetime.now()
        
        log_process_action(
            process_config_id=6,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="IF",
            records_extract=None,
            records_loaded=None,
            records_transform=None,
            message=f"Unexpected Error: {e}"
        )

        subject = f"[ETL] Process FAILED (Unexpected Error) - {load_date}"
        body = f"""
        Process: {PROCESS_NAME}
        Date: {load_date}
        Status: IF (Unexpected Error)
        Start Time: {start_time}
        End Time: {end_time}
        Error: {e}
                """
        send_email(subject, body, [SEND_TO_EMAIL])
        print(f"Unexpected Error: {e}")


if __name__ == "__main__":
    # [6.5.0] Người dùng / Scheduler gọi lệnh:
    #   python load_to_dm.py --date YYYY-MM-DD [--force] [--no-clean]
    # [6.5.1] Phân tích tham số CLI → map vào biến load_date, force_run, clean_flag
    parser = argparse.ArgumentParser(description="Run ETL process: Export from DW and Load to MART1")
    parser.add_argument("--date", type=str, default=None, help="Load date (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="Force run even if already completed")
    parser.add_argument("--no-clean", action="store_true", help="Skip cleanup (clean=0)")

    args = parser.parse_args()
    clean_flag = 0 if args.no_clean else 1

    run_full_process(load_date=args.date, clean=clean_flag, force_run=args.force)
