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

PROCESS_NAME = "load_to_dw"
PREV_PROCESS = "transform"

# Load config
config = load_config()

CONTROL_CONFIG = config["DB_CONFIGS"]['CONTROL']
MART1_CONFIG = config["DB_CONFIGS"]['M1D']
DW_CONFIG = config["DB_CONFIGS"]['DW']
STAGING_CONFIG = config["DB_CONFIGS"]['STAGING']
CONF_LOG_TABLE = config['LOG_TABLES']['CONF']
PROCESS_LOG_TABLE = config['LOG_TABLES']['PROCESS']

procedure_name = get_parameter_value('EXPORT_DATA_FROM_STG_PROCEDURE')
SEND_TO_EMAIL = get_parameter_value('SEND_TO_EMAIL')
load_to_dw_temp_folder = get_parameter_value('LOAD_TO_DW_TEMP')


def export_data(load_date, clean=1):
    
    conn = None
    cursor = None
    
    try:
        if clean == 1 and os.path.exists(load_to_dw_temp_folder):
            removed_files = 0
            for f in os.listdir(load_to_dw_temp_folder):
                if load_date in f and f.lower().endswith(".csv"):
                    file_path = os.path.join(load_to_dw_temp_folder, f)
                    try:
                        os.remove(file_path)
                        removed_files += 1
                    except Exception as e:
                        print(f"Could not delete {file_path}: {e}")
            print(f"Clean mode enabled - deleted {removed_files} CSV file(s).")

        conn = mysql.connector.connect(**STAGING_CONFIG)
        cursor = conn.cursor()

        print(f"Running stored procedure {procedure_name} with load_date={load_date}, clean={clean}")
        cursor.callproc(procedure_name, [load_date])

        cursor.execute("""
            SELECT COUNT(*) 
            FROM fact_product_price 
            WHERE DATE(load_date) = %s
        """, (load_date,))
        records_insert = cursor.fetchone()[0]

        conn.commit()
        
        print(f"Export completed: {records_insert} records")
        return True, records_insert, None

    except mysql.connector.Error as e:
        error_msg = f"MySQL Error during export: {e}"
        print(error_msg)
        return False, 0, error_msg

    except Exception as e:
        error_msg = f"Other Error during export: {e}"
        print(error_msg)
        return False, 0, error_msg

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("Closed STG MySQL connection.")


def load_to_dw(load_date):
 
    conn = None
    cursor = None
    total_records = 0

    LOAD_JOBS = [
        {
            "csv": f"export_product_{load_date}.csv",
            "table": "dim_product",
        },
        {
            "csv": f"export_province_{load_date}.csv",
            "table": "dim_province",
        },
        {
            "csv": f"export_fact_{load_date}.csv",
            "table": "fact_product_price",
        }
    ]

    try:
        dw_config = DW_CONFIG.copy()
        dw_config['allow_local_infile'] = True
        conn = mysql.connector.connect(**dw_config)
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL local_infile = 1")


        print(f"[DW LOAD] Starting load for date {load_date}")

        for job in LOAD_JOBS:
            csv_path = os.path.join(load_to_dw_temp_folder, job["csv"])
            table = job["table"]

            print(f"--> Loading file: {csv_path} → {table}")

            if not os.path.exists(csv_path):
                error_msg = f"CSV file not found: {csv_path}"
                print(error_msg)
                return False, 0, error_msg

            # ==============================
            # Incremental append logic here
            # ==============================
            if "fact" in table.lower():
                # FACT TABLE – delete đúng ngày load
                delete_sql = f"DELETE FROM {table} WHERE load_date = %s"
                cursor.execute(delete_sql, (load_date,))
                print(f"    Deleted existing records for {load_date} in {table}")
            else:
                # DIM TABLE – giữ nguyên dữ liệu
                print(f"    DIM table {table}: no delete")

            # Normalized path
            csv_path_normalized = csv_path.replace("\\", "/")

            # Load CSV
            load_sql = f"""
                LOAD DATA LOCAL INFILE '{csv_path_normalized}'
                INTO TABLE {table}
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
            """
            cursor.execute(load_sql)
            conn.commit()

            # Count rows loaded
            if "fact" in table.lower():
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE load_date = %s", (load_date,))
            else:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")

            records_loaded = cursor.fetchone()[0]
            total_records += records_loaded

            print(f"    Loaded OK ({records_loaded} rows)")


        print(f"[MART LOAD] All tables loaded successfully! Total: {total_records} rows")
        return True, total_records, None

    except Exception as e:
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
    start_time = datetime.now()
    end_time = None

    if load_date is None:
        load_date = datetime.now().strftime('%Y-%m-%d')

    try:
        if not force_run:
            load_status = get_process_log_value(PREV_PROCESS, load_date)
            if load_status != "TS":
                print(f"Process '{PREV_PROCESS}' not finished (state={load_status}). Abort.")
                print(f"Use --force to run anyway.")
                return
        else:
            print("Force mode: skipping dependency check")

        current_status = get_process_log_value(PROCESS_NAME, load_date)
        if current_status in ("TF", "TR", "TS") and not force_run:
            print(f"Process '{PROCESS_NAME}' already done (status={current_status}).")
            print(f"Use --force to run anyway.")
            return

        if force_run:
            print(f"Force run enabled. Current status: {current_status}")
        else:
            print(f"Running process '{PROCESS_NAME}' (current={current_status})...")

        print("\n=== STEP 1: EXPORT DATA ===")
        export_success, records_exported, export_error = export_data(load_date, clean)
        
        if not export_success:
            end_time = datetime.now()
            
            log_process_action(
                process_config_id=4,
                process_name=PROCESS_NAME,
                start_time=start_time,
                end_time=end_time,
                status="LF",
                records_extract=0,
                records_loaded=0,
                records_transform=None,
                message=f"Export failed: {export_error}"
            )

            subject = f"[ETL] Process FAILED (Export Error) - {load_date}"
            body = f"""
            Process: {PROCESS_NAME}
            Date: {load_date}
            Status: LF (Export Failed)
            Start Time: {start_time}
            End Time: {end_time}
            Error: {export_error}
            """
            send_email(subject, body, [SEND_TO_EMAIL])
            print(f"Process failed at export stage.")
            return

        print("\n=== STEP 2: LOAD TO MART ===") 
        load_success, records_loaded, load_error = load_to_dw(load_date)
        
        end_time = datetime.now()

        if not load_success:
            log_process_action(
                process_config_id=4,
                process_name=PROCESS_NAME,
                start_time=start_time,
                end_time=end_time,
                status="LF",
                records_extract=records_exported,
                records_loaded=0,
                records_transform=None,
                message=f"Export OK but Load failed: {load_error}"
            )

            subject = f"[ETL] Process FAILED (Load Error) - {load_date}"
            body = f"""
            Process: {PROCESS_NAME}
            Date: {load_date}
            Status: LF (Load Failed)
            Start Time: {start_time}
            End Time: {end_time}

            Export: SUCCESS ({records_exported} records)
            Load: FAILED
            Error: {load_error}
            """
            send_email(subject, body, [SEND_TO_EMAIL])
            print(f"Process failed at load stage.")
            return

        log_process_action(
            process_config_id=4,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="LS",
            records_extract=records_exported,
            records_loaded=records_loaded,
            records_transform=None,
            message="Export and Load completed successfully."
        )

        subject = f"[ETL] Process Success - {load_date}"
        body = f"""
        Process: {PROCESS_NAME}
        Date: {load_date}
        Status: LS (Success)
        Start Time: {start_time}
        End Time: {end_time}

        Export: {records_exported} records
        Load: {records_loaded} records

        All steps completed successfully.
        """
        send_email(subject, body, [SEND_TO_EMAIL])
        print("\n=== PROCESS COMPLETED SUCCESSFULLY ===")

    except Exception as e:
        end_time = datetime.now()
        
        log_process_action(
            process_config_id=4,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="LF",
            records_extract=None,
            records_loaded=None,
            records_transform=None,
            message=f"Unexpected Error: {e}"
        )

        subject = f"[ETL] Process FAILED (Unexpected Error) - {load_date}"
        body = f"""
        Process: {PROCESS_NAME}
        Date: {load_date}
        Status: LF (Unexpected Error)
        Start Time: {start_time}
        End Time: {end_time}
        Error: {e}
                """
        send_email(subject, body, [SEND_TO_EMAIL])
        print(f"Unexpected Error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL process: Export from DW and Load to MART1")
    parser.add_argument("--date", type=str, default=None, help="Load date (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="Force run even if already completed")
    parser.add_argument("--no-clean", action="store_true", help="Skip cleanup (clean=0)")

    args = parser.parse_args()
    clean_flag = 0 if args.no_clean else 1

    run_full_process(load_date=args.date, clean=clean_flag, force_run=args.force)
    