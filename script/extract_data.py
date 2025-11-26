import os
import time
import glob
import pandas as pd
import argparse
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from mysql.connector import Error
from datetime import datetime, timedelta

# Import các module tiện ích
from config import DB_CONFIGS
from log_manager import log_process_action, log_conf_action
from param_sync import get_parameter_value
from send_mail import send_email
from load_config import load_config

# --- CẤU HÌNH PROCESS ---
PROCESS_NAME = "crawling"
PROCESS_ID = 1  # ID trong bảng process_config
SEND_TO_EMAIL = get_parameter_value('SEND_TO_EMAIL')
source_url = get_parameter_value('source_url')

def download_nong_san_html_to_csv(start_date: str, end_date: str, download_dir: str = "./staging"):
    """
    Hàm logic chính: Tải và xử lý dữ liệu từ thitruongnongsan.gov.vn
    Trả về: (csv_path, record_count)
    """
    # 1. Tạo thư mục lưu trữ
    os.makedirs(download_dir, exist_ok=True)

    # 2. Cấu hình Chrome (Bắt buộc cho Docker)
    chrome_options = webdriver.ChromeOptions()
    chrome_options.binary_location = "/usr/bin/chromium"  # Quan trọng cho Docker
    
    prefs = {
        "download.default_directory": os.path.abspath(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Các cờ bắt buộc khi chạy trong container
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 30) # Tăng thời gian chờ lên 30s cho mạng chậm

    try:
        print(f"🌐 Đang truy cập website... ({start_date} - {end_date})")
        # --- LOGIC CÀO DỮ LIỆU CỦA BẠN ---
        driver.get(source_url)

        # Nhập ngày
        date_from = wait.until(EC.presence_of_element_located((By.ID, "ctl00_maincontent_tu_ngay")))
        date_from.clear()
        date_from.send_keys(start_date)
        
        driver.find_element(By.ID, "ctl00_maincontent_den_ngay").clear()
        driver.find_element(By.ID, "ctl00_maincontent_den_ngay").send_keys(end_date)

        # Chọn ngành hàng và nhóm sản phẩm
        Select(driver.find_element(By.ID, "ctl00_maincontent_Ngành_hàng")).select_by_visible_text("Rau, quả")
        time.sleep(2) # Sleep nhẹ để dropdown load dữ liệu phụ thuộc
        Select(driver.find_element(By.ID, "ctl00_maincontent_Nhóm_sản_phẩm")).select_by_visible_text("Rau củ quả")

        # Nhấn nút "Xem"
        xem_btn = wait.until(EC.element_to_be_clickable((By.ID, "ctl00_maincontent_Xem")))
        driver.execute_script("arguments[0].click();", xem_btn)

        # Chờ bảng dữ liệu xuất hiện
        wait.until(EC.presence_of_element_located((By.ID, "ctl00_maincontent_GridView1")))

        # Kiểm tra nút "Tải Excel"
        try:
            excel_btn = wait.until(
                EC.element_to_be_clickable((By.ID, "ctl00_maincontent_tai_excel")),
                message="Không tìm thấy nút tải Excel (Có thể không có dữ liệu)"
            )
        except Exception:
            print(f"⚠️ Không có dữ liệu hoặc nút tải không hiện trong khoảng {start_date} - {end_date}.")
            return None, 0

        # Xóa file cũ (xls) trong thư mục để tránh nhầm lẫn
        for f in glob.glob(os.path.join(download_dir, "*.xls")):
            os.remove(f)

        # Click tải Excel
        print("⬇️ Đang tải file Excel...")
        driver.execute_script("arguments[0].click();", excel_btn)

        # Chờ file xuất hiện (Loop check)
        html_path = None
        for _ in range(20):
            files = glob.glob(os.path.join(download_dir, "*xls"))
            if files:
                html_path = files[0]
                break
            time.sleep(1)

        if not html_path:
            raise Exception("Đã click tải nhưng không thấy file về thư mục.")

        # --- XỬ LÝ FILE (CONVERT TO CSV) ---
        print(f"📂 Đã tải: {html_path}. Đang chuyển đổi sang CSV...")
        
        # Đọc bảng HTML (File đuôi .xls của web này thực chất là HTML)
        dfs = pd.read_html(html_path)
        if not dfs:
            raise Exception("File tải về không chứa bảng dữ liệu nào.")
        df = dfs[0]

        # Tạo tên file CSV chuẩn
        safe_start = start_date.replace("/", "-")
        safe_end = end_date.replace("/", "-")
        csv_name = f"nong_san_{safe_start}_{safe_end}.csv"
        csv_path = os.path.join(download_dir, csv_name)

        # Lưu CSV
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"✅ Đã lưu CSV: {csv_path}")
        
        # Dọn dẹp file rác
        os.remove(html_path)
        
        return csv_path, len(df)

    except Exception as e:
        raise e # Ném lỗi ra ngoài để hàm run_crawling bắt và log
    finally:
        driver.quit()

import argparse
import sys
import os
from datetime import datetime, timedelta
# Giữ nguyên các import khác của bạn (log_manager, config, selenium...)

# ... (Giữ nguyên phần cấu hình và hàm download_nong_san_html_to_csv ở trên) ...

def run_crawling(target_date=None, force_run=False):
    """
    Hàm điều phối việc chạy Crawl:
    - target_date: Ngày mốc (YYYY-MM-DD). Nếu None lấy ngày hiện tại.
    - force_run: Nếu True sẽ bỏ qua check log (nếu có logic check log).
    """
    # [QUAN TRỌNG] Khởi tạo start_time ngay đầu hàm
    start_time = datetime.now() 

    # 1. Xử lý ngày tháng
    if target_date:
        try:
            # Lưu ý: Định dạng chuẩn là YYYY-MM-DD (Ví dụ: 2025-11-06)
            current_date = datetime.strptime(target_date, '%Y-%m-%d')
        except ValueError:
            print(f"❌ Lỗi định dạng ngày: {target_date}. Vui lòng dùng định dạng YYYY-MM-DD")
            return None
    else:
        current_date = datetime.now()

    # Giả sử logic cào là lấy dữ liệu 7 ngày gần nhất tính từ target_date
    end_date_str = current_date.strftime('%d/%m/%Y')
    start_date = current_date - timedelta(days=7)
    start_date_str = start_date.strftime('%d/%m/%Y')

    print(f"--- BẮT ĐẦU EXTRACT DATA (Force={force_run}) ---")
    print(f"📅 Range: {start_date_str} - {end_date_str}")

    try:
        print(f"[{PROCESS_NAME}] Bắt đầu chạy quy trình...")
        
        # Log START
        log_process_action(
            process_config_id=PROCESS_ID,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=None,
            status="START",
            message=f"Range: {start_date_str}-{end_date_str}"
        )

        staging_dir = get_parameter_value('STAGING_DIR') or "./staging"
        
        # Gọi hàm crawl (Hàm này bạn đã định nghĩa ở trên)
        csv_path, record_count = download_nong_san_html_to_csv(start_date_str, end_date_str, staging_dir)

        end_time = datetime.now()
        
        if not csv_path:
            msg = f"No data found for range {start_date_str}-{end_date_str}"
            print(msg)
            # Log SUCCESS nhưng record = 0
            log_process_action(PROCESS_ID, PROCESS_NAME, start_time, end_time, "CND", 0, 0, 0, msg)
            return None

        # Log SUCCESS
        log_process_action(
            process_config_id=PROCESS_ID,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="CS",
            records_extract=record_count,
            message=f"Saved: {os.path.basename(csv_path)}"
        )
        
        print(f"✅ Hoàn thành! File lưu tại: {csv_path}")
        return csv_path

    except Exception as e:
        end_time = datetime.now()
        error_msg = f"Crawler Error: {str(e)}"
        print(f"❌ {error_msg}")
        
        log_process_action(
            process_config_id=PROCESS_ID,
            process_name=PROCESS_NAME,
            start_time=start_time,
            end_time=end_time,
            status="CF",
            message=error_msg
        )
        
        # Gửi mail nếu có cấu hình
        if 'SEND_TO_EMAIL' in globals() and SEND_TO_EMAIL:
             send_email(f"[ETL] CRAWLING FAILED", f"Error: {e}", [SEND_TO_EMAIL])
        
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run extract process manually.")
    parser.add_argument("--date", type=str, default=None, help="Format YYYY-MM-DD (e.g., 2025-11-23)")
    parser.add_argument("--force", action="store_true", help="Force run ignoring logs")
    
    args = parser.parse_args()

    # Chạy thực tế lấy tham số từ dòng lệnh
    run_crawling(target_date=args.date, force_run=args.force)
    