import os
import time
import glob
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def download_nong_san_html_to_csv(start_date: str, end_date: str, download_dir: str = "./staging") -> str | None:
    """
    Tự động tải file “Excel” (thực chất là HTML table) từ website thitruongnongsan.gov.vn
    và chuyển thành CSV.

    start_date, end_date: định dạng 'dd/mm/yyyy'
    download_dir: thư mục lưu file và CSV
    return: đường dẫn CSV hoặc None nếu không có dữ liệu
    """

    os.makedirs(download_dir, exist_ok=True)

    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": os.path.abspath(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 20)

    try:
        # Mở trang
        driver.get("https://thitruongnongsan.gov.vn/vn/nguonwmy.aspx")

        # Nhập ngày
        driver.find_element(By.ID, "ctl00_maincontent_tu_ngay").clear()
        driver.find_element(By.ID, "ctl00_maincontent_tu_ngay").send_keys(start_date)
        driver.find_element(By.ID, "ctl00_maincontent_den_ngay").clear()
        driver.find_element(By.ID, "ctl00_maincontent_den_ngay").send_keys(end_date)

        # Chọn ngành hàng và nhóm sản phẩm
        Select(driver.find_element(By.ID, "ctl00_maincontent_Ngành_hàng")).select_by_visible_text("Rau, quả")
        time.sleep(2)
        Select(driver.find_element(By.ID, "ctl00_maincontent_Nhóm_sản_phẩm")).select_by_visible_text("Rau củ quả")
        time.sleep(1)

        # Nhấn nút "Xem"
        xem_btn = wait.until(EC.element_to_be_clickable((By.ID, "ctl00_maincontent_Xem")))
        driver.execute_script("arguments[0].click();", xem_btn)

        # Chờ bảng dữ liệu xuất hiện
        wait.until(EC.presence_of_element_located((By.ID, "ctl00_maincontent_GridView1")))

        # Kiểm tra có nút "Tải Excel" không
        try:
            excel_btn = wait.until(
                EC.element_to_be_clickable((By.ID, "ctl00_maincontent_tai_excel")),
                message="Không có nút tải Excel"
            )
        except:
            print(f"⚠️ Không có dữ liệu trong khoảng {start_date} - {end_date}.")
            return None

        # Xóa file cũ nếu có
        for f in glob.glob(os.path.join(download_dir, "*xls")):
            os.remove(f)

        # Click tải Excel
        driver.execute_script("arguments[0].click();", excel_btn)

        # Chờ file xuất hiện
        html_path = None
        for _ in range(20):  # chờ tối đa 20 giây
            files = glob.glob(os.path.join(download_dir, "*xls"))
            if files:
                html_path = files[0]
                break
            time.sleep(1)

        if not html_path:
            print("⚠️ Không tìm thấy file được tải về.")
            return None

        # Đọc bảng HTML
        try:
            df = pd.read_html(html_path)[0]
        except Exception as e:
            print(f"❌ Lỗi đọc HTML từ file: {e}")
            return None

        safe_start = start_date.replace("/", "-")
        safe_end = end_date.replace("/", "-")
        csv_path = os.path.join(download_dir, f"nong_san_{safe_start}_{safe_end}.csv")

        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

        print(f"✅ Đã lưu CSV: {csv_path}")

        os.remove(html_path)

        return csv_path

    except Exception as e:
        print(f"❌ Lỗi trong quá trình tải: {e}")
        return None

    finally:
        driver.quit()

if __name__ == "__main__":
    download_nong_san_html_to_csv("01/10/2025", "29/10/2025")
