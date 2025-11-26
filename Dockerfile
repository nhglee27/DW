# Sử dụng 'bookworm' (Debian 12 đầy đủ) để cài đặt Chromium ổn định
FROM python:3.11-bookworm

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /workspace

# Cài đặt dependencies hệ thống cho Chromium
# (Đây là danh sách bạn đã cung cấp, rất tốt)
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    fonts-liberation \
    libnss3 \
    libxss1 \
    libappindicator3-1 \
    libatk-bridge2.0-0 \
    libgbm1 \
    wget \
    unzip \
 && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường (Giữ nguyên)
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Chỉ COPY tệp requirements.txt
# Chúng ta không COPY code (như 'scripts/')
# vì tệp docker-compose.yaml sẽ dùng 'volumes' để mount code vào
COPY requirements.txt ./

# Cài đặt thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# Lệnh này giữ container "sống" vĩnh viễn
# (Thay vì CMD ["bash"] - sẽ bị thoát ngay)
CMD ["tail", "-f", "/dev/null"]