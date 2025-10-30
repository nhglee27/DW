CREATE DATABASE IF NOT EXISTS control_db;
CREATE DATABASE IF NOT EXISTS agri_staging_db;
CREATE DATABASE IF NOT EXISTS data_warehouse;

SET GLOBAL local_infile = 1;

use control_db;
CREATE TABLE config (
    id INT PRIMARY KEY AUTO_INCREMENT,  -- ID tự động tăng
    config_key VARCHAR(100) NOT NULL UNIQUE,   -- Tên tham số (ví dụ: 'MAX_ROWS_PER_LOAD')
    config_value VARCHAR(255) NOT NULL,        -- Giá trị của tham số (ví dụ: '100000')
    description VARCHAR(500),                  -- Mô tả chi tiết
    is_active TINYINT DEFAULT 1,               -- Trạng thái kích hoạt (1=Active, 0=Inactive)
    last_updated_date DATETIME DEFAULT NOW()   -- Thời gian cập nhật gần nhất
);

CREATE TABLE config_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    log_time DATETIME NOT NULL,
    action VARCHAR(50) NOT NULL COMMENT 'Hành động: READ, UPDATE, READ_FAIL, UPDATE_FAIL',
    param_key VARCHAR(100) NOT NULL COMMENT 'Tên tham số bị ảnh hưởng',
    old_value TEXT NULL COMMENT 'Giá trị cũ (NULL cho READ)',
    new_value TEXT NULL COMMENT 'Giá trị mới hoặc giá trị được đọc',
    message VARCHAR(255) NULL
);

CREATE TABLE process_config (
    id INT PRIMARY KEY AUTO_INCREMENT,
    process_name VARCHAR(100) NOT NULL UNIQUE,       -- Tên duy nhất của Quy trình ETL/ELT
    source_table VARCHAR(100),                       -- Tên bảng nguồn
    target_table VARCHAR(100),                       -- Tên bảng đích
    last_successful_watermark DATETIME,              -- Dấu thời gian (watermark) thành công gần nhất
    schedule_expression VARCHAR(50),                 -- Biểu thức lập lịch (ví dụ: 'Daily 02:00')
    is_enabled TINYINT DEFAULT 1,                    -- Quy trình có được bật để chạy không
    priority INT DEFAULT 5,                          -- Mức độ ưu tiên khi chạy
    last_updated_date DATETIME DEFAULT NOW()
);

CREATE TABLE process_config (
    id INT PRIMARY KEY AUTO_INCREMENT,
    process_name VARCHAR(100) NOT NULL UNIQUE,       -- Tên duy nhất của Quy trình ETL/ELT
    source_table VARCHAR(100),                       -- Tên bảng nguồn
    target_table VARCHAR(100),                       -- Tên bảng đích
    last_successful_watermark DATETIME,              -- Dấu thời gian (watermark) thành công gần nhất
    is_enabled TINYINT DEFAULT 1,                    -- Quy trình có được bật để chạy không
    last_updated_date DATETIME DEFAULT NOW()
);

CREATE TABLE process_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    process_config_id INT NOT NULL,                  -- Khóa ngoại tham chiếu đến process_config
    process_name VARCHAR(100) NOT NULL,              -- Tên Quy trình (để truy vấn nhanh)
    start_time DATETIME NOT NULL,                    -- Thời gian bắt đầu thực thi
    end_time DATETIME,                               -- Thời gian kết thúc thực thi
    status VARCHAR(20) NOT NULL,                     -- Trạng thái 
    watermark_used DATETIME,                         -- Dấu thời gian (watermark) được sử dụng
    records_extracted INT,                           -- Số bản ghi đã được trích xuất
    records_loaded INT,                              -- Số bản ghi đã được tải vào đích
    error_message TEXT,                              -- Chi tiết lỗi (sử dụng TEXT cho chuỗi dài)
    
    FOREIGN KEY (process_config_id) REFERENCES process_config(id)
);

use agri_staging_db;

CREATE TABLE stg_nong_san (
    ten_mat_hang VARCHAR(255),
    thi_truong VARCHAR(255),
    ngay_raw VARCHAR(20),
    gia_raw DECIMAL(15,2)
);
