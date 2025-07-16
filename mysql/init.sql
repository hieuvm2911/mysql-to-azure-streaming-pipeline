-- Tạo CSDL nếu chưa có
CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- Tạo bảng orders
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT DEFAULT 1,
    price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) GENERATED ALWAYS AS (quantity * price) STORED,
    
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
    payment_method VARCHAR(50),
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
