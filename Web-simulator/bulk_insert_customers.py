#!/usr/bin/env python3
"""
Bulk insert customer data from CSV to MySQL database.
Inserts 2000 rows per batch for optimal performance.
"""

import csv
import mysql.connector
from mysql.connector import Error
import sys
from pathlib import Path
import traceback
import os


# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME', 'oltp'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'root'),
    'charset': 'utf8mb4',
    'use_unicode': True
}

BATCH_SIZE = 2000
CSV_FILE = 'metadata/customer (3).csv'


def create_connection():
    """Create and return a MySQL database connection with detailed logging."""
    print("⏳ Attempting to connect to MySQL...")
    print(f"→ Host: {DB_CONFIG['host']}")
    print(f"→ Port: {DB_CONFIG['port']}")
    print(f"→ User: {DB_CONFIG['user']}")
    print(f"→ Database: {DB_CONFIG['database']}")

    try:
        conn = mysql.connector.connect(**DB_CONFIG)

        if conn.is_connected():
            print("✓ Connected successfully!")
            return conn
        else:
            print("✗ Connection failed (unknown reason).")
            sys.exit(1)

    except Error as e:
        print("\n🔥 CONNECTION ERROR!")
        print(f"• Error Code   : {getattr(e, 'errno', 'N/A')}")
        print(f"• SQL State    : {getattr(e, 'sqlstate', 'N/A')}")
        print(f"• Message      : {e.msg if hasattr(e, 'msg') else str(e)}")
        print("\n--- TRACEBACK (for debugging) ---")
        traceback.print_exc()

        print("\n🛠 POSSIBLE FIXES:")

        if e.errno == 1045:
            print("→ Wrong username/password. Check DB_CONFIG['user'] và ['password'].")

        elif e.errno == 2003:
            print("→ Cannot connect to MySQL. Nguyên nhân thường:")
            print("   - MySQL không chạy")
            print("   - Sai port")
            print("   - Docker container chưa start")
            print("   - Firewall chặn port")

        elif e.errno == 1049:
            print("→ Database không tồn tại. Tạo database bằng:")
            print("   CREATE DATABASE oltp;")

        elif e.errno == 2005:
            print("→ Unknown MySQL host. Sai DB_CONFIG['host'].")

        else:
            print("→ Lỗi khác. Kiểm tra lại cấu hình và container log.")

        sys.exit(1)


def read_csv_in_batches(csv_path, batch_size):
    """
    Generator to read CSV file in batches.
    
    CSV format (no header):
    customer_id, first_name, last_name, email, phone_number, gender, tier
    """
    batch = []
    
    with open(csv_path, 'r', encoding='utf-8-sig') as file:
        csv_reader = csv.reader(file)
        
        for row in csv_reader:
            if len(row) != 7:
                print(f"⚠ Skipping invalid row: {row}")
                continue
            
            # Parse row data
            customer_id = int(row[0])
            first_name = row[1].strip()
            last_name = row[2].strip()
            email = row[3].strip()
            phone_number = row[4].strip()
            gender = row[5].strip()
            tier = row[6].strip()
            
            batch.append((
                customer_id,
                first_name,
                last_name,
                email,
                phone_number,
                gender,
                tier
            ))
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield remaining rows
        if batch:
            yield batch


def bulk_insert_customers(connection, batch):
    """
    Bulk insert customer records using executemany.
    
    Schema: customers (
        customer_id INT PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE,
        phone_number VARCHAR(50),
        gender ENUM('Nam', 'Nữ', 'Khác'),
        tier VARCHAR(50),
        created_at DATETIME default now(),
        updated_at DATETIME default now()
    )
    """
    insert_query = """
        INSERT INTO customers 
        (customer_id, first_name, last_name, email, phone_number, gender, tier)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            email = VALUES(email),
            phone_number = VALUES(phone_number),
            gender = VALUES(gender),
            tier = VALUES(tier),
            updated_at = NOW()
    """
    
    cursor = connection.cursor()
    
    try:
        cursor.executemany(insert_query, batch)
        connection.commit()
        return cursor.rowcount
    except Error as e:
        connection.rollback()
        print(f"✗ Error inserting batch: {e}")
        return 0
    finally:
        cursor.close()


def main():
    """Main execution function"""
    csv_path = Path(__file__).parent / CSV_FILE
    
    if not csv_path.exists():
        print(f"✗ CSV file not found: {csv_path}")
        sys.exit(1)
    
    print(f"📂 Reading CSV: {csv_path}")
    print(f"📊 Batch size: {BATCH_SIZE} rows")
    print("-" * 60)
    
    connection = create_connection()
    
    total_inserted = 0
    batch_num = 0
    
    try:
        for batch in read_csv_in_batches(csv_path, BATCH_SIZE):
            batch_num += 1
            rows_affected = bulk_insert_customers(connection, batch)
            total_inserted += len(batch)
            
            print(f"Batch {batch_num:3d}: Inserted {len(batch):5d} rows | "
                  f"Total: {total_inserted:6d} | Affected: {rows_affected:5d}")
        
        print("-" * 60)
        print(f"✓ Successfully processed {total_inserted} customer records")
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        sys.exit(1)
    finally:
        if connection.is_connected():
            connection.close()
            print("✓ Database connection closed")


if __name__ == "__main__":
    main()
