# 🧩 Ecom K8s Lakehouse Platform


# Thiết kế hệ hống


## 🏗️ Kiến trúc tổng thể
![overal_archiect.png](/assets/overall_architect.png)
## Dữ liệu nguồn
1. **Mysql OLTP Database**

  ![oltp_schema.png](/assets/oltp_schema.png)


2. **User log files**

   ![userlogs.png](/assets/userlogs.png)
---



## Kiến trúc huy trương

![dataflow.png](/assets/dataflow.png)

1. **Dữ liệu Bronze Layer**

  ![bronze.png](/assets/bronze.png)

2. **Dữ liệu Silver Layer**
  ![silver.png](/assets/silver.png)

3.  **Dữ liệu Lớp vàng**

Dimensional Modeling

  ![gold1.png](/assets/gold1.png)

Tổng hợp, trích xuất đặc trưng từ user logs
![gold2.png](/assets/gold2.png)

Tổng hợp dữ liệu tiếp thị khách hàng
![gold3.png](/assets/gold3.png)


## Điều phối xử lý dữ liệu

![processingflow.png](/assets/processingflow.png)


## Phân quyền và truy vấn dữ liệu

![dataaccess.png](/assets/dataaccess.png)


## Triển khai


## Demo 

