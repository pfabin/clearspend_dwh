# ClearSpend Data Warehouse
Data Engineering Final Project for company ClearSpend, looking to create an ETL data pipeline which turns messy and raw credit/debit card transactions into meaningufl insights on financial analytics for dashboard and reporting purposes. Namely used for merchant performance,  customer spending behaviour, and financial trends.  

Course: EBC2199 - Data Engineering and Data Governance  
Group: Petar F., Mirza K., Andre M.P., Konstanty W.  
Supervisor: Prof. Mohsen P.  

## Setup Instructions:
### Naming Convention
snake_case

### ETL Order 
Ingestion: ingestion_write.py (create database) then ingestion_load.py (load four datasets)

Transformation: no order, four datasets transformed individually

Curated: Kimball star-schema applied

Marts: Specific department marts (Customer Analytics, Finance, Fraud, Leadership, and Merchant Partnerships)

## DWH Layers
<img width="571" height="591" alt="clearspend_dwh_layers" src="https://github.com/user-attachments/assets/faac3795-1fa6-4153-9e75-e7fd38b5994f" />

## Star Schema
<img width="962" height="762" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/aa440240-3180-4ad7-903d-04e0ec348e74" />

## Data Flow Diagram
<img width="1519" height="468" alt="image" src="https://github.com/user-attachments/assets/b21c3383-e82b-4100-bb15-d7ec382c0f1b" />
