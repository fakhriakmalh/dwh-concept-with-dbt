# 🏢 AdventureWorks Data Warehouse - OLAP Implementation

> **Demo Project**: Implementasi Data Warehouse dengan Star Schema menggunakan ClickHouse, DBT, dan Medallion Architecture untuk presentasi pembelajaran OLAP vs OLTP.

> **Dataset Required**: Download the AdventureWorks CSV files from [here](https://www.kaggle.com/datasets/ukveteran/adventure-works) before proceeding.

---

## 🎯 Overview

Project ini mendemonstrasikan:
- ✅ **OLTP vs OLAP** - Perbedaan struktur dan use case
- ✅ **ELT Process** - Extract, Load, Transform menggunakan DBT
- ✅ **Medallion Architecture** - Bronze → Silver layer
- ✅ **Star Schema** - Dimensional modeling (Fact & Dimension tables)
- ✅ **OLAP Operations** - Drill-down, Roll-up, Slice, Dice
- ✅ **Modern Data Stack** - ClickHouse + DBT

### Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database** | ClickHouse | OLAP database engine |
| **Transformation** | DBT (Data Build Tool) | ELT transformations |
| **Orchestration** | Docker Compose | Container management |
| **Language** | Python 3.8+ | Data loading scripts |

---

## Architecture

### Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CSV Files (Raw Data)                      │
│  AdventureWorks_Products.csv, Sales_2015.csv, etc.          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ Python Script (EL)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│               BRONZE LAYER (Raw/OLTP-like)                   │
│  Tables: Products, Customers, Sales_2015, Sales_2016, etc.  │
│  Purpose: Staging area, minimal transformation               │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ DBT (Transform)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              SILVER LAYER (Star Schema/OLAP)                 │
│  • Fact: Fact_Sales                                          │
│  • Dimensions: Dim_Date, Dim_Product, Dim_Customer,          │
│                Dim_Territory                                 │
│  Purpose: Analytics-ready, optimized for queries             │
└──────────────────────────────────────────────────────────────┘
```

### Star Schema Design

```
              ┌─────────────┐
              │  Dim_Date   │
              │  DateKey PK │
              │  Year       │
              │  Quarter    │
              │  Month      │
              └──────┬──────┘
                     │
┌──────────────┐    │    ┌──────────────┐
│ Dim_Product  │    │    │ Dim_Customer │
│ ProductKey PK│    │    │ CustomerKey  │
│ Name         │    │    │ FullName     │
│ Category     ├────┼────┤ City         │
│ SubCategory  │    │    │ Demographics │
└──────────────┘    │    └──────────────┘
                    │
              ┌─────▼──────┐
              │ Fact_Sales │
              │ SalesKey PK│
              │ DateKey FK │
              │ ProductKey │
              │ CustomerKey│
              │ TerrKey FK │
              │ ───────────│
              │ Quantity   │
              │ SalesAmt   │
              │ TaxAmt     │
              │ Freight    │
              │ ProfitAmt  │
              └─────┬──────┘
                    │
              ┌─────▼──────────┐
              │ Dim_Territory  │
              │ TerritoryKey PK│
              │ Region         │
              │ Country        │
              │ Continent      │
              └────────────────┘
```

---

## 📁 Project Structure

```
.

│
├── dbt/                                # DBT project folder
│   ├── adventureworks/
│   │   ├── dbt_project.yml            # DBT project config
│   │   ├── profiles.yml               # Database connection
│   │   └── models/
│   │       ├── bronze/                # (Optional) Bronze models
│   │       └── silver/                # Star schema models
│   │           ├── sources.yml        # Source table definitions
│   │           ├── schema.yml         # Model documentation
│   │           ├── dim_date.sql       # Date dimension
│   │           ├── dim_product.sql    # Product dimension
│   │           ├── dim_customer.sql   # Customer dimension
│   │           ├── dim_territory.sql  # Territory dimension
│   │           └── fact_sales.sql     # Sales fact table
│   │
│   └── logs/                          # DBT logs
│
├── docker-compose.yml                 # ClickHouse container setup
├── csv_to_clickhouse.py              # Import CSV to ClickHouse (Bronze)
├── requirements.txt                   # Python dependencies
├── profiles.yml                       # DBT profiles (alternative location)
└── README.md                          # This file
```

---

## 🔧 Prerequisites

### Required Software

- **Docker & Docker Compose** (v20.10+)
- **Python** (3.8+)
- **pip** (Python package manager)

### Check Installation

```bash
# Check Docker
docker --version
docker-compose --version

# Check Python
python --version
pip --version
```

---

## 🚀 Quick Start

### 1️⃣ Start ClickHouse Database

```bash
# Start ClickHouse container
docker-compose up -d

# Verify it's running
docker ps


```

Wait ~20 seconds for ClickHouse to fully initialize.

### 2️⃣ Load Bronze Layer (Raw Data)

```bash
# Install Python dependencies
pip install -r requirements.txt

# Place CSV files in csv_files/ folder
# Then run import script
python csv_to_clickhouse.py
```

Expected output:
```
🚀 ClickHouse CSV Importer for AdventureWorks
============================================================
✅ ClickHouse is ready!
✅ Database 'adventureworks' created/verified

📂 Found 9 CSV file(s)

============================================================
📁 Processing: AdventureWorks_Products.csv
📊 Target table: Products
   ✅ Imported 606 rows successfully

📊 IMPORT SUMMARY
============================================================
✅ Successfully imported: 9 table(s)
```

### 3️⃣ Transform to Silver Layer (Star Schema)

```bash
# Navigate to DBT project
cd dbt/adventureworks

# Install DBT
pip install dbt-core dbt-clickhouse

# Test connection
dbt debug

# Run transformations
dbt run

# Run tests
dbt test
```

Expected output:
```
Running with dbt=1.7.0
Found 5 models, 12 tests, 0 snapshots

Completed successfully

Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5
```


---

## 📚 Detailed Setup

### Step 1: Clone/Setup Project

```bash
# Create project directory
mkdir adventureworks_dwh
cd adventureworks_dwh

# Create necessary folders
mkdir csv_files
mkdir dbt
```

### Step 2: Configure ClickHouse

**File: `docker-compose.yml`**

```yaml
version: '3.8'

services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse-server
    ports:
      - "8123:8123"  # HTTP interface
      - "9000:9000"  # Native client
    environment:
      - CLICKHOUSE_DB=adventureworks
      - CLICKHOUSE_USER=default
      - CLICKHOUSE_PASSWORD=password123
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./csv_files:/csv_files:ro

volumes:
  clickhouse_data:
```

Start container:
```bash
docker-compose up -d
```

### Step 3: Load Bronze Layer

**File: `requirements.txt`**
```
clickhouse-connect==0.7.16
pandas==2.1.4
numpy==1.26.2
```

Install dependencies:
```bash
pip install -r requirements.txt
```

Run import script:
```bash
python csv_to_clickhouse.py
```

### Step 4: Setup DBT

**Navigate to DBT folder:**
```bash
cd dbt/adventureworks
```

**File: `profiles.yml`** (in `~/.dbt/` or project root)
```yaml
adventureworks:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: adventureworks_silver
      host: localhost
      port: 8123
      user: default
      password: password123
      database: adventureworks
```

**Test connection:**
```bash
dbt debug
```

### Step 5: Run DBT Models

```bash
# Run all models
dbt run

# Run specific model
dbt run --select dim_product

# Run with full refresh
dbt run --full-refresh

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve  # Open browser at http://localhost:8080
```

---

## 🔄 Data Pipeline

### Pipeline Flow

```
1. EXTRACT & LOAD (Python Script)
   ├── Read CSV files from csv_files/
   ├── Auto-detect encoding (UTF-8, Latin-1, etc.)
   ├── Skip problematic lines
   ├── Create tables in ClickHouse
   └── Load data to Bronze layer

2. TRANSFORM (DBT)
   ├── Read from Bronze tables (sources.yml)
   ├── Apply business logic (SQL models)
   ├── Create Star Schema tables
   │   ├── Dim_Date (date dimension)
   │   ├── Dim_Product (product hierarchy)
   │   ├── Dim_Customer (customer attributes)
   │   ├── Dim_Territory (geography)
   │   └── Fact_Sales (transactional metrics)
   └── Write to Silver layer

3. VALIDATE (DBT Tests)
   ├── Unique key constraints
   ├── Not null checks
   ├── Referential integrity
   └── Custom business rules
```

### Re-running Pipeline

```bash
# Full refresh (drop & recreate all tables)
cd dbt/adventureworks
dbt run --full-refresh

# Incremental update (if configured)
dbt run

# Run specific models
dbt run --select dim_product+  # dim_product and downstream models
dbt run --select +fact_sales   # fact_sales and upstream models
```

---

<!-- ## 📊 OLAP Queries Examples

### 1. ROLL-UP (Aggregate to higher level)

```sql
-- From Monthly → Quarterly
SELECT 
    d.Year,
    d.Quarter,
    SUM(f.SalesAmount) as total_sales,
    SUM(f.ProfitAmount) as total_profit
FROM adventureworks_silver.fact_sales f
JOIN adventureworks_silver.dim_date d ON f.DateKey = d.DateKey
GROUP BY d.Year, d.Quarter
ORDER BY d.Year, d.Quarter;
```

### 2. DRILL-DOWN (Breakdown to lower level)

```sql
-- From Category → Products
SELECT 
    p.ProductCategoryName,
    p.ProductName,
    SUM(f.OrderQuantity) as units_sold,
    SUM(f.SalesAmount) as revenue
FROM adventureworks_silver.fact_sales f
JOIN adventureworks_silver.dim_product p ON f.ProductKey = p.ProductKey
WHERE p.ProductCategoryName = 'Bikes'
GROUP BY p.ProductCategoryName, p.ProductName
ORDER BY revenue DESC
LIMIT 10;
```

### 3. SLICE (Filter one dimension)

```sql
-- Sales for Year 2016 only
SELECT 
    p.ProductCategoryName,
    t.Country,
    SUM(f.SalesAmount) as total_sales
FROM adventureworks_silver.fact_sales f
JOIN adventureworks_silver.dim_date d ON f.DateKey = d.DateKey
JOIN adventureworks_silver.dim_product p ON f.ProductKey = p.ProductKey
JOIN adventureworks_silver.dim_territory t ON f.TerritoryKey = t.TerritoryKey
WHERE d.Year = 2016  -- SLICE on time dimension
GROUP BY p.ProductCategoryName, t.Country
ORDER BY total_sales DESC;
```

### 4. DICE (Filter multiple dimensions)

```sql
-- Bikes in USA, Q1 2016
SELECT 
    d.MonthName,
    c.City,
    SUM(f.SalesAmount) as sales,
    SUM(f.ProfitAmount) as profit
FROM adventureworks_silver.fact_sales f
JOIN adventureworks_silver.dim_date d ON f.DateKey = d.DateKey
JOIN adventureworks_silver.dim_product p ON f.ProductKey = p.ProductKey
JOIN adventureworks_silver.dim_customer c ON f.CustomerKey = c.CustomerKey
JOIN adventureworks_silver.dim_territory t ON f.TerritoryKey = t.TerritoryKey
WHERE 
    d.Year = 2016 
    AND d.Quarter = 1
    AND p.ProductCategoryName = 'Bikes'
    AND t.Country = 'United States'
GROUP BY d.MonthName, c.City
ORDER BY sales DESC;
```

### 5. PIVOT (Rotate data)

```sql
-- Sales by Product Category and Year
SELECT 
    p.ProductCategoryName,
    SUM(CASE WHEN d.Year = 2015 THEN f.SalesAmount ELSE 0 END) as Sales_2015,
    SUM(CASE WHEN d.Year = 2016 THEN f.SalesAmount ELSE 0 END) as Sales_2016,
    SUM(CASE WHEN d.Year = 2017 THEN f.SalesAmount ELSE 0 END) as Sales_2017
FROM adventureworks_silver.fact_sales f
JOIN adventureworks_silver.dim_date d ON f.DateKey = d.DateKey
JOIN adventureworks_silver.dim_product p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductCategoryName
ORDER BY p.ProductCategoryName;
``` -->


## 📖 References

- [ClickHouse Documentation](https://clickhouse.com/docs)
- [DBT Documentation](https://docs.getdbt.com/)
- [AdventureWorks Dataset](https://github.com/Microsoft/sql-server-samples)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

---


## 🎯 Next Steps

After completing this demo:
1. ✅ Add more complex transformations (SCD Type 2)
2. ✅ Implement incremental loads
3. ✅ Add data quality checks
4. ✅ Create BI dashboard (Metabase/Superset)
5. ✅ Add Gold layer for business metrics

---

**Happy Learning! 🚀**