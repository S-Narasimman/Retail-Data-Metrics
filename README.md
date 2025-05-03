# 🛠️ Retail Data Engineering Mini Project

## 📌 Project Summary

This mini project demonstrates a real-world Data Engineering pipeline built using **Apache Spark (PySpark)** and **SQL** to process retail business data. The pipeline reads raw CSV files from **HDFS**, transforms them into valuable business insights, and writes the output to **Google Cloud Storage (GCS)** in a clean, structured format.

---

## 🎯 Business Objectives

The goal is to calculate and analyze three key performance metrics:

1. **Profit & Promo Indicator**
   - Determine profit by product and promotion status.
   - Compare profitability between promoted and non-promoted products.

2. **Demand & Supply Ratio**
   - Measure the balance between product demand and inventory supply on a daily basis.

3. **Rewards Program Metrics**
   - Evaluate customer engagement through rewards earned, redeemed, and redemption efficiency.

---

## 📂 Data Sources

Files stored in **HDFS**:

- `sales.csv`  
- `inventory.csv`  
- `promotions.csv`  
- `rewards.csv`  

Each file contains transactional or operational data relevant to daily retail activity.

---

## 🧪 Technologies Used

- **Apache Spark (PySpark)**
- **Spark SQL**
- **HDFS** (Hadoop Distributed File System)
- **Google Cloud Storage (GCS)**
- **Google Cloud Service Account** for GCS access

---

## 🧱 Pipeline Architecture

1. **Ingest** data from HDFS using PySpark.
2. **Transform** using Spark SQL for computing the metrics.
3. **Write** the final output datasets to GCS as partitioned CSV files.

---

## 📦 Output Metrics

Final outputs written to:  
`gs://your-bucket/output/`

- `/profit_metrics/`  
- `/demand_supply_metrics/`  
- `/rewards_metrics/`

Each output file contains structured business metrics ready for BI tools or dashboards.

---

## ⚙️ How to Run

1. Configure your Spark environment with the GCS connector and service account key.
2. Place the source data files in the appropriate HDFS path.
3. Update the paths in the script (`main.py`) to match your environment.
4. Run the PySpark job using:
   ```bash
   spark-submit main.py