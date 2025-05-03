from pyspark.sql import SparkSession

# Initialize Spark Session with GCS connector
spark = SparkSession.builder \
    .appName("Retail Metrics Pipeline") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/path/to/your/service-account.json") \
    .getOrCreate()

# Read data from HDFS
sales = spark.read.option("header", "true").csv("hdfs:///user/data/sales.csv", inferSchema=True)
inventory = spark.read.option("header", "true").csv("hdfs:///user/data/inventory.csv", inferSchema=True)
promotions = spark.read.option("header", "true").csv("hdfs:///user/data/promotions.csv", inferSchema=True)
rewards = spark.read.option("header", "true").csv("hdfs:///user/data/rewards.csv", inferSchema=True)

# Register temp views for SQL
sales.createOrReplaceTempView("sales")
inventory.createOrReplaceTempView("inventory")
promotions.createOrReplaceTempView("promotions")
rewards.createOrReplaceTempView("rewards")

# ------------------------
# 1. Profit & Promo Indicator
# ------------------------
profit_query = """
SELECT
    s.product_id,
    s.date,
    (s.selling_price - s.cost_price) * s.quantity_sold AS profit,
    CASE WHEN p.is_promoted = 1 THEN 'PROMO' ELSE 'NO_PROMO' END AS promo_indicator
FROM sales s
LEFT JOIN promotions p
ON s.product_id = p.product_id AND s.date = p.date
"""

profit_df = spark.sql(profit_query)
profit_df.createOrReplaceTempView("profit_metrics")

profit_agg = spark.sql("""
SELECT
    promo_indicator,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(AVG(profit), 2) AS avg_profit
FROM profit_metrics
GROUP BY promo_indicator
""")

# ------------------------
# 2. Demand & Supply Ratio
# ------------------------
demand_supply_query = """
SELECT
    s.product_id,
    s.date,
    SUM(s.quantity_sold) AS total_demand,
    SUM(i.quantity_available) AS total_supply,
    ROUND(SUM(s.quantity_sold) / NULLIF(SUM(i.quantity_available), 0), 2) AS demand_supply_ratio
FROM sales s
JOIN inventory i
ON s.product_id = i.product_id AND s.date = i.date
GROUP BY s.product_id, s.date
"""

demand_supply_df = spark.sql(demand_supply_query)

# ------------------------
# 3. Rewards Program Metrics
# ------------------------
rewards_query = """
SELECT
    customer_id,
    SUM(points_earned) AS total_points_earned,
    SUM(points_redeemed) AS total_points_redeemed,
    ROUND(SUM(points_redeemed) / NULLIF(SUM(points_earned), 0), 2) AS redemption_ratio
FROM rewards
GROUP BY customer_id
"""

rewards_metrics_df = spark.sql(rewards_query)

# ------------------------
# Write Results to GCS
# ------------------------
output_path = "gs://your-bucket/output"

profit_agg.write.mode("overwrite").option("header", "true").csv(f"{output_path}/profit_metrics")
demand_supply_df.write.mode("overwrite").option("header", "true").csv(f"{output_path}/demand_supply_metrics")
rewards_metrics_df.write.mode("overwrite").option("header", "true").csv(f"{output_path}/rewards_metrics")

print("✅ Metrics successfully written to GCS.")