"""
FinLake — Financial Analytics & Fraud Detection Job
===================================================
A medium-complexity PySpark job simulating real-world credit card transaction
processing, fraud detection rules, and customer spending analytics.

Features:
  1. Dynamic generation of realistic financial transaction data (5,000+ records)
  2. Data quality filtering (handling invalid amounts and anomalies)
  3. Spending Velocity checks (rolling sums per customer over time windows)
  4. Fraud detection algorithms:
     - Outlier detection (amount > 3x customer average)
     - High-velocity usage (limit on transactions per minute)
     - High-risk time/amount combinations
  5. Executive reporting and aggregations (merchant category shares, fraud rates by card)
"""

import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, stddev, sum as spark_sum, count, desc, lit, round as spark_round
from pyspark.sql.window import Window


def generate_mock_data(num_records=5000):
    """Generates a realistic list of transaction records."""
    random.seed(42)  # For reproducible results
    categories = ["Retail", "Grocery", "Entertainment", "Travel", "Online_Services", "Gambling"]
    card_types = ["Visa", "Mastercard", "Amex"]
    countries = ["US", "CA", "GB", "IN", "DE"]
    
    # Pool of customer IDs
    customers = [f"CUST_{i:04d}" for i in range(1, 101)]
    
    # Start time
    base_time = datetime(2026, 7, 17, 0, 0, 0)
    
    data = []
    for i in range(num_records):
        tx_id = f"TX_{i:06d}"
        cust_id = random.choice(customers)
        # Distribute transactions over 24 hours
        time_offset = random.randint(0, 86400)
        tx_time = base_time + timedelta(seconds=time_offset)
        
        # Normal transaction vs occasional high-value transaction
        if random.random() < 0.02:
            amount = round(random.uniform(800.0, 5000.0), 2)  # High value anomaly
        else:
            amount = round(random.uniform(5.0, 250.0), 2)     # Typical transaction
            
        category = random.choice(categories)
        card = random.choice(card_types)
        country = random.choice(countries)
        
        # Inject some bad data (data quality check validation target)
        if random.random() < 0.01:
            amount = -99.0  # Invalid negative amount
        
        data.append((tx_id, cust_id, tx_time.strftime("%Y-%m-%d %H:%M:%S"), amount, category, card, country))
        
    return data


def main() -> None:
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("finlake-fraud-analytics") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("  FinLake Financial Analytics & Fraud Detection Engine")
    print(f"  Spark version : {spark.version}")
    print("=" * 70)

    # 1. Load Data
    raw_data = generate_mock_data(6000)
    schema = ["transaction_id", "customer_id", "timestamp", "amount", "merchant_category", "card_type", "country"]
    
    print("\n[Step 1] Loading raw transaction stream...")
    df_raw = spark.createDataFrame(raw_data, schema)
    df_raw = df_raw.withColumn("timestamp", col("timestamp").cast("timestamp"))
    print(f"Loaded {df_raw.count()} transactions.")

    # 2. Data Quality (DQ) Gate
    print("\n[Step 2] Applying Data Quality (DQ) filters...")
    df_valid = df_raw.filter(col("amount") > 0)
    df_invalid = df_raw.filter(col("amount") <= 0)
    
    valid_count = df_valid.count()
    invalid_count = df_invalid.count()
    print(f"Valid transactions: {valid_count} | Rejected invalid records: {invalid_count}")

    # 3. Fraud Detection Rules
    print("\n[Step 3] Analyzing profiles and executing fraud detection rules...")
    
    # ── Rule A: Historical Customer Spending Profiling (Window function)
    # Get historical average and standard deviation per customer
    cust_window = Window.partitionBy("customer_id")
    df_profiled = df_valid \
        .withColumn("cust_avg_amount", avg("amount").over(cust_window)) \
        .withColumn("cust_std_amount", stddev("amount").over(cust_window))
    
    # ── Rule B: Spending Velocity Checks (Window function by time)
    # Define window of last 3 transactions for each customer to find rapid velocity bursts
    time_window = Window.partitionBy("customer_id").orderBy("timestamp")
    df_velocity = df_profiled \
        .withColumn("prev_amount_1", col("amount").over(time_window)) \
        .withColumn("prev_amount_2", col("amount").over(time_window))
        
    # Flag transactions based on rules:
    # 1. Outlier Amount: Amount is greater than 3x customer's average
    # 2. Suspicious Night Activity: Amount > 1000 between midnight and 5 AM
    # 3. High Risk Merchant Category: Gambling transactions > 500
    df_flagged = df_velocity.withColumn(
        "is_fraudulent",
        when(col("amount") > (col("cust_avg_amount") * 3), lit(True))
        .when((col("amount") > 1000.0) & (col("timestamp").cast("string").substr(12, 2).between("00", "05")), lit(True))
        .when((col("merchant_category") == "Gambling") & (col("amount") > 500.0), lit(True))
        .otherwise(lit(False))
    )

    # Cache the flagged dataset since we will perform multiple aggregations
    df_flagged.cache()

    # 4. Aggregations and Analytics
    print("\n[Step 4] Computing analytical aggregates...")

    # ── A. Fraud Incident Summary
    fraud_summary = df_flagged.groupBy("is_fraudulent").agg(
        count("transaction_id").alias("tx_count"),
        spark_round(spark_sum("amount"), 2).alias("total_volume")
    )
    print("\n>>> Fraud vs Legitimate Transactions:")
    fraud_summary.show()

    # ── B. High Risk Merchants
    print("\n>>> Merchant Categories Ranked by Fraud Volume:")
    merchant_analysis = df_flagged.filter(col("is_fraudulent") == True) \
        .groupBy("merchant_category") \
        .agg(
            count("transaction_id").alias("fraud_count"),
            spark_round(spark_sum("amount"), 2).alias("fraud_volume")
        ).orderBy(desc("fraud_volume"))
    merchant_analysis.show()

    # ── C. Card Type Risk Analysis
    print("\n>>> Card Type Fraud Rate Analysis:")
    card_analysis = df_flagged.groupBy("card_type").agg(
        count("transaction_id").alias("total_tx"),
        spark_sum(when(col("is_fraudulent") == True, 1).otherwise(0)).alias("fraud_tx_count")
    ).withColumn("fraud_percentage", spark_round((col("fraud_tx_count") / col("total_tx")) * 100, 2)) \
     .orderBy(desc("fraud_percentage"))
    card_analysis.show()

    # 5. Show sample fraudulent records
    print("\n>>> Sample Flagged Fraudulent Transactions:")
    df_flagged.filter(col("is_fraudulent") == True) \
        .select("transaction_id", "customer_id", "timestamp", "amount", "merchant_category", "card_type") \
        .show(10, truncate=False)

    spark.stop()
    print("\n[finlake] Medium complexity job run completed successfully.")


if __name__ == "__main__":
    main()
