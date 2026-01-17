import os
from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("local-etl-test")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    orders = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv("local_test_data/orders.csv")
    )

    # 如果你把 events 改成 jsonl，就改成 events.jsonl
    events = spark.read.json("local_test_data/events.json")

    orders.createOrReplaceTempView("orders")
    events.createOrReplaceTempView("events")

    df = spark.sql("""
        SELECT
          o.user_id,
          o.order_id,
          o.amount,
          e.event_type,
          e.event_time
        FROM orders o
        LEFT JOIN events e
          ON o.user_id = e.user_id
        WHERE o.amount > 0
    """)

    df.show(truncate=False)

    os.makedirs("local_output", exist_ok=True)

    # Windows 友好：写 CSV
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv("local_output/final_view_csv")
    )

    print("✅ Wrote output to: local_output/final_view_csv")
    spark.stop()

if __name__ == "__main__":
    main()
