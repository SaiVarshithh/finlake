from spark.handler import iceberg_initialisation, build_spark



@iceberg_initialisation
def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("FinLake Bronze Stock Writer Job Started Successfully")
    print(f"Spark version       : {spark.version}")
    print("=" * 80)

    print("Iceberg tables have been verified/created by the decorator.")

    spark.stop()


if __name__ == "__main__":
    main()