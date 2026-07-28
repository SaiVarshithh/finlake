from spark.handler import iceberg_initialisation, build_spark
import yfinance as yf
from spark.model.constants import popular_tickers


@iceberg_initialisation
def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    tickers = popular_tickers[:20]
    df = yf.download(tickers, period="5d", group_by="ticker")
    df.columns = df.columns.swaplevel(0, 1)

    print("FinLake Bronze Stock Writer Job Started Successfully")
    print(f"Spark version       : {spark.version}")
    print("=" * 80)

    print("Iceberg tables have been verified/created by the decorator.")

    spark.stop()


if __name__ == "__main__":
    main()