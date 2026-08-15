from data.data_generator import generate_orders
from src.anomaly_detector import detect_anomalies


### Testing Generate Orders Dataframe & Detect Anomalies

if __name__ == "__main__":
    df = generate_orders(n_rows=100)

    print("=" * 55)
    print("ANOMALY DETECTION — ORDERS PIPELINE")
    print("=" * 55)
    print(f"Input rows: {len(df)}")
    print()

    clean_df, quarantine_df = detect_anomalies(
        df,
        numeric_columns=["quantity", "price"],
        method="iqr",
        multiplier=1.5,
    )

    print(f"Clean rows      : {len(clean_df)}")
    print(f"Quarantine rows : {len(quarantine_df)}")
    print()

    if not quarantine_df.empty:
        print("Quarantined rows:")
        print(quarantine_df[["order_id", "quantity", "price"]].to_string())
    print("=" * 55)