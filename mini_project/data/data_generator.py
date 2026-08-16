# FILE      : data/data_generator.py
# LIBRARY   : pandas, numpy, random, datetime
# CONFIG    : n_normal, seed — via function parameters
# __main__  : present — standalone script, not part of pipeline
# CALLER    : standalone (run directly to generate orders.csv)

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta


def generate_orders(n_normal: int = 200, seed: int = 42) -> pd.DataFrame:
    """
    Generate a realistic orders dataset with intentional anomalies and nulls.

    Args:
        n_normal: Number of normal records to generate.
        seed: Random seed for reproducibility.

    Returns:
        DataFrame containing mixed normal, anomalous, and null records.
    """
    rng = np.random.default_rng(seed)
    random.seed(seed)

    # --- Normal records ---
    order_ids = [f"ORD-{str(i).zfill(4)}" for i in range(1, n_normal + 1)]
    customer_ids = [f"CUST-{str(rng.integers(1, 50)).zfill(3)}" for _ in range(n_normal)]
    product_ids = [f"PROD-{str(rng.integers(1, 20)).zfill(2)}" for _ in range(n_normal)]
    quantities = rng.integers(1, 11, size=n_normal).tolist()
    unit_prices = rng.uniform(10.0, 500.0, size=n_normal).round(2).tolist()
    total_amounts = [round(q * p, 2) for q, p in zip(quantities, unit_prices)]
    base_date = datetime(2024, 1, 1)
    order_dates = [
        (base_date + timedelta(days=int(rng.integers(0, 365)))).strftime("%Y-%m-%d")
        for _ in range(n_normal)
    ]
    statuses = random.choices(["completed", "pending", "cancelled"], weights=[70, 20, 10], k=n_normal)
    emails = [f"user{rng.integers(1, 100)}@example.com" for _ in range(n_normal)]

    normal_df = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": customer_ids,
        "product_id": product_ids,
        "quantity": quantities,
        "unit_price": unit_prices,
        "total_amount": total_amounts,
        "order_date": order_dates,
        "status": statuses,
        "customer_email": emails,
    })

    # --- Anomalous records: extreme values ---
    anomaly_df = pd.DataFrame({
        "order_id": ["ORD-9001", "ORD-9002", "ORD-9003"],
        "customer_id": ["CUST-001", "CUST-002", "CUST-003"],
        "product_id": ["PROD-01", "PROD-02", "PROD-03"],
        "quantity": [999, 0, 500],
        "unit_price": [99999.99, 0.001, 88888.88],
        "total_amount": [99999.99 * 999, 0.001 * 0, 88888.88 * 500],
        "order_date": ["2024-06-15", "2024-06-16", "2024-06-17"],
        "status": ["completed", "pending", "cancelled"],
        "customer_email": ["anomaly1@example.com", "anomaly2@example.com", "anomaly3@example.com"],
    })

    # --- Null records: missing required fields ---
    null_df = pd.DataFrame({
        "order_id": ["ORD-8001", "ORD-8002", "ORD-8003"],
        "customer_id": [None, "CUST-010", "CUST-011"],
        "product_id": ["PROD-05", None, "PROD-06"],
        "quantity": [3, 5, None],
        "unit_price": [25.0, 50.0, 75.0],
        "total_amount": [75.0, 250.0, None],
        "order_date": ["2024-03-01", "2024-03-02", None],
        "status": ["completed", None, "pending"],
        "customer_email": [None, None, None],
    })

    # --- Duplicate record ---
    duplicate_df = normal_df.iloc[[0]].copy()
    duplicate_df["order_id"] = "ORD-0001"

    df = pd.concat([normal_df, anomaly_df, null_df, duplicate_df], ignore_index=True)
    df = df.sample(frac=1, random_state=seed).reset_index(drop=True)

    return df


def save_orders(output_path: str = "data/orders.csv") -> None:
    """Generate orders and save to CSV."""
    df = generate_orders()
    df.to_csv(output_path, index=False)
    print(f"Generated {len(df)} records -> {output_path}")
    print(f"  Normal records : 200")
    print(f"  Anomalies      : 3")
    print(f"  Null records   : 3")
    print(f"  Duplicates     : 1")


if __name__ == "__main__":
    save_orders()