import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta



def generate_orders(n_rows: int = 100, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic e-commerce order data with intentional quality issues.
    Issues injected:
    - Null values in critical columns
    - Duplicate order IDs
    - Out-of-range quantity and price values
    - Invalid status values
    - Stale timestamps (older than SLA)
    """
    rng = np.random.default_rng(seed)
    now = datetime.now(timezone.utc)

    # base data - clean
    n = n_rows
    order_ids = [f"ORD-{i:04d}" for i in range(1, n+1)]
    customer_ids = [f"CUST-{rng.integers(1,50):03d}" for _ in range(n)]
    product_ids = [f"PROD-{rng.integers(1,20):03d}" for _ in range(n)]
    quantities = rng.integers(1, 20, size=n).tolist()
    prices = rng.uniform(10_000, 500_000, size=n).round(2).tolist()
    statuses = rng.choice(
        ["pending", "confirmed", "shipped", "delivered"], size=n
    ).tolist()
    timestamps = [
        now - timedelta(hours=rng.integers(1,4).item()) for _ in range(n)
    ]

    df = pd.DataFrame({
        "order_id":    order_ids,
        "customer_id": customer_ids,
        "product_id":  product_ids,
        "quantity":    quantities,
        "price":       prices,
        "status":      statuses,
        "ordered_at":  timestamps,
    })

    # inject issue 1 — null values
    null_indices = rng.choice(n, size=6, replace=False)
    df.loc[null_indices[:3], "customer_id"] = None
    df.loc[null_indices[3:], "price"]       = None

    # inject issue 2 — duplicate order IDs
    df.loc[10, "order_id"] = "ORD-0001"
    df.loc[20, "order_id"] = "ORD-0002"

    # inject issue 3 — out of range values
    df.loc[30, "quantity"] = -5          # negative quantity
    df.loc[40, "quantity"] = 999         # impossibly large
    df.loc[50, "price"]    = 99_999_999  # price outlier

    # inject issue 4 — invalid status
    df.loc[60, "status"] = "cancelled"   # not in valid set
    df.loc[70, "status"] = "refunded"    # not in valid set

    # inject issue 5 — stale timestamp
    df.loc[80, "ordered_at"] = now - timedelta(hours=10)  # beyond SLA
    df.loc[90, "ordered_at"] = now - timedelta(hours=24)  # way beyond SLA

    return df



if __name__ == "__main__":
    df = generate_orders(n_rows=100)

    print("=" * 55)
    print("GENERATED ORDERS DATASET")
    print("=" * 55)
    print(f"Total rows     : {len(df)}")
    print(f"Columns        : {list(df.columns)}")
    print()

    print("Sample (first 5 rows):")
    print(df.head().to_string())
    print()

    print("Injected issues summary:")
    print(f"  Null customer_id : {df['customer_id'].isna().sum()} rows")
    print(f"  Null price       : {df['price'].isna().sum()} rows")
    print(f"  Duplicate IDs    : {df['order_id'].duplicated().sum()} rows")
    print(f"  Negative qty     : {(df['quantity'] < 0).sum()} rows")
    print(f"  Invalid status   : {(~df['status'].isin(['pending','confirmed','shipped','delivered'])).sum()} rows")
    print("=" * 55)