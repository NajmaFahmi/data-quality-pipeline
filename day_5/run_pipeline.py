import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipelines.pipeline_orders import run_orders_pipeline
from jobs.run_trend import run_trend_analysis


if __name__ == "__main__":
    print("=" * 60)
    print("STEP 1 — ORDERS PIPELINE")
    print("=" * 60)
    run_orders_pipeline()

    print()
    print("=" * 60)
    print("STEP 2 — TREND ANALYSIS")
    print("=" * 60)
    run_trend_analysis(pipeline_name="orders_pipeline", simulate=True)