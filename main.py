import PostgreSQLHandler
from src.data_fetcher import run_strategy_until_deadline
import pandas as pd
import PostgreSQLHandler

def main() -> bool:
    data, ok = run_strategy_until_deadline()

    if not ok:
        print("No signal found")
        return False

    try:
        if data is not None and not data.empty:
            PostgreSQLHandler.insert_pandas_into_db(data, "fact_signal")
    except Exception as e:
        print(f"make_prediction :: Error inserting imbalance data into db: {e}")
        return False

    return True



if __name__ == "__main__":
    main()