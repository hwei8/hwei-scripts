import sys
import os
import pandas as pd
import sqlite3

def load_data(source, table=None):
    ext = os.path.splitext(source)[1].lower()

    if ext == ".csv":
        print(f"Loading CSV: {source}")
        return pd.read_csv(source)

    elif ext in [".db", ".sqlite", ".sqlite3"]:
        if not table:
            raise ValueError("For SQLite sources, you must specify --table <table_name>")

        print(f"Loading SQLite: {source}, table={table}")
        conn = sqlite3.connect(source)
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        conn.close()
        return df

    else:
        raise ValueError("Unsupported file type. Supported: .csv, .db, .sqlite")


def analyze_columns(df):
    print("\n=== COLUMN ANALYSIS ===\n")
    for col in df.columns:
        unique_values = df[col].dropna().unique()

        print(f"Column: {col}")
        print(f" - Unique count: {len(unique_values)}")

        if len(unique_values) == 1:
            print(f" - Warning: Only one unique value: {unique_values[0]}")
        else:
            print(" - OK: Multiple unique values")

        print()
        

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python check_columns.py <data_source> [--table table_name]")
        print("\nExamples:")
        print("  python check_columns.py data.csv")
        print("  python check_columns.py database.sqlite --table mytable")
        return

    source = sys.argv[1]
    table = None

    if "--table" in sys.argv:
        idx = sys.argv.index("--table")
        if idx + 1 >= len(sys.argv):
            raise ValueError("Missing table name after --table")
        table = sys.argv[idx + 1]

    df = load_data(source, table)
    analyze_columns(df)


if __name__ == "__main__":
    main()