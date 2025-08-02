import sqlite3
import pandas as pd
import csv

DB_PATH = 'Data_Engineer_ETL_Assignment.db'

def sql_solution():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    OUTPUT_CSV = 'SQL_output.csv'

    query = """
    SELECT
    c.customer_id AS Customer,
    c.age AS Age,
    i.item_name AS Item,
    SUM(o.quantity) AS Quantity
        FROM customers c
        JOIN sales s ON c.customer_id = s.customer_id
        JOIN orders o ON s.sales_id = o.sales_id
        JOIN items i ON o.item_id = i.item_id
        WHERE c.age BETWEEN 18 AND 35
        AND o.quantity IS NOT NULL
        GROUP BY c.customer_id, c.age, i.item_name
        HAVING SUM(o.quantity) > 0
        ORDER BY c.customer_id, i.item_name;
    """

    result = cursor.execute(query).fetchall()
    headers = [desc[0] for desc in cursor.description]

    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(headers)
        writer.writerows(result)

    print(f"SQL solution exported to {OUTPUT_CSV}")
    conn.close()

def pandas_solution():
    OUTPUT_CSV = 'pandas_output.csv'
    conn = sqlite3.connect(DB_PATH)

    df_customers = pd.read_sql_query("SELECT * FROM customers WHERE age BETWEEN 18 AND 35", conn)
    df_sales = pd.read_sql_query("SELECT * FROM sales", conn)
    df_orders = pd.read_sql_query("SELECT * FROM orders", conn)
    df_items = pd.read_sql_query("SELECT * FROM items", conn)

    # Join customers → sales → orders → items
    df = df_sales.merge(df_customers, on='customer_id')
    df = df.merge(df_orders, on='sales_id')
    df = df.merge(df_items, on='item_id')

    # Filter out null quantities
    df = df[df['quantity'].notnull()]
    df['quantity'] = df['quantity'].astype(int)

    # Group and summarize
    grouped = df.groupby(['customer_id', 'age', 'item_name'])['quantity'].sum().reset_index()
    grouped = grouped[grouped['quantity'] > 0]
    grouped.columns = ['Customer', 'Age', 'Item', 'Quantity']

    # Export
    grouped.to_csv(OUTPUT_CSV, sep=';', index=False)

    print(f"Pandas solution exported to {OUTPUT_CSV}")
    conn.close()

if __name__ == '__main__':
    print("Choose solution method:")
    print("1. SQL")
    print("2. Pandas")
    choice = input("Enter 1 or 2: ").strip()

    if choice == '1':
        sql_solution()
    elif choice == '2':
        pandas_solution()
    else:
        print("Invalid choice. Exiting.")
