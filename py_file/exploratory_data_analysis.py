# Exploratory Data Analysis
'''Understanding the dataset to explore how the data is present in the db and 
    if there is a need of creating some aggregated tables that can help with:-
      
      * Vendor Selection for Profitability
    *  Product Pricing Optimization '''

import pandas as pd
from sqlalchemy import create_engine

# Create database connection
connection_string = "mysql+mysqlconnector://root:root@localhost/inventory"
engine = create_engine(connection_string)

# Checks table present in the db
tables = pd.read_sql_query("SHOW TABLES", engine)

# Display the result
#print(tables)

for table in tables.iloc[:, 0]:
    #print('-'*50, f'{table}', '-'*50)
    # added backticks `{table}` to handle table names with spaces or reserved words
    count_df = pd.read_sql(f"SELECT count(*) as count FROM `{table}`", engine)
    
    # Print the result
    #print('Count of records:', count_df['count'].values[0])
    #print(pd.read_sql(f"select * from {table} limit 5", engine))

 # 1. Load the data into the 'purchases' DataFrame
    # purchases = pd.read_sql_query("select * from purchases where VendorNumber = 4466", engine)
    # print("--- Raw Purchases ---")
    # print(purchases)

    # # 2. FIX: Group by the DataFrame 'purchases', not the function
    # new_purchase = purchases.groupby(['Brand', 'PurchasePrice'])[['Quantity', 'Dollars']].sum()
    # print("\n--- Aggregated Purchases ---")
    # print(new_purchase)

# 3. FIX: For the sales line, you need an aggregation (like .sum) and to print it
# Note: Ensure the variable 'sales' is defined/loaded before running this line
# --- Analysis 2: Sales ---
    # sales = pd.read_sql_query("select * from sales", engine)
    # if 'sales' in locals():
    #   sales_summary = sales.groupby('Brand')[['SalesDollars', 'SalesPrice']].sum()
    #   print("\n--- Sales Summary ---")
    #   print(sales_summary)
    # else:
    #   print("\nError: The variable 'sales' is not defined.")

#Get Column name from Vendor Invoice Table
df_column = pd.read_sql_query("SELECT * FROM vendor_invoice LIMIT 0", engine)
print("Vendor-Invoice Column:-",df_column.columns.to_list())