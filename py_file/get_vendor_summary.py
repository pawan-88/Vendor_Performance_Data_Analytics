import pandas as pd
from sqlalchemy import create_engine
import logging 
import numpy as np  # Added for safe calculations
# from injection_db import ingest_db  # Not strictly needed if using .to_sql()

# Setup logging configuration
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(engine):
    """
    This function merges different tables to get the overall vendor summary.
    """
    print("Generating Vendor Sales Summary...")
    
    # Execute the SQL Query
    vendor_sales_summary = pd.read_sql_query("""
        WITH FreightSummary AS (
            SELECT
                VendorNumber,
                SUM(Freight) AS FreightCost
            FROM vendor_invoice
            GROUP BY VendorNumber
        ),
        
        PurchaseSummary AS (
            SELECT 
                p.VendorNumber,
                p.VendorName,
                p.Brand,
                p.Description,
                p.PurchasePrice,
                pp.Price AS ActualPrice,
                pp.Volume,
                SUM(p.Quantity) AS TotalPurchaseQuantity,
                SUM(p.Dollars) AS TotalPurchaseDollars
            FROM purchases p 
            JOIN purchase_prices pp
                ON p.Brand = pp.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY 
                p.VendorNumber, 
                p.VendorName, 
                p.Brand, 
                p.Description, 
                p.PurchasePrice, 
                pp.Price,
                pp.Volume
        ),

        SalesSummary AS (
            SELECT 
                VendorNo,
                Brand,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
        )

        SELECT 
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.PurchasePrice,
            ps.ActualPrice,
            ps.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            COALESCE(ss.TotalSalesQuantity, 0) as TotalSalesQuantity,
            COALESCE(ss.TotalSalesDollars, 0) as TotalSalesDollars,
            COALESCE(ss.TotalSalesPrice, 0) as TotalSalesPrice,
            COALESCE(ss.TotalExciseTax, 0) as TotalExciseTax,
            COALESCE(fs.FreightCost, 0) as FreightCost
        FROM PurchaseSummary ps
        LEFT JOIN SalesSummary ss
            ON ps.VendorNumber = ss.VendorNo 
            AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs 
            ON ps.VendorNumber = fs.VendorNumber
        ORDER BY ps.TotalPurchaseDollars DESC
    """, engine)

    return vendor_sales_summary

def clean_data(df):
    """
    This function cleans the data and adds derived metrics.
    """
    print("Starting data cleaning...")

    # 1. Change datatype to float (coercing errors to NaN)
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')

    # 2. Fill missing values with 0
    df.fillna(0, inplace=True)

    # 3. Remove spaces from categorical columns
    if 'VendorName' in df.columns:
        df['VendorName'] = df['VendorName'].str.strip()
    if 'Description' in df.columns:
        df['Description'] = df['Description'].str.strip()

    # 4. Calculate Derived Metrics safely (handle div by zero)
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    
    # Use np.where to avoid division by zero errors
    df['ProfitMargin'] = np.where(
        df['TotalSalesDollars'] > 0, 
        (df['GrossProfit'] / df['TotalSalesDollars']) * 100, 
        0
    )

    df['StockTurnover'] = np.where(
        df['TotalPurchaseQuantity'] > 0,
        df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'],
        0
    )
    
    df['SalesToPurchaseRatio'] = np.where(
        df['TotalPurchaseDollars'] > 0,
        df['TotalSalesDollars'] / df['TotalPurchaseDollars'],
        0
    )

    # 5. Replace Infinity with 0 to prevent SQL errors
    df.replace([np.inf, -np.inf], 0, inplace=True)

    return df

if __name__ == '__main__':
    # --- FIX: DEFINE ENGINE HERE ---
    # Update connection string with your actual DB credentials
    connection_string = 'mysql+pymysql://root:root@localhost:3306/inventory'
    engine = create_engine(connection_string)
    
    print("Database Engine Created.")
    logging.info('Creating Vendor Summary Table.....')

    # 2. Create the summary dataframe
    try:
        summary_df = create_vendor_summary(engine)
        logging.info(f"Summary DataFrame Created with shape: {summary_df.shape}")
        print(summary_df.head())
    except Exception as e:
        logging.error(f"Error creating summary: {e}")
        exit()

    logging.info('Cleaning Data.....')

    # 3. Clean the data
    clean_df = clean_data(summary_df)
    logging.info("Data Cleaning Complete.")
    print("Data Cleaned.")

    logging.info('Ingesting data into Database.....')

    # 4. Ingest the final clean data
    try:
        clean_df.to_sql('vendor_sales_summary', engine, if_exists='replace', index=False)
        logging.info("Ingestion Completed Successfully!")
        print("Success! Data saved to 'vendor_sales_summary' table.")
    except Exception as e:
        logging.error(f"Error during ingestion: {e}")
        print(f"Error saving to DB: {e}")
