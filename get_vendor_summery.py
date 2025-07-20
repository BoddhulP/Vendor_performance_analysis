import pandas as pd
import os
import sqlite3
import time
import logging
from sqlalchemy import create_engine
from ingestion_db import ingest_db

# Ensure logs directory exists
if not os.path.exists('logs'):
    os.makedirs('logs')

# Reset previous logging handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
    
logging.basicConfig(
    filename='logs/get_vendor_summery.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

def create_vendor_summery(conn):
    '''Merge different tables to create vendor summary and return the dataframe.'''
    vendor_sale_summery = pd.read_sql_query(""" 
    WITH FreightSummery AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    
    PurchaseSummery AS (
        SELECT
            p.VendorNumber, 
            p.VendorName, 
            p.Brand, 
            p.PurchasePrice,
            p.Description,
            pp.Volume, 
            pp.Price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchasesQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p 
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    
    SalesSummery AS (
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(SalesQuantity) AS TotalSalesQuantity,
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
        ps.TotalPurchaseDollars,
        ps.Volume, 
        ps.TotalPurchasesQuantity, 
        ss.TotalSalesDollars, 
        ss.TotalSalesPrice, 
        ss.TotalExciseTax, 
        fs.FreightCost, 
        ss.TotalSalesQuantity
    FROM PurchaseSummery ps 
    LEFT JOIN SalesSummery ss ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummery fs ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)
    return vendor_sale_summery

def clean_data(df):
    '''Clean the data and create new analytical columns.'''
    df['Volume'] = df['Volume'].astype('float')
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Creating new columns
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchasesQuantity']
    df['SalespurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']
    
    return df

if __name__ == '__main__':
    start_time = time.time()
    logging.info("Process started.")

    # DB connection
    conn = sqlite3.connect('inventory.db')

    # Step 1: Create summary
    logging.info('Creating Vendor Summary Table...')
    summary_df = create_vendor_summery(conn)
    logging.info(summary_df.head())

    # Step 2: Clean data
    logging.info('Cleaning Data...')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    # Step 3: Ingest data
    logging.info('Ingesting Data...')
    ingest_db(clean_df, 'vendor_sale_summery', conn)
    logging.info('Data ingestion completed.')

    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"Start Time: {time.ctime(start_time)}")
    logging.info(f"End Time: {time.ctime(end_time)}")
    logging.info(f"Total Time Taken: {total_time:.2f} seconds")
    print("Process completed successfully.")
