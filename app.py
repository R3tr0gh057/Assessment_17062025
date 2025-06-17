import streamlit as st
import pandas as pd
import os
from datetime import datetime
from warehouse_management import SKUMapper, process_sales_data
import requests
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Baserow configuration
BASEROW_TOKEN = os.getenv('BASEROW_API_TOKEN')
BASEROW_URL = "https://api.baserow.io"
TABLE_ID = "577266"

def load_excel_data(file_path):
    """Load data from Excel file."""
    try:
        return pd.read_excel(file_path)
    except Exception as e:
        st.error(f"Error loading Excel file: {str(e)}")
        return None

def load_csv_data(file_path):
    """Load data from CSV file."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        st.error(f"Error loading CSV file: {str(e)}")
        return None

def push_to_baserow(df):
    """Push processed data to Baserow table."""
    if not BASEROW_TOKEN:
        st.error("Missing Baserow API token. Please check your .env file.")
        return False
    
    headers = {
        'Authorization': f'Token {BASEROW_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    # First, get existing records to check for duplicates
    try:
        response = requests.get(
            f"{BASEROW_URL}/api/database/rows/table/{TABLE_ID}/",
            headers=headers
        )
        response.raise_for_status()
        existing_records = response.json()
        # Check for duplicates based on SKU
        existing_skus = {record['field_4647812'] for record in existing_records.get('results', [])}
    except Exception as e:
        logger.error(f"Error fetching existing records: {str(e)}")
        st.error("Failed to fetch existing records from Baserow")
        return False
    
    success_count = 0
    error_count = 0
    
    for idx, row in df.iterrows():
        try:
            # Skip if this SKU already exists
            if row['SKU'] in existing_skus:
                logger.info(f"Skipping duplicate SKU: {row['SKU']}")
                continue
            
            # Convert date to YYYY-MM-DD format
            try:
                date_value = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
            except:
                date_value = datetime.now().strftime('%Y-%m-%d')
            
            # Convert quantity and stock to integers, with fallbacks
            try:
                quantity = int(float(row['Quantity']))
            except:
                quantity = 0
                
            try:
                stock_left = max(0, int(float(row['StockLeft'])))
            except:
                stock_left = 0
            
            # Prepare the data for Baserow with the correct field IDs
            data = {
                'field_4647810': date_value,  # Date in YYYY-MM-DD format
                'field_4647811': str(row['Source'])[:255],  # Source (truncate if too long)
                'field_4647812': str(row['SKU'])[:255],  # Sku (truncate if too long)
                'field_4647904': str(row['MSKU'])[:255],  # Msku (truncate if too long)
                'field_4647908': quantity,  # Quantity as integer
                'field_4647912': str(row['OrderID'])[:255],  # OrderID (truncate if too long)
                'field_4647913': stock_left  # StockLeft as positive integer
            }
            
            # Log the data being sent
            logger.info(f"Sending data for row {idx}: {data}")
            
            # Push to Baserow
            response = requests.post(
                f"{BASEROW_URL}/api/database/rows/table/{TABLE_ID}/",
                headers=headers,
                json=data
            )
            
            if response.status_code != 200 and response.status_code != 201:
                logger.error(f"Error response from Baserow: {response.text}")
                raise Exception(f"Baserow API returned status code {response.status_code}")
                
            response.raise_for_status()
            success_count += 1
            
        except Exception as e:
            logger.error(f"Error pushing row {idx} to Baserow: {str(e)}")
            error_count += 1
            continue
    
    if success_count > 0:
        st.success(f"Successfully pushed {success_count} records to Baserow")
    if error_count > 0:
        st.warning(f"Failed to push {error_count} records to Baserow")
    
    return success_count > 0

def main():
    st.title("Warehouse Management System")
    
    # File upload section
    st.header("Upload Files")
    
    # Excel file upload
    excel_file = st.file_uploader("Upload Excel File", type=['xlsx', 'xls'])
    if excel_file:
        excel_data = load_excel_data(excel_file)
        if excel_data is not None:
            st.success("Excel file loaded successfully!")
            st.write("Excel Data Preview:")
            st.dataframe(excel_data.head())
    
    # CSV file upload
    csv_file = st.file_uploader("Upload CSV File", type=['csv'])
    if csv_file:
        csv_data = load_csv_data(csv_file)
        if csv_data is not None:
            st.success("CSV file loaded successfully!")
            st.write("CSV Data Preview:")
            st.dataframe(csv_data.head())
    
    # Process button
    if st.button("Process Data"):
        if excel_file and csv_file:
            try:
                # Initialize SKUMapper with Excel data
                sku_mapper = SKUMapper(excel_file)
                
                # Process CSV data
                processed_df = process_sales_data(csv_data, sku_mapper, csv_file.name)
                
                # Display processed data
                st.write("Processed Data Preview:")
                st.dataframe(processed_df.head())
                
                # Save processed data to CSV
                output_filename = f"processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                processed_df.to_csv(output_filename, index=False)
                
                # Provide download link
                with open(output_filename, 'rb') as f:
                    st.download_button(
                        label="Download Processed Data",
                        data=f,
                        file_name=output_filename,
                        mime='text/csv'
                    )
                
                # Automatically push to Baserow
                st.write("Pushing data to Baserow...")
                if push_to_baserow(processed_df):
                    st.success("Data successfully pushed to Baserow!")
                else:
                    st.error("Failed to push data to Baserow. Check the logs for details.")
                
            except Exception as e:
                st.error(f"Error processing data: {str(e)}")
        else:
            st.warning("Please upload both Excel and CSV files to process data.")

if __name__ == "__main__":
    main() 