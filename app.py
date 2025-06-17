import streamlit as st
import pandas as pd
from warehouse_management import SKUMapper, map_sales_data
import io
import logging
import tempfile
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('warehouse_management.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()
BASEROW_API_TOKEN = os.getenv("BASEROW_API_TOKEN")

# Baserow configuration
BASEROW_TABLE_ID = 577180
BASEROW_BASE_URL = "https://api.baserow.io/api/database/rows/table"
BASEROW_HEADERS = {
    "Authorization": f"Token {BASEROW_API_TOKEN}",
    "Content-Type": "application/json"
}

def get_existing_entries():
    """Get existing entries from Baserow table."""
    try:
        response = requests.get(
            f"{BASEROW_BASE_URL}/{BASEROW_TABLE_ID}/",
            headers=BASEROW_HEADERS,
            params={"user_field_names": "false"}
        )
        if response.status_code == 200:
            return response.json()["results"]
        return []
    except Exception as e:
        logging.error(f"Error fetching existing entries: {str(e)}")
        return []

def push_to_baserow(df, source_file_name):
    """Push processed data to Baserow table."""
    success_count = 0
    error_count = 0
    skipped_count = 0
    errors = []
    
    # Get existing entries
    existing_entries = get_existing_entries()
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Create a set of existing SKUs
    existing_skus = {
        entry["field_4646908"]
        for entry in existing_entries
        if "field_4646908" in entry
    }

    for _, row in df.iterrows():
        try:
            original_sku = str(row['Original SKU'])
            
            # Check for duplicates - if SKU exists in Baserow, skip it
            if original_sku in existing_skus:
                skipped_count += 1
                continue
                
            # Prepare the data according to Baserow's field IDs
            payload = {
                "field_4646908": original_sku,  # original_sku
                "field_4646909": str(row['Mapped MSKU']) if pd.notna(row['Mapped MSKU']) else "",  # mapped_sku
                "field_4646910": int(float(row['Quantity'])),  # quantity - convert to integer
                "field_4646918": today,  # processed_date
                "field_4646926": "Mapped" if pd.notna(row['Mapped MSKU']) else "Unmapped",  # status (Mapped/Unmapped)
                "field_4646927": source_file_name  # source_file
            }

            # Make the API request
            response = requests.post(
                f"{BASEROW_BASE_URL}/{BASEROW_TABLE_ID}/",
                headers=BASEROW_HEADERS,
                json=payload
            )

            if response.status_code in [200, 201]:
                success_count += 1
                # Add to existing SKUs to prevent duplicates within the same batch
                existing_skus.add(original_sku)
            else:
                error_count += 1
                errors.append(f"Error for SKU {original_sku}: {response.text}")

        except Exception as e:
            error_count += 1
            errors.append(f"Error for SKU {row['Original SKU']}: {str(e)}")

    return {
        "success_count": success_count,
        "error_count": error_count,
        "skipped_count": skipped_count,
        "errors": errors
    }

def save_uploaded_file(uploaded_file):
    """Save an uploaded file to a temporary location and return the path."""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            return tmp_file.name
    except Exception as e:
        st.error(f"Error saving file: {str(e)}")
        return None

def main():
    st.set_page_config(
        page_title="Warehouse Management System",
        page_icon="🏭",
        layout="wide"
    )
    
    st.title("🏭 Warehouse Management System")
    st.markdown("""
    This application processes sales data by mapping SKUs to Master SKUs using a mapping file.
    Upload your mapping file (WMS-04-02.xlsx) and sales data CSV to get started.
    """)

    # File uploaders
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. Upload Mapping File")
        mapping_file = st.file_uploader(
            "Upload WMS-04-02.xlsx",
            type=['xlsx'],
            help="Upload the Excel file containing SKU mappings"
        )
    
    with col2:
        st.subheader("2. Upload Sales Data")
        sales_file = st.file_uploader(
            "Upload Sales CSV",
            type=['csv'],
            help="Upload the CSV file containing sales data"
        )

    # Process button
    if st.button("Process Sales Data", type="primary"):
        if mapping_file is None or sales_file is None:
            st.error("Please upload both the mapping file and sales data file.")
            return

        try:
            # Save the mapping file temporarily
            mapping_file_path = save_uploaded_file(mapping_file)
            if not mapping_file_path:
                return

            # Read the sales data
            sales_df = pd.read_csv(sales_file)

            # Process the data
            with st.spinner("Processing data..."):
                try:
                    # Create SKUMapper instance with the file path
                    mapper = SKUMapper(mapping_file_path)
                    
                    # Map the sales data
                    result_df = map_sales_data(sales_df, mapper)
                    
                    # Display results
                    st.success("Data processed successfully!")
                    
                    # Show preview
                    st.subheader("Processed Data Preview")
                    st.dataframe(result_df.head(10))
                    
                    # Show statistics
                    st.subheader("Processing Statistics")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Rows", len(result_df))
                    with col2:
                        mapped_count = result_df['Mapped MSKU'].notna().sum()
                        st.metric("Successfully Mapped", mapped_count)
                    with col3:
                        unmapped_count = result_df['Mapped MSKU'].isna().sum()
                        st.metric("Unmapped SKUs", unmapped_count)

                    # Push to Baserow
                    with st.spinner("Pushing data to Baserow..."):
                        baserow_results = push_to_baserow(result_df, sales_file.name)
                        
                        # Show Baserow upload results
                        st.subheader("Baserow Upload Results")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Successfully Uploaded", baserow_results["success_count"])
                        with col2:
                            st.metric("Failed Uploads", baserow_results["error_count"])
                        with col3:
                            st.metric("Skipped (Duplicates)", baserow_results["skipped_count"])
                        
                        if baserow_results["errors"]:
                            with st.expander("View Upload Errors"):
                                for error in baserow_results["errors"]:
                                    st.error(error)

                    # Download button
                    csv = result_df.to_csv(index=False)
                    st.download_button(
                        label="Download Processed Data",
                        data=csv,
                        file_name="processed_sales_data.csv",
                        mime="text/csv"
                    )
                finally:
                    # Clean up the temporary file
                    if os.path.exists(mapping_file_path):
                        os.unlink(mapping_file_path)

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            logging.error(f"Error in Streamlit app: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 