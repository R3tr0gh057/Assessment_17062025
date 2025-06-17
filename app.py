import streamlit as st
import pandas as pd
from warehouse_management import SKUMapper, map_sales_data
import io
import logging
import tempfile
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('warehouse_management.log'),
        logging.StreamHandler()
    ]
)

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