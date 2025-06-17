import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('warehouse_management.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SKUMapper:
    def __init__(self, excel_path: str):
        """
        Initialize SKUMapper with data from Excel file.
        
        Args:
            excel_path (str): Path to the Excel file containing mapping data
        """
        self.excel_path = excel_path
        self.chronology_df = None
        self.current_inventory_df = None
        self.combos_df = None
        self.msku_with_skus_df = None
        
        # Load all required sheets
        self._load_excel_data()
        
        # Initialize mapping dictionaries
        self.sku_to_msku_map = {}
        self.msku_to_sku_map = {}
        self.combo_expansion_map = {}
        self.stock_levels = {}
        
        # Build mapping dictionaries
        self._build_mappings()
    
    def _load_excel_data(self) -> None:
        """Load all required sheets from the Excel file."""
        try:
            # Load Chronology sheet and clean it
            self.chronology_df = pd.read_excel(self.excel_path, sheet_name="Chronology")
            # Find the row with column headers (sku, msku)
            header_row = self.chronology_df[self.chronology_df['Unnamed: 7'] == 'sku'].index[0]
            self.chronology_df = self.chronology_df.iloc[header_row+1:].reset_index(drop=True)
            self.chronology_df = self.chronology_df[['Unnamed: 7', 'Unnamed: 8']].rename(
                columns={'Unnamed: 7': 'SKU', 'Unnamed: 8': 'MSKU'}
            )
            
            # Load Current Inventory sheet
            self.current_inventory_df = pd.read_excel(self.excel_path, sheet_name="Current Inventory ", header=1)
            if 'msku' not in self.current_inventory_df.columns or 'Opening Stock' not in self.current_inventory_df.columns:
                raise ValueError("Required columns not found in Current Inventory sheet")
            
            # Load Combos skus sheet
            self.combos_df = pd.read_excel(self.excel_path, sheet_name="Combos skus")
            # The structure is different - Combo column contains the combo MSKU and SKU1-SKU14 contain the base SKUs
            self.combos_df = self.combos_df[['Combo ', 'SKU1']].rename(columns={
                'Combo ': 'Combo_MSKU',
                'SKU1': 'Base_SKU'
            })
            
            # Load Msku With Skus sheet
            self.msku_with_skus_df = pd.read_excel(self.excel_path, sheet_name="Msku With Skus")
            # The structure is different - columns are already named correctly
            self.msku_with_skus_df = self.msku_with_skus_df[['sku', 'msku']].rename(columns={
                'sku': 'SKU',
                'msku': 'MSKU'
            })
            
            logger.info("Successfully loaded all Excel sheets")
        except Exception as e:
            logger.error(f"Error loading Excel data: {str(e)}")
            raise
    
    def _build_mappings(self) -> None:
        """Build all necessary mapping dictionaries from the loaded data."""
        # Build SKU to MSKU and MSKU to SKU mappings
        self._build_sku_msku_mappings()
        
        # Build combo expansion mapping
        self._build_combo_expansion_mapping()
        
        # Build stock levels
        self._build_stock_levels()
    
    def _build_sku_msku_mappings(self) -> None:
        """Build bidirectional mappings between SKUs and MSKUs."""
        # Add mappings from Chronology sheet
        if not self.chronology_df.empty:
            for _, row in self.chronology_df.iterrows():
                if pd.notna(row['SKU']) and pd.notna(row['MSKU']):
                    sku = str(row['SKU']).strip()
                    msku = str(row['MSKU']).strip()
                    self.sku_to_msku_map[sku] = msku
                    self.msku_to_sku_map[msku] = sku
        
        # Add mappings from Msku With Skus sheet
        if not self.msku_with_skus_df.empty:
            for _, row in self.msku_with_skus_df.iterrows():
                if pd.notna(row['SKU']) and pd.notna(row['MSKU']):
                    sku = str(row['SKU']).strip()
                    msku = str(row['MSKU']).strip()
                    self.sku_to_msku_map[sku] = msku
                    self.msku_to_sku_map[msku] = sku
        
        logger.info(f"Built SKU/MSKU mappings with {len(self.sku_to_msku_map)} entries")
    
    def _build_combo_expansion_mapping(self) -> None:
        """Build mapping for expanding combo MSKUs into individual SKUs."""
        if not self.combos_df.empty:
            for _, row in self.combos_df.iterrows():
                if pd.notna(row['Combo_MSKU']) and pd.notna(row['Base_SKU']):
                    combo_msku = str(row['Combo_MSKU']).strip()
                    base_sku = str(row['Base_SKU']).strip()
                    if combo_msku not in self.combo_expansion_map:
                        self.combo_expansion_map[combo_msku] = []
                    self.combo_expansion_map[combo_msku].append(base_sku)
        
        logger.info(f"Built combo expansion mapping with {len(self.combo_expansion_map)} entries")
    
    def _build_stock_levels(self) -> None:
        """Build current stock levels from Current Inventory sheet."""
        if not self.current_inventory_df.empty:
            for _, row in self.current_inventory_df.iterrows():
                if pd.notna(row['msku']) and pd.notna(row['Opening Stock']):
                    msku = str(row['msku']).strip()
                    stock = int(row['Opening Stock'])
                    self.stock_levels[msku] = stock
        logger.info(f"Built stock levels with {len(self.stock_levels)} entries")
    
    def get_stock_level(self, msku: str) -> int:
        """Get current stock level for an MSKU."""
        return self.stock_levels.get(msku, 0)
    
    def update_stock_level(self, msku: str, quantity: int) -> int:
        """Update stock level for an MSKU and return new level."""
        current_stock = self.stock_levels.get(msku, 0)
        new_stock = max(0, current_stock - quantity)  # Ensure stock doesn't go below 0
        self.stock_levels[msku] = new_stock
        return new_stock

def find_order_id_column(df: pd.DataFrame) -> Optional[str]:
    """Find the column that likely contains order IDs."""
    possible_columns = ['order', 'orderid', 'order_id', 'order no', 'order_no', 'order number']
    for col in df.columns:
        if any(keyword in col.lower() for keyword in possible_columns):
            return col
    return None

def process_sales_data(df: pd.DataFrame, sku_mapper: SKUMapper, source_file: str) -> pd.DataFrame:
    """
    Process sales data according to the new requirements.
    
    Args:
        df: Sales DataFrame
        sku_mapper: Initialized SKUMapper instance
        source_file: Name of the source CSV file
        
    Returns:
        DataFrame with processed data
    """
    processed_rows = []
    
    # Find the SKU/MSKU column
    sku_column = None
    for col in df.columns:
        if 'sku' in col.lower():
            sku_column = col
            break
    if not sku_column:
        raise ValueError("No SKU/MSKU column found in the input DataFrame.")
    
    # Find the order ID column
    order_id_column = find_order_id_column(df)
    if not order_id_column:
        logger.warning("No order ID column found, using row number as order ID")
    
    # Find the date column
    date_column = 'Date' if 'Date' in df.columns else df.columns[0]  # Use first column if no Date column
    
    for idx, row in df.iterrows():
        try:
            # Get basic information
            identifier = str(row[sku_column]).strip()
            quantity = int(float(row['Quantity']))
            date = row[date_column]
            order_id = str(row[order_id_column]) if order_id_column else f"ORDER_{idx}"
            
            # Skip if quantity is invalid
            if quantity <= 0:
                logger.warning(f"Skipping row with invalid quantity: {row}")
                continue
            
            # Determine if identifier is SKU or MSKU and get the corresponding mapping
            if identifier in sku_mapper.sku_to_msku_map:
                # It's a SKU, get the MSKU
                msku = sku_mapper.sku_to_msku_map[identifier]
                sku = identifier
            elif identifier in sku_mapper.msku_to_sku_map:
                # It's an MSKU, get the SKU
                sku = sku_mapper.msku_to_sku_map[identifier]
                msku = identifier
            else:
                # Not found in either mapping
                logger.warning(f"Identifier not found in mappings: {identifier}")
                continue
            
            # Update stock level
            stock_left = sku_mapper.update_stock_level(msku, quantity)
            
            # Create processed row
            processed_row = {
                'Date': date,
                'Source': source_file,
                'SKU': sku,
                'MSKU': msku,
                'Quantity': quantity,
                'OrderID': order_id,
                'StockLeft': stock_left
            }
            processed_rows.append(processed_row)
            
        except Exception as e:
            logger.error(f"Error processing row {idx}: {str(e)}")
            continue
    
    # Create and return the processed DataFrame
    result_df = pd.DataFrame(processed_rows)
    return result_df

def main():
    """Main function to process sales data."""
    try:
        # Initialize SKU mapper
        sku_mapper = SKUMapper("WMS-04-02.xlsx")
        
        # Load and process sales data
        sales_df = pd.read_csv("meesho.csv")
        
        # Map sales data
        mapped_df = process_sales_data(sales_df, sku_mapper, "meesho.csv")
        
        # Save processed data
        mapped_df.to_csv("processed_sales.csv", index=False)
        logging.info("Successfully processed and saved sales data")
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main() 