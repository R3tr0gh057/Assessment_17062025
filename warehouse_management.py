import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple
from pathlib import Path

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
        self.combo_expansion_map = {}
        self.active_mskus = set()
        
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
            ci_df = pd.read_excel(self.excel_path, sheet_name="Current Inventory ", header=1)
            if 'msku' in ci_df.columns:
                self.current_inventory_df = ci_df[['msku']].rename(columns={'msku': 'MSKU'})
            else:
                raise ValueError("'msku' column not found in Current Inventory sheet")
            
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
        # Build SKU to MSKU mapping from both Chronology and Msku With Skus
        self._build_sku_to_msku_mapping()
        
        # Build combo expansion mapping
        self._build_combo_expansion_mapping()
        
        # Build active MSKU set
        self._build_active_msku_set()
    
    def _build_sku_to_msku_mapping(self) -> None:
        """Build mapping from SKUs to MSKUs using both Chronology and Msku With Skus sheets."""
        # Add mappings from Chronology sheet
        if not self.chronology_df.empty:
            for _, row in self.chronology_df.iterrows():
                if pd.notna(row['SKU']) and pd.notna(row['MSKU']):
                    self.sku_to_msku_map[str(row['SKU']).strip()] = str(row['MSKU']).strip()
        
        # Add mappings from Msku With Skus sheet
        if not self.msku_with_skus_df.empty:
            for _, row in self.msku_with_skus_df.iterrows():
                if pd.notna(row['SKU']) and pd.notna(row['MSKU']):
                    self.sku_to_msku_map[str(row['SKU']).strip()] = str(row['MSKU']).strip()
        
        logger.info(f"Built SKU to MSKU mapping with {len(self.sku_to_msku_map)} entries")
    
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
    
    def _build_active_msku_set(self) -> None:
        """Build set of active MSKUs from Current Inventory sheet."""
        if not self.current_inventory_df.empty:
            self.active_mskus = set(
                str(msku).strip() 
                for msku in self.current_inventory_df['MSKU'] 
                if pd.notna(msku)
            )
        logger.info(f"Built active MSKU set with {len(self.active_mskus)} entries")
    
    def expand_combo_msku(self, msku: str) -> List[str]:
        """
        Expand a combo MSKU into its constituent base SKUs.
        
        Args:
            msku (str): The combo MSKU to expand
            
        Returns:
            List[str]: List of base SKUs
        """
        return self.combo_expansion_map.get(msku, [msku])
    
    def is_valid_msku(self, msku: str) -> bool:
        """
        Check if an MSKU is valid (exists in current inventory).
        
        Args:
            msku (str): The MSKU to validate
            
        Returns:
            bool: True if MSKU is valid, False otherwise
        """
        return msku in self.active_mskus

    def map_sku_to_msku(self, sku: str) -> str:
        """
        Map a SKU to its corresponding MSKU using the mapping dictionary.
        Args:
            sku (str): The SKU to map
        Returns:
            str: The mapped MSKU, or the input if not found
        """
        return self.sku_to_msku_map.get(sku, sku)

def map_sales_data(df: pd.DataFrame, sku_mapper: SKUMapper) -> pd.DataFrame:
    """
    Maps sales data SKUs to MSKUs and expands combo products.
    
    Args:
        df: Sales DataFrame with SKU/MSKU and quantity columns
        sku_mapper: Initialized SKUMapper instance
        
    Returns:
        DataFrame with mapped MSKUs and expanded combos
    """
    expanded_rows = []
    skipped_rows = []
    skipped_count = 0
    
    # Detect the SKU/MSKU column based on keywords
    sku_column = None
    for col in df.columns:
        if 'sku' in col.lower():
            sku_column = col
            break
    if not sku_column:
        raise ValueError("No SKU/MSKU column found in the input DataFrame.")
    
    for _, row in df.iterrows():
        # Get the SKU/MSKU value
        msku = row[sku_column]
        # Convert quantity safely
        try:
            quantity = int(row['Quantity'])
        except Exception:
            logging.warning(f"Could not convert quantity for row: {row}")
            skipped_rows.append(row)
            skipped_count += 1
            continue
        # Use a default date if 'Date' column is missing
        date = row.get('Date', 'Unknown')
        
        # Skip if quantity is zero or negative
        if quantity <= 0:
            logging.warning(f"Skipping row with non-positive quantity: {row}")
            skipped_rows.append(row)
            skipped_count += 1
            continue
        
        # Skip if MSKU is invalid
        if not sku_mapper.is_valid_msku(msku):
            logging.warning(f"Invalid MSKU found in sales data: {msku}")
            continue
        
        # Handle combo MSKUs
        if msku in sku_mapper.combo_expansion_map:
            base_skus = sku_mapper.combo_expansion_map[msku]
            for base_sku in base_skus:
                if base_sku:  # Skip empty SKUs
                    new_row = {
                        'Date': date,
                        'Mapped MSKU': base_sku,
                        'Original SKU': msku,
                        'Quantity': quantity,
                        'Source': 'Combo Expansion'
                    }
                    expanded_rows.append(new_row)
        else:
            # Handle regular SKUs
            mapped_msku = sku_mapper.map_sku_to_msku(msku)
            if mapped_msku:
                new_row = {
                    'Date': date,
                    'Mapped MSKU': mapped_msku,
                    'Original SKU': msku,
                    'Quantity': quantity,
                    'Source': 'Direct Mapping'
                }
                expanded_rows.append(new_row)
            else:
                logging.warning(f"No mapping found for SKU: {msku}")
    
    # Save skipped rows with invalid quantities
    if skipped_rows:
        skipped_df = pd.DataFrame(skipped_rows)
        skipped_df.to_csv("invalid_sales_rows.csv", index=False)
        logging.info(f"Saved {skipped_count} skipped rows with invalid quantities to invalid_sales_rows.csv")
    else:
        logging.info("No rows skipped due to invalid quantities.")
    
    # Create DataFrame from expanded rows
    if expanded_rows:
        result_df = pd.DataFrame(expanded_rows)
        logging.info(f"Processed {len(df)} rows into {len(result_df)} rows after expansion (skipped {skipped_count} invalid quantity rows)")
        return result_df
    else:
        logging.warning("No valid mappings found in sales data")
        return pd.DataFrame(columns=['Date', 'Mapped MSKU', 'Original SKU', 'Quantity', 'Source'])

def main():
    """Main function to process sales data."""
    try:
        # Initialize SKU mapper
        sku_mapper = SKUMapper("WMS-04-02.xlsx")
        
        # Load and process sales data
        sales_df = pd.read_csv("meesho.csv")
        
        # Map sales data
        mapped_df = map_sales_data(sales_df, sku_mapper)
        
        # Save processed data
        mapped_df.to_csv("processed_sales.csv", index=False)
        logging.info("Successfully processed and saved sales data")
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main() 