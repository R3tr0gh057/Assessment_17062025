import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional
from pathlib import Path
from datetime import datetime
import os
import requests

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

def fetch_baserow_stock_levels(api_token: str, table_id: str, baserow_url: str = "https://api.baserow.io") -> dict:
    """Fetch all current stock values from Baserow for every SKU/MSKU."""
    headers = {
        'Authorization': f'Token {api_token}',
        'Content-Type': 'application/json'
    }
    stock_map = {}
    try:
        response = requests.get(
            f"{baserow_url}/api/database/rows/table/{table_id}/",
            headers=headers
        )
        response.raise_for_status()
        records = response.json().get('results', [])
        for record in records:
            sku = str(record.get('field_4647812', '')).strip()
            msku = str(record.get('field_4647904', '')).strip()
            stock = int(record.get('field_4647913', 0))
            if msku:
                stock_map[msku] = stock
            elif sku:
                stock_map[sku] = stock
    except Exception as e:
        logger.error(f"Error fetching stock levels from Baserow: {str(e)}")
    return stock_map

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
    
    def get_msku(self, sku: str) -> Optional[str]:
        """Get MSKU for a given SKU."""
        return self.sku_to_msku_map.get(sku.strip(), None)
    
    def get_sku(self, msku: str) -> Optional[str]:
        """Get SKU for a given MSKU."""
        return self.msku_to_sku_map.get(msku.strip(), None)
    
    def set_stock_levels(self, stock_dict: dict):
        """Override stock_levels with values from an external dict (e.g., from Baserow)."""
        self.stock_levels.update(stock_dict)

def load_order_id_patterns() -> List[str]:
    """Load order ID patterns from configuration file."""
    patterns = []
    config_file = "order_id_patterns.txt"
    
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    # Skip empty lines and comments
                    if line and not line.startswith('#'):
                        patterns.append(line.lower())
        else:
            logger.warning(f"Configuration file {config_file} not found, using default patterns")
            # Fallback to default patterns
            patterns = [
                'order', 'orderid', 'order_id', 'order no', 'order_no', 'order number',
                'reference', 'referenceid', 'reference_id', 'reference no', 'reference_no',
                'invoice', 'invoiceid', 'invoice_id', 'invoice no', 'invoice_no',
                'transaction', 'transactionid', 'transaction_id',
                'purchase', 'purchaseid', 'purchase_id',
                'sale', 'saleid', 'sale_id',
                'id', 'itemid', 'item_id'
            ]
    except Exception as e:
        logger.error(f"Error loading order ID patterns: {str(e)}")
        # Fallback to basic patterns
        patterns = ['order', 'reference', 'invoice', 'id']
    
    logger.info(f"Loaded {len(patterns)} order ID patterns")
    return patterns

def analyze_dataframe_columns(df: pd.DataFrame) -> None:
    """Analyze and log information about DataFrame columns for debugging."""
    logger.info("=== DataFrame Column Analysis ===")
    logger.info(f"Total columns: {len(df.columns)}")
    logger.info(f"Column names: {list(df.columns)}")
    
    # Analyze each column
    for i, col in enumerate(df.columns):
        col_lower = col.lower()
        sample_values = df[col].dropna().head(3).tolist()
        logger.info(f"Column {i+1}: '{col}' (lowercase: '{col_lower}') - Sample values: {sample_values}")
    
    # Check for potential order ID columns
    patterns = load_order_id_patterns()
    potential_order_cols = []
    
    for col in df.columns:
        col_lower = col.lower()
        for pattern in patterns:
            if pattern in col_lower:
                potential_order_cols.append((col, pattern))
                break
    
    if potential_order_cols:
        logger.info(f"Potential order ID columns found: {potential_order_cols}")
    else:
        logger.warning("No potential order ID columns found!")
    
    logger.info("=== End Column Analysis ===")

def find_order_id_column(df: pd.DataFrame) -> Optional[str]:
    """Find the column that likely contains order IDs using dynamic patterns."""
    patterns = load_order_id_patterns()
    
    # Split patterns into exact and partial matches
    exact_patterns = []
    partial_patterns = []
    
    for pattern in patterns:
        # Check if pattern contains spaces or special characters (likely exact match)
        if ' ' in pattern or '_' in pattern or pattern in ['orderid', 'orderitemid', 'invoiceid', 'referenceid', 'transactionid', 'purchaseid', 'saleid', 'shipmentid']:
            exact_patterns.append(pattern)
        else:
            partial_patterns.append(pattern)
    
    # First, try exact matches (highest priority)
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in exact_patterns:
            logger.info(f"Found exact order ID column match: {col}")
            return col
    
    # Then, try partial matches but avoid date columns
    for col in df.columns:
        col_lower = col.lower()
        for pattern in partial_patterns:
            if pattern in col_lower:
                # Skip if this looks like a date column
                if any(date_word in col_lower for date_word in ['date', 'on', 'time', 'created', 'updated']):
                    logger.info(f"Skipping date-like column: {col}")
                    continue
                logger.info(f"Found partial order ID column match: {col} (matches pattern: {pattern})")
                return col
    
    # If no matches found, analyze columns for debugging
    analyze_dataframe_columns(df)
    return None

def load_date_column_patterns() -> List[str]:
    """Load date column patterns from configuration file."""
    patterns = []
    config_file = "date_column_patterns.txt"
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        patterns.append(line.lower())
        else:
            logger.warning(f"Configuration file {config_file} not found, using default patterns")
            patterns = [
                'date', 'order date', 'invoice date', 'dispatch date', 'shipped date', 'delivery date',
                'created date', 'updated date', 'date and time', 'datetime', 'timestamp',
                'order datetime', 'invoice datetime', 'created datetime', 'updated datetime'
            ]
    except Exception as e:
        logger.error(f"Error loading date column patterns: {str(e)}")
        patterns = ['date', 'date and time', 'datetime', 'timestamp']
    logger.info(f"Loaded {len(patterns)} date column patterns")
    return patterns

def find_date_column(df: pd.DataFrame) -> Optional[str]:
    """Find the column that likely contains the main date using dynamic patterns. Extract date from datetime if needed."""
    patterns = load_date_column_patterns()
    # Split patterns into exact and partial matches
    exact_patterns = []
    partial_patterns = []
    for pattern in patterns:
        if ' ' in pattern or '_' in pattern:
            exact_patterns.append(pattern)
        else:
            partial_patterns.append(pattern)
    # Helper to check if a column is datetime-like
    def is_datetime_col(col_name):
        col_lower = col_name.lower()
        return (
            ('date' in col_lower and 'time' in col_lower) or
            'datetime' in col_lower or
            'timestamp' in col_lower
        )
    # First, try exact matches (highest priority), skipping datetime-like columns
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in exact_patterns and not is_datetime_col(col):
            logger.info(f"Found exact date column match: {col}")
            return col
    # Then, try partial matches, skipping datetime-like columns
    for col in df.columns:
        col_lower = col.lower()
        for pattern in partial_patterns:
            if pattern in col_lower and not is_datetime_col(col):
                logger.info(f"Found partial date column match: {col} (matches pattern: {pattern})")
                return col
    # If no matches found, fallback to any column with 'date' in the name, skipping datetime-like columns
    for col in df.columns:
        if 'date' in col.lower() and not is_datetime_col(col):
            logger.info(f"Fallback: Found column with 'date' in name: {col}")
            return col
    # Last resort: look for datetime columns and extract date
    for col in df.columns:
        if is_datetime_col(col):
            logger.info(f"Found datetime column for date extraction: {col}")
            return col
    logger.warning(f"No date or datetime column found. Available columns: {list(df.columns)}")
    return None

def extract_date_from_datetime(date_value) -> str:
    """Extract date part from datetime value."""
    try:
        if pd.isna(date_value):
            return datetime.now().strftime('%Y-%m-%d')
        
        # Convert to datetime if it's a string
        if isinstance(date_value, str):
            # Try different datetime formats
            for fmt in ['%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
                try:
                    dt = datetime.strptime(date_value, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # If no specific format works, try pandas parsing
            try:
                dt = pd.to_datetime(date_value)
                return dt.strftime('%Y-%m-%d')
            except:
                pass
        
        # If it's already a datetime object
        elif hasattr(date_value, 'strftime'):
            return date_value.strftime('%Y-%m-%d')
        
        # Fallback
        return datetime.now().strftime('%Y-%m-%d')
        
    except Exception as e:
        logger.warning(f"Error extracting date from {date_value}: {str(e)}")
        return datetime.now().strftime('%Y-%m-%d')

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
    skipped_rows = 0
    duplicate_skus = 0
    empty_rows = 0
    invalid_quantity_rows = 0
    
    # Find the SKU/MSKU column
    sku_column = None
    for col in df.columns:
        if 'sku' in col.lower():
            sku_column = col
            break
    if not sku_column:
        raise ValueError("No SKU/MSKU column found in the input DataFrame.")
    
    # Find the order ID column with better logging
    order_id_column = find_order_id_column(df)
    if not order_id_column:
        logger.warning("No order ID column found, will generate order IDs")
    else:
        logger.info(f"Using '{order_id_column}' as order ID column")
    
    # Find the date column
    date_column = find_date_column(df)
    if not date_column:
        logger.warning("No date column found, using row number as date")
        logger.info(f"Available columns in DataFrame: {list(df.columns)}")
    else:
        logger.info(f"Using '{date_column}' as date column")
    
    # Check if the date column is a datetime column
    is_datetime_column = False
    if date_column:
        col_lower = date_column.lower()
        is_datetime_column = (
            ('date' in col_lower and 'time' in col_lower) or
            'datetime' in col_lower or
            'timestamp' in col_lower
        )
        if is_datetime_column:
            logger.info(f"Date column '{date_column}' is a datetime column, will extract date part")
    
    # Track processed SKUs to avoid duplicates
    processed_skus = set()
    
    for idx, row in df.iterrows():
        try:
            # Get basic information
            identifier = str(row[sku_column]).strip()
            
            # Handle NaN values in quantity
            if pd.isna(row['Quantity']):
                empty_rows += 1
                logger.info(f"Skipping row {idx + 1}: Empty quantity found (NaN value)")
                continue
                
            quantity = int(float(row['Quantity']))
            
            # Handle date extraction
            if date_column:
                if is_datetime_column:
                    date = extract_date_from_datetime(row[date_column])
                else:
                    try:
                        date = pd.to_datetime(row[date_column]).strftime('%Y-%m-%d')
                    except:
                        date = datetime.now().strftime('%Y-%m-%d')
            else:
                date = datetime.now().strftime('%Y-%m-%d')
            
            # Handle order ID
            if order_id_column and pd.notna(row[order_id_column]):
                order_id = str(row[order_id_column]).strip()
            else:
                order_id = f"GEN_{idx + 1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Check for duplicate SKUs in the same file
            if identifier in processed_skus:
                duplicate_skus += 1
                logger.info(f"Skipping row {idx + 1}: Duplicate SKU '{identifier}' found in data file")
                continue
            
            # Validate quantity
            if quantity <= 0:
                invalid_quantity_rows += 1
                logger.info(f"Skipping row {idx + 1}: Invalid quantity ({quantity}) - must be positive")
                continue
            
            # Get MSKU from mapper
            if sku_mapper:
                msku = sku_mapper.get_msku(identifier)
                if not msku:
                    logger.warning(f"Row {idx + 1}: No MSKU mapping found for SKU '{identifier}'")
                    msku = "UNKNOWN"
            else:
                msku = "NO_MAPPER"
            
            # Update stock level and get new stock left
            if sku_mapper and msku != "UNKNOWN" and msku != "NO_MAPPER":
                stock_left = sku_mapper.update_stock_level(msku, quantity)
            else:
                stock_left = max(0, quantity)
            
            # Create processed row
            processed_row = {
                'Date': date,
                'Source': source_file,
                'SKU': identifier,
                'MSKU': msku,
                'Quantity': quantity,
                'OrderID': order_id,
                'StockLeft': stock_left
            }
            
            processed_rows.append(processed_row)
            processed_skus.add(identifier)
            
        except Exception as e:
            skipped_rows += 1
            logger.error(f"Error processing row {idx + 1}: {str(e)}")
            continue
    
    # Log summary statistics
    logger.info(f"=== Processing Summary ===")
    logger.info(f"Total rows in file: {len(df)}")
    logger.info(f"Successfully processed: {len(processed_rows)} rows")
    logger.info(f"Skipped empty rows (NaN): {empty_rows} rows")
    logger.info(f"Skipped duplicate SKUs: {duplicate_skus} rows")
    logger.info(f"Skipped invalid quantities: {invalid_quantity_rows} rows")
    logger.info(f"Skipped due to errors: {skipped_rows} rows")
    
    if empty_rows > 0:
        logger.info(f"Note: {empty_rows} empty rows found in data file - these were skipped")
    if duplicate_skus > 0:
        logger.info(f"Note: {duplicate_skus} duplicate entries found - these were skipped")
    if invalid_quantity_rows > 0:
        logger.info(f"Note: {invalid_quantity_rows} rows with invalid quantities - these were skipped")
    
    return pd.DataFrame(processed_rows)

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
        mapped_df.to_csv("LocalOutput/processed_sales.csv", index=False)
        logging.info("Successfully processed and saved sales data")
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main() 