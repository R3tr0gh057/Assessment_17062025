# Warehouse Management System MVP

This system processes sales data by mapping and validating SKUs using reference data from Excel sheets.

## Features

- Loads and processes data from multiple Excel sheets
- Maps SKUs to MSKUs using reference data
- Handles combo MSKUs by expanding them into individual SKUs
- Validates MSKUs against current inventory
- Processes sales data with error logging
- Saves cleaned and mapped data to CSV

## Requirements

- Python 3.8+
- Required packages listed in `requirements.txt`

## Installation

1. Clone this repository
2. Install required packages:
```bash
pip install -r requirements.txt
```

## Input Files Required

1. Excel file (`mapping_data.xlsx`) with the following sheets:
   - "Chronology"
   - "Current Inventory"
   - "Combos skus"
   - "Msku With Skus"

2. Sales data CSV file (`sales_data.csv`) with columns:
   - Date
   - MSKU
   - Quantity
   - Fulfillment Center
   - (other relevant columns)

## Usage

1. Place your input files in the project directory:
   - `mapping_data.xlsx`
   - `sales_data.csv`

2. Run the script:
```bash
python warehouse_management.py
```

3. The script will:
   - Process the input files
   - Map and validate SKUs
   - Expand combo MSKUs
   - Generate a cleaned output file: `cleaned_sales_data.csv`
   - Create a log file: `warehouse_management.log`

## Output

- `cleaned_sales_data.csv`: Processed sales data with expanded combo MSKUs
- `warehouse_management.log`: Detailed log of the processing steps and any errors

## Error Handling

The system logs warnings for:
- Invalid MSKUs found in sales data
- Missing or malformed input files
- Data processing errors

Check the log file for detailed information about any issues encountered during processing.
