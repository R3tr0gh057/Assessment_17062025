# Warehouse Management System

A modern warehouse management system that processes sales data by mapping SKUs to Master SKUs, with automatic stock tracking and Baserow integration.

## 🏗️ Tech Stack

- **Frontend**: Streamlit (Python web framework)
- **Backend**: Python
- **Data Processing**: Pandas, NumPy
- **Database**: Baserow (API-based database)
- **Authentication**: Environment variables (.env)
- **Logging**: Python's built-in logging module

## 🤖 AI Tools Used

This project was built using Cursor IDE's AI capabilities:
1. **Code Generation**: Used AI to generate the initial project structure and core functionality
2. **Code Refactoring**: AI-assisted in improving code quality and implementing best practices
3. **Error Handling**: AI helped identify and fix bugs, particularly in data processing and API integration
4. **UI/UX Design**: AI suggested improvements for the Streamlit interface
5. **Documentation**: AI assisted in creating this comprehensive README

## 🚀 Features

- Excel and CSV file processing
- SKU to MSKU mapping
- Automatic stock level tracking
- Duplicate detection and prevention
- Real-time data validation
- Baserow database integration
- Modern, responsive UI
- Comprehensive error handling
- Detailed logging

## 🛠️ Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd warehouse-management-system
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   Create a `.env` file in the root directory with:
   ```
   BASEROW_API_TOKEN=your_baserow_api_token
   ```

5. **Run the application**
   ```bash
   streamlit run app.py
   ```

## 📋 Requirements

Create a `requirements.txt` file with:
```
streamlit
pandas
numpy
python-dotenv
requests
openpyxl
```

## 💻 Usage

1. **Launch the Application**
   - Run `streamlit run app.py`
   - The application will open in your default web browser

2. **Upload Files**
   - Upload your Excel mapping file (containing SKU mappings)
   - Upload your CSV sales data file

3. **Process Data**
   - Click the "Process Data" button
   - The system will:
     - Map SKUs to MSKUs
     - Track stock levels
     - Validate data
     - Push to Baserow

4. **View Results**
   - Preview processed data
   - Download processed data as CSV
   - View Baserow integration status

## 📊 Data Structure

### Excel Mapping File
Should contain the following sheets:
- Chronology
- Current Inventory
- Combos skus
- Msku With Skus

### CSV Sales Data
Should contain:
- Date
- SKU/MSKU
- Quantity
- OrderID

### Baserow Table Structure
- Date (field_4647810)
- Source (field_4647811)
- Sku (field_4647812)
- Msku (field_4647904)
- Quantity (field_4647908)
- OrderID (field_4647912)
- StockLeft (field_4647913)

## 🔍 Error Handling

The system includes comprehensive error handling for:
- File format validation
- Data type conversion
- API communication
- Duplicate detection
- Stock level validation

## 📝 Logging

Logs are stored in:
- `app.log`: Application logs
- `warehouse_management.log`: Core processing logs

## Demo Video
https://drive.google.com/file/d/1EPAHLgf-8Fs1dvHs47Bw7SLKqPZr8L-6/view?usp=sharing