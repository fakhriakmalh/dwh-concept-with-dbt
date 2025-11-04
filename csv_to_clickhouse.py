import os
import glob
import pandas as pd
import clickhouse_connect
from typing import Dict
import time
import chardet

# ClickHouse connection settings
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'password123'
CLICKHOUSE_DATABASE = 'bronze'

# CSV files directory
CSV_DIR = './csv_files'
OUTPUT_DIR = './csv_files_utf8'

def get_clickhouse_type(dtype) -> str:
    """Convert pandas dtype to ClickHouse data type"""
    dtype_str = str(dtype)
    
    if 'int64' in dtype_str:
        return 'Int64'
    elif 'int32' in dtype_str or 'int' in dtype_str:
        return 'Int32'
    elif 'float64' in dtype_str or 'float' in dtype_str:
        return 'Float64'
    elif 'bool' in dtype_str:
        return 'UInt8'
    elif 'datetime' in dtype_str:
        return 'DateTime'
    elif 'object' in dtype_str:
        return 'String'
    else:
        return 'String'

def wait_for_clickhouse(client, max_retries=30, delay=2):
    """Wait for ClickHouse to be ready"""
    for i in range(max_retries):
        try:
            client.command('SELECT 1')
            print("✅ ClickHouse is ready!")
            return True
        except Exception as e:
            print(f"⏳ Waiting for ClickHouse... ({i+1}/{max_retries})")
            time.sleep(delay)
    
    raise Exception("❌ ClickHouse failed to start")

def create_database(client):
    """Create database if not exists"""
    try:
        client.command(f'CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}')
        print(f"✅ Database '{CLICKHOUSE_DATABASE}' created/verified")
    except Exception as e:
        print(f"❌ Error creating database: {e}")
        raise

def sanitize_column_name(col_name: str) -> str:
    """Sanitize column names for ClickHouse"""
    # Remove special characters and spaces
    col_name = col_name.strip()
    col_name = col_name.replace(' ', '_')
    col_name = col_name.replace('-', '_')
    col_name = col_name.replace('(', '')
    col_name = col_name.replace(')', '')
    col_name = col_name.replace('/', '_')
    col_name = col_name.replace('\\', '_')
    col_name = col_name.replace('.', '_')
    
    return col_name

def get_table_name_from_file(file_path: str) -> str:
    """Extract table name from CSV file name"""
    # Get filename without path and extension
    filename = os.path.basename(file_path)
    table_name = os.path.splitext(filename)[0]
    
    # Remove 'AdventureWorks_' prefix if exists
    if table_name.startswith('AdventureWorks_'):
        table_name = table_name.replace('AdventureWorks_', '', 1)
    
    # Sanitize table name
    table_name = sanitize_column_name(table_name)
    
    return table_name

def create_table_from_csv(client, csv_file: str):
    """Create ClickHouse table from CSV file and import data"""
    
    table_name = get_table_name_from_file(csv_file)
    
    print(f"\n{'='*60}")
    print(f"📁 Processing: {os.path.basename(csv_file)}")
    print(f"📊 Target table: {table_name}")
    
    try:
        # Read CSV to infer schema
        # Strategy 1: Try common encodings with error skip
        encodings = ['utf-8', 'iso-8859-1', 'cp1252', 'windows-1252']
        for encoding in encodings:
            try:
                print(f"   Trying encoding: {encoding}")
                df = pd.read_csv(csv_file, 
                                 encoding=encoding, 
                                 on_bad_lines='skip')

                print(f"   ✅ Successfully read with encoding: {encoding}")
                final_encoding = encoding
                read_success = True
                break
                # return df, encoding
            except Exception as e:
                continue
        
        if not read_success:
            print(f"❌ Failed to read CSV with common encodings: {csv_file}")
            return # Keluar dari fungsi jika gagal membaca skema

        if df.empty:
            print(f"⚠️  Skipping empty file or failed schema inference: {csv_file}")
            return
        
        # print(df.info())

        # Sanitize column names
        df.columns = [sanitize_column_name(col) for col in df.columns]
        
        # Create column definitions
        columns = []
        for col_name, dtype in df.dtypes.items():
            ch_type = get_clickhouse_type(dtype)
            columns.append(f"`{col_name}` Nullable({ch_type})")
        
        # Drop table if exists
        drop_query = f"DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{table_name}"
        client.command(drop_query)
        
        # Create table
        create_query = f"""
        CREATE TABLE {CLICKHOUSE_DATABASE}.{table_name}
        (
            {', '.join(columns)}
        )
        ENGINE = MergeTree()
        ORDER BY tuple()
        """
        
        print("   Creating table...")
        client.command(create_query)
        print(f"   ✅ Table created: {table_name}")
        
        # Read full CSV and import
        print("   Reading full CSV file...")
        df_full = pd.read_csv(csv_file, encoding=final_encoding, 
                                 on_bad_lines='skip')
        df_full.columns = [sanitize_column_name(col) for col in df_full.columns]
        
        # Replace NaN with None for ClickHouse
        df_full = df_full.where(pd.notnull(df_full), None)
        
        print(f"   Importing {len(df_full)} rows...")
        client.insert_df(f'{CLICKHOUSE_DATABASE}.{table_name}', df_full)
        
        # Verify import
        count = client.command(f'SELECT count() FROM {CLICKHOUSE_DATABASE}.{table_name}')
        print(f"   ✅ Imported {count} rows successfully")
        
        # Show sample data
        sample = client.query(f'SELECT * FROM {CLICKHOUSE_DATABASE}.{table_name} LIMIT 3')
        print(f"   📋 Sample data preview:")
        print(f"      Columns: {', '.join(df_full.columns[:5])}{'...' if len(df_full.columns) > 5 else ''}")
        
    except Exception as e:
        print(f"   ❌ Error processing {csv_file}: {e}")
        raise


def detect_encoding(file_path):
    """Detect file encoding using chardet"""
    with open(file_path, 'rb') as f:
        raw_data = f.read(100000)  # Read first 100KB
        result = chardet.detect(raw_data)
        return result['encoding'], result['confidence']

def convert_to_utf8(input_file, output_file):
    """Convert CSV file to UTF-8 encoding"""
    try:
        # Detect encoding
        encoding, confidence = detect_encoding(input_file)
        print(f"   Detected: {encoding} (confidence: {confidence:.2%})")
        
        # Read with detected encoding
        with open(input_file, 'r', encoding=encoding, errors='replace') as f:
            content = f.read()
        
        # Write as UTF-8
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"   ✅ Converted to UTF-8")
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    

def main():
    """Main function to import all CSV files"""


    print("="*60)
    print("🚀 ClickHouse CSV Importer for AdventureWorks")
    print("="*60)
    
    # Connect to ClickHouse
    print("\n📡 Connecting to ClickHouse...")
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        
        # Wait for ClickHouse to be ready
        wait_for_clickhouse(client)
        
        # Create database
        create_database(client)
        
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        print("\n💡 Make sure ClickHouse is running:")
        print("   docker-compose up -d")
        return
    
    # Find all CSV files
    csv_pattern = os.path.join(CSV_DIR, '*.csv')
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        print(f"\n⚠️  No CSV files found in {CSV_DIR}")
        print(f"💡 Please place your AdventureWorks CSV files in: {CSV_DIR}")
        return
    
    print(f"\n📂 Found {len(csv_files)} CSV file(s)")
    
    # Process each CSV file
    success_count = 0
    failed_count = 0
    
    for csv_file in csv_files:
        try:
            create_table_from_csv(client, csv_file)
            success_count += 1
        except Exception as e:
            failed_count += 1
            print(f"❌ Failed to process {csv_file}")
    
    # Summary
    print("\n" + "="*60)
    print("📊 IMPORT SUMMARY")
    print("="*60)
    print(f"✅ Successfully imported: {success_count} table(s)")
    print(f"❌ Failed: {failed_count} table(s)")
    print(f"📁 Total files processed: {len(csv_files)}")
    
    # List all tables
    print("\n📋 Tables in database:")
    try:
        tables = client.query(f'SHOW TABLES FROM {CLICKHOUSE_DATABASE}')
        for table in tables.result_rows:
            count = client.command(f'SELECT count() FROM {CLICKHOUSE_DATABASE}.{table[0]}')
            print(f"   • {table[0]}: {count:,} rows")
    except Exception as e:
        print(f"   ❌ Error listing tables: {e}")
    
    print("\n✨ Done!")
    print(f"🔗 Access ClickHouse at: http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")

if __name__ == '__main__':
    main()