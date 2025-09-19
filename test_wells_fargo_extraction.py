import pandas as pd
import tabula
import pdfplumber

def test_wells_fargo_extraction(pdf_path):
    """Test script to see exactly what we get from Wells Fargo PDF extraction"""
    
    print("=" * 80)
    print("WELLS FARGO PDF EXTRACTION TEST")
    print("=" * 80)
    print(f"Testing file: {pdf_path}")
    print()
    
    # Test 1: Tabula extraction
    print("🔍 TABULA EXTRACTION:")
    print("-" * 40)
    
    try:
        # Extract all tables
        tables = tabula.read_pdf(
            pdf_path, 
            pages="all", 
            multiple_tables=True,
            pandas_options={"header": None}
        )
        
        print(f"Found {len(tables)} tables")
        print()
        
        # Examine each table
        for i, df in enumerate(tables):
            print(f"📊 TABLE {i}:")
            print(f"   Shape: {df.shape[0]} rows x {df.shape[1]} columns")
            print(f"   Columns: {list(df.columns)}")
            print()
            
            # Show first 10 rows
            print("   First 10 rows:")
            print(df.head(10))
            print()
            
            # Show data types
            print("   Data types:")
            for col_idx, col in enumerate(df.columns):
                print(f"     Column {col_idx}: {df[col].dtype}")
            print()
            
            # Look for potential Wells Fargo transaction table
            if 4 <= df.shape[1] <= 7 and df.shape[0] > 10:
                print("   ⭐ POTENTIAL WELLS FARGO TRANSACTION TABLE!")
                print("   Let's examine this one closely...")
                examine_potential_wf_table(df, i)
            
            print("-" * 60)
            
    except Exception as e:
        print(f"❌ Tabula extraction error: {e}")
    
    print("=" * 80)

def examine_potential_wf_table(df, table_index):
    """Closely examine a table that might be the Wells Fargo transaction table"""
    
    print(f"\n🔬 DETAILED ANALYSIS OF TABLE {table_index}:")
    print(f"   Shape: {df.shape}")
    
    # Look at each column
    for col_idx in range(min(6, df.shape[1])):  # Look at first 6 columns max
        col_data = df.iloc[:, col_idx]
        print(f"\n   📋 COLUMN {col_idx} ANALYSIS:")
        
        # Sample values (non-null)
        sample_values = col_data.dropna().head(10).tolist()
        print(f"      Sample values: {sample_values}")
        
        # Count different types of data
        total_non_null = col_data.notna().sum()
        null_count = col_data.isna().sum()
        
        print(f"      Non-null count: {total_non_null}")
        print(f"      Null/empty count: {null_count}")
        
        # Analyze what type of data this column contains
        if total_non_null > 0:
            # Check for dates
            date_like = sum(1 for val in sample_values if looks_like_date(str(val)))
            
            # Check for amounts
            amount_like = sum(1 for val in sample_values if looks_like_amount(str(val)))
            
            # Check for check numbers (short digits)
            check_like = sum(1 for val in sample_values if looks_like_check_number(str(val)))
            
            print(f"      Date-like values: {date_like}/{len(sample_values)}")
            print(f"      Amount-like values: {amount_like}/{len(sample_values)}")
            print(f"      Check-like values: {check_like}/{len(sample_values)}")
            
            # Guess what this column is
            if date_like > len(sample_values) * 0.5:
                print(f"      🗓️  LIKELY: DATE COLUMN")
            elif amount_like > len(sample_values) * 0.5:
                print(f"      💰 LIKELY: AMOUNT COLUMN")
            elif check_like > 0 and total_non_null < df.shape[0] * 0.3:  # Sparse check numbers
                print(f"      🧾 LIKELY: CHECK NUMBER COLUMN")
            elif col_idx == 2:  # Third column is usually description
                print(f"      📝 LIKELY: DESCRIPTION COLUMN")
    
    # Show some specific rows to understand the structure
    print(f"\n   📋 SAMPLE ROWS FROM TABLE {table_index}:")
    for row_idx in range(min(15, len(df))):
        row_data = []
        for col_idx in range(min(6, df.shape[1])):
            cell_value = df.iloc[row_idx, col_idx]
            if pd.isna(cell_value):
                row_data.append("EMPTY")
            else:
                row_data.append(str(cell_value)[:15])  # Truncate for display
        
        print(f"      Row {row_idx:2d}: {row_data}")

def looks_like_date(value):
    """Quick check if value looks like a Wells Fargo date"""
    import re
    return bool(re.match(r"^\d{1,2}/\d{1,2}$", str(value).strip()))

def looks_like_amount(value):
    """Quick check if value looks like a monetary amount"""
    try:
        cleaned = str(value).replace(',', '').replace('$', '').strip()
        amount = float(cleaned)
        return 0.01 <= amount <= 1000000
    except:
        return False

def looks_like_check_number(value):
    """Quick check if value looks like a check number"""
    val_str = str(value).strip()
    return val_str.isdigit() and 3 <= len(val_str) <= 5

if __name__ == "__main__":
    # Test with your Wells Fargo PDF
    pdf_path = input("Enter path to your Wells Fargo PDF: ")
    test_wells_fargo_extraction(pdf_path)