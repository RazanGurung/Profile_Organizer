"""
Wells Fargo PDF Parser
Extracts raw transaction data from Wells Fargo bank statements
"""

import pandas as pd
import tabula
import re

class WellsFargoParser:
    def __init__(self):
        self.transaction_tables = []
        self.raw_data = []
    
    def extract_tables_from_pdf(self, pdf_path):
        """
        Extract all tables from Wells Fargo PDF
        """
        print("🔍 Extracting tables from Wells Fargo PDF...")
        
        try:
            tables = tabula.read_pdf(
                pdf_path, 
                pages="all", 
                multiple_tables=True,
                pandas_options={"header": None}
            )
            print(f"Found {len(tables)} tables")
            return tables
        except Exception as e:
            print(f"❌ Error extracting tables: {e}")
            return []
    
    def is_transaction_table(self, table):
        """
        Check if table contains Wells Fargo transactions
        """
        if table.shape[0] < 3 or table.shape[1] < 4:
            return False
        
        # Count transaction indicators
        date_count = 0
        amount_count = 0
        keyword_count = 0
        
        # Check first 15 rows for patterns
        for row_idx in range(min(15, len(table))):
            for col_idx in range(table.shape[1]):
                cell = str(table.iloc[row_idx, col_idx]).strip()
                
                # Wells Fargo date format
                if re.match(r'^\d{1,2}/\d{1,2}$', cell):
                    date_count += 1
                
                # Amount format
                if re.match(r'^\d{1,3}(,\d{3})*\.\d{2}$', cell):
                    amount_count += 1
                
                # Transaction keywords
                cell_lower = cell.lower()
                keywords = ['bankcard', 'purchase', 'ach', 'deposit', 'payment', 'tobacco', 'mtot']
                if any(keyword in cell_lower for keyword in keywords):
                    keyword_count += 1
        
        return (date_count >= 2 and amount_count >= 2) or keyword_count >= 3
    
    def is_check_summary_table(self, table):
        """
        Detect check summary tables and check image tables to exclude them
        """
        # Check all cells for summary indicators
        for row_idx in range(len(table)):
            for col_idx in range(table.shape[1]):
                cell = str(table.iloc[row_idx, col_idx]).strip().lower()
                
                summary_keywords = [
                    'summary of checks', 'checks written', 'check images',
                    'account number:', 'check number:', 'amount:',
                    'gap in check sequence', 'checks listed are also displayed',
                    'number date amount'
                ]
                
                if any(keyword in cell for keyword in summary_keywords):
                    return True
        
        # Look for repeating "Number Date Amount" pattern
        if len(table) > 0:
            first_row = []
            for col_idx in range(min(9, table.shape[1])):
                cell = str(table.iloc[0, col_idx]).strip().lower()
                first_row.append(cell)
            
            header_pattern_count = 0
            for i in range(0, len(first_row) - 2, 3):
                if (i + 2 < len(first_row) and 
                    'number' in first_row[i] and 
                    'date' in first_row[i + 1] and 
                    'amount' in first_row[i + 2]):
                    header_pattern_count += 1
            
            if header_pattern_count >= 2:
                return True
        
        # Count check numbers in first column
        check_count_first_col = 0
        total_first_col = 0
        
        for row_idx in range(1, min(10, len(table))):
            if table.shape[1] > 0:
                cell = str(table.iloc[row_idx, 0]).strip()
                total_first_col += 1
                
                if re.match(r'^\d{4}$', cell):
                    check_count_first_col += 1
        
        if total_first_col > 0 and (check_count_first_col / total_first_col) > 0.5:
            return True
        
        # Look for check number + date pattern
        check_date_pattern_count = 0
        
        for row_idx in range(1, min(8, len(table))):
            if table.shape[1] >= 2:
                col1 = str(table.iloc[row_idx, 0]).strip()
                col2 = str(table.iloc[row_idx, 1]).strip()
                
                if (re.match(r'^\d{4}$', col1) and 
                    re.match(r'^\d{1,2}/\d{1,2}$', col2)):
                    check_date_pattern_count += 1
        
        return check_date_pattern_count >= 3
    
    def remove_ending_balance_column(self, all_rows):
        """
        Remove the ending balance column if detected
        """
        if not all_rows or len(all_rows[0]) <= 4:
            return all_rows
        
        last_col_idx = len(all_rows[0]) - 1
        balance_count = 0
        non_empty_count = 0
        
        # Check last column for balance patterns
        for i, row in enumerate(all_rows[:25]):
            if last_col_idx < len(row):
                cell = str(row[last_col_idx]).strip()
                
                if cell and cell != "EMPTY":
                    non_empty_count += 1
                    
                    if re.match(r'^\d{1,3}(,\d{3})*\.\d{2}$', cell):
                        balance_count += 1
        
        # Remove if >60% look like balances
        balance_ratio = balance_count / non_empty_count if non_empty_count > 0 else 0
        
        if balance_ratio > 0.6:
            print(f"🗑️  Removing balance column: {balance_count}/{non_empty_count} cells are amounts")
            for row in all_rows:
                if len(row) > last_col_idx:
                    row.pop()
        
        return all_rows
    
    def extract_raw_transactions(self, pdf_path):
        """
        Main method to extract raw transaction data from Wells Fargo PDF
        Returns list of transaction rows
        """
        print("📄 Extracting Wells Fargo transactions...")
        
        # Extract all tables
        tables = self.extract_tables_from_pdf(pdf_path)
        if not tables:
            return []
        
        # Find transaction tables
        transaction_tables = []
        
        for i, table in enumerate(tables):
            print(f"\nTable {i}: {table.shape[0]} rows x {table.shape[1]} cols")
            
            if self.is_check_summary_table(table):
                print(f"  📋 Table {i} is check summary - SKIPPING")
                continue
            
            if self.is_transaction_table(table):
                print(f"  ✅ Table {i} looks like transactions!")
                transaction_tables.append((i, table))
            else:
                print(f"  ❌ Table {i} doesn't look like transactions")
        
        if not transaction_tables:
            print("❌ No transaction tables found")
            return []
        
        print(f"\n📊 Found {len(transaction_tables)} transaction tables")
        
        # Combine all transaction tables
        all_rows = []
        max_cols = 0
        
        for table_idx, table in transaction_tables:
            print(f"Processing table {table_idx}...")
            max_cols = max(max_cols, table.shape[1])
            
            for row_idx in range(len(table)):
                row_data = []
                for col_idx in range(table.shape[1]):
                    cell = table.iloc[row_idx, col_idx]
                    if pd.isna(cell):
                        row_data.append("")
                    else:
                        row_data.append(str(cell).strip())
                
                # Pad row to max columns
                while len(row_data) < max_cols:
                    row_data.append("")
                
                all_rows.append(row_data)
        
        print(f"Combined {len(all_rows)} total rows from all transaction tables")
        
        # Remove ending balance column
        all_rows = self.remove_ending_balance_column(all_rows)
        
        self.raw_data = all_rows
        return all_rows

def parse_wells_fargo_pdf(pdf_path):
    """
    Convenience function to parse Wells Fargo PDF
    Returns raw transaction data as list of lists
    """
    parser = WellsFargoParser()
    return parser.extract_raw_transactions(pdf_path)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
        raw_data = parse_wells_fargo_pdf(pdf_path)
        print(f"\n✅ Extracted {len(raw_data)} raw transaction rows")
        
        # Show preview
        print("\n📋 Raw data preview:")
        for i, row in enumerate(raw_data[:10]):
            print(f"Row {i}: {row}")
    else:
        print("Usage: python wf_parser.py <pdf_path>")