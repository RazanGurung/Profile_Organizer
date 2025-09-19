

# import pandas as pd
# import tabula
# import os
# import csv
# import re

# def dump_wells_fargo_raw(pdf_path, output_folder="test"):
#     """
#     Dump Wells Fargo transaction data to CSV, excluding check summaries and balance columns
#     """
#     print("📄 DUMPING WELLS FARGO RAW DATA")
#     print("=" * 35)
    
#     # Create test folder
#     if not os.path.exists(output_folder):
#         os.makedirs(output_folder)
#         print(f"📁 Created {output_folder} folder")
    
#     # Extract all tables
#     print("🔍 Extracting all tables...")
#     try:
#         tables = tabula.read_pdf(
#             pdf_path, 
#             pages="all", 
#             multiple_tables=True,
#             pandas_options={"header": None}
#         )
#         print(f"Found {len(tables)} tables")
#     except Exception as e:
#         print(f"❌ Error extracting tables: {e}")
#         return
    
#     # Find transaction tables
#     transaction_tables = []
    
#     for i, table in enumerate(tables):
#         print(f"\nTable {i}: {table.shape[0]} rows x {table.shape[1]} cols")
        
#         # Check if it's a check summary table first
#         if is_check_summary_table(table):
#             print(f"  📋 Table {i} is check summary/images - SKIPPING")
#             continue
        
#         # Check if it's a transaction table
#         if is_transaction_table(table):
#             print(f"  ✅ Table {i} looks like transactions!")
#             transaction_tables.append((i, table))
#         else:
#             print(f"  ❌ Table {i} doesn't look like transactions")
    
#     if not transaction_tables:
#         print("❌ No transaction tables found")
#         return
    
#     print(f"\n📊 Found {len(transaction_tables)} transaction tables")
    
#     # Combine all transaction tables
#     all_rows = []
#     max_cols = 0
    
#     for table_idx, table in transaction_tables:
#         print(f"Processing table {table_idx}...")
#         max_cols = max(max_cols, table.shape[1])
        
#         for row_idx in range(len(table)):
#             row_data = []
#             for col_idx in range(table.shape[1]):
#                 cell = table.iloc[row_idx, col_idx]
#                 if pd.isna(cell):
#                     row_data.append("")
#                 else:
#                     row_data.append(str(cell).strip())
            
#             # Pad row to max columns
#             while len(row_data) < max_cols:
#                 row_data.append("")
            
#             all_rows.append(row_data)
    
#     print(f"Combined {len(all_rows)} total rows from all transaction tables")
    
#     # Remove ending balance column
#     all_rows = remove_ending_balance_column(all_rows)
    
#     # Update max_cols after removing balance column
#     if all_rows:
#         max_cols = len(all_rows[0])
    
#     # Add monthly summary at the top
#     all_rows = add_monthly_summary(all_rows)
    
#     # Remove regular deposits but keep EDI payments
#     all_rows = filter_deposits_keep_edi(all_rows)
    
#     # Sort transactions in specified order
#     all_rows = sort_transactions_by_type(all_rows)
    
#     # Ensure withdrawals are negative
#     all_rows = ensure_negative_withdrawals(all_rows)
    
#     # Save to CSV
#     csv_filename = os.path.join(output_folder, "wells_fargo_transactions_clean.csv")
    
#     with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.writer(csvfile)
        
#         # Write header
#         headers = [f"Column_{i+1}" for i in range(max_cols)]
#         writer.writerow(headers)
        
#         # Write all rows
#         writer.writerows(all_rows)
    
#     print(f"💾 Clean transaction data saved to: {csv_filename}")
    
#     # Show preview
#     print(f"\n📋 PREVIEW (first 15 rows):")
#     headers = [f"Col_{i+1}" for i in range(max_cols)]
#     print(",".join(headers))
    
#     for row_idx in range(min(15, len(all_rows))):
#         row_data = []
#         for cell in all_rows[row_idx]:
#             if not cell:
#                 row_data.append("EMPTY")
#             else:
#                 cell_str = str(cell)[:20]
#                 row_data.append(cell_str)
#         print(",".join(row_data))
    
#     print(f"\n✅ Complete! {len(all_rows)} rows saved to {csv_filename}")

# def is_transaction_table(table):
#     """
#     Check if table contains Wells Fargo transactions
#     """
#     if table.shape[0] < 3:
#         return False
    
#     if table.shape[1] < 4:
#         return False
    
#     # Count transaction indicators
#     date_count = 0
#     amount_count = 0
#     keyword_count = 0
    
#     # Check first 15 rows for patterns
#     for row_idx in range(min(15, len(table))):
#         for col_idx in range(table.shape[1]):
#             cell = str(table.iloc[row_idx, col_idx]).strip()
            
#             # Wells Fargo date format
#             if re.match(r'^\d{1,2}/\d{1,2}$', cell):
#                 date_count += 1
            
#             # Amount format
#             if re.match(r'^\d{1,3}(,\d{3})*\.\d{2}$', cell):
#                 amount_count += 1
            
#             # Transaction keywords
#             cell_lower = cell.lower()
#             keywords = ['bankcard', 'purchase', 'ach', 'deposit', 'payment', 'tobacco', 'mtot']
#             if any(keyword in cell_lower for keyword in keywords):
#                 keyword_count += 1
    
#     # Need at least some dates, amounts, and keywords
#     return (date_count >= 2 and amount_count >= 2) or keyword_count >= 3

# def is_check_summary_table(table):
#     """
#     Detect check summary tables and check image tables
#     """
#     # Check all cells for summary indicators
#     for row_idx in range(len(table)):
#         for col_idx in range(table.shape[1]):
#             cell = str(table.iloc[row_idx, col_idx]).strip().lower()
            
#             # Summary table keywords
#             summary_keywords = [
#                 'summary of checks',
#                 'checks written', 
#                 'check images',
#                 'account number:',
#                 'check number:',
#                 'amount:',
#                 'gap in check sequence',
#                 'checks listed are also displayed',
#                 'number date amount'
#             ]
            
#             if any(keyword in cell for keyword in summary_keywords):
#                 print(f"    🚫 Found summary keyword: '{cell[:40]}'")
#                 return True
    
#     # Look for the specific Wells Fargo check summary pattern
#     if len(table) > 0:
#         first_row = []
#         for col_idx in range(min(9, table.shape[1])):
#             cell = str(table.iloc[0, col_idx]).strip().lower()
#             first_row.append(cell)
        
#         # Look for repeating "Number Date Amount" pattern
#         header_pattern_count = 0
#         for i in range(0, len(first_row) - 2, 3):
#             if (i + 2 < len(first_row) and 
#                 'number' in first_row[i] and 
#                 'date' in first_row[i + 1] and 
#                 'amount' in first_row[i + 2]):
#                 header_pattern_count += 1
        
#         if header_pattern_count >= 2:
#             print(f"    🚫 Found check summary header pattern")
#             return True
    
#     # Count check numbers in first column
#     check_count_first_col = 0
#     total_first_col = 0
    
#     for row_idx in range(1, min(10, len(table))):
#         if table.shape[1] > 0:
#             cell = str(table.iloc[row_idx, 0]).strip()
#             total_first_col += 1
            
#             if re.match(r'^\d{4}$', cell):
#                 check_count_first_col += 1
    
#     # If >50% of first column are check numbers, it's a check summary
#     if total_first_col > 0 and (check_count_first_col / total_first_col) > 0.5:
#         print(f"    🚫 First column has check numbers: {check_count_first_col}/{total_first_col}")
#         return True
    
#     # Look for check number + date pattern
#     check_date_pattern_count = 0
    
#     for row_idx in range(1, min(8, len(table))):
#         if table.shape[1] >= 2:
#             col1 = str(table.iloc[row_idx, 0]).strip()
#             col2 = str(table.iloc[row_idx, 1]).strip()
            
#             if (re.match(r'^\d{4}$', col1) and 
#                 re.match(r'^\d{1,2}/\d{1,2}$', col2)):
#                 check_date_pattern_count += 1
    
#     if check_date_pattern_count >= 3:
#         print(f"    🚫 Found check-date pattern in {check_date_pattern_count} rows")
#         return True
    
#     return False

# def remove_ending_balance_column(all_rows):
#     """
#     Remove the ending balance column if detected
#     """
#     if not all_rows or len(all_rows[0]) <= 4:
#         return all_rows
    
#     last_col_idx = len(all_rows[0]) - 1
#     balance_count = 0
#     non_empty_count = 0
    
#     print(f"🔍 Analyzing last column (Column_{last_col_idx + 1})...")
    
#     # Check last column for balance patterns
#     for i, row in enumerate(all_rows[:25]):
#         if last_col_idx < len(row):
#             cell = str(row[last_col_idx]).strip()
            
#             if cell and cell != "EMPTY":
#                 non_empty_count += 1
                
#                 if re.match(r'^\d{1,3}(,\d{3})*\.\d{2}$', cell):
#                     balance_count += 1
#                     if i < 8:
#                         print(f"    Balance-like: Row {i} = {cell}")
    
#     # Remove if >60% look like balances
#     balance_ratio = balance_count / non_empty_count if non_empty_count > 0 else 0
    
#     if balance_ratio > 0.6:
#         print(f"🗑️  Removing balance column: {balance_count}/{non_empty_count} cells are amounts")
#         for row in all_rows:
#             if len(row) > last_col_idx:
#                 row.pop()
#     else:
#         print(f"✅ Keeping last column: only {balance_count}/{non_empty_count} are amounts")
    
#     return all_rows

# def add_monthly_summary(all_rows):
#     """
#     Add monthly summary row at the top with deposits total
#     """
#     if not all_rows:
#         return all_rows
    
#     print("📊 Creating monthly summary...")
    
#     # Determine the month/year from the transaction dates
#     month_year = None
#     deposits_total = 0.0
#     withdrawals_total = 0.0
    
#     for row in all_rows:
#         # Look for date in first column (format: M/D)
#         if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
#             if not month_year:
#                 # Use first date to determine month/year
#                 month, day = row[0].strip().split('/')
#                 # Assume year based on your statement (modify as needed)
#                 year = "2022"
#                 month_year = f"{month.zfill(2)}/{year}"
            
#             # Calculate totals from deposits/credits and withdrawals/debits columns
#             # Assuming structure: Date, Check#, Description, Deposits, Withdrawals
#             if len(row) >= 5:
#                 # Deposits (Column 4)
#                 if row[3] and row[3].strip():
#                     try:
#                         amount = float(row[3].strip().replace(',', ''))
#                         deposits_total += amount
#                     except ValueError:
#                         pass
                
#                 # Withdrawals (Column 5)
#                 if row[4] and row[4].strip():
#                     try:
#                         amount = float(row[4].strip().replace(',', ''))
#                         withdrawals_total += amount
#                     except ValueError:
#                         pass
    
#     if month_year:
#         # Get last day of the month
#         month_num = int(month_year.split('/')[0])
#         year_num = int(month_year.split('/')[1])
        
#         # Simple last day calculation
#         if month_num in [1, 3, 5, 7, 8, 10, 12]:
#             last_day = 31
#         elif month_num in [4, 6, 9, 11]:
#             last_day = 30
#         elif month_num == 2:
#             # Leap year check
#             if year_num % 4 == 0 and (year_num % 100 != 0 or year_num % 400 == 0):
#                 last_day = 29
#             else:
#                 last_day = 28
        
#         last_date = f"{month_num:02d}/{last_day:02d}/{year_num}"
        
#         # Create summary row
#         if len(all_rows[0]) >= 5:
#             summary_row = [
#                 last_date,                    # Date (last day of month)
#                 "",                           # Check Number (empty)
#                 "Deposits",                   # Description
#                 f"{deposits_total:.2f}",      # Deposits amount
#                 f"-{withdrawals_total:.2f}"   # Withdrawals (negative)
#             ]
            
#             # Pad to match row length
#             while len(summary_row) < len(all_rows[0]):
#                 summary_row.append("")
            
#             print(f"✅ Summary: {last_date} | Deposits: ${deposits_total:.2f} | Withdrawals: -${withdrawals_total:.2f}")
            
#             # Insert at the beginning
#             all_rows.insert(0, summary_row)
#         else:
#             print("⚠️  Cannot create summary - insufficient columns")
#     else:
#         print("⚠️  Cannot create summary - no dates found")
    
#     return all_rows

# def filter_deposits_keep_edi(all_rows):
#     """
#     Remove regular deposit entries but keep EDI payments
#     """
#     if not all_rows:
#         return all_rows
    
#     print("🔍 Filtering deposits - keeping only EDI payments...")
    
#     filtered_rows = []
#     deposits_removed = 0
#     edi_kept = 0
    
#     for i, row in enumerate(all_rows):
#         # Keep the first row (summary row)
#         if i == 0:
#             filtered_rows.append(row)
#             continue
        
#         # Check if this is a deposit entry (has amount in deposits column but not withdrawals)
#         is_deposit = False
#         is_edi = False
        
#         if len(row) >= 5:
#             # Has deposit amount but no withdrawal amount
#             has_deposit = row[3] and row[3].strip() and row[3].strip() != ""
#             has_withdrawal = row[4] and row[4].strip() and row[4].strip() != ""
            
#             if has_deposit and not has_withdrawal:
#                 is_deposit = True
                
#                 # Check if it's an EDI payment
#                 description = ""
#                 if len(row) >= 3:
#                     description = row[2].lower()
                
#                 if any(edi_keyword in description for edi_keyword in [
#                     'edi', 'edi payment', 'edi pymnts', 'japan tobac', 'itg brands'
#                 ]):
#                     is_edi = True
        
#         # Keep non-deposits, or EDI payments
#         if not is_deposit or is_edi:
#             filtered_rows.append(row)
#             if is_edi:
#                 edi_kept += 1
#         else:
#             deposits_removed += 1
    
#     print(f"✅ Removed {deposits_removed} regular deposits, kept {edi_kept} EDI payments")
#     print(f"   Total rows: {len(all_rows)} → {len(filtered_rows)}")
    
#     return filtered_rows

# def sort_transactions_by_type(all_rows):
#     """
#     Sort transactions: Summary → EDI Payments → Withdrawals → Checks
#     """
#     if not all_rows:
#         return all_rows
    
#     print("📋 Sorting transactions by type...")
    
#     # Separate transactions by type
#     summary_rows = []
#     edi_payments = []
#     withdrawals = []
#     checks = []
#     other_rows = []
    
#     for i, row in enumerate(all_rows):
#         # First row is always summary
#         if i == 0:
#             summary_rows.append(row)
#             continue
        
#         # Classify each transaction
#         transaction_type = classify_transaction(row)
        
#         if transaction_type == "EDI":
#             edi_payments.append(row)
#         elif transaction_type == "CHECK":
#             checks.append(row)
#         elif transaction_type == "WITHDRAWAL":
#             withdrawals.append(row)
#         else:
#             other_rows.append(row)
    
#     # Sort each category by date (if possible)
#     edi_payments = sort_by_date(edi_payments)
#     withdrawals = sort_by_date(withdrawals)
#     checks = sort_by_date(checks)
    
#     # Combine in desired order
#     sorted_rows = summary_rows + edi_payments + withdrawals + checks + other_rows
    
#     print(f"✅ Sorted transactions:")
#     print(f"   📊 Summary: {len(summary_rows)} rows")
#     print(f"   💰 EDI Payments: {len(edi_payments)} rows")
#     print(f"   💳 Withdrawals: {len(withdrawals)} rows")
#     print(f"   🧾 Checks: {len(checks)} rows")
#     if other_rows:
#         print(f"   ❓ Other: {len(other_rows)} rows")
    
#     return sorted_rows

# def classify_transaction(row):
#     """
#     Classify a transaction as EDI, CHECK, WITHDRAWAL, or OTHER
#     """
#     if len(row) < 3:
#         return "OTHER"
    
#     # Check if it has a check number (column 2)
#     check_number = row[1].strip() if len(row) > 1 else ""
#     if check_number and re.match(r'^\d{4}$', check_number):
#         return "CHECK"
    
#     # Check description for EDI keywords
#     description = row[2].lower() if len(row) > 2 else ""
#     edi_keywords = ['edi', 'edi payment', 'edi pymnts', 'japan tobac', 'itg brands']
#     if any(keyword in description for keyword in edi_keywords):
#         return "EDI"
    
#     # Check if it's a withdrawal (has amount in withdrawal column)
#     if len(row) >= 5:
#         withdrawal_amount = row[4].strip() if row[4] else ""
#         if withdrawal_amount and withdrawal_amount != "":
#             return "WITHDRAWAL"
    
#     return "OTHER"

# def sort_by_date(rows):
#     """
#     Sort rows by date (if date is available in first column)
#     """
#     def get_date_key(row):
#         if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
#             try:
#                 month, day = row[0].strip().split('/')
#                 return (int(month), int(day))
#             except:
#                 pass
#         return (99, 99)  # Put undated items at end
    
#     return sorted(rows, key=get_date_key)

# def ensure_negative_withdrawals(all_rows):
#     """
#     Ensure withdrawal amounts are negative
#     """
#     for row in all_rows:
#         if len(row) >= 5:  # Assuming withdrawals are in column 5
#             withdrawal_cell = row[4].strip()
#             if withdrawal_cell and withdrawal_cell != "":
#                 try:
#                     amount = float(withdrawal_cell.replace(',', ''))
#                     if amount > 0:  # Make it negative if it's positive
#                         row[4] = f"-{amount:.2f}"
#                 except ValueError:
#                     pass
#     return all_rows

# if __name__ == "__main__":
#     import sys
    
#     if len(sys.argv) > 1:
#         pdf_path = sys.argv[1]
#     else:
#         pdf_path = "09_22_TH&V.pdf"  # Change this to your PDF filename
    
#     if not os.path.exists(pdf_path):
#         print(f"❌ PDF not found: {pdf_path}")
#         print("Usage: python test_wells_fargo_extraction.py <pdf_path>")
#     else:
#         dump_wells_fargo_raw(pdf_path)


import pandas as pd
import tabula
import os
import csv
import re

def dump_wells_fargo_raw(pdf_path, output_folder="test"):
    """
    Dump Wells Fargo transaction data to CSV, excluding check summaries and balance columns
    """
    print("📄 DUMPING WELLS FARGO RAW DATA")
    print("=" * 35)
    
    # Create test folder
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"📁 Created {output_folder} folder")
    
    # Extract all tables
    print("🔍 Extracting all tables...")
    try:
        tables = tabula.read_pdf(
            pdf_path, 
            pages="all", 
            multiple_tables=True,
            pandas_options={"header": None}
        )
        print(f"Found {len(tables)} tables")
    except Exception as e:
        print(f"❌ Error extracting tables: {e}")
        return
    
    # Find transaction tables
    transaction_tables = []
    
    for i, table in enumerate(tables):
        print(f"\nTable {i}: {table.shape[0]} rows x {table.shape[1]} cols")
        
        # Check if it's a check summary table first
        if is_check_summary_table(table):
            print(f"  📋 Table {i} is check summary/images - SKIPPING")
            continue
        
        # Check if it's a transaction table
        if is_transaction_table(table):
            print(f"  ✅ Table {i} looks like transactions!")
            transaction_tables.append((i, table))
        else:
            print(f"  ❌ Table {i} doesn't look like transactions")
    
    if not transaction_tables:
        print("❌ No transaction tables found")
        return
    
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
    all_rows = remove_ending_balance_column(all_rows)
    
    # Update max_cols after removing balance column
    if all_rows:
        max_cols = len(all_rows[0])
    
    # Add monthly summary at the top
    all_rows = add_monthly_summary(all_rows)
    
    # Remove regular deposits but keep EDI payments
    all_rows = filter_deposits_keep_edi(all_rows)
    
    # Sort transactions in specified order
    all_rows = sort_transactions_by_type(all_rows)
    
    # Merge deposit and withdrawal columns into one amount column
    all_rows = merge_amount_columns(all_rows)
    
    # Remove rows without dates or amounts (description-only rows)
    all_rows = remove_description_only_rows(all_rows)
    
    # Save to CSV
    csv_filename = os.path.join(output_folder, "wells_fargo_transactions_clean.csv")
    
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header - now only 4 columns
        headers = ["Date", "Check No", "Description", "Amount"]
        writer.writerow(headers)
        
        # Write all rows
        writer.writerows(all_rows)
    
    print(f"💾 Clean transaction data saved to: {csv_filename}")
    
    # Show preview
    print(f"\n📋 PREVIEW (first 15 rows):")
    print("Date,Check No,Description,Amount")
    
    for row_idx in range(min(15, len(all_rows))):
        row_data = []
        for i, cell in enumerate(all_rows[row_idx][:4]):  # Only show first 4 columns
            if not cell:
                row_data.append("EMPTY")
            else:
                cell_str = str(cell)[:25]
                row_data.append(cell_str)
        print(",".join(row_data))
    
    print(f"\n✅ Complete! {len(all_rows)} rows saved to {csv_filename}")

def is_transaction_table(table):
    """
    Check if table contains Wells Fargo transactions
    """
    if table.shape[0] < 3:
        return False
    
    if table.shape[1] < 4:
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
    
    # Need at least some dates, amounts, and keywords
    return (date_count >= 2 and amount_count >= 2) or keyword_count >= 3

def is_check_summary_table(table):
    """
    Detect check summary tables and check image tables
    """
    # Check all cells for summary indicators
    for row_idx in range(len(table)):
        for col_idx in range(table.shape[1]):
            cell = str(table.iloc[row_idx, col_idx]).strip().lower()
            
            # Summary table keywords
            summary_keywords = [
                'summary of checks',
                'checks written', 
                'check images',
                'account number:',
                'check number:',
                'amount:',
                'gap in check sequence',
                'checks listed are also displayed',
                'number date amount'
            ]
            
            if any(keyword in cell for keyword in summary_keywords):
                print(f"    🚫 Found summary keyword: '{cell[:40]}'")
                return True
    
    # Look for the specific Wells Fargo check summary pattern
    if len(table) > 0:
        first_row = []
        for col_idx in range(min(9, table.shape[1])):
            cell = str(table.iloc[0, col_idx]).strip().lower()
            first_row.append(cell)
        
        # Look for repeating "Number Date Amount" pattern
        header_pattern_count = 0
        for i in range(0, len(first_row) - 2, 3):
            if (i + 2 < len(first_row) and 
                'number' in first_row[i] and 
                'date' in first_row[i + 1] and 
                'amount' in first_row[i + 2]):
                header_pattern_count += 1
        
        if header_pattern_count >= 2:
            print(f"    🚫 Found check summary header pattern")
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
    
    # If >50% of first column are check numbers, it's a check summary
    if total_first_col > 0 and (check_count_first_col / total_first_col) > 0.5:
        print(f"    🚫 First column has check numbers: {check_count_first_col}/{total_first_col}")
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
    
    if check_date_pattern_count >= 3:
        print(f"    🚫 Found check-date pattern in {check_date_pattern_count} rows")
        return True
    
    return False

def remove_ending_balance_column(all_rows):
    """
    Remove the ending balance column if detected
    """
    if not all_rows or len(all_rows[0]) <= 4:
        return all_rows
    
    last_col_idx = len(all_rows[0]) - 1
    balance_count = 0
    non_empty_count = 0
    
    print(f"🔍 Analyzing last column (Column_{last_col_idx + 1})...")
    
    # Check last column for balance patterns
    for i, row in enumerate(all_rows[:25]):
        if last_col_idx < len(row):
            cell = str(row[last_col_idx]).strip()
            
            if cell and cell != "EMPTY":
                non_empty_count += 1
                
                if re.match(r'^\d{1,3}(,\d{3})*\.\d{2}$', cell):
                    balance_count += 1
                    if i < 8:
                        print(f"    Balance-like: Row {i} = {cell}")
    
    # Remove if >60% look like balances
    balance_ratio = balance_count / non_empty_count if non_empty_count > 0 else 0
    
    if balance_ratio > 0.6:
        print(f"🗑️  Removing balance column: {balance_count}/{non_empty_count} cells are amounts")
        for row in all_rows:
            if len(row) > last_col_idx:
                row.pop()
    else:
        print(f"✅ Keeping last column: only {balance_count}/{non_empty_count} are amounts")
    
    return all_rows

def add_monthly_summary(all_rows):
    """
    Add monthly summary row at the top with deposits total
    """
    if not all_rows:
        return all_rows
    
    print("📊 Creating monthly summary...")
    
    # Determine the month/year from the transaction dates
    month_year = None
    deposits_total = 0.0
    withdrawals_total = 0.0
    
    for row in all_rows:
        # Look for date in first column (format: M/D)
        if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
            if not month_year:
                # Use first date to determine month/year
                month, day = row[0].strip().split('/')
                # Assume year based on your statement (modify as needed)
                year = "2022"
                month_year = f"{month.zfill(2)}/{year}"
            
            # Calculate totals from deposits/credits and withdrawals/debits columns
            # Assuming structure: Date, Check#, Description, Deposits, Withdrawals
            if len(row) >= 5:
                # Deposits (Column 4)
                if row[3] and row[3].strip():
                    try:
                        amount = float(row[3].strip().replace(',', ''))
                        deposits_total += amount
                    except ValueError:
                        pass
                
                # Withdrawals (Column 5)
                if row[4] and row[4].strip():
                    try:
                        amount = float(row[4].strip().replace(',', ''))
                        withdrawals_total += amount
                    except ValueError:
                        pass
    
    if month_year:
        # Get last day of the month
        month_num = int(month_year.split('/')[0])
        year_num = int(month_year.split('/')[1])
        
        # Simple last day calculation
        if month_num in [1, 3, 5, 7, 8, 10, 12]:
            last_day = 31
        elif month_num in [4, 6, 9, 11]:
            last_day = 30
        elif month_num == 2:
            # Leap year check
            if year_num % 4 == 0 and (year_num % 100 != 0 or year_num % 400 == 0):
                last_day = 29
            else:
                last_day = 28
        
        last_date = f"{month_num:02d}/{last_day:02d}/{year_num}"
        
        # Create summary row
        if len(all_rows[0]) >= 5:
            summary_row = [
                last_date,                    # Date (last day of month)
                "",                           # Check Number (empty)
                "Deposits",                   # Description
                f"{deposits_total:.2f}"       # Combined amount (positive for summary)
            ]
            
            # Pad to match row length
            while len(summary_row) < len(all_rows[0]):
                summary_row.append("")
            
            print(f"✅ Summary: {last_date} | Deposits: ${deposits_total:.2f} | Withdrawals: -${withdrawals_total:.2f}")
            
            # Insert at the beginning
            all_rows.insert(0, summary_row)
        else:
            print("⚠️  Cannot create summary - insufficient columns")
    else:
        print("⚠️  Cannot create summary - no dates found")
    
    return all_rows

def filter_deposits_keep_edi(all_rows):
    """
    Remove regular deposit entries but keep EDI payments
    """
    if not all_rows:
        return all_rows
    
    print("🔍 Filtering deposits - keeping only EDI payments...")
    
    filtered_rows = []
    deposits_removed = 0
    edi_kept = 0
    
    for i, row in enumerate(all_rows):
        # Keep the first row (summary row)
        if i == 0:
            filtered_rows.append(row)
            continue
        
        # Check if this is a deposit entry (has amount in deposits column but not withdrawals)
        is_deposit = False
        is_edi = False
        
        if len(row) >= 5:
            # Has deposit amount but no withdrawal amount
            has_deposit = row[3] and row[3].strip() and row[3].strip() != ""
            has_withdrawal = row[4] and row[4].strip() and row[4].strip() != ""
            
            if has_deposit and not has_withdrawal:
                is_deposit = True
                
                # Check if it's an EDI payment
                description = ""
                if len(row) >= 3:
                    description = row[2].lower()
                
                if any(edi_keyword in description for edi_keyword in [
                    'edi', 'edi payment', 'edi pymnts', 'japan tobac', 'itg brands'
                ]):
                    is_edi = True
        
        # Keep non-deposits, or EDI payments
        if not is_deposit or is_edi:
            filtered_rows.append(row)
            if is_edi:
                edi_kept += 1
        else:
            deposits_removed += 1
    
    print(f"✅ Removed {deposits_removed} regular deposits, kept {edi_kept} EDI payments")
    print(f"   Total rows: {len(all_rows)} → {len(filtered_rows)}")
    
    return filtered_rows

def sort_transactions_by_type(all_rows):
    """
    Sort transactions: Summary → EDI Payments → Withdrawals → Checks
    """
    if not all_rows:
        return all_rows
    
    print("📋 Sorting transactions by type...")
    
    # Separate transactions by type
    summary_rows = []
    edi_payments = []
    withdrawals = []
    checks = []
    other_rows = []
    
    for i, row in enumerate(all_rows):
        # First row is always summary
        if i == 0:
            summary_rows.append(row)
            continue
        
        # Classify each transaction
        transaction_type = classify_transaction(row)
        
        if transaction_type == "EDI":
            edi_payments.append(row)
        elif transaction_type == "CHECK":
            checks.append(row)
        elif transaction_type == "WITHDRAWAL":
            withdrawals.append(row)
        else:
            other_rows.append(row)
    
    # Sort each category by date (if possible)
    edi_payments = sort_by_date(edi_payments)
    withdrawals = sort_by_date(withdrawals)
    checks = sort_by_date(checks)
    
    # Combine in desired order
    sorted_rows = summary_rows + edi_payments + withdrawals + checks + other_rows
    
    print(f"✅ Sorted transactions:")
    print(f"   📊 Summary: {len(summary_rows)} rows")
    print(f"   💰 EDI Payments: {len(edi_payments)} rows")
    print(f"   💳 Withdrawals: {len(withdrawals)} rows")
    print(f"   🧾 Checks: {len(checks)} rows")
    if other_rows:
        print(f"   ❓ Other: {len(other_rows)} rows")
    
    return sorted_rows

def classify_transaction(row):
    """
    Classify a transaction as EDI, CHECK, WITHDRAWAL, or OTHER
    """
    if len(row) < 3:
        return "OTHER"
    
    # Check if it has a check number (column 2)
    check_number = row[1].strip() if len(row) > 1 else ""
    if check_number and re.match(r'^\d{4}$', check_number):
        return "CHECK"
    
    # Check description for EDI keywords
    description = row[2].lower() if len(row) > 2 else ""
    edi_keywords = ['edi', 'edi payment', 'edi pymnts', 'japan tobac', 'itg brands']
    if any(keyword in description for keyword in edi_keywords):
        return "EDI"
    
    # Check if it's a withdrawal (has amount in withdrawal column)
    if len(row) >= 5:
        withdrawal_amount = row[4].strip() if row[4] else ""
        if withdrawal_amount and withdrawal_amount != "":
            return "WITHDRAWAL"
    
    # Check if it's a deposit (has amount in deposit column)
    if len(row) >= 4:
        deposit_amount = row[3].strip() if row[3] else ""
        if deposit_amount and deposit_amount != "":
            return "EDI"  # Remaining deposits should be EDI payments
    
    return "OTHER"

def sort_by_date(rows):
    """
    Sort rows by date (if date is available in first column)
    """
    def get_date_key(row):
        if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
            try:
                month, day = row[0].strip().split('/')
                return (int(month), int(day))
            except:
                pass
        return (99, 99)  # Put undated items at end
    
    return sorted(rows, key=get_date_key)

def merge_amount_columns(all_rows):
    """
    Merge deposit and withdrawal columns into one amount column
    Deposits = positive, Withdrawals = negative
    """
    if not all_rows:
        return all_rows
    
    print("🔀 Merging deposit and withdrawal columns into one amount column...")
    
    merged_rows = []
    
    for row in all_rows:
        if len(row) >= 5:
            # Get deposit and withdrawal amounts
            deposit_str = row[3].strip() if row[3] else ""
            withdrawal_str = row[4].strip() if row[4] else ""
            
            # Calculate final amount
            final_amount = ""
            
            if deposit_str and deposit_str != "":
                try:
                    deposit_amount = float(deposit_str.replace(',', ''))
                    final_amount = f"{deposit_amount:.2f}"  # Positive
                except ValueError:
                    pass
            
            if withdrawal_str and withdrawal_str != "":
                try:
                    withdrawal_amount = float(withdrawal_str.replace(',', '').replace('-', ''))
                    final_amount = f"-{withdrawal_amount:.2f}"  # Negative
                except ValueError:
                    pass
            
            # Create new row: Date, Check#, Description, Amount
            new_row = [
                row[0],  # Date
                row[1],  # Check Number
                row[2],  # Description
                final_amount  # Combined Amount
            ]
            
            merged_rows.append(new_row)
        
        elif len(row) >= 4:
            # Handle rows with fewer columns
            merged_rows.append(row[:4])
        else:
            # Keep shorter rows as-is
            merged_rows.append(row)
    
    print(f"✅ Merged columns: {len(all_rows)} rows processed")
    return merged_rows

def remove_description_only_rows(all_rows):
    """
    Remove rows that only have descriptions but no dates or amounts
    """
    if not all_rows:
        return all_rows
    
    print("🗑️  Removing description-only rows (no date or amount)...")
    
    cleaned_rows = []
    removed_count = 0
    
    for i, row in enumerate(all_rows):
        # Always keep the first row (summary)
        if i == 0:
            cleaned_rows.append(row)
            continue
        
        # Check if row has date and/or amount
        has_date = False
        has_amount = False
        
        if len(row) >= 1:
            date_cell = row[0].strip()
            if date_cell and re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_cell):
                has_date = True
        
        if len(row) >= 4:
            amount_cell = row[3].strip()
            if amount_cell and amount_cell != "":
                try:
                    float(amount_cell.replace(',', '').replace('-', ''))
                    has_amount = True
                except ValueError:
                    pass
        
        # Keep rows that have either date or amount (or both)
        if has_date or has_amount:
            cleaned_rows.append(row)
        else:
            removed_count += 1
            print(f"    Removed: {row[:3]}")  # Show first 3 columns of removed row
    
    print(f"✅ Removed {removed_count} description-only rows")
    print(f"   Total rows: {len(all_rows)} → {len(cleaned_rows)}")
    
    return cleaned_rows

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
    else:
        pdf_path = "09_22_TH&V.pdf"  # Change this to your PDF filename
    
    if not os.path.exists(pdf_path):
        print(f"❌ PDF not found: {pdf_path}")
        print("Usage: python test_wells_fargo_extraction.py <pdf_path>")
    else:
        dump_wells_fargo_raw(pdf_path)