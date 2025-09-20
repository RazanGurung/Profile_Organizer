# import pandas as pd
# import tabula
# import pdfplumber
# from typing import List, Optional, Tuple
# from datetime import datetime
# import re
# from ...interfaces.transaction import Transaction
# from .wf_parser import WellsFargoParser

# class WellsFargoProcessor:
#     """Wells Fargo specific PDF processor - exact implementation from test file"""
    
#     def __init__(self):
#         # Wells Fargo-specific parser
#         self.parser = WellsFargoParser()
#         self.bank_name = "wells_fargo"

#     def extract_transactions(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
#         """
#         Wells Fargo specific transaction extraction using exact test file logic
#         Returns: (bank_name, list_of_transactions)
#         """
#         print(f"Processing Wells Fargo PDF: {pdf_path}")
        
#         # Wells Fargo-specific table extraction using exact test file method
#         tables = self._extract_tables_exact_test_method(pdf_path)
        
#         # Process using Wells Fargo parser with test file logic
#         transactions = self.parser.process_tables(tables)

#         print(f"Extracted {len(transactions)} Wells Fargo transactions")
#         return self.bank_name, transactions
    
#     def _extract_tables_exact_test_method(self, pdf_path: str) -> List[pd.DataFrame]:
#         """
#         Extract tables using the exact method from test file
#         """
#         print("📄 Extracting Wells Fargo tables using test file method...")
        
#         try:
#             tables = tabula.read_pdf(
#                 pdf_path, 
#                 pages="all", 
#                 multiple_tables=True,
#                 pandas_options={"header": None}
#             )
#             print(f"Found {len(tables)} tables")
#             return tables
#         except Exception as e:
#             print(f"❌ Error extracting tables: {e}")
#             return []
    
#     def export_to_csv(self, transactions: List[Transaction], output_path: str):
#         """Export Wells Fargo transactions to CSV in exact test file format"""
#         data = []
#         for txn in transactions:
#             data.append({
#                 "Date": txn.date,
#                 "Check No": txn.check_number or "",
#                 "Description": txn.description,
#                 "Amount": txn.amount
#             })
        
#         df = pd.DataFrame(data)
#         df.to_csv(output_path, index=False)
#         print(f"Exported {len(data)} Wells Fargo transactions to {output_path}")



import re
from typing import List, Optional
import pandas as pd
from ...interfaces.base_parser import BaseParser
from ...interfaces.transaction import Transaction

class WellsFargoParser(BaseParser):
    """Wells Fargo bank statement parser - exact implementation from test file"""
    
    def __init__(self):
        super().__init__()
        self.date_pat = re.compile(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b")
        self.money_pat = re.compile(r"[-+]?\$?\d[\d,]*\.\d{2}")
        self.check_word_pat = re.compile(r"\bCHECK(?!CARD)\b", re.IGNORECASE)
        self.checknum_pat = re.compile(r"\b\d{3,}\*?\b")
    
    def get_bank_name(self) -> str:
        return "wells_fargo"
    
    def get_detection_keywords(self) -> List[str]:
        return ["WELLS FARGO", "NAVIGATE BUSINESS CHECKING", "WELLSFARGO.COM/BIZ"]
    
    def process_tables(self, tables: List[pd.DataFrame]) -> List[Transaction]:
        """Process Wells Fargo tables using exact logic from test file"""
        print(f"Processing {len(tables)} tables for Wells Fargo...")

        # Find transaction tables (exclude check summaries)
        transaction_tables = []
        
        for i, table in enumerate(tables):
            if table.empty:
                print(f"Table {i}: Empty - skipping")
                continue

            print(f"Table {i}: {table.shape[0]} rows, {table.shape[1]} cols")
            
            # Debug: Print sample of table content for small tables
            if table.shape[0] <= 5:
                print(f"  Small table {i} sample content:")
                for row_idx in range(min(3, len(table))):
                    row_content = []
                    for col_idx in range(min(5, table.shape[1])):
                        cell = str(table.iloc[row_idx, col_idx]).strip()
                        row_content.append(cell[:20] if cell else "EMPTY")
                    print(f"    Row {row_idx}: {' | '.join(row_content)}")

            if self._is_check_summary_table(table):
                print(f"  -> Skipping: Check summary table")
                continue
            
            if self._is_transaction_table(table):
                print(f"  -> Including: Transaction table")
                transaction_tables.append((i, table))
            else:
                print(f"  -> Skipping: Not a transaction table")

        if not transaction_tables:
            print("No transaction tables found")
            return []

        # Combine all transaction tables into raw rows with column standardization
        all_rows = []
        max_cols = 0
        
        for table_idx, table in transaction_tables:
            print(f"Processing table {table_idx}...")
            
            # Standardize table structure before processing
            standardized_table = self._standardize_table_structure(table, table_idx)
            max_cols = max(max_cols, standardized_table.shape[1])
            
            for row_idx in range(len(standardized_table)):
                row_data = []
                for col_idx in range(standardized_table.shape[1]):
                    cell = standardized_table.iloc[row_idx, col_idx]
                    if pd.isna(cell):
                        row_data.append("")
                    else:
                        row_data.append(str(cell).strip())
                
                # Pad row to max columns
                while len(row_data) < max_cols:
                    row_data.append("")
                
                all_rows.append(row_data)

        print(f"Combined {len(all_rows)} total rows from all transaction tables")

        # Process the raw rows using exact test file logic
        processed_rows = self._process_raw_rows(all_rows)
        
        # Convert to Transaction objects
        transactions = self._convert_to_transactions(processed_rows)
        
        print(f"Total transactions extracted: {len(transactions)}")
        return transactions
    
    def _standardize_table_structure(self, table: pd.DataFrame, table_idx: int) -> pd.DataFrame:
        """Standardize table structure to ensure columns are in correct order"""
        print(f"  Standardizing table {table_idx} structure...")
        
        # If table has fewer than expected columns, it might be malformed
        if table.shape[1] < 5:
            print(f"    Table {table_idx} has only {table.shape[1]} columns - might be text-based extraction")
            # For text-based extractions, try to restructure
            return self._restructure_text_table(table, table_idx)
        
        # Check if columns are in wrong order by examining content
        # Look for check numbers in wrong column
        check_in_wrong_col = False
        for row_idx in range(min(5, len(table))):
            for col_idx in range(table.shape[1]):
                cell = str(table.iloc[row_idx, col_idx]).strip()
                
                # If we find a 4-digit number (check number) in description column (col 2)
                if col_idx == 2 and re.match(r'^\d{4}
    
    def _is_transaction_table(self, table: pd.DataFrame) -> bool:
        """Check if table contains Wells Fargo transactions"""
        if table.shape[0] < 1:
            return False
        
        if table.shape[1] < 3:
            return False
        
        # Count transaction indicators
        date_count = 0
        amount_count = 0
        keyword_count = 0
        
        # Check all rows for patterns
        for row_idx in range(len(table)):
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
                keywords = ['bankcard', 'purchase', 'ach', 'deposit', 'payment', 'tobacco', 'mtot', 'shell', 'authorized']
                if any(keyword in cell_lower for keyword in keywords):
                    keyword_count += 1
        
        # More lenient criteria for small tables
        if table.shape[0] <= 3:
            return (date_count >= 1 and amount_count >= 1) or keyword_count >= 2
        else:
            return (date_count >= 2 and amount_count >= 2) or keyword_count >= 3

    def _is_check_summary_table(self, table: pd.DataFrame) -> bool:
        """Detect check summary tables and check image tables to exclude them"""
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
                
                if (re.match(r'^\d{4}$', col1) and re.match(r'^\d{1,2}/\d{1,2}$', col2)):
                    check_date_pattern_count += 1
        
        return check_date_pattern_count >= 3

    def _process_raw_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Process raw rows through the Wells Fargo pipeline"""
        print("Processing raw Wells Fargo data...")
        print(f"Starting with {len(all_rows)} total rows")
        
        # Debug: Look for Shell Oil transaction in raw data
        shell_found = False
        for i, row in enumerate(all_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found = True
                print(f"🔍 Found Shell Oil transaction at row {i}: {row[:4]}")
        
        if not shell_found:
            print("⚠️  Shell Oil transaction not found in raw data")
        
        # Step 1: Remove ending balance column
        processed_rows = self._remove_ending_balance_column(all_rows)
        print(f"After removing balance column: {len(processed_rows)} rows")
        
        # Step 2: Add monthly summary
        processed_rows = self._add_monthly_summary(processed_rows)
        print(f"After adding monthly summary: {len(processed_rows)} rows")
        
        # Step 3: Filter deposits (keep only EDI)
        processed_rows = self._filter_deposits_keep_edi(processed_rows)
        print(f"After filtering deposits: {len(processed_rows)} rows")
        
        # Debug: Check if Shell Oil is still there after filtering
        shell_found_after_filter = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_after_filter = True
                print(f"🔍 Shell Oil still present after filtering at row {i}: {row[:4]}")
        
        if shell_found and not shell_found_after_filter:
            print("❌ Shell Oil transaction was removed during deposit filtering!")
        
        # Step 4: Sort by transaction type
        processed_rows = self._sort_transactions_by_type(processed_rows)
        print(f"After sorting by type: {len(processed_rows)} rows")
        
        # Step 5: Merge amount columns
        processed_rows = self._merge_amount_columns(processed_rows)
        print(f"After merging amount columns: {len(processed_rows)} rows")
        
        # Step 6: Remove description-only rows
        processed_rows = self._remove_description_only_rows(processed_rows)
        print(f"Final processed rows: {len(processed_rows)} rows")
        
        # Final debug: Check if Shell Oil made it through the entire pipeline
        shell_found_final = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_final = True
                print(f"✅ Shell Oil transaction final position at row {i}: {row}")
        
        if shell_found and not shell_found_final:
            print("❌ Shell Oil transaction was lost somewhere in the processing pipeline!")
        
        return processed_rows

    def _remove_ending_balance_column(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove the ending balance column if detected"""
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
            print(f"Removing balance column: {balance_count}/{non_empty_count} cells are amounts")
            for row in all_rows:
                if len(row) > last_col_idx:
                    row.pop()
        
        return all_rows

    def _add_monthly_summary(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Add monthly summary row at the top with deposits total"""
        if not all_rows:
            return all_rows
        
        print("Creating monthly summary...")
        
        # Calculate totals from all transactions
        deposits_total = 0.0
        month_year = None
        
        for row in all_rows:
            # Look for date in first column (format: M/D)
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                if not month_year:
                    # Use first date to determine month/year
                    month, day = row[0].strip().split('/')
                    month_year = f"{month.zfill(2)}/2022"
                
                # Calculate totals from deposits/credits column
                if len(row) >= 5:
                    # Deposits (Column 4)
                    if row[3] and row[3].strip():
                        try:
                            amount = float(row[3].strip().replace(',', ''))
                            deposits_total += amount
                        except ValueError:
                            pass
        
        if month_year:
            # Get last day of the month
            month_num = int(month_year.split('/')[0])
            year_num = int(month_year.split('/')[1])
            
            # Calculate last day of month
            if month_num in [1, 3, 5, 7, 8, 10, 12]:
                last_day = 31
            elif month_num in [4, 6, 9, 11]:
                last_day = 30
            elif month_num == 2:
                if year_num % 4 == 0 and (year_num % 100 != 0 or year_num % 400 == 0):
                    last_day = 29
                else:
                    last_day = 28
            
            last_date = f"{month_num:02d}/{last_day:02d}/{year_num}"
            
            # Create summary row
            if len(all_rows[0]) >= 4:
                summary_row = [
                    last_date,
                    "",
                    "Deposits",
                    f"{deposits_total:.2f}"
                ]
                
                # Pad to match row length
                while len(summary_row) < len(all_rows[0]):
                    summary_row.append("")
                
                print(f"Summary: {last_date} | Deposits: ${deposits_total:.2f}")
                
                # Insert at the beginning
                all_rows.insert(0, summary_row)
        
        return all_rows

    def _filter_deposits_keep_edi(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove regular deposit entries but keep EDI payments"""
        if not all_rows:
            return all_rows
        
        print("Filtering deposits - keeping only EDI payments...")
        
        filtered_rows = []
        deposits_removed = 0
        edi_kept = 0
        
        for i, row in enumerate(all_rows):
            # Keep the first row (summary row)
            if i == 0:
                filtered_rows.append(row)
                continue
            
            # Debug Shell Oil specifically
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                print(f"🔍 Processing Shell Oil row: {row[:5]}")
            
            # Check if this is a deposit entry
            is_deposit = False
            is_edi = False
            
            if len(row) >= 5:
                # Has deposit amount but no withdrawal amount
                has_deposit = row[3] and row[3].strip() and row[3].strip() != ""
                has_withdrawal = row[4] and row[4].strip() and row[4].strip() != ""
                
                if "shell oil" in row_text:
                    print(f"   Shell Oil - has_deposit: {has_deposit}, has_withdrawal: {has_withdrawal}")
                    print(f"   Shell Oil - deposit col: '{row[3]}', withdrawal col: '{row[4]}'")
                
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
                        
                    if "shell oil" in row_text:
                        print(f"   Shell Oil - is_deposit: {is_deposit}, is_edi: {is_edi}")
            
            # Keep non-deposits, or EDI payments
            if not is_deposit or is_edi:
                filtered_rows.append(row)
                if is_edi:
                    edi_kept += 1
                if "shell oil" in row_text:
                    print(f"   ✅ Shell Oil KEPT (not deposit or is EDI)")
            else:
                deposits_removed += 1
                if "shell oil" in row_text:
                    print(f"   ❌ Shell Oil REMOVED (regular deposit)")
        
        print(f"Removed {deposits_removed} regular deposits, kept {edi_kept} EDI payments")
        return filtered_rows

    def _sort_transactions_by_type(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Sort transactions: Summary → EDI Payments → Withdrawals → Checks"""
        if not all_rows:
            return all_rows
        
        print("Sorting transactions by type...")
        
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
            transaction_type = self._classify_transaction(row)
            
            if transaction_type == "EDI":
                edi_payments.append(row)
            elif transaction_type == "CHECK":
                checks.append(row)
            elif transaction_type == "WITHDRAWAL":
                withdrawals.append(row)
            else:
                other_rows.append(row)
        
        # Sort each category by date
        edi_payments = self._sort_by_date(edi_payments)
        withdrawals = self._sort_by_date(withdrawals)
        checks = self._sort_by_date(checks)
        
        # Combine in desired order
        sorted_rows = summary_rows + edi_payments + withdrawals + checks + other_rows
        
        print(f"Sorted: {len(summary_rows)} summary, {len(edi_payments)} EDI, {len(withdrawals)} withdrawals, {len(checks)} checks")
        
        return sorted_rows

    def _classify_transaction(self, row: List[str]) -> str:
        """Classify a transaction as EDI, CHECK, WITHDRAWAL, or OTHER"""
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
        
        # Check if it's a withdrawal
        if len(row) >= 5:
            withdrawal_amount = row[4].strip() if row[4] else ""
            if withdrawal_amount and withdrawal_amount != "":
                return "WITHDRAWAL"
        
        # Check if it's a deposit
        if len(row) >= 4:
            deposit_amount = row[3].strip() if row[3] else ""
            if deposit_amount and deposit_amount != "":
                return "EDI"
        
        return "OTHER"

    def _sort_by_date(self, rows: List[List[str]]) -> List[List[str]]:
        """Sort rows by date (if date is available in first column)"""
        def get_date_key(row):
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                try:
                    month, day = row[0].strip().split('/')
                    return (int(month), int(day))
                except:
                    pass
            return (99, 99)
        
        return sorted(rows, key=get_date_key)

    def _merge_amount_columns(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Merge deposit and withdrawal columns into one amount column"""
        if not all_rows:
            return all_rows
        
        print("Merging deposit and withdrawal columns into one amount column...")
        
        merged_rows = []
        year = "2022"
        
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
                        final_amount = f"{deposit_amount:.2f}"
                    except ValueError:
                        pass
                
                if withdrawal_str and withdrawal_str != "":
                    try:
                        withdrawal_amount = float(withdrawal_str.replace(',', '').replace('-', ''))
                        final_amount = f"-{withdrawal_amount:.2f}"
                    except ValueError:
                        pass
                
                # Fix date format to include year
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                # Create new row: Date, Check#, Description, Amount
                new_row = [
                    date_str,
                    row[1],
                    row[2],
                    final_amount
                ]
                
                merged_rows.append(new_row)
            
            elif len(row) >= 4:
                # Handle rows with fewer columns
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                new_row = [date_str, row[1], row[2], row[3]]
                merged_rows.append(new_row)
            else:
                # Keep shorter rows as-is
                merged_rows.append(row)
        
        return merged_rows

    def _remove_description_only_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove rows that only have descriptions but no dates or amounts"""
        if not all_rows:
            return all_rows
        
        print("Removing description-only rows...")
        
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
                if date_cell and (re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_cell) or re.match(r'^\d{1,2}/\d{1,2}$', date_cell)):
                    has_date = True
            
            if len(row) >= 4:
                amount_cell = row[3].strip()
                if amount_cell and amount_cell != "":
                    try:
                        float(amount_cell.replace(',', '').replace('-', ''))
                        has_amount = True
                    except ValueError:
                        pass
            
            # Special case: if row contains transaction keywords but no date/amount, check if it's a partial transaction
            if not has_date and not has_amount:
                row_text = " ".join(row).lower()
                transaction_keywords = ['purchase authorized', 'shell oil', 'bankcard', 'mtot dep', 'ach debit']
                if any(keyword in row_text for keyword in transaction_keywords):
                    print(f"    Found potential transaction fragment: {row[:3]}")
                    cleaned_rows.append(row)
                    continue
            
            # Keep rows that have either date or amount (or both)
            if has_date or has_amount:
                cleaned_rows.append(row)
            else:
                removed_count += 1
                print(f"    Removed description-only: {row[:3]}")
        
        print(f"Removed {removed_count} description-only rows")
        return cleaned_rows

    def _convert_to_transactions(self, processed_rows: List[List[str]]) -> List[Transaction]:
        """Convert processed rows to Transaction objects"""
        transactions = []
        
        for row in processed_rows:
            if len(row) < 4:
                continue
                
            date_str = row[0].strip()
            check_number = row[1].strip() if row[1].strip() else None
            description = row[2].strip()
            amount_str = row[3].strip()
            
            if not date_str or not amount_str:
                continue
                
            try:
                amount = float(amount_str.replace(',', ''))
            except ValueError:
                continue
            
            # Determine transaction type
            if description == "Deposits":
                transaction_type = "deposit_summary"
            elif check_number and re.match(r'^\d{4}$', check_number):
                transaction_type = "check"
            elif any(keyword in description.lower() for keyword in ['edi', 'japan tobac', 'itg brands']):
                transaction_type = "edi_payment"
            elif amount < 0:
                transaction_type = "withdrawal"
            else:
                transaction_type = "deposit"
            
            transactions.append(Transaction(
                date=self._standardize_date(date_str),
                description=description,
                amount=amount,
                check_number=check_number,
                transaction_type=transaction_type
            ))
        
        return transactions, cell):
                    print(f"    Found check number '{cell}' in description column - table needs restructuring")
                    check_in_wrong_col = True
                    break
            if check_in_wrong_col:
                break
        
        if check_in_wrong_col:
            print(f"    Restructuring table {table_idx} columns...")
            return self._fix_column_order(table)
        
        print(f"    Table {table_idx} structure looks correct")
        return table
    
    def _restructure_text_table(self, table: pd.DataFrame, table_idx: int) -> pd.DataFrame:
        """Restructure tables that came from text-based extraction"""
        print(f"    Restructuring text-based table {table_idx}...")
        
        restructured_rows = []
        
        for row_idx in range(len(table)):
            # Get all non-empty cells from this row
            row_cells = []
            for col_idx in range(table.shape[1]):
                cell = str(table.iloc[row_idx, col_idx]).strip()
                if cell and cell != "nan":
                    row_cells.append(cell)
            
            if not row_cells:
                continue
            
            # Try to parse the row into: Date, Check#, Description, Deposits, Withdrawals
            parsed_row = self._parse_text_row(row_cells)
            if parsed_row:
                restructured_rows.append(parsed_row)
        
        if restructured_rows:
            # Create new DataFrame with proper structure
            return pd.DataFrame(restructured_rows)
        else:
            return table
    
    def _parse_text_row(self, cells: List[str]) -> Optional[List[str]]:
        """Parse a row of text cells into proper transaction format"""
        # Join all cells to work with full text
        full_text = " ".join(cells)
        
        # Look for date pattern
        date_match = re.search(r'\b(\d{1,2}/\d{1,2})\b', full_text)
        
        # Look for amount pattern
        amount_matches = re.findall(r'\b\d{1,3}(?:,\d{3})*\.\d{2}\b', full_text)
        
        # Look for check number pattern
        check_match = re.search(r'\b(\d{4})\b', full_text)
        
        if date_match and amount_matches:
            date_str = date_match.group(1)
            
            # Check number (if found)
            check_num = check_match.group(1) if check_match else ""
            
            # Description (everything between date and amounts, excluding check number)
            desc_start = date_match.end()
            desc_text = full_text[desc_start:].strip()
            
            # Remove check number and amounts from description
            if check_num:
                desc_text = desc_text.replace(check_num, "").strip()
            for amount in amount_matches:
                desc_text = desc_text.replace(amount, "").strip()
            
            # Clean up description
            desc_text = re.sub(r'\s+', ' ', desc_text).strip()
            
            # Determine if it's deposit or withdrawal based on context
            # If it has "Purchase" or "Check" in description, it's likely a withdrawal
            is_withdrawal = any(keyword in desc_text.lower() for keyword in 
                              ['purchase', 'check', 'fee', 'withdrawal', 'ach debit'])
            
            # Assign amount to appropriate column
            main_amount = amount_matches[-1]  # Use last/largest amount
            
            if is_withdrawal or check_num:
                # Withdrawal: amount goes in withdrawals column
                return [date_str, check_num, desc_text, "", main_amount]
            else:
                # Deposit: amount goes in deposits column  
                return [date_str, check_num, desc_text, main_amount, ""]
        
        return None
    
    def _fix_column_order(self, table: pd.DataFrame) -> pd.DataFrame:
        """Fix table where columns are in wrong order"""
        print("    Attempting to fix column order...")
        
        fixed_rows = []
        
        for row_idx in range(len(table)):
            row_data = []
            for col_idx in range(table.shape[1]):
                cell = table.iloc[row_idx, col_idx]
                if pd.isna(cell):
                    row_data.append("")
                else:
                    row_data.append(str(cell).strip())
            
            # Try to identify and rearrange columns
            # Expected order: Date, Check#, Description, Deposits, Withdrawals
            fixed_row = self._rearrange_row_columns(row_data)
            fixed_rows.append(fixed_row)
        
        return pd.DataFrame(fixed_rows)
    
    def _rearrange_row_columns(self, row: List[str]) -> List[str]:
        """Rearrange a single row to correct column order"""
        # Find date
        date_col = None
        for i, cell in enumerate(row):
            if re.match(r'^\d{1,2}/\d{1,2}
    
    def _is_transaction_table(self, table: pd.DataFrame) -> bool:
        """Check if table contains Wells Fargo transactions"""
        if table.shape[0] < 1:
            return False
        
        if table.shape[1] < 3:
            return False
        
        # Count transaction indicators
        date_count = 0
        amount_count = 0
        keyword_count = 0
        
        # Check all rows for patterns
        for row_idx in range(len(table)):
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
                keywords = ['bankcard', 'purchase', 'ach', 'deposit', 'payment', 'tobacco', 'mtot', 'shell', 'authorized']
                if any(keyword in cell_lower for keyword in keywords):
                    keyword_count += 1
        
        # More lenient criteria for small tables
        if table.shape[0] <= 3:
            return (date_count >= 1 and amount_count >= 1) or keyword_count >= 2
        else:
            return (date_count >= 2 and amount_count >= 2) or keyword_count >= 3

    def _is_check_summary_table(self, table: pd.DataFrame) -> bool:
        """Detect check summary tables and check image tables to exclude them"""
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
                
                if (re.match(r'^\d{4}$', col1) and re.match(r'^\d{1,2}/\d{1,2}$', col2)):
                    check_date_pattern_count += 1
        
        return check_date_pattern_count >= 3

    def _process_raw_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Process raw rows through the Wells Fargo pipeline"""
        print("Processing raw Wells Fargo data...")
        print(f"Starting with {len(all_rows)} total rows")
        
        # Debug: Look for Shell Oil transaction in raw data
        shell_found = False
        for i, row in enumerate(all_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found = True
                print(f"🔍 Found Shell Oil transaction at row {i}: {row[:4]}")
        
        if not shell_found:
            print("⚠️  Shell Oil transaction not found in raw data")
        
        # Step 1: Remove ending balance column
        processed_rows = self._remove_ending_balance_column(all_rows)
        print(f"After removing balance column: {len(processed_rows)} rows")
        
        # Step 2: Add monthly summary
        processed_rows = self._add_monthly_summary(processed_rows)
        print(f"After adding monthly summary: {len(processed_rows)} rows")
        
        # Step 3: Filter deposits (keep only EDI)
        processed_rows = self._filter_deposits_keep_edi(processed_rows)
        print(f"After filtering deposits: {len(processed_rows)} rows")
        
        # Debug: Check if Shell Oil is still there after filtering
        shell_found_after_filter = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_after_filter = True
                print(f"🔍 Shell Oil still present after filtering at row {i}: {row[:4]}")
        
        if shell_found and not shell_found_after_filter:
            print("❌ Shell Oil transaction was removed during deposit filtering!")
        
        # Step 4: Sort by transaction type
        processed_rows = self._sort_transactions_by_type(processed_rows)
        print(f"After sorting by type: {len(processed_rows)} rows")
        
        # Step 5: Merge amount columns
        processed_rows = self._merge_amount_columns(processed_rows)
        print(f"After merging amount columns: {len(processed_rows)} rows")
        
        # Step 6: Remove description-only rows
        processed_rows = self._remove_description_only_rows(processed_rows)
        print(f"Final processed rows: {len(processed_rows)} rows")
        
        # Final debug: Check if Shell Oil made it through the entire pipeline
        shell_found_final = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_final = True
                print(f"✅ Shell Oil transaction final position at row {i}: {row}")
        
        if shell_found and not shell_found_final:
            print("❌ Shell Oil transaction was lost somewhere in the processing pipeline!")
        
        return processed_rows

    def _remove_ending_balance_column(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove the ending balance column if detected"""
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
            print(f"Removing balance column: {balance_count}/{non_empty_count} cells are amounts")
            for row in all_rows:
                if len(row) > last_col_idx:
                    row.pop()
        
        return all_rows

    def _add_monthly_summary(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Add monthly summary row at the top with deposits total"""
        if not all_rows:
            return all_rows
        
        print("Creating monthly summary...")
        
        # Calculate totals from all transactions
        deposits_total = 0.0
        month_year = None
        
        for row in all_rows:
            # Look for date in first column (format: M/D)
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                if not month_year:
                    # Use first date to determine month/year
                    month, day = row[0].strip().split('/')
                    month_year = f"{month.zfill(2)}/2022"
                
                # Calculate totals from deposits/credits column
                if len(row) >= 5:
                    # Deposits (Column 4)
                    if row[3] and row[3].strip():
                        try:
                            amount = float(row[3].strip().replace(',', ''))
                            deposits_total += amount
                        except ValueError:
                            pass
        
        if month_year:
            # Get last day of the month
            month_num = int(month_year.split('/')[0])
            year_num = int(month_year.split('/')[1])
            
            # Calculate last day of month
            if month_num in [1, 3, 5, 7, 8, 10, 12]:
                last_day = 31
            elif month_num in [4, 6, 9, 11]:
                last_day = 30
            elif month_num == 2:
                if year_num % 4 == 0 and (year_num % 100 != 0 or year_num % 400 == 0):
                    last_day = 29
                else:
                    last_day = 28
            
            last_date = f"{month_num:02d}/{last_day:02d}/{year_num}"
            
            # Create summary row
            if len(all_rows[0]) >= 4:
                summary_row = [
                    last_date,
                    "",
                    "Deposits",
                    f"{deposits_total:.2f}"
                ]
                
                # Pad to match row length
                while len(summary_row) < len(all_rows[0]):
                    summary_row.append("")
                
                print(f"Summary: {last_date} | Deposits: ${deposits_total:.2f}")
                
                # Insert at the beginning
                all_rows.insert(0, summary_row)
        
        return all_rows

    def _filter_deposits_keep_edi(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove regular deposit entries but keep EDI payments"""
        if not all_rows:
            return all_rows
        
        print("Filtering deposits - keeping only EDI payments...")
        
        filtered_rows = []
        deposits_removed = 0
        edi_kept = 0
        
        for i, row in enumerate(all_rows):
            # Keep the first row (summary row)
            if i == 0:
                filtered_rows.append(row)
                continue
            
            # Debug Shell Oil specifically
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                print(f"🔍 Processing Shell Oil row: {row[:5]}")
            
            # Check if this is a deposit entry
            is_deposit = False
            is_edi = False
            
            if len(row) >= 5:
                # Has deposit amount but no withdrawal amount
                has_deposit = row[3] and row[3].strip() and row[3].strip() != ""
                has_withdrawal = row[4] and row[4].strip() and row[4].strip() != ""
                
                if "shell oil" in row_text:
                    print(f"   Shell Oil - has_deposit: {has_deposit}, has_withdrawal: {has_withdrawal}")
                    print(f"   Shell Oil - deposit col: '{row[3]}', withdrawal col: '{row[4]}'")
                
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
                        
                    if "shell oil" in row_text:
                        print(f"   Shell Oil - is_deposit: {is_deposit}, is_edi: {is_edi}")
            
            # Keep non-deposits, or EDI payments
            if not is_deposit or is_edi:
                filtered_rows.append(row)
                if is_edi:
                    edi_kept += 1
                if "shell oil" in row_text:
                    print(f"   ✅ Shell Oil KEPT (not deposit or is EDI)")
            else:
                deposits_removed += 1
                if "shell oil" in row_text:
                    print(f"   ❌ Shell Oil REMOVED (regular deposit)")
        
        print(f"Removed {deposits_removed} regular deposits, kept {edi_kept} EDI payments")
        return filtered_rows

    def _sort_transactions_by_type(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Sort transactions: Summary → EDI Payments → Withdrawals → Checks"""
        if not all_rows:
            return all_rows
        
        print("Sorting transactions by type...")
        
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
            transaction_type = self._classify_transaction(row)
            
            if transaction_type == "EDI":
                edi_payments.append(row)
            elif transaction_type == "CHECK":
                checks.append(row)
            elif transaction_type == "WITHDRAWAL":
                withdrawals.append(row)
            else:
                other_rows.append(row)
        
        # Sort each category by date
        edi_payments = self._sort_by_date(edi_payments)
        withdrawals = self._sort_by_date(withdrawals)
        checks = self._sort_by_date(checks)
        
        # Combine in desired order
        sorted_rows = summary_rows + edi_payments + withdrawals + checks + other_rows
        
        print(f"Sorted: {len(summary_rows)} summary, {len(edi_payments)} EDI, {len(withdrawals)} withdrawals, {len(checks)} checks")
        
        return sorted_rows

    def _classify_transaction(self, row: List[str]) -> str:
        """Classify a transaction as EDI, CHECK, WITHDRAWAL, or OTHER"""
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
        
        # Check if it's a withdrawal
        if len(row) >= 5:
            withdrawal_amount = row[4].strip() if row[4] else ""
            if withdrawal_amount and withdrawal_amount != "":
                return "WITHDRAWAL"
        
        # Check if it's a deposit
        if len(row) >= 4:
            deposit_amount = row[3].strip() if row[3] else ""
            if deposit_amount and deposit_amount != "":
                return "EDI"
        
        return "OTHER"

    def _sort_by_date(self, rows: List[List[str]]) -> List[List[str]]:
        """Sort rows by date (if date is available in first column)"""
        def get_date_key(row):
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                try:
                    month, day = row[0].strip().split('/')
                    return (int(month), int(day))
                except:
                    pass
            return (99, 99)
        
        return sorted(rows, key=get_date_key)

    def _merge_amount_columns(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Merge deposit and withdrawal columns into one amount column"""
        if not all_rows:
            return all_rows
        
        print("Merging deposit and withdrawal columns into one amount column...")
        
        merged_rows = []
        year = "2022"
        
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
                        final_amount = f"{deposit_amount:.2f}"
                    except ValueError:
                        pass
                
                if withdrawal_str and withdrawal_str != "":
                    try:
                        withdrawal_amount = float(withdrawal_str.replace(',', '').replace('-', ''))
                        final_amount = f"-{withdrawal_amount:.2f}"
                    except ValueError:
                        pass
                
                # Fix date format to include year
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                # Create new row: Date, Check#, Description, Amount
                new_row = [
                    date_str,
                    row[1],
                    row[2],
                    final_amount
                ]
                
                merged_rows.append(new_row)
            
            elif len(row) >= 4:
                # Handle rows with fewer columns
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                new_row = [date_str, row[1], row[2], row[3]]
                merged_rows.append(new_row)
            else:
                # Keep shorter rows as-is
                merged_rows.append(row)
        
        return merged_rows

    def _remove_description_only_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove rows that only have descriptions but no dates or amounts"""
        if not all_rows:
            return all_rows
        
        print("Removing description-only rows...")
        
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
                if date_cell and (re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_cell) or re.match(r'^\d{1,2}/\d{1,2}$', date_cell)):
                    has_date = True
            
            if len(row) >= 4:
                amount_cell = row[3].strip()
                if amount_cell and amount_cell != "":
                    try:
                        float(amount_cell.replace(',', '').replace('-', ''))
                        has_amount = True
                    except ValueError:
                        pass
            
            # Special case: if row contains transaction keywords but no date/amount, check if it's a partial transaction
            if not has_date and not has_amount:
                row_text = " ".join(row).lower()
                transaction_keywords = ['purchase authorized', 'shell oil', 'bankcard', 'mtot dep', 'ach debit']
                if any(keyword in row_text for keyword in transaction_keywords):
                    print(f"    Found potential transaction fragment: {row[:3]}")
                    cleaned_rows.append(row)
                    continue
            
            # Keep rows that have either date or amount (or both)
            if has_date or has_amount:
                cleaned_rows.append(row)
            else:
                removed_count += 1
                print(f"    Removed description-only: {row[:3]}")
        
        print(f"Removed {removed_count} description-only rows")
        return cleaned_rows

    def _convert_to_transactions(self, processed_rows: List[List[str]]) -> List[Transaction]:
        """Convert processed rows to Transaction objects"""
        transactions = []
        
        for row in processed_rows:
            if len(row) < 4:
                continue
                
            date_str = row[0].strip()
            check_number = row[1].strip() if row[1].strip() else None
            description = row[2].strip()
            amount_str = row[3].strip()
            
            if not date_str or not amount_str:
                continue
                
            try:
                amount = float(amount_str.replace(',', ''))
            except ValueError:
                continue
            
            # Determine transaction type
            if description == "Deposits":
                transaction_type = "deposit_summary"
            elif check_number and re.match(r'^\d{4}$', check_number):
                transaction_type = "check"
            elif any(keyword in description.lower() for keyword in ['edi', 'japan tobac', 'itg brands']):
                transaction_type = "edi_payment"
            elif amount < 0:
                transaction_type = "withdrawal"
            else:
                transaction_type = "deposit"
            
            transactions.append(Transaction(
                date=self._standardize_date(date_str),
                description=description,
                amount=amount,
                check_number=check_number,
                transaction_type=transaction_type
            ))
        
        return transactions, cell.strip()):
                date_col = i
                break
        
        # Find check number (4 digits)
        check_col = None
        check_num = ""
        for i, cell in enumerate(row):
            if re.match(r'^\d{4}
    
    def _is_transaction_table(self, table: pd.DataFrame) -> bool:
        """Check if table contains Wells Fargo transactions"""
        if table.shape[0] < 1:
            return False
        
        if table.shape[1] < 3:
            return False
        
        # Count transaction indicators
        date_count = 0
        amount_count = 0
        keyword_count = 0
        
        # Check all rows for patterns
        for row_idx in range(len(table)):
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
                keywords = ['bankcard', 'purchase', 'ach', 'deposit', 'payment', 'tobacco', 'mtot', 'shell', 'authorized']
                if any(keyword in cell_lower for keyword in keywords):
                    keyword_count += 1
        
        # More lenient criteria for small tables
        if table.shape[0] <= 3:
            return (date_count >= 1 and amount_count >= 1) or keyword_count >= 2
        else:
            return (date_count >= 2 and amount_count >= 2) or keyword_count >= 3

    def _is_check_summary_table(self, table: pd.DataFrame) -> bool:
        """Detect check summary tables and check image tables to exclude them"""
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
                
                if (re.match(r'^\d{4}$', col1) and re.match(r'^\d{1,2}/\d{1,2}$', col2)):
                    check_date_pattern_count += 1
        
        return check_date_pattern_count >= 3

    def _process_raw_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Process raw rows through the Wells Fargo pipeline"""
        print("Processing raw Wells Fargo data...")
        print(f"Starting with {len(all_rows)} total rows")
        
        # Debug: Look for Shell Oil transaction in raw data
        shell_found = False
        for i, row in enumerate(all_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found = True
                print(f"🔍 Found Shell Oil transaction at row {i}: {row[:4]}")
        
        if not shell_found:
            print("⚠️  Shell Oil transaction not found in raw data")
        
        # Step 1: Remove ending balance column
        processed_rows = self._remove_ending_balance_column(all_rows)
        print(f"After removing balance column: {len(processed_rows)} rows")
        
        # Step 2: Add monthly summary
        processed_rows = self._add_monthly_summary(processed_rows)
        print(f"After adding monthly summary: {len(processed_rows)} rows")
        
        # Step 3: Filter deposits (keep only EDI)
        processed_rows = self._filter_deposits_keep_edi(processed_rows)
        print(f"After filtering deposits: {len(processed_rows)} rows")
        
        # Debug: Check if Shell Oil is still there after filtering
        shell_found_after_filter = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_after_filter = True
                print(f"🔍 Shell Oil still present after filtering at row {i}: {row[:4]}")
        
        if shell_found and not shell_found_after_filter:
            print("❌ Shell Oil transaction was removed during deposit filtering!")
        
        # Step 4: Sort by transaction type
        processed_rows = self._sort_transactions_by_type(processed_rows)
        print(f"After sorting by type: {len(processed_rows)} rows")
        
        # Step 5: Merge amount columns
        processed_rows = self._merge_amount_columns(processed_rows)
        print(f"After merging amount columns: {len(processed_rows)} rows")
        
        # Step 6: Remove description-only rows
        processed_rows = self._remove_description_only_rows(processed_rows)
        print(f"Final processed rows: {len(processed_rows)} rows")
        
        # Final debug: Check if Shell Oil made it through the entire pipeline
        shell_found_final = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_final = True
                print(f"✅ Shell Oil transaction final position at row {i}: {row}")
        
        if shell_found and not shell_found_final:
            print("❌ Shell Oil transaction was lost somewhere in the processing pipeline!")
        
        return processed_rows

    def _remove_ending_balance_column(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove the ending balance column if detected"""
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
            print(f"Removing balance column: {balance_count}/{non_empty_count} cells are amounts")
            for row in all_rows:
                if len(row) > last_col_idx:
                    row.pop()
        
        return all_rows

    def _add_monthly_summary(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Add monthly summary row at the top with deposits total"""
        if not all_rows:
            return all_rows
        
        print("Creating monthly summary...")
        
        # Calculate totals from all transactions
        deposits_total = 0.0
        month_year = None
        
        for row in all_rows:
            # Look for date in first column (format: M/D)
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                if not month_year:
                    # Use first date to determine month/year
                    month, day = row[0].strip().split('/')
                    month_year = f"{month.zfill(2)}/2022"
                
                # Calculate totals from deposits/credits column
                if len(row) >= 5:
                    # Deposits (Column 4)
                    if row[3] and row[3].strip():
                        try:
                            amount = float(row[3].strip().replace(',', ''))
                            deposits_total += amount
                        except ValueError:
                            pass
        
        if month_year:
            # Get last day of the month
            month_num = int(month_year.split('/')[0])
            year_num = int(month_year.split('/')[1])
            
            # Calculate last day of month
            if month_num in [1, 3, 5, 7, 8, 10, 12]:
                last_day = 31
            elif month_num in [4, 6, 9, 11]:
                last_day = 30
            elif month_num == 2:
                if year_num % 4 == 0 and (year_num % 100 != 0 or year_num % 400 == 0):
                    last_day = 29
                else:
                    last_day = 28
            
            last_date = f"{month_num:02d}/{last_day:02d}/{year_num}"
            
            # Create summary row
            if len(all_rows[0]) >= 4:
                summary_row = [
                    last_date,
                    "",
                    "Deposits",
                    f"{deposits_total:.2f}"
                ]
                
                # Pad to match row length
                while len(summary_row) < len(all_rows[0]):
                    summary_row.append("")
                
                print(f"Summary: {last_date} | Deposits: ${deposits_total:.2f}")
                
                # Insert at the beginning
                all_rows.insert(0, summary_row)
        
        return all_rows

    def _filter_deposits_keep_edi(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove regular deposit entries but keep EDI payments"""
        if not all_rows:
            return all_rows
        
        print("Filtering deposits - keeping only EDI payments...")
        
        filtered_rows = []
        deposits_removed = 0
        edi_kept = 0
        
        for i, row in enumerate(all_rows):
            # Keep the first row (summary row)
            if i == 0:
                filtered_rows.append(row)
                continue
            
            # Debug Shell Oil specifically
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                print(f"🔍 Processing Shell Oil row: {row[:5]}")
            
            # Check if this is a deposit entry
            is_deposit = False
            is_edi = False
            
            if len(row) >= 5:
                # Has deposit amount but no withdrawal amount
                has_deposit = row[3] and row[3].strip() and row[3].strip() != ""
                has_withdrawal = row[4] and row[4].strip() and row[4].strip() != ""
                
                if "shell oil" in row_text:
                    print(f"   Shell Oil - has_deposit: {has_deposit}, has_withdrawal: {has_withdrawal}")
                    print(f"   Shell Oil - deposit col: '{row[3]}', withdrawal col: '{row[4]}'")
                
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
                        
                    if "shell oil" in row_text:
                        print(f"   Shell Oil - is_deposit: {is_deposit}, is_edi: {is_edi}")
            
            # Keep non-deposits, or EDI payments
            if not is_deposit or is_edi:
                filtered_rows.append(row)
                if is_edi:
                    edi_kept += 1
                if "shell oil" in row_text:
                    print(f"   ✅ Shell Oil KEPT (not deposit or is EDI)")
            else:
                deposits_removed += 1
                if "shell oil" in row_text:
                    print(f"   ❌ Shell Oil REMOVED (regular deposit)")
        
        print(f"Removed {deposits_removed} regular deposits, kept {edi_kept} EDI payments")
        return filtered_rows

    def _sort_transactions_by_type(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Sort transactions: Summary → EDI Payments → Withdrawals → Checks"""
        if not all_rows:
            return all_rows
        
        print("Sorting transactions by type...")
        
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
            transaction_type = self._classify_transaction(row)
            
            if transaction_type == "EDI":
                edi_payments.append(row)
            elif transaction_type == "CHECK":
                checks.append(row)
            elif transaction_type == "WITHDRAWAL":
                withdrawals.append(row)
            else:
                other_rows.append(row)
        
        # Sort each category by date
        edi_payments = self._sort_by_date(edi_payments)
        withdrawals = self._sort_by_date(withdrawals)
        checks = self._sort_by_date(checks)
        
        # Combine in desired order
        sorted_rows = summary_rows + edi_payments + withdrawals + checks + other_rows
        
        print(f"Sorted: {len(summary_rows)} summary, {len(edi_payments)} EDI, {len(withdrawals)} withdrawals, {len(checks)} checks")
        
        return sorted_rows

    def _classify_transaction(self, row: List[str]) -> str:
        """Classify a transaction as EDI, CHECK, WITHDRAWAL, or OTHER"""
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
        
        # Check if it's a withdrawal
        if len(row) >= 5:
            withdrawal_amount = row[4].strip() if row[4] else ""
            if withdrawal_amount and withdrawal_amount != "":
                return "WITHDRAWAL"
        
        # Check if it's a deposit
        if len(row) >= 4:
            deposit_amount = row[3].strip() if row[3] else ""
            if deposit_amount and deposit_amount != "":
                return "EDI"
        
        return "OTHER"

    def _sort_by_date(self, rows: List[List[str]]) -> List[List[str]]:
        """Sort rows by date (if date is available in first column)"""
        def get_date_key(row):
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                try:
                    month, day = row[0].strip().split('/')
                    return (int(month), int(day))
                except:
                    pass
            return (99, 99)
        
        return sorted(rows, key=get_date_key)

    def _merge_amount_columns(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Merge deposit and withdrawal columns into one amount column"""
        if not all_rows:
            return all_rows
        
        print("Merging deposit and withdrawal columns into one amount column...")
        
        merged_rows = []
        year = "2022"
        
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
                        final_amount = f"{deposit_amount:.2f}"
                    except ValueError:
                        pass
                
                if withdrawal_str and withdrawal_str != "":
                    try:
                        withdrawal_amount = float(withdrawal_str.replace(',', '').replace('-', ''))
                        final_amount = f"-{withdrawal_amount:.2f}"
                    except ValueError:
                        pass
                
                # Fix date format to include year
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                # Create new row: Date, Check#, Description, Amount
                new_row = [
                    date_str,
                    row[1],
                    row[2],
                    final_amount
                ]
                
                merged_rows.append(new_row)
            
            elif len(row) >= 4:
                # Handle rows with fewer columns
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                new_row = [date_str, row[1], row[2], row[3]]
                merged_rows.append(new_row)
            else:
                # Keep shorter rows as-is
                merged_rows.append(row)
        
        return merged_rows

    def _remove_description_only_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove rows that only have descriptions but no dates or amounts"""
        if not all_rows:
            return all_rows
        
        print("Removing description-only rows...")
        
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
                if date_cell and (re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_cell) or re.match(r'^\d{1,2}/\d{1,2}$', date_cell)):
                    has_date = True
            
            if len(row) >= 4:
                amount_cell = row[3].strip()
                if amount_cell and amount_cell != "":
                    try:
                        float(amount_cell.replace(',', '').replace('-', ''))
                        has_amount = True
                    except ValueError:
                        pass
            
            # Special case: if row contains transaction keywords but no date/amount, check if it's a partial transaction
            if not has_date and not has_amount:
                row_text = " ".join(row).lower()
                transaction_keywords = ['purchase authorized', 'shell oil', 'bankcard', 'mtot dep', 'ach debit']
                if any(keyword in row_text for keyword in transaction_keywords):
                    print(f"    Found potential transaction fragment: {row[:3]}")
                    cleaned_rows.append(row)
                    continue
            
            # Keep rows that have either date or amount (or both)
            if has_date or has_amount:
                cleaned_rows.append(row)
            else:
                removed_count += 1
                print(f"    Removed description-only: {row[:3]}")
        
        print(f"Removed {removed_count} description-only rows")
        return cleaned_rows

    def _convert_to_transactions(self, processed_rows: List[List[str]]) -> List[Transaction]:
        """Convert processed rows to Transaction objects"""
        transactions = []
        
        for row in processed_rows:
            if len(row) < 4:
                continue
                
            date_str = row[0].strip()
            check_number = row[1].strip() if row[1].strip() else None
            description = row[2].strip()
            amount_str = row[3].strip()
            
            if not date_str or not amount_str:
                continue
                
            try:
                amount = float(amount_str.replace(',', ''))
            except ValueError:
                continue
            
            # Determine transaction type
            if description == "Deposits":
                transaction_type = "deposit_summary"
            elif check_number and re.match(r'^\d{4}$', check_number):
                transaction_type = "check"
            elif any(keyword in description.lower() for keyword in ['edi', 'japan tobac', 'itg brands']):
                transaction_type = "edi_payment"
            elif amount < 0:
                transaction_type = "withdrawal"
            else:
                transaction_type = "deposit"
            
            transactions.append(Transaction(
                date=self._standardize_date(date_str),
                description=description,
                amount=amount,
                check_number=check_number,
                transaction_type=transaction_type
            ))
        
        return transactions, cell.strip()):
                check_col = i
                check_num = cell.strip()
                break
        
        # Find amounts (deposits/withdrawals)
        amount_cols = []
        for i, cell in enumerate(row):
            if re.match(r'^\d{1,3}(,\d{3})*\.\d{2}
    
    def _is_transaction_table(self, table: pd.DataFrame) -> bool:
        """Check if table contains Wells Fargo transactions"""
        if table.shape[0] < 1:
            return False
        
        if table.shape[1] < 3:
            return False
        
        # Count transaction indicators
        date_count = 0
        amount_count = 0
        keyword_count = 0
        
        # Check all rows for patterns
        for row_idx in range(len(table)):
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
                keywords = ['bankcard', 'purchase', 'ach', 'deposit', 'payment', 'tobacco', 'mtot', 'shell', 'authorized']
                if any(keyword in cell_lower for keyword in keywords):
                    keyword_count += 1
        
        # More lenient criteria for small tables
        if table.shape[0] <= 3:
            return (date_count >= 1 and amount_count >= 1) or keyword_count >= 2
        else:
            return (date_count >= 2 and amount_count >= 2) or keyword_count >= 3

    def _is_check_summary_table(self, table: pd.DataFrame) -> bool:
        """Detect check summary tables and check image tables to exclude them"""
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
                
                if (re.match(r'^\d{4}$', col1) and re.match(r'^\d{1,2}/\d{1,2}$', col2)):
                    check_date_pattern_count += 1
        
        return check_date_pattern_count >= 3

    def _process_raw_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Process raw rows through the Wells Fargo pipeline"""
        print("Processing raw Wells Fargo data...")
        print(f"Starting with {len(all_rows)} total rows")
        
        # Debug: Look for Shell Oil transaction in raw data
        shell_found = False
        for i, row in enumerate(all_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found = True
                print(f"🔍 Found Shell Oil transaction at row {i}: {row[:4]}")
        
        if not shell_found:
            print("⚠️  Shell Oil transaction not found in raw data")
        
        # Step 1: Remove ending balance column
        processed_rows = self._remove_ending_balance_column(all_rows)
        print(f"After removing balance column: {len(processed_rows)} rows")
        
        # Step 2: Add monthly summary
        processed_rows = self._add_monthly_summary(processed_rows)
        print(f"After adding monthly summary: {len(processed_rows)} rows")
        
        # Step 3: Filter deposits (keep only EDI)
        processed_rows = self._filter_deposits_keep_edi(processed_rows)
        print(f"After filtering deposits: {len(processed_rows)} rows")
        
        # Debug: Check if Shell Oil is still there after filtering
        shell_found_after_filter = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_after_filter = True
                print(f"🔍 Shell Oil still present after filtering at row {i}: {row[:4]}")
        
        if shell_found and not shell_found_after_filter:
            print("❌ Shell Oil transaction was removed during deposit filtering!")
        
        # Step 4: Sort by transaction type
        processed_rows = self._sort_transactions_by_type(processed_rows)
        print(f"After sorting by type: {len(processed_rows)} rows")
        
        # Step 5: Merge amount columns
        processed_rows = self._merge_amount_columns(processed_rows)
        print(f"After merging amount columns: {len(processed_rows)} rows")
        
        # Step 6: Remove description-only rows
        processed_rows = self._remove_description_only_rows(processed_rows)
        print(f"Final processed rows: {len(processed_rows)} rows")
        
        # Final debug: Check if Shell Oil made it through the entire pipeline
        shell_found_final = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_final = True
                print(f"✅ Shell Oil transaction final position at row {i}: {row}")
        
        if shell_found and not shell_found_final:
            print("❌ Shell Oil transaction was lost somewhere in the processing pipeline!")
        
        return processed_rows

    def _remove_ending_balance_column(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove the ending balance column if detected"""
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
            print(f"Removing balance column: {balance_count}/{non_empty_count} cells are amounts")
            for row in all_rows:
                if len(row) > last_col_idx:
                    row.pop()
        
        return all_rows

    def _add_monthly_summary(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Add monthly summary row at the top with deposits total"""
        if not all_rows:
            return all_rows
        
        print("Creating monthly summary...")
        
        # Calculate totals from all transactions
        deposits_total = 0.0
        month_year = None
        
        for row in all_rows:
            # Look for date in first column (format: M/D)
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                if not month_year:
                    # Use first date to determine month/year
                    month, day = row[0].strip().split('/')
                    month_year = f"{month.zfill(2)}/2022"
                
                # Calculate totals from deposits/credits column
                if len(row) >= 5:
                    # Deposits (Column 4)
                    if row[3] and row[3].strip():
                        try:
                            amount = float(row[3].strip().replace(',', ''))
                            deposits_total += amount
                        except ValueError:
                            pass
        
        if month_year:
            # Get last day of the month
            month_num = int(month_year.split('/')[0])
            year_num = int(month_year.split('/')[1])
            
            # Calculate last day of month
            if month_num in [1, 3, 5, 7, 8, 10, 12]:
                last_day = 31
            elif month_num in [4, 6, 9, 11]:
                last_day = 30
            elif month_num == 2:
                if year_num % 4 == 0 and (year_num % 100 != 0 or year_num % 400 == 0):
                    last_day = 29
                else:
                    last_day = 28
            
            last_date = f"{month_num:02d}/{last_day:02d}/{year_num}"
            
            # Create summary row
            if len(all_rows[0]) >= 4:
                summary_row = [
                    last_date,
                    "",
                    "Deposits",
                    f"{deposits_total:.2f}"
                ]
                
                # Pad to match row length
                while len(summary_row) < len(all_rows[0]):
                    summary_row.append("")
                
                print(f"Summary: {last_date} | Deposits: ${deposits_total:.2f}")
                
                # Insert at the beginning
                all_rows.insert(0, summary_row)
        
        return all_rows

    def _filter_deposits_keep_edi(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove regular deposit entries but keep EDI payments"""
        if not all_rows:
            return all_rows
        
        print("Filtering deposits - keeping only EDI payments...")
        
        filtered_rows = []
        deposits_removed = 0
        edi_kept = 0
        
        for i, row in enumerate(all_rows):
            # Keep the first row (summary row)
            if i == 0:
                filtered_rows.append(row)
                continue
            
            # Debug Shell Oil specifically
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                print(f"🔍 Processing Shell Oil row: {row[:5]}")
            
            # Check if this is a deposit entry
            is_deposit = False
            is_edi = False
            
            if len(row) >= 5:
                # Has deposit amount but no withdrawal amount
                has_deposit = row[3] and row[3].strip() and row[3].strip() != ""
                has_withdrawal = row[4] and row[4].strip() and row[4].strip() != ""
                
                if "shell oil" in row_text:
                    print(f"   Shell Oil - has_deposit: {has_deposit}, has_withdrawal: {has_withdrawal}")
                    print(f"   Shell Oil - deposit col: '{row[3]}', withdrawal col: '{row[4]}'")
                
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
                        
                    if "shell oil" in row_text:
                        print(f"   Shell Oil - is_deposit: {is_deposit}, is_edi: {is_edi}")
            
            # Keep non-deposits, or EDI payments
            if not is_deposit or is_edi:
                filtered_rows.append(row)
                if is_edi:
                    edi_kept += 1
                if "shell oil" in row_text:
                    print(f"   ✅ Shell Oil KEPT (not deposit or is EDI)")
            else:
                deposits_removed += 1
                if "shell oil" in row_text:
                    print(f"   ❌ Shell Oil REMOVED (regular deposit)")
        
        print(f"Removed {deposits_removed} regular deposits, kept {edi_kept} EDI payments")
        return filtered_rows

    def _sort_transactions_by_type(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Sort transactions: Summary → EDI Payments → Withdrawals → Checks"""
        if not all_rows:
            return all_rows
        
        print("Sorting transactions by type...")
        
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
            transaction_type = self._classify_transaction(row)
            
            if transaction_type == "EDI":
                edi_payments.append(row)
            elif transaction_type == "CHECK":
                checks.append(row)
            elif transaction_type == "WITHDRAWAL":
                withdrawals.append(row)
            else:
                other_rows.append(row)
        
        # Sort each category by date
        edi_payments = self._sort_by_date(edi_payments)
        withdrawals = self._sort_by_date(withdrawals)
        checks = self._sort_by_date(checks)
        
        # Combine in desired order
        sorted_rows = summary_rows + edi_payments + withdrawals + checks + other_rows
        
        print(f"Sorted: {len(summary_rows)} summary, {len(edi_payments)} EDI, {len(withdrawals)} withdrawals, {len(checks)} checks")
        
        return sorted_rows

    def _classify_transaction(self, row: List[str]) -> str:
        """Classify a transaction as EDI, CHECK, WITHDRAWAL, or OTHER"""
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
        
        # Check if it's a withdrawal
        if len(row) >= 5:
            withdrawal_amount = row[4].strip() if row[4] else ""
            if withdrawal_amount and withdrawal_amount != "":
                return "WITHDRAWAL"
        
        # Check if it's a deposit
        if len(row) >= 4:
            deposit_amount = row[3].strip() if row[3] else ""
            if deposit_amount and deposit_amount != "":
                return "EDI"
        
        return "OTHER"

    def _sort_by_date(self, rows: List[List[str]]) -> List[List[str]]:
        """Sort rows by date (if date is available in first column)"""
        def get_date_key(row):
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                try:
                    month, day = row[0].strip().split('/')
                    return (int(month), int(day))
                except:
                    pass
            return (99, 99)
        
        return sorted(rows, key=get_date_key)

    def _merge_amount_columns(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Merge deposit and withdrawal columns into one amount column"""
        if not all_rows:
            return all_rows
        
        print("Merging deposit and withdrawal columns into one amount column...")
        
        merged_rows = []
        year = "2022"
        
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
                        final_amount = f"{deposit_amount:.2f}"
                    except ValueError:
                        pass
                
                if withdrawal_str and withdrawal_str != "":
                    try:
                        withdrawal_amount = float(withdrawal_str.replace(',', '').replace('-', ''))
                        final_amount = f"-{withdrawal_amount:.2f}"
                    except ValueError:
                        pass
                
                # Fix date format to include year
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                # Create new row: Date, Check#, Description, Amount
                new_row = [
                    date_str,
                    row[1],
                    row[2],
                    final_amount
                ]
                
                merged_rows.append(new_row)
            
            elif len(row) >= 4:
                # Handle rows with fewer columns
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                new_row = [date_str, row[1], row[2], row[3]]
                merged_rows.append(new_row)
            else:
                # Keep shorter rows as-is
                merged_rows.append(row)
        
        return merged_rows

    def _remove_description_only_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove rows that only have descriptions but no dates or amounts"""
        if not all_rows:
            return all_rows
        
        print("Removing description-only rows...")
        
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
                if date_cell and (re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_cell) or re.match(r'^\d{1,2}/\d{1,2}$', date_cell)):
                    has_date = True
            
            if len(row) >= 4:
                amount_cell = row[3].strip()
                if amount_cell and amount_cell != "":
                    try:
                        float(amount_cell.replace(',', '').replace('-', ''))
                        has_amount = True
                    except ValueError:
                        pass
            
            # Special case: if row contains transaction keywords but no date/amount, check if it's a partial transaction
            if not has_date and not has_amount:
                row_text = " ".join(row).lower()
                transaction_keywords = ['purchase authorized', 'shell oil', 'bankcard', 'mtot dep', 'ach debit']
                if any(keyword in row_text for keyword in transaction_keywords):
                    print(f"    Found potential transaction fragment: {row[:3]}")
                    cleaned_rows.append(row)
                    continue
            
            # Keep rows that have either date or amount (or both)
            if has_date or has_amount:
                cleaned_rows.append(row)
            else:
                removed_count += 1
                print(f"    Removed description-only: {row[:3]}")
        
        print(f"Removed {removed_count} description-only rows")
        return cleaned_rows

    def _convert_to_transactions(self, processed_rows: List[List[str]]) -> List[Transaction]:
        """Convert processed rows to Transaction objects"""
        transactions = []
        
        for row in processed_rows:
            if len(row) < 4:
                continue
                
            date_str = row[0].strip()
            check_number = row[1].strip() if row[1].strip() else None
            description = row[2].strip()
            amount_str = row[3].strip()
            
            if not date_str or not amount_str:
                continue
                
            try:
                amount = float(amount_str.replace(',', ''))
            except ValueError:
                continue
            
            # Determine transaction type
            if description == "Deposits":
                transaction_type = "deposit_summary"
            elif check_number and re.match(r'^\d{4}$', check_number):
                transaction_type = "check"
            elif any(keyword in description.lower() for keyword in ['edi', 'japan tobac', 'itg brands']):
                transaction_type = "edi_payment"
            elif amount < 0:
                transaction_type = "withdrawal"
            else:
                transaction_type = "deposit"
            
            transactions.append(Transaction(
                date=self._standardize_date(date_str),
                description=description,
                amount=amount,
                check_number=check_number,
                transaction_type=transaction_type
            ))
        
        return transactions, cell.strip()):
                amount_cols.append(i)
        
        # Build description from remaining text columns
        description_parts = []
        for i, cell in enumerate(row):
            if (i != date_col and i != check_col and i not in amount_cols and 
                cell.strip() and not re.match(r'^\d+\.?\d*
    
    def _is_transaction_table(self, table: pd.DataFrame) -> bool:
        """Check if table contains Wells Fargo transactions"""
        if table.shape[0] < 1:
            return False
        
        if table.shape[1] < 3:
            return False
        
        # Count transaction indicators
        date_count = 0
        amount_count = 0
        keyword_count = 0
        
        # Check all rows for patterns
        for row_idx in range(len(table)):
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
                keywords = ['bankcard', 'purchase', 'ach', 'deposit', 'payment', 'tobacco', 'mtot', 'shell', 'authorized']
                if any(keyword in cell_lower for keyword in keywords):
                    keyword_count += 1
        
        # More lenient criteria for small tables
        if table.shape[0] <= 3:
            return (date_count >= 1 and amount_count >= 1) or keyword_count >= 2
        else:
            return (date_count >= 2 and amount_count >= 2) or keyword_count >= 3

    def _is_check_summary_table(self, table: pd.DataFrame) -> bool:
        """Detect check summary tables and check image tables to exclude them"""
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
                
                if (re.match(r'^\d{4}$', col1) and re.match(r'^\d{1,2}/\d{1,2}$', col2)):
                    check_date_pattern_count += 1
        
        return check_date_pattern_count >= 3

    def _process_raw_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Process raw rows through the Wells Fargo pipeline"""
        print("Processing raw Wells Fargo data...")
        print(f"Starting with {len(all_rows)} total rows")
        
        # Debug: Look for Shell Oil transaction in raw data
        shell_found = False
        for i, row in enumerate(all_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found = True
                print(f"🔍 Found Shell Oil transaction at row {i}: {row[:4]}")
        
        if not shell_found:
            print("⚠️  Shell Oil transaction not found in raw data")
        
        # Step 1: Remove ending balance column
        processed_rows = self._remove_ending_balance_column(all_rows)
        print(f"After removing balance column: {len(processed_rows)} rows")
        
        # Step 2: Add monthly summary
        processed_rows = self._add_monthly_summary(processed_rows)
        print(f"After adding monthly summary: {len(processed_rows)} rows")
        
        # Step 3: Filter deposits (keep only EDI)
        processed_rows = self._filter_deposits_keep_edi(processed_rows)
        print(f"After filtering deposits: {len(processed_rows)} rows")
        
        # Debug: Check if Shell Oil is still there after filtering
        shell_found_after_filter = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_after_filter = True
                print(f"🔍 Shell Oil still present after filtering at row {i}: {row[:4]}")
        
        if shell_found and not shell_found_after_filter:
            print("❌ Shell Oil transaction was removed during deposit filtering!")
        
        # Step 4: Sort by transaction type
        processed_rows = self._sort_transactions_by_type(processed_rows)
        print(f"After sorting by type: {len(processed_rows)} rows")
        
        # Step 5: Merge amount columns
        processed_rows = self._merge_amount_columns(processed_rows)
        print(f"After merging amount columns: {len(processed_rows)} rows")
        
        # Step 6: Remove description-only rows
        processed_rows = self._remove_description_only_rows(processed_rows)
        print(f"Final processed rows: {len(processed_rows)} rows")
        
        # Final debug: Check if Shell Oil made it through the entire pipeline
        shell_found_final = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_final = True
                print(f"✅ Shell Oil transaction final position at row {i}: {row}")
        
        if shell_found and not shell_found_final:
            print("❌ Shell Oil transaction was lost somewhere in the processing pipeline!")
        
        return processed_rows

    def _remove_ending_balance_column(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove the ending balance column if detected"""
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
            print(f"Removing balance column: {balance_count}/{non_empty_count} cells are amounts")
            for row in all_rows:
                if len(row) > last_col_idx:
                    row.pop()
        
        return all_rows

    def _add_monthly_summary(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Add monthly summary row at the top with deposits total"""
        if not all_rows:
            return all_rows
        
        print("Creating monthly summary...")
        
        # Calculate totals from all transactions
        deposits_total = 0.0
        month_year = None
        
        for row in all_rows:
            # Look for date in first column (format: M/D)
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                if not month_year:
                    # Use first date to determine month/year
                    month, day = row[0].strip().split('/')
                    month_year = f"{month.zfill(2)}/2022"
                
                # Calculate totals from deposits/credits column
                if len(row) >= 5:
                    # Deposits (Column 4)
                    if row[3] and row[3].strip():
                        try:
                            amount = float(row[3].strip().replace(',', ''))
                            deposits_total += amount
                        except ValueError:
                            pass
        
        if month_year:
            # Get last day of the month
            month_num = int(month_year.split('/')[0])
            year_num = int(month_year.split('/')[1])
            
            # Calculate last day of month
            if month_num in [1, 3, 5, 7, 8, 10, 12]:
                last_day = 31
            elif month_num in [4, 6, 9, 11]:
                last_day = 30
            elif month_num == 2:
                if year_num % 4 == 0 and (year_num % 100 != 0 or year_num % 400 == 0):
                    last_day = 29
                else:
                    last_day = 28
            
            last_date = f"{month_num:02d}/{last_day:02d}/{year_num}"
            
            # Create summary row
            if len(all_rows[0]) >= 4:
                summary_row = [
                    last_date,
                    "",
                    "Deposits",
                    f"{deposits_total:.2f}"
                ]
                
                # Pad to match row length
                while len(summary_row) < len(all_rows[0]):
                    summary_row.append("")
                
                print(f"Summary: {last_date} | Deposits: ${deposits_total:.2f}")
                
                # Insert at the beginning
                all_rows.insert(0, summary_row)
        
        return all_rows

    def _filter_deposits_keep_edi(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove regular deposit entries but keep EDI payments"""
        if not all_rows:
            return all_rows
        
        print("Filtering deposits - keeping only EDI payments...")
        
        filtered_rows = []
        deposits_removed = 0
        edi_kept = 0
        
        for i, row in enumerate(all_rows):
            # Keep the first row (summary row)
            if i == 0:
                filtered_rows.append(row)
                continue
            
            # Debug Shell Oil specifically
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                print(f"🔍 Processing Shell Oil row: {row[:5]}")
            
            # Check if this is a deposit entry
            is_deposit = False
            is_edi = False
            
            if len(row) >= 5:
                # Has deposit amount but no withdrawal amount
                has_deposit = row[3] and row[3].strip() and row[3].strip() != ""
                has_withdrawal = row[4] and row[4].strip() and row[4].strip() != ""
                
                if "shell oil" in row_text:
                    print(f"   Shell Oil - has_deposit: {has_deposit}, has_withdrawal: {has_withdrawal}")
                    print(f"   Shell Oil - deposit col: '{row[3]}', withdrawal col: '{row[4]}'")
                
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
                        
                    if "shell oil" in row_text:
                        print(f"   Shell Oil - is_deposit: {is_deposit}, is_edi: {is_edi}")
            
            # Keep non-deposits, or EDI payments
            if not is_deposit or is_edi:
                filtered_rows.append(row)
                if is_edi:
                    edi_kept += 1
                if "shell oil" in row_text:
                    print(f"   ✅ Shell Oil KEPT (not deposit or is EDI)")
            else:
                deposits_removed += 1
                if "shell oil" in row_text:
                    print(f"   ❌ Shell Oil REMOVED (regular deposit)")
        
        print(f"Removed {deposits_removed} regular deposits, kept {edi_kept} EDI payments")
        return filtered_rows

    def _sort_transactions_by_type(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Sort transactions: Summary → EDI Payments → Withdrawals → Checks"""
        if not all_rows:
            return all_rows
        
        print("Sorting transactions by type...")
        
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
            transaction_type = self._classify_transaction(row)
            
            if transaction_type == "EDI":
                edi_payments.append(row)
            elif transaction_type == "CHECK":
                checks.append(row)
            elif transaction_type == "WITHDRAWAL":
                withdrawals.append(row)
            else:
                other_rows.append(row)
        
        # Sort each category by date
        edi_payments = self._sort_by_date(edi_payments)
        withdrawals = self._sort_by_date(withdrawals)
        checks = self._sort_by_date(checks)
        
        # Combine in desired order
        sorted_rows = summary_rows + edi_payments + withdrawals + checks + other_rows
        
        print(f"Sorted: {len(summary_rows)} summary, {len(edi_payments)} EDI, {len(withdrawals)} withdrawals, {len(checks)} checks")
        
        return sorted_rows

    def _classify_transaction(self, row: List[str]) -> str:
        """Classify a transaction as EDI, CHECK, WITHDRAWAL, or OTHER"""
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
        
        # Check if it's a withdrawal
        if len(row) >= 5:
            withdrawal_amount = row[4].strip() if row[4] else ""
            if withdrawal_amount and withdrawal_amount != "":
                return "WITHDRAWAL"
        
        # Check if it's a deposit
        if len(row) >= 4:
            deposit_amount = row[3].strip() if row[3] else ""
            if deposit_amount and deposit_amount != "":
                return "EDI"
        
        return "OTHER"

    def _sort_by_date(self, rows: List[List[str]]) -> List[List[str]]:
        """Sort rows by date (if date is available in first column)"""
        def get_date_key(row):
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                try:
                    month, day = row[0].strip().split('/')
                    return (int(month), int(day))
                except:
                    pass
            return (99, 99)
        
        return sorted(rows, key=get_date_key)

    def _merge_amount_columns(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Merge deposit and withdrawal columns into one amount column"""
        if not all_rows:
            return all_rows
        
        print("Merging deposit and withdrawal columns into one amount column...")
        
        merged_rows = []
        year = "2022"
        
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
                        final_amount = f"{deposit_amount:.2f}"
                    except ValueError:
                        pass
                
                if withdrawal_str and withdrawal_str != "":
                    try:
                        withdrawal_amount = float(withdrawal_str.replace(',', '').replace('-', ''))
                        final_amount = f"-{withdrawal_amount:.2f}"
                    except ValueError:
                        pass
                
                # Fix date format to include year
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                # Create new row: Date, Check#, Description, Amount
                new_row = [
                    date_str,
                    row[1],
                    row[2],
                    final_amount
                ]
                
                merged_rows.append(new_row)
            
            elif len(row) >= 4:
                # Handle rows with fewer columns
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                new_row = [date_str, row[1], row[2], row[3]]
                merged_rows.append(new_row)
            else:
                # Keep shorter rows as-is
                merged_rows.append(row)
        
        return merged_rows

    def _remove_description_only_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove rows that only have descriptions but no dates or amounts"""
        if not all_rows:
            return all_rows
        
        print("Removing description-only rows...")
        
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
                if date_cell and (re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_cell) or re.match(r'^\d{1,2}/\d{1,2}$', date_cell)):
                    has_date = True
            
            if len(row) >= 4:
                amount_cell = row[3].strip()
                if amount_cell and amount_cell != "":
                    try:
                        float(amount_cell.replace(',', '').replace('-', ''))
                        has_amount = True
                    except ValueError:
                        pass
            
            # Special case: if row contains transaction keywords but no date/amount, check if it's a partial transaction
            if not has_date and not has_amount:
                row_text = " ".join(row).lower()
                transaction_keywords = ['purchase authorized', 'shell oil', 'bankcard', 'mtot dep', 'ach debit']
                if any(keyword in row_text for keyword in transaction_keywords):
                    print(f"    Found potential transaction fragment: {row[:3]}")
                    cleaned_rows.append(row)
                    continue
            
            # Keep rows that have either date or amount (or both)
            if has_date or has_amount:
                cleaned_rows.append(row)
            else:
                removed_count += 1
                print(f"    Removed description-only: {row[:3]}")
        
        print(f"Removed {removed_count} description-only rows")
        return cleaned_rows

    def _convert_to_transactions(self, processed_rows: List[List[str]]) -> List[Transaction]:
        """Convert processed rows to Transaction objects"""
        transactions = []
        
        for row in processed_rows:
            if len(row) < 4:
                continue
                
            date_str = row[0].strip()
            check_number = row[1].strip() if row[1].strip() else None
            description = row[2].strip()
            amount_str = row[3].strip()
            
            if not date_str or not amount_str:
                continue
                
            try:
                amount = float(amount_str.replace(',', ''))
            except ValueError:
                continue
            
            # Determine transaction type
            if description == "Deposits":
                transaction_type = "deposit_summary"
            elif check_number and re.match(r'^\d{4}$', check_number):
                transaction_type = "check"
            elif any(keyword in description.lower() for keyword in ['edi', 'japan tobac', 'itg brands']):
                transaction_type = "edi_payment"
            elif amount < 0:
                transaction_type = "withdrawal"
            else:
                transaction_type = "deposit"
            
            transactions.append(Transaction(
                date=self._standardize_date(date_str),
                description=description,
                amount=amount,
                check_number=check_number,
                transaction_type=transaction_type
            ))
        
        return transactions, cell.strip())):
                description_parts.append(cell.strip())
        
        description = " ".join(description_parts).strip()
        
        # Determine deposits vs withdrawals
        deposits = ""
        withdrawals = ""
        
        if amount_cols:
            # If there's a check number or "purchase" in description, it's likely a withdrawal
            if check_num or "purchase" in description.lower():
                withdrawals = row[amount_cols[0]] if amount_cols else ""
            else:
                deposits = row[amount_cols[0]] if amount_cols else ""
        
        # Return in correct order: Date, Check#, Description, Deposits, Withdrawals
        return [
            row[date_col] if date_col is not None else "",
            check_num,
            description,
            deposits,
            withdrawals
        ]
    
    def _is_transaction_table(self, table: pd.DataFrame) -> bool:
        """Check if table contains Wells Fargo transactions"""
        if table.shape[0] < 1:
            return False
        
        if table.shape[1] < 3:
            return False
        
        # Count transaction indicators
        date_count = 0
        amount_count = 0
        keyword_count = 0
        
        # Check all rows for patterns
        for row_idx in range(len(table)):
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
                keywords = ['bankcard', 'purchase', 'ach', 'deposit', 'payment', 'tobacco', 'mtot', 'shell', 'authorized']
                if any(keyword in cell_lower for keyword in keywords):
                    keyword_count += 1
        
        # More lenient criteria for small tables
        if table.shape[0] <= 3:
            return (date_count >= 1 and amount_count >= 1) or keyword_count >= 2
        else:
            return (date_count >= 2 and amount_count >= 2) or keyword_count >= 3

    def _is_check_summary_table(self, table: pd.DataFrame) -> bool:
        """Detect check summary tables and check image tables to exclude them"""
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
                
                if (re.match(r'^\d{4}$', col1) and re.match(r'^\d{1,2}/\d{1,2}$', col2)):
                    check_date_pattern_count += 1
        
        return check_date_pattern_count >= 3

    def _process_raw_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Process raw rows through the Wells Fargo pipeline"""
        print("Processing raw Wells Fargo data...")
        print(f"Starting with {len(all_rows)} total rows")
        
        # Debug: Look for Shell Oil transaction in raw data
        shell_found = False
        for i, row in enumerate(all_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found = True
                print(f"🔍 Found Shell Oil transaction at row {i}: {row[:4]}")
        
        if not shell_found:
            print("⚠️  Shell Oil transaction not found in raw data")
        
        # Step 1: Remove ending balance column
        processed_rows = self._remove_ending_balance_column(all_rows)
        print(f"After removing balance column: {len(processed_rows)} rows")
        
        # Step 2: Add monthly summary
        processed_rows = self._add_monthly_summary(processed_rows)
        print(f"After adding monthly summary: {len(processed_rows)} rows")
        
        # Step 3: Filter deposits (keep only EDI)
        processed_rows = self._filter_deposits_keep_edi(processed_rows)
        print(f"After filtering deposits: {len(processed_rows)} rows")
        
        # Debug: Check if Shell Oil is still there after filtering
        shell_found_after_filter = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_after_filter = True
                print(f"🔍 Shell Oil still present after filtering at row {i}: {row[:4]}")
        
        if shell_found and not shell_found_after_filter:
            print("❌ Shell Oil transaction was removed during deposit filtering!")
        
        # Step 4: Sort by transaction type
        processed_rows = self._sort_transactions_by_type(processed_rows)
        print(f"After sorting by type: {len(processed_rows)} rows")
        
        # Step 5: Merge amount columns
        processed_rows = self._merge_amount_columns(processed_rows)
        print(f"After merging amount columns: {len(processed_rows)} rows")
        
        # Step 6: Remove description-only rows
        processed_rows = self._remove_description_only_rows(processed_rows)
        print(f"Final processed rows: {len(processed_rows)} rows")
        
        # Final debug: Check if Shell Oil made it through the entire pipeline
        shell_found_final = False
        for i, row in enumerate(processed_rows):
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                shell_found_final = True
                print(f"✅ Shell Oil transaction final position at row {i}: {row}")
        
        if shell_found and not shell_found_final:
            print("❌ Shell Oil transaction was lost somewhere in the processing pipeline!")
        
        return processed_rows

    def _remove_ending_balance_column(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove the ending balance column if detected"""
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
            print(f"Removing balance column: {balance_count}/{non_empty_count} cells are amounts")
            for row in all_rows:
                if len(row) > last_col_idx:
                    row.pop()
        
        return all_rows

    def _add_monthly_summary(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Add monthly summary row at the top with deposits total"""
        if not all_rows:
            return all_rows
        
        print("Creating monthly summary...")
        
        # Calculate totals from all transactions
        deposits_total = 0.0
        month_year = None
        
        for row in all_rows:
            # Look for date in first column (format: M/D)
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                if not month_year:
                    # Use first date to determine month/year
                    month, day = row[0].strip().split('/')
                    month_year = f"{month.zfill(2)}/2022"
                
                # Calculate totals from deposits/credits column
                if len(row) >= 5:
                    # Deposits (Column 4)
                    if row[3] and row[3].strip():
                        try:
                            amount = float(row[3].strip().replace(',', ''))
                            deposits_total += amount
                        except ValueError:
                            pass
        
        if month_year:
            # Get last day of the month
            month_num = int(month_year.split('/')[0])
            year_num = int(month_year.split('/')[1])
            
            # Calculate last day of month
            if month_num in [1, 3, 5, 7, 8, 10, 12]:
                last_day = 31
            elif month_num in [4, 6, 9, 11]:
                last_day = 30
            elif month_num == 2:
                if year_num % 4 == 0 and (year_num % 100 != 0 or year_num % 400 == 0):
                    last_day = 29
                else:
                    last_day = 28
            
            last_date = f"{month_num:02d}/{last_day:02d}/{year_num}"
            
            # Create summary row
            if len(all_rows[0]) >= 4:
                summary_row = [
                    last_date,
                    "",
                    "Deposits",
                    f"{deposits_total:.2f}"
                ]
                
                # Pad to match row length
                while len(summary_row) < len(all_rows[0]):
                    summary_row.append("")
                
                print(f"Summary: {last_date} | Deposits: ${deposits_total:.2f}")
                
                # Insert at the beginning
                all_rows.insert(0, summary_row)
        
        return all_rows

    def _filter_deposits_keep_edi(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove regular deposit entries but keep EDI payments"""
        if not all_rows:
            return all_rows
        
        print("Filtering deposits - keeping only EDI payments...")
        
        filtered_rows = []
        deposits_removed = 0
        edi_kept = 0
        
        for i, row in enumerate(all_rows):
            # Keep the first row (summary row)
            if i == 0:
                filtered_rows.append(row)
                continue
            
            # Debug Shell Oil specifically
            row_text = " ".join(row).lower()
            if "shell oil" in row_text:
                print(f"🔍 Processing Shell Oil row: {row[:5]}")
            
            # Check if this is a deposit entry
            is_deposit = False
            is_edi = False
            
            if len(row) >= 5:
                # Has deposit amount but no withdrawal amount
                has_deposit = row[3] and row[3].strip() and row[3].strip() != ""
                has_withdrawal = row[4] and row[4].strip() and row[4].strip() != ""
                
                if "shell oil" in row_text:
                    print(f"   Shell Oil - has_deposit: {has_deposit}, has_withdrawal: {has_withdrawal}")
                    print(f"   Shell Oil - deposit col: '{row[3]}', withdrawal col: '{row[4]}'")
                
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
                        
                    if "shell oil" in row_text:
                        print(f"   Shell Oil - is_deposit: {is_deposit}, is_edi: {is_edi}")
            
            # Keep non-deposits, or EDI payments
            if not is_deposit or is_edi:
                filtered_rows.append(row)
                if is_edi:
                    edi_kept += 1
                if "shell oil" in row_text:
                    print(f"   ✅ Shell Oil KEPT (not deposit or is EDI)")
            else:
                deposits_removed += 1
                if "shell oil" in row_text:
                    print(f"   ❌ Shell Oil REMOVED (regular deposit)")
        
        print(f"Removed {deposits_removed} regular deposits, kept {edi_kept} EDI payments")
        return filtered_rows

    def _sort_transactions_by_type(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Sort transactions: Summary → EDI Payments → Withdrawals → Checks"""
        if not all_rows:
            return all_rows
        
        print("Sorting transactions by type...")
        
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
            transaction_type = self._classify_transaction(row)
            
            if transaction_type == "EDI":
                edi_payments.append(row)
            elif transaction_type == "CHECK":
                checks.append(row)
            elif transaction_type == "WITHDRAWAL":
                withdrawals.append(row)
            else:
                other_rows.append(row)
        
        # Sort each category by date
        edi_payments = self._sort_by_date(edi_payments)
        withdrawals = self._sort_by_date(withdrawals)
        checks = self._sort_by_date(checks)
        
        # Combine in desired order
        sorted_rows = summary_rows + edi_payments + withdrawals + checks + other_rows
        
        print(f"Sorted: {len(summary_rows)} summary, {len(edi_payments)} EDI, {len(withdrawals)} withdrawals, {len(checks)} checks")
        
        return sorted_rows

    def _classify_transaction(self, row: List[str]) -> str:
        """Classify a transaction as EDI, CHECK, WITHDRAWAL, or OTHER"""
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
        
        # Check if it's a withdrawal
        if len(row) >= 5:
            withdrawal_amount = row[4].strip() if row[4] else ""
            if withdrawal_amount and withdrawal_amount != "":
                return "WITHDRAWAL"
        
        # Check if it's a deposit
        if len(row) >= 4:
            deposit_amount = row[3].strip() if row[3] else ""
            if deposit_amount and deposit_amount != "":
                return "EDI"
        
        return "OTHER"

    def _sort_by_date(self, rows: List[List[str]]) -> List[List[str]]:
        """Sort rows by date (if date is available in first column)"""
        def get_date_key(row):
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                try:
                    month, day = row[0].strip().split('/')
                    return (int(month), int(day))
                except:
                    pass
            return (99, 99)
        
        return sorted(rows, key=get_date_key)

    def _merge_amount_columns(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Merge deposit and withdrawal columns into one amount column"""
        if not all_rows:
            return all_rows
        
        print("Merging deposit and withdrawal columns into one amount column...")
        
        merged_rows = []
        year = "2022"
        
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
                        final_amount = f"{deposit_amount:.2f}"
                    except ValueError:
                        pass
                
                if withdrawal_str and withdrawal_str != "":
                    try:
                        withdrawal_amount = float(withdrawal_str.replace(',', '').replace('-', ''))
                        final_amount = f"-{withdrawal_amount:.2f}"
                    except ValueError:
                        pass
                
                # Fix date format to include year
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                # Create new row: Date, Check#, Description, Amount
                new_row = [
                    date_str,
                    row[1],
                    row[2],
                    final_amount
                ]
                
                merged_rows.append(new_row)
            
            elif len(row) >= 4:
                # Handle rows with fewer columns
                date_str = row[0]
                if re.match(r'^\d{1,2}/\d{1,2}$', date_str.strip()):
                    date_str = f"{date_str.strip()}/{year}"
                
                new_row = [date_str, row[1], row[2], row[3]]
                merged_rows.append(new_row)
            else:
                # Keep shorter rows as-is
                merged_rows.append(row)
        
        return merged_rows

    def _remove_description_only_rows(self, all_rows: List[List[str]]) -> List[List[str]]:
        """Remove rows that only have descriptions but no dates or amounts"""
        if not all_rows:
            return all_rows
        
        print("Removing description-only rows...")
        
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
                if date_cell and (re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_cell) or re.match(r'^\d{1,2}/\d{1,2}$', date_cell)):
                    has_date = True
            
            if len(row) >= 4:
                amount_cell = row[3].strip()
                if amount_cell and amount_cell != "":
                    try:
                        float(amount_cell.replace(',', '').replace('-', ''))
                        has_amount = True
                    except ValueError:
                        pass
            
            # Special case: if row contains transaction keywords but no date/amount, check if it's a partial transaction
            if not has_date and not has_amount:
                row_text = " ".join(row).lower()
                transaction_keywords = ['purchase authorized', 'shell oil', 'bankcard', 'mtot dep', 'ach debit']
                if any(keyword in row_text for keyword in transaction_keywords):
                    print(f"    Found potential transaction fragment: {row[:3]}")
                    cleaned_rows.append(row)
                    continue
            
            # Keep rows that have either date or amount (or both)
            if has_date or has_amount:
                cleaned_rows.append(row)
            else:
                removed_count += 1
                print(f"    Removed description-only: {row[:3]}")
        
        print(f"Removed {removed_count} description-only rows")
        return cleaned_rows

    def _convert_to_transactions(self, processed_rows: List[List[str]]) -> List[Transaction]:
        """Convert processed rows to Transaction objects"""
        transactions = []
        
        for row in processed_rows:
            if len(row) < 4:
                continue
                
            date_str = row[0].strip()
            check_number = row[1].strip() if row[1].strip() else None
            description = row[2].strip()
            amount_str = row[3].strip()
            
            if not date_str or not amount_str:
                continue
                
            try:
                amount = float(amount_str.replace(',', ''))
            except ValueError:
                continue
            
            # Determine transaction type
            if description == "Deposits":
                transaction_type = "deposit_summary"
            elif check_number and re.match(r'^\d{4}$', check_number):
                transaction_type = "check"
            elif any(keyword in description.lower() for keyword in ['edi', 'japan tobac', 'itg brands']):
                transaction_type = "edi_payment"
            elif amount < 0:
                transaction_type = "withdrawal"
            else:
                transaction_type = "deposit"
            
            transactions.append(Transaction(
                date=self._standardize_date(date_str),
                description=description,
                amount=amount,
                check_number=check_number,
                transaction_type=transaction_type
            ))
        
        return transactions