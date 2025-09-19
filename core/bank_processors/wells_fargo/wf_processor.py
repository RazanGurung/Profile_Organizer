"""
Wells Fargo Data Processor
Processes raw Wells Fargo transaction data into standardized format
"""

import re
import csv
import os
from datetime import datetime

class WellsFargoProcessor:
    def __init__(self, year="2022"):
        self.year = year
        self.processed_data = []
    
    def add_monthly_summary(self, raw_rows):
        """
        Add monthly summary row at the top with deposits total
        """
        if not raw_rows:
            return raw_rows
        
        print("📊 Creating monthly summary...")
        
        # Calculate totals from all transactions
        deposits_total = 0.0
        withdrawals_total = 0.0
        month_year = None
        
        for row in raw_rows:
            # Look for date in first column (format: M/D)
            if len(row) > 0 and re.match(r'^\d{1,2}/\d{1,2}$', row[0].strip()):
                if not month_year:
                    # Use first date to determine month/year
                    month, day = row[0].strip().split('/')
                    month_year = f"{month.zfill(2)}/{self.year}"
                
                # Calculate totals from deposits/credits and withdrawals/debits columns
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
            
            # Calculate last day of month
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
            if len(raw_rows[0]) >= 5:
                summary_row = [
                    last_date,                    # Date (last day of month)
                    "",                           # Check Number (empty)
                    "Deposits",                   # Description
                    f"{deposits_total:.2f}"       # Total deposits amount
                ]
                
                # Pad to match row length
                while len(summary_row) < len(raw_rows[0]):
                    summary_row.append("")
                
                print(f"✅ Summary: {last_date} | Deposits: ${deposits_total:.2f} | Withdrawals: -${withdrawals_total:.2f}")
                
                # Insert at the beginning
                raw_rows.insert(0, summary_row)
        
        return raw_rows
    
    def filter_deposits_keep_edi(self, raw_rows):
        """
        Remove regular deposit entries but keep EDI payments
        """
        if not raw_rows:
            return raw_rows
        
        print("🔍 Filtering deposits - keeping only EDI payments...")
        
        filtered_rows = []
        deposits_removed = 0
        edi_kept = 0
        
        for i, row in enumerate(raw_rows):
            # Keep the first row (summary row)
            if i == 0:
                filtered_rows.append(row)
                continue
            
            # Check if this is a deposit entry
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
        return filtered_rows
    
    def classify_transaction(self, row):
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
        
        # Check if it's a withdrawal
        if len(row) >= 5:
            withdrawal_amount = row[4].strip() if row[4] else ""
            if withdrawal_amount and withdrawal_amount != "":
                return "WITHDRAWAL"
        
        # Check if it's a deposit
        if len(row) >= 4:
            deposit_amount = row[3].strip() if row[3] else ""
            if deposit_amount and deposit_amount != "":
                return "EDI"  # Remaining deposits should be EDI payments
        
        return "OTHER"
    
    def sort_by_date(self, rows):
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
    
    def sort_transactions_by_type(self, raw_rows):
        """
        Sort transactions: Summary → EDI Payments → Withdrawals → Checks
        """
        if not raw_rows:
            return raw_rows
        
        print("📋 Sorting transactions by type...")
        
        # Separate transactions by type
        summary_rows = []
        edi_payments = []
        withdrawals = []
        checks = []
        other_rows = []
        
        for i, row in enumerate(raw_rows):
            # First row is always summary
            if i == 0:
                summary_rows.append(row)
                continue
            
            # Classify each transaction
            transaction_type = self.classify_transaction(row)
            
            if transaction_type == "EDI":
                edi_payments.append(row)
            elif transaction_type == "CHECK":
                checks.append(row)
            elif transaction_type == "WITHDRAWAL":
                withdrawals.append(row)
            else:
                other_rows.append(row)
        
        # Sort each category by date
        edi_payments = self.sort_by_date(edi_payments)
        withdrawals = self.sort_by_date(withdrawals)
        checks = self.sort_by_date(checks)
        
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
    
    def merge_amount_columns(self, raw_rows):
        """
        Merge deposit and withdrawal columns into one amount column
        """
        if not raw_rows:
            return raw_rows
        
        print("🔀 Merging deposit and withdrawal columns into one amount column...")
        
        merged_rows = []
        
        for row in raw_rows:
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
        
        print(f"✅ Merged columns: {len(raw_rows)} rows processed")
        return merged_rows
    
    def remove_description_only_rows(self, raw_rows):
        """
        Remove rows that only have descriptions but no dates or amounts
        """
        if not raw_rows:
            return raw_rows
        
        print("🗑️  Removing description-only rows...")
        
        cleaned_rows = []
        removed_count = 0
        
        for i, row in enumerate(raw_rows):
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
        
        print(f"✅ Removed {removed_count} description-only rows")
        return cleaned_rows
    
    def process_raw_data(self, raw_rows):
        """
        Main processing method - converts raw Wells Fargo data to standardized format
        """
        print("🔧 Processing Wells Fargo data...")
        
        # Step 1: Add monthly summary
        processed_rows = self.add_monthly_summary(raw_rows.copy())
        
        # Step 2: Filter deposits (keep only EDI)
        processed_rows = self.filter_deposits_keep_edi(processed_rows)
        
        # Step 3: Sort by transaction type
        processed_rows = self.sort_transactions_by_type(processed_rows)
        
        # Step 4: Merge amount columns
        processed_rows = self.merge_amount_columns(processed_rows)
        
        # Step 5: Remove description-only rows
        processed_rows = self.remove_description_only_rows(processed_rows)
        
        self.processed_data = processed_rows
        return processed_rows
    
    def save_to_csv(self, processed_rows, output_path):
        """
        Save processed data to CSV file
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            headers = ["Date", "Check No", "Description", "Amount"]
            writer.writerow(headers)
            
            # Write all rows
            writer.writerows(processed_rows)
        
        print(f"💾 Processed data saved to: {output_path}")
        return output_path

def process_wells_fargo_data(raw_rows, year="2022"):