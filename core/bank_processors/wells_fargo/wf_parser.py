# import re
# from typing import List, Optional
# import pandas as pd
# from ...interfaces.base_parser import BaseParser
# from ...interfaces.transaction import Transaction

# class WellsFargoParser(BaseParser):
#     """Wells Fargo enhanced parser - same quality as BoA"""
    
#     def __init__(self):
#         super().__init__()
#         self.date_pat = re.compile(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b")
#         self.money_pat = re.compile(r"[-+]?\$?\d[\d,]*\.\d{2}")
#         self.check_word_pat = re.compile(r"\bCHECK(?!CARD)\b", re.IGNORECASE)
#         self.checknum_pat = re.compile(r"\b\d{3,}\*?\b")
        
#         # Wells Fargo specific patterns
#         self.pure_ledger_pat = re.compile(
#             r"^(?:\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?\s+[-+]?\$?\d[\d,]*\.\d{2}\s*){2,}$"
#         )
#         self.single_ledger_pat = re.compile(
#             r"^\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?\s+[-+]?\$?\d[\d,]*\.\d{2}\s*$"
#         )
    
#     def get_bank_name(self) -> str:
#         return "wells_fargo"
    
#     def get_detection_keywords(self) -> List[str]:
#         return ["WELLS FARGO", "NAVIGATE BUSINESS CHECKING"]
    
#     def process_tables(self, tables: List[pd.DataFrame]) -> List[Transaction]:
#         """Wells Fargo specific processing logic - same structure as BoA"""
#         transactions: List[Transaction] = []
#         print(f"Processing {len(tables)} tables for Wells Fargo...")

#         for t_idx, df in enumerate(tables):
#             if df.empty:
#                 print(f"Table {t_idx}: Empty - skipping")
#                 continue

#             print(f"Table {t_idx}: {df.shape[0]} rows, {df.shape[1]} cols")

#             # Skip Wells Fargo balance tables
#             probe_text = " ".join(self._norm(self._row_to_text(r)) for _, r in df.head(3).iterrows())
            
#             # Wells Fargo specific skip patterns
#             wf_skip_patterns = [
#                 "ENDING DAILY BALANCE", "ACCOUNT BALANCE", "DAILY BALANCE", 
#                 "SUMMARY OF CHECKS", "MONTHLY SERVICE FEE", "ACCOUNT TRANSACTION FEES",
#                 "INTEREST SUMMARY", "OVERDRAFT PROTECTION"
#             ]
            
#             if any(pattern in probe_text.upper() for pattern in wf_skip_patterns):
#                 print(f"  -> Skipping: Wells Fargo {probe_text[:50]}... table")
#                 continue
            
#             if self._is_simple_daily_ledger_table(df):
#                 print(f"  -> Skipping: Daily ledger balances table")
#                 continue

#             # Process transactions
#             table_transactions = 0
#             for row_idx, row in df.iterrows():
#                 row_text = self._norm(self._row_to_text(row))
#                 if len(row_text) < 8:
#                     continue

#                 if self._is_pure_ledger_row(row_text):
#                     continue

#                 # Wells Fargo check processing
#                 multi_checks = self._parse_checks_row_multi(row_text)
#                 if multi_checks:
#                     transactions.extend(multi_checks)
#                     table_transactions += len(multi_checks)
#                     print(f"    Row {row_idx}: Found {len(multi_checks)} WF checks")
#                     continue

#                 # Regular transaction processing
#                 date_str = self._extract_date_any(row_text)
#                 if not date_str:
#                     continue
                    
#                 amount = self._pick_amount(row_text)
#                 if amount is None:
#                     continue

#                 upper = row_text.upper()

#                 # Wells Fargo specific transaction classification
#                 transaction = self._classify_wells_fargo_transaction(row_text, amount, upper, date_str)
#                 if transaction:
#                     transactions.append(transaction)
#                     table_transactions += 1
#                     print(f"    Row {row_idx}: WF {transaction.transaction_type.title()}, Amount: {amount}")

#             print(f"  -> Extracted {table_transactions} transactions from WF table {t_idx}")

#         print(f"Total Wells Fargo transactions extracted: {len(transactions)}")
#         return transactions
    
#     def _classify_wells_fargo_transaction(self, row_text: str, amount: float, upper_text: str, date_str: str) -> Optional[Transaction]:
#         """Wells Fargo specific transaction classification"""
        
#         # Wells Fargo check detection
#         has_check_word = "CHECK" in upper_text and "CHECKCARD" not in upper_text
#         has_check_number = self.checknum_pat.search(row_text) is not None
#         is_negative_amount = amount < 0
        
#         # Wells Fargo specific exclusions
#         excluded_terms = ["RETURN", "DEPOSIT", "FEE", "CREDIT", "RECEIVED", "BANKCARD", "MTOT DEP"]
#         is_excluded = any(term in upper_text for term in excluded_terms)
        
#         if has_check_word and has_check_number and is_negative_amount and not is_excluded:
#             check_number = self._extract_check_number(row_text)
#             return Transaction(
#                 date=self._standardize_date(date_str),
#                 description=self._clean_description(row_text) or "Check",
#                 amount=amount,
#                 check_number=check_number,
#                 transaction_type="check",
#             )

#         # Wells Fargo EDI/ACH payments
#         if amount > 0:
#             if self._is_wells_fargo_edi_payment(row_text):
#                 txn_type = "edi_payment"
#             else:
#                 txn_type = "deposit"
#         else:
#             txn_type = "withdrawal"
        
#         return Transaction(
#             date=self._standardize_date(date_str),
#             description=self._clean_description(row_text),
#             amount=amount,
#             check_number=None,
#             transaction_type=txn_type,
#         )
    
#     def _is_wells_fargo_edi_payment(self, text: str) -> bool:
#         """Wells Fargo specific EDI payment detection"""
#         upper_text = text.upper()
        
#         wells_fargo_edi_companies = [
#             "ITG BRANDS", "JAPAN TOBAC", "LIGGETT VECTOR", 
#             "EDI PYMNTS", "ACH CREDIT", "WIRE TRANSFER",
#             "DIRECT DEPOSIT", "PAYROLL"
#         ]
        
#         return any(company in upper_text for company in wells_fargo_edi_companies)
    
#     # Copy utility methods from BoA (same implementation)
#     def _norm(self, s: str) -> str:
#         return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

#     def _is_pure_ledger_row(self, text: str) -> bool:
#         t = self._norm(re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", text))
#         leftover = self.date_pat.sub("", t)
#         leftover = self.money_pat.sub("", leftover)
#         leftover = re.sub(r"[()\s,$\-]+", "", leftover)

#         if len(leftover) > 3:
#             return False

#         if self.pure_ledger_pat.match(t) is not None:
#             return True

#         if self.single_ledger_pat.match(t) is not None and len(leftover) <= 1:
#             return True

#         return False
        
#     def _parse_checks_row_multi(self, row_text: str) -> List[Transaction]:
#         txns: List[Transaction] = []
#         text = re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", row_text)

#         triplet = re.compile(
#             r"(?P<date>\b\d{1,2}/\d{1,2}/\d{2,4}\b)\s+(?P<check>\d{3,}\*?)\s+(?P<amount>[-+]?\$?\d[\d,]*\.\d{2})",
#             re.VERBOSE,
#         )

#         for m in triplet.finditer(text):
#             date_str = m.group("date")
#             check_raw = m.group("check")
#             amt_str = m.group("amount")

#             check_no = check_raw.rstrip("*")
#             try:
#                 amount = float(amt_str.replace("$", "").replace(",", ""))
#             except ValueError:
#                 continue

#             txns.append(
#                 Transaction(
#                     date=self._standardize_date(date_str),
#                     description="Check",
#                     amount=amount,
#                     check_number=check_no,
#                     transaction_type="check",
#                 )
#             )

#         return txns
    
#     def _is_simple_daily_ledger_table(self, df: pd.DataFrame) -> bool:
#         if df.empty or df.shape[0] < 3:
#             return False

#         sample_text = " ".join(self._norm(self._row_to_text(r)) for _, r in df.head(5).iterrows())
#         if re.search(r"\bcheck\b.*\d{3,}", sample_text, re.IGNORECASE):
#             return False

#         simple_rows = 0
#         for _, row in df.head(5).iterrows():
#             row_text = self._norm(self._row_to_text(row))

#             if self.pure_ledger_pat.match(row_text) or self.single_ledger_pat.match(row_text):
#                 simple_rows += 1
#                 continue

#             temp = re.sub(r"\b\d{1,2}/\d{1,2}\b", "", row_text)
#             temp = re.sub(r"\d[\d,]*\.\d{2}", "", temp)
#             temp = re.sub(r"[(),\s$]+", "", temp).strip()
#             if len(temp) <= 5:
#                 simple_rows += 1

#         return simple_rows >= 3

#     def _extract_check_number(self, text: str) -> Optional[str]:
#         """Extract check number from text"""
#         m = re.search(r"Check\s*#?\s*(\d{3,})", text, flags=re.IGNORECASE)
#         if m:
#             return m.group(1)
#         m2 = re.search(r"\b(\d{3,})\b(?=.*-?\$?\d[\d,]*\.\d{2})", text)
#         if m2:
#             return m2.group(1)
#         numbers = re.findall(r"\b\d{3,}\b", text)
#         if numbers:
#             return numbers[-1]
#         return None


import re
from typing import List, Optional, Dict, Any
import pandas as pd
from ...interfaces.base_parser import BaseParser
from ...interfaces.transaction import Transaction

class WellsFargoParser(BaseParser):
    """Wells Fargo parser - handles multi-row transactions (1-3+ rows each)"""
    
    def __init__(self):
        super().__init__()
        self.date_pat = re.compile(r"^\d{1,2}/\d{1,2}$")
        self.statement_year = None
    
    def get_bank_name(self) -> str:
        return "wells_fargo"
    
    def get_detection_keywords(self) -> List[str]:
        return ["WELLS FARGO", "NAVIGATE BUSINESS CHECKING"]
    
    def process_tables(self, tables: List[pd.DataFrame]) -> List[Transaction]:
        """Wells Fargo multi-row transaction processing"""
        transactions: List[Transaction] = []
        print(f"Processing {len(tables)} tables for Wells Fargo...")
        
        # Infer statement year from tables
        self._infer_statement_year(tables)

        for t_idx, df in enumerate(tables):
            if df.empty:
                print(f"WF Table {t_idx}: Empty - skipping")
                continue

            print(f"WF Table {t_idx}: {df.shape[0]} rows, {df.shape[1]} cols")

            # Check if this looks like the main Wells Fargo transaction table
            if self._is_wf_transaction_table(df):
                print(f"  -> Found WF transaction table")
                table_transactions = self._process_wf_transaction_table(df)
                transactions.extend(table_transactions)
                print(f"  -> Extracted {len(table_transactions)} transactions")
            else:
                print(f"  -> Skipping: Not a WF transaction table")

        print(f"Total Wells Fargo transactions extracted: {len(transactions)}")
        return transactions
    
    def _infer_statement_year(self, tables: List[pd.DataFrame]):
        """Try to infer the statement year from table content"""
        for df in tables:
            for _, row in df.iterrows():
                row_text = self._row_to_text(row)
                # Look for patterns like "September 30, 2022"
                year_match = re.search(r"\b(20\d{2})\b", row_text)
                if year_match:
                    self.statement_year = int(year_match.group(1))
                    print(f"  Inferred statement year: {self.statement_year}")
                    return
        
        # Fallback to current year
        from datetime import datetime
        self.statement_year = datetime.now().year
        print(f"  Using fallback year: {self.statement_year}")
    
    def _is_wf_transaction_table(self, df: pd.DataFrame) -> bool:
        """Detect Wells Fargo transaction table by structure and data patterns"""
        
        # Must have 6 columns (Date, Check#, Description, Deposits, Withdrawals, Balance)
        if df.shape[1] != 6:
            return False
        
        # Must have reasonable number of rows
        if df.shape[0] < 10:
            return False
        
        # Analyze data patterns
        date_score = self._count_wf_dates_in_column(df, 0)
        amount_score = self._count_amounts_in_column(df, 3) + self._count_amounts_in_column(df, 4)
        
        print(f"    WF table analysis - dates: {date_score}, amounts: {amount_score}")
        
        # High confidence if we see many dates and amounts
        return date_score >= 5 and amount_score >= 5
    
    def _count_wf_dates_in_column(self, df: pd.DataFrame, col_idx: int) -> int:
        """Count Wells Fargo format dates in a specific column"""
        if col_idx >= df.shape[1]:
            return 0
        
        date_count = 0
        for _, row in df.iterrows():
            cell_value = str(row.iloc[col_idx]).strip()
            if self._is_wf_date(cell_value):
                date_count += 1
        
        return date_count
    
    def _count_amounts_in_column(self, df: pd.DataFrame, col_idx: int) -> int:
        """Count monetary amounts in a specific column"""
        if col_idx >= df.shape[1]:
            return 0
        
        amount_count = 0
        for _, row in df.iterrows():
            cell_value = str(row.iloc[col_idx]).strip()
            if self._is_valid_amount(cell_value):
                amount_count += 1
        
        return amount_count
    
    def _process_wf_transaction_table(self, df: pd.DataFrame) -> List[Transaction]:
        """Process Wells Fargo transaction table with multi-row grouping"""
        
        # Step 1: Group multi-row transactions
        transaction_groups = self._group_wf_transactions(df)
        print(f"    Grouped into {len(transaction_groups)} transaction groups")
        
        # Step 2: Process each group into a transaction
        transactions = []
        for group_idx, row_group in enumerate(transaction_groups):
            transaction = self._process_wf_transaction_group(row_group, group_idx)
            if transaction:
                transactions.append(transaction)
        
        return transactions
    
    def _group_wf_transactions(self, df: pd.DataFrame) -> List[List[pd.Series]]:
        """Group Wells Fargo multi-row transactions"""
        
        transaction_groups = []
        current_group = []
        
        for row_idx, row in df.iterrows():
            date_cell = str(row.iloc[0]).strip()
            
            # NEW TRANSACTION: Row has a date
            if self._is_wf_date(date_cell):
                # Save previous group if exists
                if current_group:
                    if self._validate_wf_transaction_group(current_group):
                        transaction_groups.append(current_group)
                    else:
                        print(f"    Warning: Invalid transaction group ending at row {row_idx-1}")
                
                # Start new group
                current_group = [row]
                
            # CONTINUATION ROW: No date, but has description content
            elif self._is_continuation_row(row):
                if current_group:  # Add to current transaction
                    current_group.append(row)
                else:
                    print(f"    Warning: Orphaned continuation row at {row_idx}")
                    
            # EMPTY/INVALID ROW: Skip
            else:
                continue
        
        # Don't forget the last group
        if current_group:
            if self._validate_wf_transaction_group(current_group):
                transaction_groups.append(current_group)
        
        return transaction_groups
    
    def _is_wf_date(self, date_str: str) -> bool:
        """Check if string is a Wells Fargo date (M/D format)"""
        if not date_str or date_str.lower() in ['empty', 'nan', '']:
            return False
        
        return bool(self.date_pat.match(date_str.strip()))
    
    def _is_continuation_row(self, row: pd.Series) -> bool:
        """Check if this row continues a previous transaction"""
        date_cell = str(row.iloc[0]).strip()
        desc_cell = str(row.iloc[2]).strip() if len(row) > 2 else ""
        
        # Must have no date but have meaningful description content
        has_no_date = not self._is_wf_date(date_cell)
        has_description = desc_cell and desc_cell.lower() not in ['empty', 'nan', ''] and len(desc_cell) > 3
        
        return has_no_date and has_description
    
    def _validate_wf_transaction_group(self, row_group: List[pd.Series]) -> bool:
        """Validate that a grouped transaction makes sense"""
        
        if not row_group:
            return False
        
        # Rule 1: First row must have date
        first_row = row_group[0]
        if not self._is_wf_date(str(first_row.iloc[0])):
            return False
        
        # Rule 2: First row should have at least one amount (deposit or withdrawal)
        deposits = str(first_row.iloc[3]).strip()
        withdrawals = str(first_row.iloc[4]).strip()
        
        has_deposit = self._is_valid_amount(deposits)
        has_withdrawal = self._is_valid_amount(withdrawals)
        
        if not (has_deposit or has_withdrawal):
            return False
        
        # Rule 3: Continuation rows should NOT have amounts
        for i, row in enumerate(row_group[1:], 1):
            cont_deposits = str(row.iloc[3]).strip()
            cont_withdrawals = str(row.iloc[4]).strip()
            
            if self._is_valid_amount(cont_deposits) or self._is_valid_amount(cont_withdrawals):
                print(f"    Warning: Continuation row {i} has amounts - possible grouping error")
                return False
        
        return True
    
    def _process_wf_transaction_group(self, row_group: List[pd.Series], group_idx: int) -> Optional[Transaction]:
        """Process a group of 1-3+ rows into a single transaction"""
        
        try:
            # Extract data from first row
            main_row = row_group[0]
            
            date = str(main_row.iloc[0]).strip()
            check_num = str(main_row.iloc[1]).strip() if str(main_row.iloc[1]).strip().lower() not in ['empty', 'nan', ''] else None
            deposits = str(main_row.iloc[3]).strip()
            withdrawals = str(main_row.iloc[4]).strip()
            
            # Merge descriptions from all rows
            description_parts = []
            for row in row_group:
                desc_part = str(row.iloc[2]).strip()
                if desc_part and desc_part.lower() not in ['empty', 'nan', '']:
                    description_parts.append(desc_part)
            
            full_description = ' '.join(description_parts)
            
            # Create transaction based on amount type
            if self._is_valid_amount(deposits):
                # DEPOSIT transaction
                amount = self._parse_amount(deposits)
                return Transaction(
                    date=self._standardize_wf_date(date),
                    description=self._clean_wf_description(full_description),
                    amount=amount,  # Positive for deposits
                    check_number=None,
                    transaction_type="deposit"
                )
            
            elif self._is_valid_amount(withdrawals):
                # WITHDRAWAL or CHECK transaction
                amount = -abs(self._parse_amount(withdrawals))  # Negative for withdrawals
                
                # Check if this is a check transaction
                if check_num and (check_num.isdigit() or len(check_num) <= 6):
                    return Transaction(
                        date=self._standardize_wf_date(date),
                        description=self._clean_wf_description(full_description) or "Check",
                        amount=amount,
                        check_number=check_num if check_num.isdigit() else None,
                        transaction_type="check"
                    )
                else:
                    return Transaction(
                        date=self._standardize_wf_date(date),
                        description=self._clean_wf_description(full_description),
                        amount=amount,
                        check_number=None,
                        transaction_type="withdrawal"
                    )
            
            else:
                print(f"    Warning: Group {group_idx} has no valid amounts")
                return None
                
        except Exception as e:
            print(f"    Error processing WF transaction group {group_idx}: {e}")
            return None
    
    def _is_valid_amount(self, amount_str: str) -> bool:
        """Check if string represents a valid monetary amount"""
        if not amount_str or amount_str.lower() in ['empty', 'nan', '']:
            return False
        
        try:
            cleaned = amount_str.replace(',', '').replace('$', '').strip()
            amount = float(cleaned)
            return 0.01 <= amount <= 10000000  # Reasonable range
        except:
            return False
    
    def _parse_amount(self, amount_str: str) -> float:
        """Parse amount string to float"""
        cleaned = amount_str.replace(',', '').replace('$', '').strip()
        return float(cleaned)
    
    def _standardize_wf_date(self, date_str: str) -> str:
        """Convert Wells Fargo M/D format to MM/DD/YYYY"""
        try:
            parts = date_str.strip().split("/")
            if len(parts) == 2:
                month, day = int(parts[0]), int(parts[1])
                year = self.statement_year or 2024
                return f"{month:02d}/{day:02d}/{year}"
            return date_str
        except:
            return date_str
    
    def _clean_wf_description(self, description: str) -> str:
        """Clean and standardize Wells Fargo descriptions"""
        if not description:
            return "Transaction"
        
        # Clean up the merged description
        desc = description.strip()
        
        # Handle common Wells Fargo patterns
        if "Bankcard" in desc and "Mtot Dep" in desc:
            return "Credit Card Deposit"
        elif "Purchase author" in desc:
            # Try to extract meaningful merchant info
            if "Step.Com" in desc:
                return "Step.Com Purchase"
            elif "Shell Oil" in desc:
                return "Shell Oil"
            elif "Cox Roanoke" in desc:
                return "Cox Communications"
            elif "Rocky Tobacco" in desc:
                return "Rocky Tobacco"
            elif "Tobacco City" in desc:
                return "Tobacco City & Vap"
            else:
                return "Debit Card Purchase"
        elif "Business to Bus" in desc:
            # Extract company name from ACH description
            if "Ias Group Inc" in desc:
                return "IAS Group ACH Debit"
            elif "Tobacco House" in desc:
                return "Business ACH Debit - Tobacco House"
            else:
                return "Business ACH Debit"
        elif "Interest Payment" in desc:
            return "Interest Payment"
        elif "Monthly Service Fee" in desc:
            return "Monthly Service Fee"
        elif desc.strip().lower() == "check":
            return "Check"
        
        # For other long descriptions, clean and truncate
        # Remove card numbers, transaction IDs
        cleaned = re.sub(r'\b[A-Z]\d{11,}\b', '', desc)  # Remove long transaction IDs
        cleaned = re.sub(r'\bCard \d{4}\b', '', cleaned)  # Remove "Card 0057" 
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()  # Clean whitespace
        
        if len(cleaned) > 50:
            return cleaned[:47] + "..."
        
        return cleaned if cleaned else "Transaction"