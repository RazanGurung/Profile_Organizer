# import re
# from typing import List, Optional
# import pandas as pd
# from .base_parser import BaseParser, Transaction

# class BankOfAmericaParser(BaseParser):
#     """Bank of America bank statement parser - trust the statement amounts as-is"""
    
#     def __init__(self):
#         super().__init__()
#         # Precompiled patterns for performance
#         self.date_pat = re.compile(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b")
#         self.money_pat = re.compile(r"[-+]?\$?\d[\d,]*\.\d{2}")
#         self.check_word_pat = re.compile(r"\bCHECK(?!CARD)\b", re.IGNORECASE)
#         self.checknum_pat = re.compile(r"\b\d{3,5}\*?\b")
        
#         # Very strict pattern for pure ledger rows (date amount pairs only)
#         self.pure_ledger_pat = re.compile(
#             r"^(?:\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?\s+[-+]?\$?\d[\d,]*\.\d{2}\s*){2,}$"
#         )
    
#     def get_bank_name(self) -> str:
#         return "bank_of_america"
    
#     def get_detection_keywords(self) -> List[str]:
#         return ["BANK OF AMERICA", "BANKOFAMERICA.COM", "BUSINESS ADVANTAGE"]
    
#     def process_tables(self, tables: List[pd.DataFrame]) -> List[Transaction]:
#         """
#         Simple Bank of America parsing:
#         - Use amount signs exactly as they appear (positive = deposit, negative = withdrawal)
#         - Only skip obvious "Daily ledger balances" tables
#         - Process ALL other tables, including small/broken ones
#         """
#         transactions: List[Transaction] = []
#         print(f"Processing {len(tables)} tables for Bank of America...")

#         for t_idx, df in enumerate(tables):
#             if df.empty:
#                 print(f"Table {t_idx}: Empty - skipping")
#                 continue

#             print(f"Table {t_idx}: {df.shape[0]} rows, {df.shape[1]} cols")

#             # ONLY skip if the table header explicitly says "Daily ledger balances"
#             probe_text = " ".join(self._norm(self._row_to_text(r)) for _, r in df.head(3).iterrows())
#             if "DAILY LEDGER BALANCES" in probe_text.upper():
#                 print(f"  -> Skipping: Daily ledger balances table")
#                 continue

#             # Process ALL other tables
#             table_transactions = 0
#             for row_idx, row in df.iterrows():
#                 row_text = self._norm(self._row_to_text(row))
#                 if len(row_text) < 8:
#                     continue

#                 # Skip ONLY pure ledger rows (date amount date amount...)
#                 if self._is_pure_ledger_row(row_text):
#                     continue

#                 # Handle side-by-side check tables
#                 multi_checks = self._parse_checks_row_multi(row_text)
#                 if multi_checks:
#                     transactions.extend(multi_checks)
#                     table_transactions += len(multi_checks)
#                     print(f"    Row {row_idx}: Found {len(multi_checks)} checks")
#                     continue

#                 # Need a date and amount for regular transactions
#                 date_str = self._extract_date_any(row_text)
#                 if not date_str:
#                     continue
                    
#                 amount = self._pick_amount(row_text)
#                 if amount is None:
#                     continue

#                 upper = row_text.upper()

#                 # FIXED: More specific check detection
#                 # Only classify as check if: has CHECK word, has number, is negative, and is NOT a return/deposit/fee
#                 has_check_word = self.check_word_pat.search(upper) is not None
#                 has_check_number = self.checknum_pat.search(row_text) is not None
#                 is_negative_amount = amount < 0
                
#                 # Exclude descriptions that indicate it's NOT an actual check transaction
#                 excluded_terms = ["RETURN", "DEPOSIT", "FEE", "REFUND", "CREDIT", "REVERSAL", "VOID", "RECEIVED"]
#                 is_excluded = any(term in upper for term in excluded_terms)
                
#                 is_actual_check = (has_check_word and has_check_number and 
#                                  is_negative_amount and not is_excluded and 
#                                  "CHECKCARD" not in upper)
                
#                 if is_actual_check:
#                     check_number = self._extract_check_number(row_text)
#                     transactions.append(
#                         Transaction(
#                             date=self._standardize_date(date_str),
#                             description=self._clean_description(row_text) or "Check",
#                             amount=amount,  # Use amount exactly as extracted
#                             check_number=check_number,
#                             transaction_type="check",
#                         )
#                     )
#                     table_transactions += 1
#                     print(f"    Row {row_idx}: Check #{check_number}, Amount: {amount}")
#                     continue

#                 # For all other transactions: Use sign to determine type
#                 # Positive = deposit, Negative = withdrawal
#                 if amount > 0:
#                     txn_type = "deposit"
#                 else:
#                     txn_type = "withdrawal"
                
#                 transactions.append(
#                     Transaction(
#                         date=self._standardize_date(date_str),
#                         description=self._clean_description(row_text),
#                         amount=amount,  # Use amount exactly as extracted
#                         check_number=None,
#                         transaction_type=txn_type,
#                     )
#                 )
#                 table_transactions += 1
#                 print(f"    Row {row_idx}: {txn_type.title()}, Amount: {amount}")

#             print(f"  -> Extracted {table_transactions} transactions from table {t_idx}")

#         print(f"Total transactions extracted: {len(transactions)}")
#         return transactions
    
#     def _norm(self, s: str) -> str:
#         """Normalize whitespace"""
#         return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

#     def _is_pure_ledger_row(self, text: str) -> bool:
#         """
#         Only skip rows that are pure ledger format: date amount date amount...
#         Very conservative - only skip obvious ledger balance rows
#         """
#         # Normalize parentheses: (1,234.56) -> -1,234.56
#         t = self._norm(re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", text))
        
#         # If it contains any text (not just numbers and dates), it's likely a real transaction
#         # Remove dates and amounts, see what's left
#         temp = re.sub(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b", "", t)  # Remove dates
#         temp = re.sub(r"[-+]?\$?\d[\d,]*\.\d{2}", "", temp)  # Remove amounts
#         temp = re.sub(r"\s+", " ", temp).strip()  # Clean whitespace
        
#         # If there's substantial text left, it's a transaction description
#         if len(temp) > 3:
#             return False
        
#         # Check if it matches pure ledger pattern (date amount){2,}
#         return self.pure_ledger_pat.match(t) is not None
    
#     def _parse_checks_row_multi(self, row_text: str) -> List[Transaction]:
#         """
#         Parse side-by-side check entries: Date Check# Amount Date Check# Amount
#         """
#         txns: List[Transaction] = []

#         # Normalize parentheses negatives: (1,234.56) -> -1,234.56
#         text = re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", row_text)

#         # Pattern for: DATE  CHECKNO  AMOUNT
#         triplet = re.compile(
#             r"""
#             (?P<date>\b\d{1,2}/\d{1,2}/\d{2,4}\b)    # date
#             \s+
#             (?P<check>\d{3,5}\*?)                    # check number
#             \s+
#             (?P<amount>[-+]?\$?\d[\d,]*\.\d{2})      # amount
#             """,
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
#                     amount=amount,  # Use amount exactly as extracted
#                     check_number=check_no,
#                     transaction_type="check",
#                 )
#             )

#         return txns



import re
from typing import List, Optional
import pandas as pd
from .base_parser import BaseParser, Transaction

class BankOfAmericaParser(BaseParser):
    """Bank of America bank statement parser - trust the statement amounts as-is"""
    
    def __init__(self):
        super().__init__()
        # Precompiled patterns for performance
        self.date_pat = re.compile(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b")
        self.money_pat = re.compile(r"[-+]?\$?\d[\d,]*\.\d{2}")
        self.check_word_pat = re.compile(r"\bCHECK(?!CARD)\b", re.IGNORECASE)
        self.checknum_pat = re.compile(r"\b\d{3,}\*?\b")
        
        # Enhanced patterns for daily ledger detection
        self.ledger_header_pat = re.compile(
            r"DAILY\s+LEDGER\s+BALANCES?(?:\s*-?\s*CONTINUED?)?", 
            re.IGNORECASE
        )
        
        # Pattern for ledger row: date, balance, date, balance, date, balance
        self.ledger_row_pat = re.compile(
            r"^(?:\s*\d{1,2}/\d{1,2}\s+[-]?\d[\d,]*\.\d{2}\s*){2,}$"
        )
        
        # Pattern for ledger header row (Date Balance($) Date Balance($) etc.)
        self.ledger_column_header_pat = re.compile(
            r"Date\s+Balance\s*\(\$?\)\s+Date\s+Balance",
            re.IGNORECASE
        )
    
    def get_bank_name(self) -> str:
        return "bank_of_america"
    
    def get_detection_keywords(self) -> List[str]:
        return ["BANK OF AMERICA", "BANKOFAMERICA.COM", "BUSINESS ADVANTAGE"]
    
    def process_tables(self, tables: List[pd.DataFrame]) -> List[Transaction]:
        """
        Enhanced Bank of America parsing with improved daily ledger detection
        """
        transactions: List[Transaction] = []
        print(f"Processing {len(tables)} tables for Bank of America...")

        for t_idx, df in enumerate(tables):
            if df.empty:
                print(f"Table {t_idx}: Empty - skipping")
                continue

            print(f"Table {t_idx}: {df.shape[0]} rows, {df.shape[1]} cols")

            # Enhanced daily ledger detection
            if self._is_daily_ledger_table(df, t_idx):
                print(f"  -> Skipping: Daily ledger balances table")
                continue

            # Process ALL other tables
            table_transactions = 0
            for row_idx, row in df.iterrows():
                row_text = self._norm(self._row_to_text(row))
                if len(row_text) < 8:
                    continue

                # Skip individual ledger rows that might have slipped through
                if self._is_ledger_row(row_text):
                    print(f"    Row {row_idx}: Skipping ledger row")
                    continue

                # Handle side-by-side check tables
                multi_checks = self._parse_checks_row_multi(row_text)
                if multi_checks:
                    transactions.extend(multi_checks)
                    table_transactions += len(multi_checks)
                    print(f"    Row {row_idx}: Found {len(multi_checks)} checks")
                    continue

                # Need a date and amount for regular transactions
                date_str = self._extract_date_any(row_text)
                if not date_str:
                    continue
                    
                amount = self._pick_amount(row_text)
                if amount is None:
                    continue

                upper = row_text.upper()

                # Check detection
                has_check_word = self.check_word_pat.search(upper) is not None
                has_check_number = self.checknum_pat.search(row_text) is not None
                is_negative_amount = amount < 0
                
                # Exclude descriptions that indicate it's NOT an actual check transaction
                excluded_terms = ["RETURN", "DEPOSIT", "FEE", "REFUND", "CREDIT", "REVERSAL", "VOID", "RECEIVED"]
                is_excluded = any(term in upper for term in excluded_terms)
                
                is_actual_check = (has_check_word and has_check_number and 
                                 is_negative_amount and not is_excluded and 
                                 "CHECKCARD" not in upper)
                
                if is_actual_check:
                    check_number = self._extract_check_number(row_text)
                    transactions.append(
                        Transaction(
                            date=self._standardize_date(date_str),
                            description=self._clean_description(row_text) or "Check",
                            amount=amount,
                            check_number=check_number,
                            transaction_type="check",
                        )
                    )
                    table_transactions += 1
                    print(f"    Row {row_idx}: Check #{check_number}, Amount: {amount}")
                    continue

                # For all other transactions: Use sign to determine type
                if amount > 0:
                    # Check if this is an EDI payment from specific companies
                    if self._is_edi_payment(row_text):
                        txn_type = "edi_payment"
                    else:
                        txn_type = "deposit"  # Regular deposit
                else:
                    txn_type = "withdrawal"
                
                transactions.append(
                    Transaction(
                        date=self._standardize_date(date_str),
                        description=self._clean_description(row_text),
                        amount=amount,
                        check_number=None,
                        transaction_type=txn_type,
                    )
                )
                table_transactions += 1
                print(f"    Row {row_idx}: {txn_type.title()}, Amount: {amount}")

            print(f"  -> Extracted {table_transactions} transactions from table {t_idx}")

        print(f"Total transactions extracted: {len(transactions)}")
        return transactions
    
    def _is_daily_ledger_table(self, df: pd.DataFrame, table_idx: int) -> bool:
        """
        Comprehensive detection for daily ledger balance tables
        """
        if df.empty or df.shape[0] < 2:
            return False
        
        # Method 1: Check for explicit headers in the first few rows
        probe_text = " ".join(self._norm(self._row_to_text(r)) for _, r in df.head(3).iterrows())
        
        # Look for various header patterns
        if self.ledger_header_pat.search(probe_text):
            print(f"    Table {table_idx}: Header pattern detected - 'Daily ledger balances'")
            return True
        
        if self.ledger_column_header_pat.search(probe_text):
            print(f"    Table {table_idx}: Column header pattern detected - 'Date Balance($)'")
            return True
        
        # Method 2: Check for characteristic patterns
        # Look for multiple occurrences of the pattern: MM/DD followed by amount
        ledger_rows = 0
        non_ledger_rows = 0
        
        for _, row in df.head(8).iterrows():  # Check more rows for better detection
            row_text = self._norm(self._row_to_text(row))
            if len(row_text.strip()) < 5:
                continue
                
            if self._is_ledger_row(row_text):
                ledger_rows += 1
            else:
                # Check if it contains transaction-like content
                if self._contains_transaction_content(row_text):
                    non_ledger_rows += 1
        
        # If majority of rows look like ledger rows and no transaction content
        if ledger_rows >= 3 and non_ledger_rows == 0:
            print(f"    Table {table_idx}: Pattern analysis - {ledger_rows} ledger rows, {non_ledger_rows} transaction rows")
            return True
        
        # Method 3: Check for three-column date/balance layout
        if self._has_three_column_date_balance_layout(df):
            print(f"    Table {table_idx}: Three-column date/balance layout detected")
            return True
        
        return False
    
    def _is_ledger_row(self, text: str) -> bool:
        """
        Check if a single row is a ledger balance row
        Looks for pattern: date amount date amount (possibly date amount)
        """
        # Clean up the text
        normalized = self._norm(text.replace("(", "-").replace(")", ""))
        
        # Count dates and amounts
        dates = self.date_pat.findall(normalized)
        amounts = self.money_pat.findall(normalized)
        
        # Ledger rows typically have 2-3 date/amount pairs
        if len(dates) >= 2 and len(amounts) >= 2:
            # Remove all dates and amounts, see what's left
            temp = self.date_pat.sub("", normalized)
            temp = self.money_pat.sub("", temp)
            temp = re.sub(r"[(),\s$\-]+", "", temp).strip()
            
            # If very little non-date/amount content remains, it's likely a ledger row
            if len(temp) <= 8:  # Allow for some column headers like "Balance($)"
                return True
        
        return False
    
    def _contains_transaction_content(self, text: str) -> bool:
        """
        Check if text contains transaction-like content (descriptions, company names, etc.)
        """
        upper = text.upper()
        
        # Transaction indicators
        transaction_keywords = [
            "MECCA", "PAYMENT", "CHECKCARD", "PURCHASE", "DEPOSIT", "WITHDRAWAL",
            "ACH", "TRANSFER", "SQUARE", "EDI", "CHECK", "CARD", "ATM",
            "MERCHANT", "REVENUE", "SECURITY", "BRANDS", "TOBACCO"
        ]
        
        return any(keyword in upper for keyword in transaction_keywords)
    
    def _has_three_column_date_balance_layout(self, df: pd.DataFrame) -> bool:
        """
        Check if the table has the characteristic three-column layout of daily ledger tables:
        Date Balance($) Date Balance($) Date Balance($)
        """
        if df.shape[1] < 6:  # Need at least 6 columns for 3 date/balance pairs
            return False
        
        # Check if first row looks like column headers
        first_row = self._norm(self._row_to_text(df.iloc[0]))
        if re.search(r"Date.*Balance.*Date.*Balance", first_row, re.IGNORECASE):
            return True
        
        # Check if data rows have the expected pattern
        date_balance_pairs = 0
        for _, row in df.head(5).iterrows():
            row_values = [str(val) if pd.notna(val) else "" for val in row.tolist()]
            
            # Count alternating date/balance pattern
            pairs_in_row = 0
            for i in range(0, len(row_values) - 1, 2):
                date_val = row_values[i].strip()
                balance_val = row_values[i + 1].strip()
                
                if (self.date_pat.match(date_val) and 
                    re.match(r"[-]?\d[\d,]*\.\d{2}$", balance_val)):
                    pairs_in_row += 1
            
            if pairs_in_row >= 2:
                date_balance_pairs += 1
        
        return date_balance_pairs >= 3
    
    def _norm(self, s: str) -> str:
        """Normalize whitespace"""
        return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()
    
    def _parse_checks_row_multi(self, row_text: str) -> List[Transaction]:
        """
        Parse side-by-side check entries: Date Check# Amount Date Check# Amount
        """
        txns: List[Transaction] = []

        # Normalize parentheses negatives: (1,234.56) -> -1,234.56
        text = re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", row_text)

        # Pattern for: DATE  CHECKNO  AMOUNT
        triplet = re.compile(
            r"""
            (?P<date>\b\d{1,2}/\d{1,2}/\d{2,4}\b)    # date
            \s+
            (?P<check>\d{3,}\*?)                     # check number (3+ digits)
            \s+
            (?P<amount>[-+]?\$?\d[\d,]*\.\d{2})      # amount
            """,
            re.VERBOSE,
        )

        for m in triplet.finditer(text):
            date_str = m.group("date")
            check_raw = m.group("check")
            amt_str = m.group("amount")

            check_no = check_raw.rstrip("*")
            try:
                amount = float(amt_str.replace("$", "").replace(",", ""))
            except ValueError:
                continue

            txns.append(
                Transaction(
                    date=self._standardize_date(date_str),
                    description="Check",
                    amount=amount,
                    check_number=check_no,
                    transaction_type="check",
                )
            )

        return txns
    
    def _is_edi_payment(self, text: str) -> bool:
        """
        Check if transaction is an EDI payment from specific companies.
        """
        upper_text = text.upper()
        
        edi_companies = [
            "ITG BRANDS",
            "HELIX PAYMENT", 
            "REYNOLDS",
            "PM USA",
            "USSMOKELESS", 
            "JAPAN TOBAC"
        ]
        
        for company in edi_companies:
            if company in upper_text:
                return True
                
        return False
    
    def _extract_check_number(self, text: str) -> Optional[str]:
        """Extract check number from text"""
        # First try: Look for "Check #123456" format
        m = re.search(r"Check\s*#?\s*(\d{3,})", text, flags=re.IGNORECASE)
        if m:
            return m.group(1)
        
        # Second try: Look for any sequence of 3+ digits near an amount
        m2 = re.search(r"\b(\d{3,})\b(?=.*-?\$?\d[\d,]*\.\d{2})", text)
        if m2:
            return m2.group(1)
        
        # Third try: Just find the longest number sequence
        numbers = re.findall(r"\b\d{3,}\b", text)
        if numbers:
            return numbers[-1]
        
        return None



