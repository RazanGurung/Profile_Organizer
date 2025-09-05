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
        self.checknum_pat = re.compile(r"\b\d{3,}\*?\b")  # FIXED: 3 or more digits
        
        # Very strict pattern for pure ledger rows (date amount pairs only)
        self.pure_ledger_pat = re.compile(
            r"^(?:\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?\s+[-+]?\$?\d[\d,]*\.\d{2}\s*){2,}$"
        )
    
    def get_bank_name(self) -> str:
        return "bank_of_america"
    
    def get_detection_keywords(self) -> List[str]:
        return ["BANK OF AMERICA", "BANKOFAMERICA.COM", "BUSINESS ADVANTAGE"]
    
    def process_tables(self, tables: List[pd.DataFrame]) -> List[Transaction]:
        """
        Simple Bank of America parsing:
        - Use amount signs exactly as they appear (positive = deposit, negative = withdrawal)
        - Only skip obvious "Daily ledger balances" tables
        - Process ALL other tables, including small/broken ones
        """
        transactions: List[Transaction] = []
        print(f"Processing {len(tables)} tables for Bank of America...")

        for t_idx, df in enumerate(tables):
            if df.empty:
                print(f"Table {t_idx}: Empty - skipping")
                continue

            print(f"Table {t_idx}: {df.shape[0]} rows, {df.shape[1]} cols")

            # Enhanced detection for Daily ledger balances tables
            probe_text = " ".join(self._norm(self._row_to_text(r)) for _, r in df.head(3).iterrows())
            
            # Check for explicit header
            if "DAILY LEDGER BALANCES" in probe_text.upper():
                print(f"  -> Skipping: Daily ledger balances table (header detected)")
                continue
            
            # Additional check: if it's mostly simple date-balance pairs without check numbers
            if self._is_simple_daily_ledger_table(df):
                print(f"  -> Skipping: Daily ledger balances table (pattern detected)")
                continue

            # Process ALL other tables
            table_transactions = 0
            for row_idx, row in df.iterrows():
                row_text = self._norm(self._row_to_text(row))
                if len(row_text) < 8:
                    continue

                # Skip ONLY pure ledger rows (date amount date amount...)
                if self._is_pure_ledger_row(row_text):
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

                # FIXED: More specific check detection
                # Only classify as check if: has CHECK word, has number, is negative, and is NOT a return/deposit/fee
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
                            amount=amount,  # Use amount exactly as extracted
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
                        amount=amount,  # Use amount exactly as extracted
                        check_number=None,
                        transaction_type=txn_type,
                    )
                )
                table_transactions += 1
                print(f"    Row {row_idx}: {txn_type.title()}, Amount: {amount}")

            print(f"  -> Extracted {table_transactions} transactions from table {t_idx}")

        print(f"Total transactions extracted: {len(transactions)}")
        return transactions
    
    def _norm(self, s: str) -> str:
        """Normalize whitespace"""
        return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

    def _is_pure_ledger_row(self, text: str) -> bool:
        """
        Only skip rows that are pure ledger format: date amount date amount...
        Very conservative - only skip obvious ledger balance rows
        """
        # Normalize parentheses: (1,234.56) -> -1,234.56
        t = self._norm(re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", text))
        
        # If it contains any text (not just numbers and dates), it's likely a real transaction
        # Remove dates and amounts, see what's left
        temp = re.sub(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b", "", t)  # Remove dates
        temp = re.sub(r"[-+]?\$?\d[\d,]*\.\d{2}", "", temp)  # Remove amounts
        temp = re.sub(r"\s+", " ", temp).strip()  # Clean whitespace
        
        # If there's substantial text left, it's a transaction description
        if len(temp) > 3:
            return False
        
        # Check if it matches pure ledger pattern (date amount){2,}
        return self.pure_ledger_pat.match(t) is not None
    
    def _parse_checks_row_multi(self, row_text: str) -> List[Transaction]:
        """
        Parse side-by-side check entries: Date Check# Amount Date Check# Amount
        """
        txns: List[Transaction] = []

        # Normalize parentheses negatives: (1,234.56) -> -1,234.56
        text = re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", row_text)

        # Pattern for: DATE  CHECKNO  AMOUNT (FIXED: 3+ digits for check number)
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
                    amount=amount,  # Use amount exactly as extracted
                    check_number=check_no,
                    transaction_type="check",
                )
            )

        return txns
    
    def _is_simple_daily_ledger_table(self, df: pd.DataFrame) -> bool:
        """Simple check for daily ledger tables without being too aggressive"""
        if df.empty or df.shape[0] < 3:
            return False
        
        # Quick check: if we see check numbers, it's definitely not a ledger table
        sample_text = " ".join(self._norm(self._row_to_text(r)) for _, r in df.head(5).iterrows())
        if re.search(r"\bcheck\b.*\d{3,}", sample_text, re.IGNORECASE):
            return False
        
        # If it has very simple structure (mostly just dates and amounts), likely a ledger
        simple_rows = 0
        for _, row in df.head(5).iterrows():
            row_text = self._norm(self._row_to_text(row))
            if len(row_text) < 5:
                continue
            
            # Remove dates and amounts, see what's left
            temp = re.sub(r"\b\d{1,2}/\d{1,2}\b", "", row_text)
            temp = re.sub(r"\d[\d,]*\.\d{2}", "", temp)
            temp = re.sub(r"[(),\s$]+", "", temp).strip()
            
            if len(temp) <= 5:  # Very little text besides dates/amounts
                simple_rows += 1
        
        return simple_rows >= 3  # If 3+ rows are very simple, likely a ledger table
    
    def _is_edi_payment(self, text: str) -> bool:
        """
        Check if transaction is an EDI payment from specific companies.
        Look for: ITG Brands, Helix Payment, Reynolds Marketing, PM USA, USSmokeless, Japan Tobac
        """
        upper_text = text.upper()
        
        # List of specific companies to include as EDI payments
        edi_companies = [
            "ITG BRANDS",
            "HELIX PAYMENT", 
            "REYNOLDS",
            "PM USA",
            "USSMOKELESS", 
            "JAPAN TOBAC"
        ]
        
        # Check if any of these companies appear in the transaction description
        for company in edi_companies:
            if company in upper_text:
                return True
                
        return False
    
    def _extract_check_number(self, text: str) -> Optional[str]:
        """Extract check number from text - updated for longer check numbers"""
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
            return numbers[-1]  # Return the last (rightmost) number found
        
        return None



