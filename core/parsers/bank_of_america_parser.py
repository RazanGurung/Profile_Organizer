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
        self.checknum_pat = re.compile(r"\b\d{3,5}\*?\b")
        
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

            # ONLY skip if the table header explicitly says "Daily ledger balances"
            probe_text = " ".join(self._norm(self._row_to_text(r)) for _, r in df.head(3).iterrows())
            if "DAILY LEDGER BALANCES" in probe_text.upper():
                print(f"  -> Skipping: Daily ledger balances table")
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

                # Handle individual checks (with check numbers)
                is_check_line = (self.check_word_pat.search(upper) is not None) and (self.checknum_pat.search(row_text) is not None)
                if is_check_line and "CHECKCARD" not in upper:
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
                # Positive = deposit, Negative = withdrawal
                if amount > 0:
                    txn_type = "deposit"
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

        # Pattern for: DATE  CHECKNO  AMOUNT
        triplet = re.compile(
            r"""
            (?P<date>\b\d{1,2}/\d{1,2}/\d{2,4}\b)    # date
            \s+
            (?P<check>\d{3,5}\*?)                    # check number
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


