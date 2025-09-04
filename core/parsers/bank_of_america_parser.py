import re
from typing import List, Optional
import pandas as pd
from .base_parser import BaseParser, Transaction

class BankOfAmericaParser(BaseParser):
    """Bank of America bank statement parser"""
    
    def __init__(self):
        super().__init__()
        # Strong withdrawal/debit indicators for BofA rows
        self.withdrawal_keywords = [
            "CHECKCARD", "PURCHASE", "MERCHANT SRVS", "DEPT REVENUE",
            "VGP DISTRIBUTORS", "CPI SECURITY", "OVERDRAFT", "ADP PAYROLL FEES",
            "COMPORIUM", "DEMAND VAPE", "ALPHA BRANDS", "MOREB WHOLESALE",
            "MIDWEST GOODS", "SNS IMPORTS", "CASH APP", "AMAZON", "AMZN", "DUKE-ENERGY"
        ]
        
        # Precompiled patterns for performance
        self.date_pat = re.compile(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b")
        self.money_pat = re.compile(r"[-+]?\$?\d[\d,]*\.\d{2}")
        self.check_word_pat = re.compile(r"\bCHECK(?!CARD)\b", re.IGNORECASE)
        self.checknum_pat = re.compile(r"\b\d{3,5}\*?\b")
        # A row that is ONLY (date amount) pairs, repeated ≥2 times (pure ledger line)
        self.ledger_row_full_pat = re.compile(
            r"^(?:\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?\s+[-+]?\$?\d[\d,]*\.\d{2}\s*){2,}$"
        )
    
    def get_bank_name(self) -> str:
        return "bank_of_america"
    
    def get_detection_keywords(self) -> List[str]:
        return ["BANK OF AMERICA", "BANKOFAMERICA.COM", "BUSINESS ADVANTAGE"]
    
    def process_tables(self, tables: List[pd.DataFrame]) -> List[Transaction]:
        """
        Bank of America parsing:
        • Deposits/credits
        • Withdrawals/debits (CHECKCARD, PURCHASE, ACH, etc.)
        • Checks (handles side-by-side 'Checks' section via multi-triplet split)
        Guarantees:
        • 'Daily ledger balances' tables/rows are skipped.
        • Only true Checks get a check_number (not CHECKCARD).
        """
        transactions: List[Transaction] = []
        print(f"Processing {len(tables)} tables for Bank of America...")

        for t_idx, df in enumerate(tables):
            if df.empty:
                continue

            # --- Table-level skip: header says 'Daily ledger balances'
            probe_text = " ".join(self._norm(self._row_to_text(r)) for _, r in df.head(6).iterrows())
            if self._is_ledger_header(probe_text):
                print(f"Skipping ledger-balance table at index {t_idx} (header detected)")
                continue

            # --- Secondary table heuristic: mostly pure-ledger rows → skip
            non_empty = 0
            ledgerish = 0
            for _, r in df.head(10).iterrows():
                rt = self._norm(self._row_to_text(r))
                if len(rt) < 6:
                    continue
                non_empty += 1
                if self._row_is_pure_ledger(rt):
                    ledgerish += 1
            if non_empty >= 6 and ledgerish / max(non_empty, 1) >= 0.7:
                print(f"Skipping ledger-balance table at index {t_idx} (row-shape heuristic)")
                continue

            print(f"Table {t_idx}: {df.shape[0]} rows, {df.shape[1]} cols")

            for _, row in df.iterrows():
                row_text = self._norm(self._row_to_text(row))
                if len(row_text) < 10:
                    continue

                # Row-level guard (very strict; won't drop Checks)
                if self._row_is_pure_ledger(row_text):
                    continue

                # 1) Split two-on-a-line "Checks" rows into individual transactions
                multi_checks = self._parse_checks_row_multi(row_text)
                if multi_checks:
                    transactions.extend(multi_checks)
                    continue

                # 2) Need a date and amount for anything else
                date_str = self._extract_date_any(row_text)
                if not date_str:
                    continue
                amount = self._pick_amount(row_text)
                if amount is None:
                    continue

                upper = row_text.upper()

                # 3) Force CHECKCARD / PURCHASE to withdrawals (never set check_number)
                if "CHECKCARD" in upper or "PURCHASE" in upper:
                    transactions.append(
                        Transaction(
                            date=self._standardize_date(date_str),
                            description=self._clean_description(row_text),
                            amount=-abs(amount),
                            check_number=None,
                            transaction_type="withdrawal",
                        )
                    )
                    continue

                # 4) Strict check detection (exclude CHECKCARD) and require a check number
                is_check_line = (self.check_word_pat.search(upper) is not None) and (self.checknum_pat.search(row_text) is not None)
                if is_check_line:
                    check_number = self._extract_check_number(row_text)
                    transactions.append(
                        Transaction(
                            date=self._standardize_date(date_str),
                            description=self._clean_description(row_text) or "Check",
                            amount=-abs(amount),  # checks are debits
                            check_number=check_number,
                            transaction_type="check",
                        )
                    )
                    continue

                # 5) As a final guard against ledger noise:
                # Require at least a few alphabetic characters (descriptive words).
                alpha_count = len(re.findall(r"[A-Za-z]", row_text))
                if alpha_count < 4:
                    continue

                # 6) Everything else: classify by keywords/sign; NEVER set check_number
                if any(k in upper for k in self.withdrawal_keywords) or amount < 0:
                    txn_type = "withdrawal"
                    amount = -abs(amount)
                else:
                    txn_type = "deposit"
                    amount = abs(amount)

                transactions.append(
                    Transaction(
                        date=self._standardize_date(date_str),
                        description=self._clean_description(row_text),
                        amount=amount,
                        check_number=None,
                        transaction_type=txn_type,
                    )
                )

        return transactions
    
    def _norm(self, s: str) -> str:
        """Normalize whitespace"""
        return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

    def _is_ledger_header(self, text: str) -> bool:
        """Check if text contains ledger balance header"""
        return "DAILY LEDGER BALANCES" in text.upper()

    def _row_is_pure_ledger(self, text: str) -> bool:
        """
        True ONLY for the ledger section rows:
        - no words (just repeated date+amount pairs)
        - never matches checks or normal transactions
        """
        t = self._norm(re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", text))  # (1,234.56) -> -1,234.56
        up = t.upper()
        # Not ledger if any of these appear
        if "CHECKCARD" in up or "PURCHASE" in up:
            return False
        if self.check_word_pat.search(up) or self.checknum_pat.search(t):
            return False
        # Pure (date amount){2,}
        return self.ledger_row_full_pat.match(t) is not None
    
    def _parse_checks_row_multi(self, row_text: str) -> List[Transaction]:
        """
        Bank of America 'Checks' section often prints TWO check lines side-by-side
        on one visual row. This function scans the whole row and emits ONE
        Transaction per (date, check#, amount) triplet it finds.
        """
        txns: List[Transaction] = []

        # Normalize parentheses negatives: (1,234.56) -> -1,234.56
        text = re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", row_text)

        # Triplet pattern: DATE  CHECKNO  AMOUNT
        triplet = re.compile(
            r"""
            (?P<date>\b\d{1,2}/\d{1,2}/\d{2,4}\b)    # date (MM/DD/YY or MM/DD/YYYY)
            \s+
            (?P<check>\d{3,5}\*?)                    # check number (e.g., 230 or 230*)
            \s+
            (?P<amount>[-+]?\$?\d[\d,]*\.\d{2})      # amount (e.g., -1,470.65)
            """,
            re.VERBOSE,
        )

        for m in triplet.finditer(text):
            date_str  = m.group("date")
            check_raw = m.group("check")
            amt_str   = m.group("amount")

            # Clean/convert fields
            check_no = check_raw.rstrip("*")
            try:
                amount = float(amt_str.replace("$", "").replace(",", ""))
            except ValueError:
                continue

            # Checks are debits; enforce negative sign
            if amount > 0:
                amount = -amount

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


