import re
from typing import List, Optional
import pandas as pd
from .base_parser import BaseParser, Transaction

class WellsFargoParser(BaseParser):
    """Wells Fargo bank statement parser"""
    
    def get_bank_name(self) -> str:
        return "wells_fargo"
    
    def get_detection_keywords(self) -> List[str]:
        return ["WELLS FARGO", "NAVIGATE BUSINESS CHECKING"]
    
    def process_tables(self, tables: List[pd.DataFrame]) -> List[Transaction]:
        """Process Wells Fargo tables and extract transactions"""
        transactions: List[Transaction] = []
        
        for df in tables:
            if df.empty:
                continue
                
            for _, row in df.iterrows():
                row_text = self._row_to_text(row)
                if not row_text:
                    continue
                    
                # Date pattern mm/dd (WF statement detail often omits year in rows)
                if re.search(r"\b\d{1,2}/\d{1,2}\b", row_text):
                    transaction = self._parse_transaction_row(row_text)
                    if transaction:
                        transactions.append(transaction)
        
        return transactions
    
    def _parse_transaction_row(self, row_text: str) -> Optional[Transaction]:
        """Parse individual Wells Fargo transaction row"""
        try:
            # Extract date
            date_match = re.search(r"\b(\d{1,2}/\d{1,2})\b", row_text)
            if not date_match:
                return None
            date_str = date_match.group(1)

            # Get transaction amount
            amount = self._pick_amount(row_text)
            if amount is None:
                return None

            # Determine transaction type
            txn_type = "deposit" if amount > 0 else "withdrawal"

            # Check for check number
            check_number = self._extract_check_number(row_text)
            if check_number:
                txn_type = "check"
                if amount > 0:
                    amount = -abs(amount)  # checks should be debits

            # Clean description
            description = self._clean_description(row_text)
            
            return Transaction(
                date=self._standardize_date(date_str),
                description=description,
                amount=amount,
                check_number=check_number,
                transaction_type=txn_type
            )
            
        except Exception as e:
            print(f"Error parsing Wells Fargo row: {e}")
            return None