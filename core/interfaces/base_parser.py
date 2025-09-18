from abc import ABC, abstractmethod
from typing import List, Optional
import pandas as pd
import re
from dataclasses import dataclass

@dataclass
class Transaction:
    date: str
    description: str
    amount: float
    check_number: Optional[str] = None
    balance: Optional[float] = None
    transaction_type: str = "unknown"  # deposit, withdrawal, check

class BaseParser(ABC):
    """Abstract base class for bank statement parsers"""
    
    def __init__(self):
        self.bank_name = self.get_bank_name()
        self.detection_keywords = self.get_detection_keywords()
    
    @abstractmethod
    def get_bank_name(self) -> str:
        """Return the bank name identifier"""
        pass
    
    @abstractmethod
    def get_detection_keywords(self) -> List[str]:
        """Return keywords to detect this bank type from PDF content"""
        pass
    
    @abstractmethod
    def process_tables(self, tables: List[pd.DataFrame]) -> List[Transaction]:
        """Process extracted tables and return transactions"""
        pass
    
    def can_parse(self, pdf_text: str) -> bool:
        """Check if this parser can handle the given PDF content"""
        pdf_upper = pdf_text.upper()
        return any(keyword in pdf_upper for keyword in self.detection_keywords)
    
    # Common utility methods that all parsers can use
    def _row_to_text(self, row: pd.Series) -> str:
        """Convert pandas row to clean text"""
        parts = [(str(x) if pd.notna(x) else "") for x in row.tolist()]
        txt = " ".join(p.strip() for p in parts if p is not None).strip()
        return re.sub(r"\s+", " ", txt)
    
    def _extract_date_any(self, text: str) -> Optional[str]:
        """Extract date from text - matches various formats"""
        m = re.search(r"\b(\d{1,2}/\d{1,2}(?:/\d{2,4})?)\b", text)
        return m.group(1) if m else None
    
    def _extract_check_number(self, text: str) -> Optional[str]:
        """Extract check number from text"""
        m = re.search(r"Check\s*#?\s*(\d{3,5})", text, flags=re.IGNORECASE)
        if m:
            return m.group(1)
        m2 = re.search(r"\b(\d{3,5})\b(?=.*-?\$?\d[\d,]*\.\d{2})", text)
        return m2.group(1) if m2 else None
    
    def _pick_amount(self, text: str) -> Optional[float]:
        """Choose the rightmost plausible monetary value"""
        t = re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", text)
        candidates = re.findall(r"[-+]?\$?\d[\d,]*\.\d{2}", t)
        if not candidates:
            return None
        amount = None
        for c in candidates:
            try:
                v = float(c.replace("$", "").replace(",", ""))
                if abs(v) < 1_000_000:  # sanity bound
                    amount = v  # keep last plausible (rightmost)
            except ValueError:
                continue
        return amount
    
    def _clean_description(self, text: str) -> str:
        """Clean and standardize transaction descriptions"""
        cleaned = re.sub(r"\s+", " ", text).strip()
        # Strip dates
        cleaned = re.sub(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b", " ", cleaned)
        # Strip currency/amounts
        cleaned = re.sub(r"[-+]?\$?\d[\d,]*\.\d{2}", " ", cleaned)
        # Strip common bank codes/tokens
        cleaned = re.sub(r"\b(CKCD|CCD|PPD)\b\s*\d*", " ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"Card\s+\d{4}", " ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"X{4,}\d+", " ", cleaned)  # masked digits
        cleaned = re.sub(r"DES:[A-Z0-9\s]*", " ", cleaned)
        cleaned = re.sub(r"ID:\s*[A-Z0-9\-]+", " ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"INDN:[A-Z0-9\s]*", " ", cleaned)
        cleaned = re.sub(r"Check\s*#?\s*\d{3,5}", " Check ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        
        # Fallback labels
        up = text.upper()
        if len(cleaned) < 5:
            if "CHECKCARD" in up or "PURCHASE" in up:
                cleaned = "Debit Card Purchase"
            elif "CHECK" in up:
                cleaned = "Check"
            elif "MERCHANT" in up:
                cleaned = "Merchant Services"
            else:
                cleaned = "Transaction"
        return cleaned[:80]
    
    def _standardize_date(self, date_str: str) -> str:
        """Convert to MM/DD/YYYY format"""
        try:
            parts = date_str.split("/")
            if len(parts) == 3:
                mm, dd, yy = parts
                if len(yy) == 2:
                    return f"{mm}/{dd}/20{yy}"
                return f"{mm}/{dd}/{yy}"
            return date_str  # keep MM/DD; caller can add year context
        except Exception:
            return date_str