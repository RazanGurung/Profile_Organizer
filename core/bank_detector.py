import pdfplumber
from typing import Optional
import re

class BankDetector:
    """Service to detect which bank a PDF statement belongs to"""
    
    def __init__(self):
        # Start with just BoA, we'll add more banks later
        self.bank_patterns = {
            "bank_of_america": {
                "keywords": ["BANK OF AMERICA", "BANKOFAMERICA.COM", "BUSINESS ADVANTAGE"],
                "strong_indicators": ["P.O. Box 25118", "Tampa, FL 33622-5118", "1.888.BUSINESS"],
                "account_patterns": [r"Account number:\s*\d{4}\s*\d{4}\s*\d{4}"],
                "header_patterns": ["Your Business Advantage Fundamentals"]
            }
        }
    
    def detect_bank(self, pdf_path: str) -> Optional[str]:
        """Detect which bank this PDF belongs to"""
        try:
            text_content = self._extract_pdf_text(pdf_path)
            if not text_content:
                print("Could not extract text from PDF")
                return None
            
            # For now, just check BoA
            if self._is_bank_of_america(text_content):
                print("Detected: Bank of America")
                return "bank_of_america"
            
            print("No bank patterns matched")
            return None
                
        except Exception as e:
            print(f"Error detecting bank: {e}")
            return None
    
    def _extract_pdf_text(self, pdf_path: str) -> str:
        """Extract text from first few pages of PDF"""
        text_content = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages[:3]):
                    page_text = page.extract_text() or ""
                    text_content += page_text + " "
        except Exception as e:
            print(f"Error extracting PDF text: {e}")
        return text_content
    
    def _is_bank_of_america(self, text: str) -> bool:
        """Check if this is a Bank of America statement"""
        text_upper = text.upper()
        patterns = self.bank_patterns["bank_of_america"]
        
        # Check keywords
        keyword_matches = sum(1 for keyword in patterns["keywords"] if keyword in text_upper)
        
        # Check strong indicators  
        indicator_matches = sum(1 for indicator in patterns["strong_indicators"] if indicator.upper() in text_upper)
        
        # Check account patterns
        pattern_matches = sum(1 for pattern in patterns["account_patterns"] if re.search(pattern, text, re.IGNORECASE))
        
        total_score = keyword_matches + (indicator_matches * 2) + (pattern_matches * 3)
        
        print(f"BoA detection score: {total_score} (keywords: {keyword_matches}, indicators: {indicator_matches}, patterns: {pattern_matches})")
        
        return total_score >= 2  # Require at least 2 points to be confident