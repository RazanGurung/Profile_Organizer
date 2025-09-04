import pandas as pd
import tabula
import pdfplumber
from typing import List, Optional, Tuple
from .parsers import BaseParser, Transaction, AVAILABLE_PARSERS

class BankStatementProcessor:
    """Main coordinator class for processing bank statement PDFs"""
    
    def __init__(self):
        # Initialize all available parsers
        self.parsers = [parser_class() for parser_class in AVAILABLE_PARSERS]
        self.supported_banks = [parser.bank_name for parser in self.parsers]
    
    def detect_bank_type(self, pdf_path: str) -> Tuple[str, Optional[BaseParser]]:
        """
        Detect bank type from PDF content and return the appropriate parser
        Returns: (bank_name, parser_instance)
        """
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Extract text from first few pages for detection
                text_content = ""
                for page_num, page in enumerate(pdf.pages[:3]):  # Check first 3 pages
                    page_text = page.extract_text() or ""
                    text_content += page_text + " "
            
            # Try each parser to see which one can handle this PDF
            for parser in self.parsers:
                if parser.can_parse(text_content):
                    print(f"Detected bank: {parser.bank_name}")
                    return parser.bank_name, parser
            
            print("Unknown bank type detected")
            return "unknown", None
            
        except Exception as e:
            print(f"Error detecting bank type: {e}")
            return "unknown", None
    
    def extract_tables_tabula(self, pdf_path: str, bank_name: str) -> List[pd.DataFrame]:
        """
        Extract tables using tabula-py with bank-specific optimizations
        Try both lattice and stream methods for maximum coverage
        """
        frames: List[pd.DataFrame] = []
        seen_signatures = set()

        def _collect(dfs: List[pd.DataFrame]):
            """Deduplicate and collect dataframes"""
            for df in dfs or []:
                # Create signature to avoid duplicates
                sig = (df.shape[0], df.shape[1], tuple(df.head(2).fillna("").astype(str).agg("|".join, axis=1)))
                if sig not in seen_signatures and not df.empty:
                    seen_signatures.add(sig)
                    frames.append(df)

        try:
            # First pass: lattice (good for ruled tables)
            dfs_lat = tabula.read_pdf(
                pdf_path, pages="all", multiple_tables=True, lattice=True, guess=False,
                pandas_options={"header": None}
            )
            _collect(dfs_lat)
        except Exception as e:
            print(f"Tabula lattice error: {e}")

        try:
            # Second pass: stream (good for whitespace-separated tables)
            dfs_str = tabula.read_pdf(
                pdf_path, pages="all", multiple_tables=True, stream=True, guess=True,
                pandas_options={"header": None}
            )
            _collect(dfs_str)
        except Exception as e:
            print(f"Tabula stream error: {e}")

        # Fallback: pdfplumber tables if tabula failed
        if not frames:
            tables_plumber = self.extract_tables_pdfplumber(pdf_path)
            for tbl in tables_plumber:
                try:
                    df = pd.DataFrame(tbl)
                    if not df.empty:
                        frames.append(df)
                except Exception:
                    continue

        return frames

    def extract_tables_pdfplumber(self, pdf_path: str) -> List[List[List]]:
        """Backup extraction using pdfplumber"""
        all_tables = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    if tables:
                        all_tables.extend(tables)
        except Exception as e:
            print(f"PDFPlumber extraction error: {e}")
        return all_tables
    
    def extract_transactions(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
        """
        Main method to extract transactions from PDF
        Returns: (bank_name, list_of_transactions)
        """
        print(f"Processing: {pdf_path}")
        
        # Detect bank type and get appropriate parser
        bank_name, parser = self.detect_bank_type(pdf_path)
        
        if parser is None:
            print("No suitable parser found, trying generic extraction...")
            # Fallback: try all parsers and use the one that returns most transactions
            return self._try_generic_extraction(pdf_path)
        
        # Extract tables from PDF
        tables = self.extract_tables_tabula(pdf_path, bank_name)
        print(f"Found {len(tables)} tables")
        
        # Use the specific parser to process the tables
        transactions = parser.process_tables(tables)
        
        print(f"Extracted {len(transactions)} transactions")
        return bank_name, transactions
    
    def _try_generic_extraction(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
        """Fallback method when bank type is unknown"""
        best_result = ("unknown", [])
        
        # Extract tables once
        tables = self.extract_tables_tabula(pdf_path, "unknown")
        
        # Try each parser and use the one that finds the most transactions
        for parser in self.parsers:
            try:
                transactions = parser.process_tables(tables)
                if len(transactions) > len(best_result[1]):
                    best_result = (parser.bank_name, transactions)
            except Exception as e:
                print(f"Error trying parser {parser.bank_name}: {e}")
                continue
        
        return best_result
    
    def export_to_csv(self, transactions: List[Transaction], output_path: str):
        """Export transactions to CSV in standardized format"""
        data = []
        for txn in transactions:
            data.append({
                "Date": txn.date,
                "Check No": txn.check_number or "",
                "Description": txn.description,
                "Amount": txn.amount
            })
        
        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)
        print(f"Exported {len(data)} transactions to {output_path}")
    
    def get_supported_banks(self) -> List[str]:
        """Return list of supported bank names"""
        return self.supported_banks.copy()
    
    def add_parser(self, parser_class) -> None:
        """Add a new parser to the processor (for extensibility)"""
        parser = parser_class()
        self.parsers.append(parser)
        self.supported_banks.append(parser.bank_name)
        print(f"Added parser for {parser.bank_name}")

# For backward compatibility - expose the Transaction class
__all__ = ['BankStatementProcessor', 'Transaction']