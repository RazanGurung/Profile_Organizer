import pandas as pd
import tabula
import pdfplumber
from typing import List, Optional, Tuple
from .parsers import BaseParser, Transaction, AVAILABLE_PARSERS
from datetime import datetime
import re

class BankStatementProcessor:
    """Main coordinator class for processing bank statement PDFs"""
    
    def __init__(self):
        # Initialize all available parsers
        self.parsers = [parser_class() for parser_class in AVAILABLE_PARSERS]
        self.supported_banks = [parser.bank_name for parser in self.parsers]



    def _parse_date_for_sort(self, s: str) -> datetime:
        """Robust date key (supports ISO and MM/DD/YY or MM/DD/YYYY)."""
        if not s:
            return datetime.max
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(s.strip(), fmt)
            except Exception:
                continue
        return datetime.max

    def _sort_txns(self, txns: List[Transaction]) -> List[Transaction]:
        """
        Sort so UI naturally shows sections in expected order:
        Deposits → Withdrawals → Checks, each by date ascending.
        """
        priority = {"deposit": 0, "withdrawal": 1, "check": 2}
        def key(t: Transaction):
            ttype = (getattr(t, "transaction_type", "") or "").lower()
            return (priority.get(ttype, 9), self._parse_date_for_sort(getattr(t, "date", "")), getattr(t, "description", ""))
        return sorted(txns, key=key)

    
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
        Enhanced table extraction using tabula-py with better coverage for small tables.
        Includes a robust BoA-specific pass with fallback when the Tabula build
        doesn't support relative_area/area.
        """
        frames: List[pd.DataFrame] = []
        seen_signatures = set()

        def _collect(dfs: List[pd.DataFrame], method_name: str):
            """Deduplicate and collect dataframes"""
            for i, df in enumerate(dfs or []):
                if df is None or df.empty:
                    continue
                # Signature: shape + a tiny content sample (first 2 rows)
                sig = (df.shape[0], df.shape[1],
                    tuple(df.head(2).fillna("").astype(str).agg("|".join, axis=1)))
                if sig not in seen_signatures:
                    seen_signatures.add(sig)
                    frames.append(df)
                    print(f"  Collected table from {method_name}: {df.shape[0]}x{df.shape[1]}")

        print("Extracting tables with tabula-py...")

        # Method 1: lattice (ruled tables)
        try:
            print("  Trying lattice method...")
            dfs_lat = tabula.read_pdf(
                pdf_path, pages="all", multiple_tables=True, lattice=True, guess=False,
                pandas_options={"header": None}
            )
            _collect(dfs_lat, "lattice")
        except Exception as e:
            print(f"  Tabula lattice error: {e}")

        # Method 2: stream (whitespace-separated tables)
        try:
            print("  Trying stream method...")
            dfs_str = tabula.read_pdf(
                pdf_path, pages="all", multiple_tables=True, stream=True, guess=True,
                pandas_options={"header": None}
            )
            _collect(dfs_str, "stream")
        except Exception as e:
            print(f"  Tabula stream error: {e}")

        # Method 3: more aggressive pass for Bank of America
        # (normalize bank name to a slug so this reliably triggers)
        try:
            slug = (bank_name or "").lower().strip().replace(" ", "_")
            if slug in {"bank_of_america", "boa", "bofa"}:
                print("  Trying BofA-specific extraction...")
                try:
                    # Preferred: lenient stream with area hints
                    dfs_area = tabula.read_pdf(
                        pdf_path, pages="all", multiple_tables=True,
                        lattice=False, stream=True, guess=False,
                        pandas_options={"header": None},
                        # These help catch tiny, broken “– continued” mini-tables.
                        # Some tabula builds ignore/raise on these — we catch and fallback.
                        relative_area=True, area=(0, 0, 100, 100)
                    )
                    _collect(dfs_area, "BofA-specific")
                except TypeError:
                    # Fallback: same call without the extra args
                    dfs_area_fb = tabula.read_pdf(
                        pdf_path, pages="all", multiple_tables=True,
                        lattice=False, stream=True, guess=False,
                        pandas_options={"header": None},
                    )
                    _collect(dfs_area_fb, "BofA-specific (fallback)")
                except Exception as e:
                    print(f"  BofA-specific extraction error: {e}")
        except Exception as e:
            print(f"  BofA slug handling error: {e}")

        # Fallback: pdfplumber if very few tables found
        if len(frames) < 5:
            print("  Few tables found, trying pdfplumber as fallback...")
            tables_plumber = self.extract_tables_pdfplumber(pdf_path)
            for i, tbl in enumerate(tables_plumber):
                try:
                    df = pd.DataFrame(tbl)
                    if not df.empty:
                        frames.append(df)
                        print(f"  Collected table from pdfplumber: {df.shape[0]}x{df.shape[1]}")
                except Exception:
                    continue

        print(f"Total tables extracted: {len(frames)}")
        return frames


    def extract_tables_pdfplumber(self, pdf_path: str) -> List[List[List]]:
        """Enhanced backup extraction using pdfplumber"""
        all_tables = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    # Try different table extraction settings
                    tables = page.extract_tables()
                    if tables:
                        all_tables.extend(tables)
                        print(f"    Page {page_num + 1}: found {len(tables)} tables")
                    
                    # Also try with more lenient settings for small/broken tables
                    tables_lenient = page.extract_tables(
                        table_settings={
                            "vertical_strategy": "lines_strict",
                            "horizontal_strategy": "lines_strict",
                            "intersection_tolerance": 3,
                            "join_tolerance": 3
                        }
                    )
                    if tables_lenient:
                        for tbl in tables_lenient:
                            if tbl not in all_tables:  # Simple duplicate check
                                all_tables.append(tbl)
                                
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
        
        # Additional debugging for small tables that might contain end-of-statement transactions
        small_tables = [t for t in tables if t.shape[0] <= 5]
        if small_tables:
            print(f"Found {len(small_tables)} small tables (≤5 rows) - these may contain end-of-statement transactions")
            for i, t in enumerate(small_tables):
                print(f"  Small table {i}: {t.shape[0]}x{t.shape[1]} rows")
        
        # Use the specific parser to process the tables
        transactions = parser.process_tables(tables)
        uniq = {}
        for tx in transactions:
            uniq[self._txn_key(tx)] = tx
        transactions = list(uniq.values())

        # NEW: stable, section-aware ordering
        transactions = self._sort_txns(transactions)

        print(f"Extracted {len(transactions)} transactions")
        return bank_name, transactions
    
    def _txn_key(self, t: Transaction):
        """
        Stable key to collapse duplicate transactions coming from multiple
        extraction passes or near-duplicate tables.
        """
        import re
        desc = re.sub(r"\s+", " ", (t.description or "")).strip().upper()
        chk  = (t.check_number or "").strip()
        amt  = round(float(t.amount), 2)   # avoid float jitter
        dt   = (t.date or "").strip()
        typ  = (getattr(t, "transaction_type", "") or "").lower()
        return (dt, amt, chk, desc, typ)

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
