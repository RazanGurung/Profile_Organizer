import pandas as pd
import tabula
import pdfplumber
from typing import List, Optional, Tuple
from datetime import datetime
import re
from ...interfaces.transaction import Transaction
from .wf_parser import WellsFargoParser

class WellsFargoProcessor:
    """Wells Fargo specific PDF processor - optimized for Wells Fargo statements"""
    
    def __init__(self):
        # Wells Fargo-specific parser
        self.parser = WellsFargoParser()
        self.bank_name = "wells_fargo"

    def extract_transactions(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
        """
        Wells Fargo specific transaction extraction
        Returns: (bank_name, list_of_transactions)
        """
        print(f"Processing Wells Fargo PDF: {pdf_path}")
        
        # Wells Fargo-specific table extraction
        tables = self.extract_tables_tabula_wf(pdf_path)
        
        # Process using Wells Fargo parser
        transactions = self.parser.process_tables(tables)

        print(f"Extracted {len(transactions)} Wells Fargo transactions")
        return self.bank_name, transactions
    
    def extract_tables_tabula_wf(self, pdf_path: str) -> List[pd.DataFrame]:
        """
        Wells Fargo optimized table extraction using tabula-py
        """
        frames: List[pd.DataFrame] = []
        seen_signatures = set()

        def _collect(dfs: List[pd.DataFrame], method_name: str):
            """Deduplicate and collect dataframes"""
            for i, df in enumerate(dfs or []):
                if df is None or df.empty:
                    continue
                sig = (df.shape[0], df.shape[1],
                    tuple(df.head(2).fillna("").astype(str).agg("|".join, axis=1)))
                if sig not in seen_signatures:
                    seen_signatures.add(sig)
                    frames.append(df)
                    print(f"  Collected Wells Fargo table from {method_name}: {df.shape[0]}x{df.shape[1]}")

        print("Extracting Wells Fargo tables with tabula-py...")

        # Wells Fargo-specific extraction method
        try:
            print("  Using Wells Fargo-specific extraction...")
            try:
                dfs_area = tabula.read_pdf(
                    pdf_path, pages="all", multiple_tables=True,
                    lattice=False, stream=True, guess=False,
                    pandas_options={"header": None},
                    relative_area=True, area=(0, 0, 100, 100)
                )
                _collect(dfs_area, "Wells Fargo-specific")
            except TypeError:
                dfs_area_fb = tabula.read_pdf(
                    pdf_path, pages="all", multiple_tables=True,
                    lattice=False, stream=True, guess=False,
                    pandas_options={"header": None},
                )
                _collect(dfs_area_fb, "Wells Fargo-specific (fallback)")
            except Exception as e:
                print(f"  Wells Fargo-specific extraction error: {e}")
        except Exception as e:
            print(f"  Wells Fargo extraction error: {e}")

        # Wells Fargo-specific pdfplumber fallback
        if len(frames) < 3:
            print("  Very few Wells Fargo tables found, trying pdfplumber as fallback...")
            tables_plumber = self.extract_tables_pdfplumber_wf(pdf_path)
            for i, tbl in enumerate(tables_plumber):
                try:
                    df = pd.DataFrame(tbl)
                    if not df.empty:
                        frames.append(df)
                        print(f"  Collected Wells Fargo table from pdfplumber: {df.shape[0]}x{df.shape[1]}")
                except Exception:
                    continue

        print(f"Total Wells Fargo tables extracted: {len(frames)}")
        return frames

    def extract_tables_pdfplumber_wf(self, pdf_path: str) -> List[List[List]]:
        """Wells Fargo-specific pdfplumber extraction"""
        all_tables = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    tables = page.extract_tables()
                    if tables:
                        all_tables.extend(tables)
                        print(f"    Wells Fargo Page {page_num + 1}: found {len(tables)} tables")
                    
                    # Wells Fargo-specific lenient settings
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
                            if tbl not in all_tables:
                                all_tables.append(tbl)
                                
        except Exception as e:
            print(f"Wells Fargo PDFPlumber extraction error: {e}")
        return all_tables
    
    def export_to_csv(self, transactions: List[Transaction], output_path: str):
        """Export Wells Fargo transactions to CSV"""
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
        print(f"Exported {len(data)} Wells Fargo transactions to {output_path}")
    
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
        priority = {"deposit_summary": 0, "edi_payment": 1, "withdrawal": 2, "check": 3}
        def key(t: Transaction):
            ttype = (getattr(t, "transaction_type", "") or "").lower()
            return (priority.get(ttype, 9), self._parse_date_for_sort(getattr(t, "date", "")), getattr(t, "description", ""))
        return sorted(txns, key=key)