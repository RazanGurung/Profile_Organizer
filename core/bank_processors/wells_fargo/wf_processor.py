# import pandas as pd
# import tabula
# import pdfplumber
# from typing import List, Optional, Tuple
# from datetime import datetime
# import re
# from ...interfaces.transaction import Transaction
# from .wf_parser import WellsFargoParser

# class WellsFargoProcessor:
#     """Wells Fargo specific PDF processor - optimized for WF statements"""
    
#     def __init__(self):
#         # Wells Fargo-specific parser
#         self.parser = WellsFargoParser()
#         self.bank_name = "wells_fargo"

#     def extract_transactions(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
#         """
#         Wells Fargo specific transaction extraction
#         Returns: (bank_name, list_of_transactions)
#         """
#         print(f"Processing Wells Fargo PDF: {pdf_path}")
        
#         # Wells Fargo-specific table extraction
#         tables = self.extract_tables_tabula_wf(pdf_path)
        
#         # Additional debugging for WF small tables
#         small_tables = [t for t in tables if t.shape[0] <= 5]
#         if small_tables:
#             print(f"Found {len(small_tables)} small WF tables (≤5 rows)")
#             for i, t in enumerate(small_tables):
#                 print(f"  Small WF table {i}: {t.shape[0]}x{t.shape[1]} rows")

#         # Process using Wells Fargo parser
#         transactions = self.parser.process_tables(tables)
        
#         # Wells Fargo uses simple sorting (no monthly summaries like BoA)
#         transactions = self._sort_txns(transactions)

#         print(f"Extracted {len(transactions)} Wells Fargo transactions")
#         return self.bank_name, transactions
    
#     def extract_tables_tabula_wf(self, pdf_path: str) -> List[pd.DataFrame]:
#         """
#         Wells Fargo optimized table extraction
#         """
#         frames: List[pd.DataFrame] = []
#         seen_signatures = set()

#         def _collect(dfs: List[pd.DataFrame], method_name: str):
#             """Deduplicate and collect dataframes"""
#             for i, df in enumerate(dfs or []):
#                 if df is None or df.empty:
#                     continue
#                 sig = (df.shape[0], df.shape[1],
#                     tuple(df.head(2).fillna("").astype(str).agg("|".join, axis=1)))
#                 if sig not in seen_signatures:
#                     seen_signatures.add(sig)
#                     frames.append(df)
#                     print(f"  Collected WF table from {method_name}: {df.shape[0]}x{df.shape[1]}")

#         print("Extracting Wells Fargo tables with tabula-py...")

#         # Wells Fargo specific extraction - try lattice first (WF has more structured tables)
#         try:
#             print("  Using WF lattice extraction...")
#             dfs_lattice = tabula.read_pdf(
#                 pdf_path, pages="all", multiple_tables=True,
#                 lattice=True, guess=False,
#                 pandas_options={"header": None}
#             )
#             _collect(dfs_lattice, "WF-lattice")
#         except Exception as e:
#             print(f"  WF lattice extraction error: {e}")

#         # Wells Fargo stream extraction
#         try:
#             print("  Using WF stream extraction...")
#             dfs_stream = tabula.read_pdf(
#                 pdf_path, pages="all", multiple_tables=True,
#                 lattice=False, stream=True, guess=True,
#                 pandas_options={"header": None}
#             )
#             _collect(dfs_stream, "WF-stream")
#         except Exception as e:
#             print(f"  WF stream extraction error: {e}")

#         # Wells Fargo pdfplumber fallback
#         if len(frames) < 3:
#             print("  Very few WF tables found, trying pdfplumber as fallback...")
#             tables_plumber = self.extract_tables_pdfplumber_wf(pdf_path)
#             for i, tbl in enumerate(tables_plumber):
#                 try:
#                     df = pd.DataFrame(tbl)
#                     if not df.empty:
#                         frames.append(df)
#                         print(f"  Collected WF table from pdfplumber: {df.shape[0]}x{df.shape[1]}")
#                 except Exception:
#                     continue

#         print(f"Total Wells Fargo tables extracted: {len(frames)}")
#         return frames

#     def extract_tables_pdfplumber_wf(self, pdf_path: str) -> List[List[List]]:
#         """Wells Fargo-specific pdfplumber extraction"""
#         all_tables = []
#         try:
#             with pdfplumber.open(pdf_path) as pdf:
#                 for page_num, page in enumerate(pdf.pages):
#                     tables = page.extract_tables()
#                     if tables:
#                         all_tables.extend(tables)
#                         print(f"    WF Page {page_num + 1}: found {len(tables)} tables")
                    
#                     # Wells Fargo-specific lenient settings
#                     tables_lenient = page.extract_tables(
#                         table_settings={
#                             "vertical_strategy": "lines_strict",
#                             "horizontal_strategy": "lines_strict", 
#                             "intersection_tolerance": 5,
#                             "join_tolerance": 5
#                         }
#                     )
#                     if tables_lenient:
#                         for tbl in tables_lenient:
#                             if tbl not in all_tables:
#                                 all_tables.append(tbl)
                                
#         except Exception as e:
#             print(f"Wells Fargo PDFPlumber extraction error: {e}")
#         return all_tables
    
#     def export_to_csv(self, transactions: List[Transaction], output_path: str):
#         """Export Wells Fargo transactions to CSV"""
#         data = []
#         for txn in transactions:
#             data.append({
#                 "Date": txn.date,
#                 "Check No": txn.check_number or "",
#                 "Description": txn.description,
#                 "Amount": txn.amount
#             })
        
#         df = pd.DataFrame(data)
#         df.to_csv(output_path, index=False)
#         print(f"Exported {len(data)} Wells Fargo transactions to {output_path}")
    
#     def _parse_date_for_sort(self, s: str) -> datetime:
#         """Robust date key"""
#         if not s:
#             return datetime.max
#         for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
#             try:
#                 return datetime.strptime(s.strip(), fmt)
#             except Exception:
#                 continue
#         return datetime.max

#     def _sort_txns(self, txns: List[Transaction]) -> List[Transaction]:
#         """Sort Wells Fargo transactions by date and type"""
#         priority = {"deposit": 0, "edi_payment": 1, "withdrawal": 2, "check": 3}
#         def key(t: Transaction):
#             ttype = (getattr(t, "transaction_type", "") or "").lower()
#             return (priority.get(ttype, 9), self._parse_date_for_sort(getattr(t, "date", "")), getattr(t, "description", ""))
#         return sorted(txns, key=key)

import pandas as pd
import tabula
import pdfplumber
from typing import List, Optional, Tuple
from datetime import datetime
import re
from ...interfaces.transaction import Transaction
from .wf_parser import WellsFargoParser

class WellsFargoProcessor:
    """Wells Fargo specific PDF processor with multi-row transaction support"""
    
    def __init__(self):
        self.parser = WellsFargoParser()
        self.bank_name = "wells_fargo"

    def extract_transactions(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
        """
        Wells Fargo specific transaction extraction
        """
        print(f"Processing Wells Fargo PDF: {pdf_path}")
        
        # Wells Fargo-specific table extraction
        tables = self.extract_tables_tabula_wf(pdf_path)
        
        # Debug info
        print(f"Extracted {len(tables)} tables from Wells Fargo PDF")
        for i, table in enumerate(tables):
            print(f"  Table {i}: {table.shape[0]} rows x {table.shape[1]} columns")

        # Process using Wells Fargo parser
        transactions = self.parser.process_tables(tables)
        
        # Simple sorting for Wells Fargo (no monthly summaries)
        transactions = self._sort_txns(transactions)

        print(f"Final result: {len(transactions)} Wells Fargo transactions")
        return self.bank_name, transactions
    
    def extract_tables_tabula_wf(self, pdf_path: str) -> List[pd.DataFrame]:
        """Wells Fargo optimized table extraction"""
        frames: List[pd.DataFrame] = []
        
        print("Extracting Wells Fargo tables...")

        # Wells Fargo Method 1: Lattice (structured tables)
        try:
            print("  Trying WF lattice extraction...")
            dfs_lattice = tabula.read_pdf(
                pdf_path, 
                pages="all", 
                multiple_tables=True,
                lattice=True, 
                guess=False,
                pandas_options={"header": None}
            )
            frames.extend(dfs_lattice)
            print(f"    Lattice found {len(dfs_lattice)} tables")
        except Exception as e:
            print(f"    WF lattice error: {e}")

        # Wells Fargo Method 2: Stream (for backup)
        try:
            print("  Trying WF stream extraction...")
            dfs_stream = tabula.read_pdf(
                pdf_path, 
                pages="all", 
                multiple_tables=True,
                lattice=False, 
                stream=True, 
                guess=True,
                pandas_options={"header": None}
            )
            
            # Only add stream results if lattice didn't work well
            if len(frames) < 3:
                frames.extend(dfs_stream)
                print(f"    Stream found {len(dfs_stream)} additional tables")
                
        except Exception as e:
            print(f"    WF stream error: {e}")

        # Fallback: pdfplumber
        if len(frames) < 2:
            print("  Trying pdfplumber fallback...")
            try:
                with pdfplumber.open(pdf_path) as pdf:
                    for page_num, page in enumerate(pdf.pages):
                        tables = page.extract_tables()
                        if tables:
                            for tbl in tables:
                                df = pd.DataFrame(tbl)
                                if not df.empty:
                                    frames.append(df)
                            print(f"    Page {page_num + 1}: found {len(tables)} tables")
            except Exception as e:
                print(f"    pdfplumber error: {e}")

        print(f"Total Wells Fargo tables extracted: {len(frames)}")
        return frames
    
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
        """Date parsing for sorting"""
        if not s:
            return datetime.max
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(s.strip(), fmt)
            except Exception:
                continue
        return datetime.max

    def _sort_txns(self, txns: List[Transaction]) -> List[Transaction]:
        """Sort Wells Fargo transactions"""
        priority = {"deposit": 0, "withdrawal": 1, "check": 2}
        def key(t: Transaction):
            ttype = (getattr(t, "transaction_type", "") or "").lower()
            return (priority.get(ttype, 9), self._parse_date_for_sort(getattr(t, "date", "")), getattr(t, "description", ""))
        return sorted(txns, key=key)