import pandas as pd
import tabula
import pdfplumber
from typing import List, Optional, Tuple
from datetime import datetime
import re
from ...interfaces.transaction import Transaction
from .bofa_parser import BankOfAmericaParser

class BankOfAmericaProcessor:
    """Bank of America specific PDF processor - optimized for BoA statements"""
    
    def __init__(self):
        # BoA-specific parser only
        self.parser = BankOfAmericaParser()
        self.bank_name = "bank_of_america"

    def extract_transactions(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
        """
        Bank of America specific transaction extraction
        Returns: (bank_name, list_of_transactions)
        """
        print(f"Processing BoA PDF: {pdf_path}")
        
        # BoA-specific table extraction
        tables = self.extract_tables_tabula_boa(pdf_path)
        
        # Additional debugging for BoA small tables
        small_tables = [t for t in tables if t.shape[0] <= 5]
        if small_tables:
            print(f"Found {len(small_tables)} small BoA tables (≤5 rows)")
            for i, t in enumerate(small_tables):
                print(f"  Small BoA table {i}: {t.shape[0]}x{t.shape[1]} rows")

        # Process using BoA parser
        transactions = self.parser.process_tables(tables)
        
        # BoA-specific monthly summaries
        transactions = self._add_boa_monthly_summaries(transactions)

        print(f"Extracted {len(transactions)} BoA transactions")
        return self.bank_name, transactions
    
    def extract_tables_tabula_boa(self, pdf_path: str) -> List[pd.DataFrame]:
        """
        Bank of America optimized table extraction
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
                    print(f"  Collected BoA table from {method_name}: {df.shape[0]}x{df.shape[1]}")

        print("Extracting BoA tables with tabula-py (using advanced methods only)...")

        # BoA-specific extraction method
        try:
            print("  Using BoA-specific extraction...")
            try:
                dfs_area = tabula.read_pdf(
                    pdf_path, pages="all", multiple_tables=True,
                    lattice=False, stream=True, guess=False,
                    pandas_options={"header": None},
                    relative_area=True, area=(0, 0, 100, 100)
                )
                _collect(dfs_area, "BoA-specific")
            except TypeError:
                dfs_area_fb = tabula.read_pdf(
                    pdf_path, pages="all", multiple_tables=True,
                    lattice=False, stream=True, guess=False,
                    pandas_options={"header": None},
                )
                _collect(dfs_area_fb, "BoA-specific (fallback)")
            except Exception as e:
                print(f"  BoA-specific extraction error: {e}")
        except Exception as e:
            print(f"  BoA extraction error: {e}")

        # BoA-specific pdfplumber fallback
        if len(frames) < 3:
            print("  Very few BoA tables found, trying pdfplumber as fallback...")
            tables_plumber = self.extract_tables_pdfplumber_boa(pdf_path)
            for i, tbl in enumerate(tables_plumber):
                try:
                    df = pd.DataFrame(tbl)
                    if not df.empty:
                        frames.append(df)
                        print(f"  Collected BoA table from pdfplumber: {df.shape[0]}x{df.shape[1]}")
                except Exception:
                    continue

        print(f"Total BoA tables extracted: {len(frames)}")
        return frames

    def extract_tables_pdfplumber_boa(self, pdf_path: str) -> List[List[List]]:
        """BoA-specific pdfplumber extraction"""
        all_tables = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    tables = page.extract_tables()
                    if tables:
                        all_tables.extend(tables)
                        print(f"    BoA Page {page_num + 1}: found {len(tables)} tables")
                    
                    # BoA-specific lenient settings
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
            print(f"BoA PDFPlumber extraction error: {e}")
        return all_tables
    
    def export_to_csv(self, transactions: List[Transaction], output_path: str):
        """Export BoA transactions to CSV"""
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
        print(f"Exported {len(data)} BoA transactions to {output_path}")
    
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

    def _add_boa_monthly_summaries(self, transactions: List[Transaction]) -> List[Transaction]:
        """Add monthly deposit summaries for Bank of America with EDI payment structure"""
        from collections import defaultdict
        import calendar
        
        print("Adding BoA monthly summaries with EDI structure...")
        
        # DEBUG: Check what transaction types we have
        type_counts = {}
        for txn in transactions:
            ttype = txn.transaction_type
            type_counts[ttype] = type_counts.get(ttype, 0) + 1
        print(f"Transaction types before filtering: {type_counts}")
        
        # Group by month and calculate deposit totals
        monthly_groups = defaultdict(list)
        monthly_deposit_totals = defaultdict(float)
        
        for txn in transactions:
            try:
                date_parts = txn.date.split("/")
                month = int(date_parts[0])
                year = int(date_parts[2]) if len(date_parts) == 3 else 2024
                if year < 100:
                    year += 2000
                
                month_key = f"{year}-{month:02d}"
                monthly_groups[month_key].append(txn)
                
                # Sum ALL deposits (both regular deposits and EDI payments) for summary
                if txn.transaction_type in ["deposit", "edi_payment"] and txn.amount > 0:
                    monthly_deposit_totals[month_key] += txn.amount
                    
            except (ValueError, IndexError):
                monthly_groups["unknown"].append(txn)
        
        # Build final list with new structure
        final_transactions = []
        
        for month_key in sorted(monthly_groups.keys()):
            if month_key == "unknown":
                final_transactions.extend(monthly_groups[month_key])
                continue
            
            month_txns = monthly_groups[month_key]
            deposit_total = monthly_deposit_totals[month_key]
            
            print(f"Month {month_key}: ${deposit_total:.2f} total deposits, {len(month_txns)} transactions")
            
            # 1. Add deposit summary first (based on ALL deposits)
            if deposit_total > 0:
                year, month = month_key.split("-")
                year, month = int(year), int(month)
                last_day = calendar.monthrange(year, month)[1]
                
                summary = Transaction(
                    date=f"{month:02d}/{last_day:02d}/{year}",
                    description="Deposits",
                    amount=deposit_total,
                    check_number=None,
                    transaction_type="deposit_summary"
                )
                final_transactions.append(summary)
                print(f"Added summary: {summary.date} - ${deposit_total:.2f}")
            
            # 2. Filter and sort transactions for this month
            filtered_month_txns = []
            
            # DEBUG: Check transaction types in this month
            month_type_counts = {}
            for txn in month_txns:
                ttype = txn.transaction_type
                month_type_counts[ttype] = month_type_counts.get(ttype, 0) + 1
            print(f"  Month {month_key} transaction types: {month_type_counts}")
            
            for txn in month_txns:
                # Include: EDI payments, withdrawals, and checks
                # EXCLUDE: regular deposits (keep only EDI payments from deposits)
                if txn.transaction_type == "edi_payment":
                    filtered_month_txns.append(txn)
                    print(f"  Included EDI: {txn.description[:30]} - ${txn.amount}")
                elif txn.transaction_type == "withdrawal":
                    filtered_month_txns.append(txn)
                    print(f"  Included withdrawal: {txn.description[:30]} - ${txn.amount}")
                elif txn.transaction_type == "check":
                    filtered_month_txns.append(txn)
                    print(f"  Included check #{txn.check_number}: {txn.description[:30]} - ${txn.amount}")
                elif txn.transaction_type == "deposit":
                    # Skip regular deposits - they're summarized but not listed individually
                    print(f"  Filtered out regular deposit: {txn.description[:30]} - ${txn.amount}")
                else:
                    print(f"  Unknown type '{txn.transaction_type}': {txn.description[:30]} - ${txn.amount}")
            
            # 3. Sort: EDI payments first, then withdrawals, then checks
            def sort_key(t):
                priority = {"edi_payment": 0, "withdrawal": 1, "check": 2}
                return (priority.get(t.transaction_type, 9), t.date, t.description)
            
            sorted_filtered_txns = sorted(filtered_month_txns, key=sort_key)
            final_transactions.extend(sorted_filtered_txns)
            
            print(f"  Added {len(sorted_filtered_txns)} individual transactions")
        
        print(f"Final structure: {len(final_transactions)} transactions")
        return final_transactions