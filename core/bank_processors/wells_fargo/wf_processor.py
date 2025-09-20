import pandas as pd
import tabula
import pdfplumber
from typing import List, Optional, Tuple
from datetime import datetime
import re
from ...interfaces.transaction import Transaction
from .wf_parser import WellsFargoParser

class WellsFargoProcessor:
    """Wells Fargo specific PDF processor - exact implementation from test file"""
    
    def __init__(self):
        # Wells Fargo-specific parser
        self.parser = WellsFargoParser()
        self.bank_name = "wells_fargo"

    # def extract_transactions(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
    #     """
    #     Wells Fargo specific transaction extraction using exact test file logic
    #     Returns: (bank_name, list_of_transactions)
    #     """
    #     print(f"Processing Wells Fargo PDF: {pdf_path}")
        
    #     # Wells Fargo-specific table extraction using exact test file method
    #     tables = self._extract_tables_exact_test_method(pdf_path)
        
    #     # Process using Wells Fargo parser with test file logic
    #     transactions = self.parser.process_tables(tables)

    #     print(f"Extracted {len(transactions)} Wells Fargo transactions")
    #     return self.bank_name, transactions
    

    #it did not capture the end line sometimes

    # def _extract_tables_exact_test_method(self, pdf_path: str) -> List[pd.DataFrame]:
    #     """
    #     Extract tables using the exact method from test file
    #     """
    #     print("📄 Extracting Wells Fargo tables using test file method...")
        
    #     try:
    #         tables = tabula.read_pdf(
    #             pdf_path, 
    #             pages="all", 
    #             multiple_tables=True,
    #             pandas_options={"header": None}
    #         )
    #         print(f"Found {len(tables)} tables")
    #         return tables
    #     except Exception as e:
    #         print(f"❌ Error extracting tables: {e}")
    #         return []


    # def _extract_tables_exact_test_method(self, pdf_path: str) -> List[pd.DataFrame]:
    #     print("📄 Extracting Wells Fargo tables using test file method...")

    #     tables = []
    #     try:
    #         tables = tabula.read_pdf(
    #             pdf_path,
    #             pages="all",
    #             multiple_tables=True,
    #             pandas_options={"header": None}
    #         )
    #         print(f"Found {len(tables)} tables with Tabula")
    #     except Exception as e:
    #         print(f"❌ Error extracting tables: {e}")
    #         return []

    #     # --- SAFEGUARD: scan bottom of every page ---
    #     try:
    #         import pdfplumber, itertools, re

    #         with pdfplumber.open(pdf_path) as pdf:
    #             for page_num, page in enumerate(pdf.pages, start=1):
    #                 chars = [c for c in page.chars if c["top"] > page.height - 120]  # bottom 120px
    #                 if not chars:
    #                     continue

    #                 chars.sort(key=lambda c: (round(c["top"]), c["x0"]))
    #                 for y, group in itertools.groupby(chars, key=lambda c: round(c["top"])):
    #                     row_chars = list(group)
    #                     row_text = "".join(c["text"] for c in row_chars).strip()

    #                     if re.match(r"^\d{1,2}/\d{1,2}", row_text):
    #                         already_exists = any(row_text in str(df.values) for df in tables)
    #                         if not already_exists:
    #                             print(f"⚠️ Adding missed row on page {page_num}: {row_text}")

    #                             # Split into words based on spacing
    #                             words = []
    #                             current = row_chars[0]["text"]
    #                             last_x1 = row_chars[0]["x1"]
    #                             for ch in row_chars[1:]:
    #                                 if ch["x0"] - last_x1 > 3:  # gap = new word
    #                                     words.append(current.strip())
    #                                     current = ch["text"]
    #                                 else:
    #                                     current += ch["text"]
    #                                 last_x1 = ch["x1"]
    #                             words.append(current.strip())

    #                             # Pad/truncate to 6 columns (WF structure)
    #                             while len(words) < 6:
    #                                 words.append("")
    #                             words = words[:6]

    #                             # Append to the table for this page
    #                             if page_num <= len(tables):
    #                                 tables[page_num - 1].loc[len(tables[page_num - 1])] = words
    #                             else:
    #                                 tables.append(pd.DataFrame([words]))
    #     except Exception as e:
    #         print(f"⚠️ pdfplumber safeguard failed: {e}")

    #     return tables


    def _extract_tables_exact_test_method(self, pdf_path: str) -> List[pd.DataFrame]:
        print("📄 Extracting Wells Fargo tables using test file method...")

        tables = []
        try:
            tables = tabula.read_pdf(
                pdf_path,
                pages="all",
                multiple_tables=True,
                pandas_options={"header": None}
            )
            print(f"Found {len(tables)} tables with Tabula")
        except Exception as e:
            print(f"❌ Error extracting tables: {e}")
            return []

        # --- SAFEGUARD: scan bottom of every page ---
        try:
            import pdfplumber, itertools, re

            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, start=1):
                    chars = [c for c in page.chars if c["top"] > page.height - 120]  # bottom 120px
                    if not chars:
                        continue

                    chars.sort(key=lambda c: (round(c["top"]), c["x0"]))
                    for y, group in itertools.groupby(chars, key=lambda c: round(c["top"])):
                        row_chars = list(group)
                        row_text = "".join(c["text"] for c in row_chars).strip()

                        if re.match(r"^\d{1,2}/\d{1,2}", row_text):
                            already_exists = any(row_text in str(df.values) for df in tables)
                            if already_exists:
                                continue  # ✅ skip duplicates here

                            print(f"⚠️ Adding missed row on page {page_num}: {row_text}")

                            # Split into words based on spacing
                            words = []
                            current = row_chars[0]["text"]
                            last_x1 = row_chars[0]["x1"]
                            for ch in row_chars[1:]:
                                if ch["x0"] - last_x1 > 3:  # gap = new word
                                    words.append(current.strip())
                                    current = ch["text"]
                                else:
                                    current += ch["text"]
                                last_x1 = ch["x1"]
                            words.append(current.strip())

                            # Pad/truncate to 6 columns (WF structure)
                            while len(words) < 6:
                                words.append("")
                            words = words[:6]

                            # Append to the table for this page
                            if page_num <= len(tables):
                                tables[page_num - 1].loc[len(tables[page_num - 1])] = words
                            else:
                                tables.append(pd.DataFrame([words]))
        except Exception as e:
            print(f"⚠️ pdfplumber safeguard failed: {e}")

        # ✅ Deduplicate across all tables
        cleaned_tables = []
        seen = set()
        for df in tables:
            new_rows = []
            for row in df.itertuples(index=False, name=None):
                signature = tuple(str(x).strip() for x in row)
                if signature not in seen:
                    seen.add(signature)
                    new_rows.append(row)
            cleaned_tables.append(pd.DataFrame(new_rows))

        return cleaned_tables


    def extract_transactions(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
        """
        Wells Fargo specific transaction extraction using exact test file logic
        Returns: (bank_name, list_of_transactions)
        """
        print(f"Processing Wells Fargo PDF: {pdf_path}")
        
        # Wells Fargo-specific table extraction using exact test file method
        tables = self._extract_tables_exact_test_method(pdf_path)
        
        # Process using Wells Fargo parser with test file logic
        transactions = self.parser.process_tables(tables)

        # # ✅ Final safeguard: deduplicate after parsing
        # unique_txns = []
        # seen = set()
        # for txn in transactions:
        #     signature = (txn.date, txn.check_number, txn.description, txn.amount)
        #     if signature not in seen:
        #         seen.add(signature)
        #         unique_txns.append(txn)

        print(f"Extracted {len(transactions)} unique Wells Fargo transactions")
        return self.bank_name, transactions


    def export_to_csv(self, transactions: List[Transaction], output_path: str):
        """Export Wells Fargo transactions to CSV in exact test file format"""
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