# import pandas as pd
# import tabula
# import pdfplumber
# import re
# from typing import List, Dict, Optional, Tuple
# from dataclasses import dataclass
# from datetime import datetime

# @dataclass
# class Transaction:
#     date: str
#     description: str
#     amount: float
#     check_number: Optional[str] = None
#     balance: Optional[float] = None
#     transaction_type: str = "unknown"  # deposit, withdrawal, check

# class BankStatementProcessor:
#     def __init__(self):
#         self.supported_banks = ["wells_fargo", "bank_of_america", "unknown"]
    
#     def detect_bank_type(self, pdf_path: str) -> str:
#         """Detect bank type from PDF content"""
#         try:
#             with pdfplumber.open(pdf_path) as pdf:
#                 first_page_text = pdf.pages[0].extract_text().upper()
                
#                 if "WELLS FARGO" in first_page_text:
#                     return "wells_fargo"
#                 elif "BANK OF AMERICA" in first_page_text:
#                     return "bank_of_america"
#                 else:
#                     return "unknown"
#         except Exception as e:
#             print(f"Error detecting bank type: {e}")
#             return "unknown"
    
#     def extract_tables_tabula(self, pdf_path: str, bank_type: str) -> List[pd.DataFrame]:
#         """Extract tables using tabula-py"""
#         try:
#             if bank_type == "wells_fargo":
#                 # Wells Fargo specific extraction
#                 tables = tabula.read_pdf(
#                     pdf_path, 
#                     pages='all',
#                     multiple_tables=True,
#                     pandas_options={'header': None}
#                 )
#             elif bank_type == "bank_of_america":
#                 # Bank of America specific extraction
#                 tables = tabula.read_pdf(
#                     pdf_path,
#                     pages='all',
#                     multiple_tables=True,
#                     pandas_options={'header': None}
#                 )
#             else:
#                 # Generic extraction
#                 tables = tabula.read_pdf(
#                     pdf_path,
#                     pages='all',
#                     multiple_tables=True
#                 )
            
#             return tables
#         except Exception as e:
#             print(f"Tabula extraction error: {e}")
#             return []
    
#     def extract_tables_pdfplumber(self, pdf_path: str) -> List[List[List]]:
#         """Backup extraction using pdfplumber"""
#         all_tables = []
#         try:
#             with pdfplumber.open(pdf_path) as pdf:
#                 for page in pdf.pages:
#                     tables = page.extract_tables()
#                     if tables:
#                         all_tables.extend(tables)
#             return all_tables
#         except Exception as e:
#             print(f"PDFPlumber extraction error: {e}")
#             return []
    
#     def process_wells_fargo(self, tables: List[pd.DataFrame]) -> List[Transaction]:
#         """Process Wells Fargo specific format"""
#         transactions = []
        
#         for df in tables:
#             if df.empty:
#                 continue
                
#             # Look for transaction-like patterns
#             for index, row in df.iterrows():
#                 row_str = ' '.join(str(cell) for cell in row if pd.notna(cell))
                
#                 # Check if row contains date pattern (MM/DD format)
#                 date_pattern = r'\d{1,2}/\d{1,2}'
#                 if re.search(date_pattern, row_str):
#                     transaction = self._parse_wells_fargo_row(row)
#                     if transaction:
#                         transactions.append(transaction)
        
#         return transactions
    
#     # def process_bank_of_america(self, tables: List[pd.DataFrame]) -> List[Transaction]:
#     #     """Process Bank of America specific format"""
#     #     transactions = []
        
#     #     for df in tables:
#     #         if df.empty:
#     #             continue
                
#     #         for index, row in df.iterrows():
#     #             row_str = ' '.join(str(cell) for cell in row if pd.notna(cell))
                
#     #             # BofA date pattern (MM/DD/YY)
#     #             date_pattern = r'\d{2}/\d{2}/\d{2}'
#     #             if re.search(date_pattern, row_str):
#     #                 transaction = self._parse_bofa_row(row)
#     #                 if transaction:
#     #                     transactions.append(transaction)
        
#     #     return transactions
#     def process_bank_of_america(self, tables: List[pd.DataFrame]) -> List[Transaction]:
#         """Enhanced Bank of America processing - handles all transaction types"""
#         transactions = []
        
#         print(f"Processing {len(tables)} tables for Bank of America...")
        
#         for table_idx, df in enumerate(tables):
#             if df.empty:
#                 continue
            
#             print(f"Table {table_idx}: {df.shape[0]} rows, {df.shape[1]} columns")
            
#             # Process each row in the table
#             for row_idx, row in df.iterrows():
#                 # Convert row to string for analysis
#                 row_str = ' '.join(str(cell) if pd.notna(cell) else '' for cell in row)
                
#                 # Skip empty or header rows
#                 if len(row_str.strip()) < 10:
#                     continue
                    
#                 # Look for different transaction patterns
#                 transaction = None
                
#                 # Pattern 1: Standard date MM/DD/YY format
#                 if re.search(r'\d{2}/\d{2}/\d{2,4}', row_str):
#                     transaction = self._parse_bofa_row_enhanced(row, row_str)
                
#                 # Pattern 2: Withdrawals/Debits section (often different format)
#                 elif any(keyword in row_str.upper() for keyword in [
#                     'CHECKCARD', 'PURCHASE', 'MERCHANT SRVS', 'DEPT REVENUE', 
#                     'VGP DISTRIBUTORS', 'CPI SECURITY', 'OVERDRAFT'
#                 ]):
#                     transaction = self._parse_bofa_withdrawal(row, row_str)
                
#                 # Pattern 3: Check transactions
#                 elif 'CHECK' in row_str.upper() and re.search(r'\d{3,4}', row_str):
#                     transaction = self._parse_bofa_check(row, row_str)
                
#                 if transaction:
#                     transactions.append(transaction)
#                     print(f"  Found: {transaction.date} | {transaction.description[:30]} | ${transaction.amount}")
        
#         return transactions

#     def _parse_wells_fargo_row(self, row) -> Optional[Transaction]:
#         """Parse individual Wells Fargo transaction row"""
#         try:
#             row_list = [str(cell) if pd.notna(cell) else '' for cell in row]
#             row_text = ' '.join(row_list)
            
#             # Extract date
#             date_match = re.search(r'(\d{1,2}/\d{1,2})', row_text)
#             if not date_match:
#                 return None
            
#             date_str = date_match.group(1)
            
#             # Extract amounts (look for dollar amounts)
#             amount_pattern = r'[\d,]+\.\d{2}'
#             amounts = re.findall(amount_pattern, row_text)
            
#             if not amounts:
#                 return None
            
#             # Determine transaction type and amount
#             amount = 0.0
#             transaction_type = "unknown"
            
#             # Check for check number
#             check_match = re.search(r'\b(\d{4})\s+Check\b', row_text)
#             check_number = check_match.group(1) if check_match else None
            
#             # Parse amount (last amount found is usually the transaction amount)
#             try:
#                 amount_str = amounts[-2] if len(amounts) >= 2 else amounts[0]
#                 amount = float(amount_str.replace(',', ''))
                
#                 # Determine if it's a debit or credit based on context
#                 if 'Check' in row_text or 'Purchase' in row_text or 'ACH Debit' in row_text:
#                     amount = -amount
#                     transaction_type = "withdrawal"
#                 elif 'Dep' in row_text or 'EDI' in row_text or 'Payment' in row_text:
#                     transaction_type = "deposit"
                    
#             except (ValueError, IndexError):
#                 return None
            
#             # Extract description
#             description = self._clean_description(row_text)
            
#             return Transaction(
#                 date=date_str,
#                 description=description,
#                 amount=amount,
#                 check_number=check_number,
#                 transaction_type=transaction_type
#             )
            
#         except Exception as e:
#             print(f"Error parsing Wells Fargo row: {e}")
#             return None
    
#     def _parse_bofa_row(self, row) -> Optional[Transaction]:
#         """Parse individual Bank of America transaction row"""
#         try:
#             row_list = [str(cell) if pd.notna(cell) else '' for cell in row]
#             row_text = ' '.join(row_list)
            
#             # Extract date
#             date_match = re.search(r'(\d{2}/\d{2}/\d{2})', row_text)
#             if not date_match:
#                 return None
            
#             date_str = date_match.group(1)
            
#             # Extract amount (negative or positive)
#             amount_pattern = r'-?[\d,]+\.\d{2}'
#             amount_match = re.search(amount_pattern, row_text)
            
#             if not amount_match:
#                 return None
            
#             amount = float(amount_match.group().replace(',', ''))
#             transaction_type = "deposit" if amount > 0 else "withdrawal"
            
#             # Extract check number if present
#             check_match = re.search(r'\b(\d{3,4})\b', row_text)
#             check_number = check_match.group(1) if check_match else None
            
#             description = self._clean_description(row_text)
            
#             return Transaction(
#                 date=date_str,
#                 description=description,
#                 amount=amount,
#                 check_number=check_number,
#                 transaction_type=transaction_type
#             )
            
#         except Exception as e:
#             print(f"Error parsing BofA row: {e}")
#             return None

#     def _parse_bofa_row_enhanced(self, row, row_text: str) -> Optional[Transaction]:
#         """Enhanced BofA row parsing for standard transactions"""
#         try:
#             # Extract date (MM/DD/YY or MM/DD/YYYY)
#             date_match = re.search(r'(\d{2}/\d{2}/\d{2,4})', row_text)
#             if not date_match:
#                 return None
            
#             date_str = date_match.group(1)
            
#             # Look for amount patterns - both positive and negative
#             amount_patterns = [
#                 r'-[\d,]+\.\d{2}',  # Negative amounts
#                 r'[\d,]+\.\d{2}',   # Positive amounts
#             ]
            
#             amounts = []
#             for pattern in amount_patterns:
#                 matches = re.findall(pattern, row_text)
#                 amounts.extend(matches)
            
#             if not amounts:
#                 return None
            
#             # Get the transaction amount (usually the last amount that's not a balance)
#             amount = 0.0
#             for amt_str in amounts:
#                 try:
#                     amt_val = float(amt_str.replace(',', ''))
#                     # Skip very large numbers that are likely account numbers
#                     if abs(amt_val) < 100000:
#                         amount = amt_val
#                 except ValueError:
#                     continue
            
#             if amount == 0:
#                 return None
            
#             # Determine transaction type
#             transaction_type = "deposit" if amount > 0 else "withdrawal"
            
#             # Extract check number if present
#             check_number = self._extract_check_number(row_text)
            
#             # Clean description
#             description = self._clean_description(row_text)
            
#             return Transaction(
#                 date=date_str,
#                 description=description,
#                 amount=amount,
#                 check_number=check_number,
#                 transaction_type=transaction_type
#             )
            
#         except Exception as e:
#             print(f"Error parsing enhanced BofA row: {e}")
#             return None

#     def _parse_bofa_withdrawal(self, row, row_text: str) -> Optional[Transaction]:
#         """Parse BofA withdrawal/debit transactions"""
#         try:
#             # Extract date from the row text
#             date_match = re.search(r'(\d{2}/\d{2}/\d{2,4})', row_text)
#             if not date_match:
#                 # Sometimes date is in previous column, try to find any date pattern
#                 date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', row_text)
            
#             if not date_match:
#                 return None
                
#             date_str = date_match.group(1)
            
#             # Look for negative amounts or amounts in withdrawal context
#             amount_match = re.search(r'-?[\d,]+\.\d{2}', row_text)
#             if not amount_match:
#                 return None
            
#             amount = float(amount_match.group().replace(',', ''))
            
#             # Ensure it's negative (withdrawal)
#             if amount > 0:
#                 amount = -amount
            
#             # Extract description
#             description = self._clean_description(row_text)
            
#             # Look for check number
#             check_number = self._extract_check_number(row_text)
            
#             return Transaction(
#                 date=date_str,
#                 description=description,
#                 amount=amount,
#                 check_number=check_number,
#                 transaction_type="withdrawal"
#             )
            
#         except Exception as e:
#             print(f"Error parsing BofA withdrawal: {e}")
#             return None

#     def _parse_bofa_check(self, row, row_text: str) -> Optional[Transaction]:
#         """Parse BofA check transactions"""
#         try:
#             # Extract date
#             date_match = re.search(r'(\d{2}/\d{2}/\d{2,4})', row_text)
#             if not date_match:
#                 return None
                
#             date_str = date_match.group(1)
            
#             # Extract check number
#             check_match = re.search(r'(\d{3,4})', row_text)
#             check_number = check_match.group(1) if check_match else None
            
#             # Extract amount
#             amount_match = re.search(r'-?[\d,]+\.\d{2}', row_text)
#             if not amount_match:
#                 return None
            
#             amount = float(amount_match.group().replace(',', ''))
            
#             # Checks are typically negative
#             if amount > 0:
#                 amount = -amount
            
#             description = "Check" if not self._clean_description(row_text) else self._clean_description(row_text)
            
#             return Transaction(
#                 date=date_str,
#                 description=description,
#                 amount=amount,
#                 check_number=check_number,
#                 transaction_type="check"
#             )
            
#         except Exception as e:
#             print(f"Error parsing BofA check: {e}")
#             return None

#     def _extract_check_number(self, text: str) -> Optional[str]:
#         """Extract check number from transaction text"""
#         # Look for check number patterns
#         patterns = [
#             r'Check\s+#?\s*(\d{3,4})',
#             r'(\d{3,4})\s*-?\s*[\d,]+\.\d{2}',
#             r'Check\s*(\d{3,4})',
#         ]
        
#         for pattern in patterns:
#             match = re.search(pattern, text, re.IGNORECASE)
#             if match:
#                 return match.group(1)
        
#         return None
#     # def export_to_csv(self, transactions: List[Transaction], output_path: str, bank_type: str):
#     #     """Export transactions to CSV in standardized format"""
#     #     data = []
        
#     #     for txn in transactions:
#     #         # Add month/year if missing from date
#     #         date_clean = self._standardize_date(txn.date)
            
#     #         data.append({
#     #             'Date': date_clean,
#     #             'Check No': txn.check_number if txn.check_number else '',
#     #             'Description': txn.description,
#     #             'Amount': txn.amount
#     #         })
        
#     #     df = pd.DataFrame(data)
#     #     df.to_csv(output_path, index=False)
#     #     print(f"Exported {len(data)} transactions to {output_path}")

#     def _standardize_date(self, date_str: str) -> str:
#         """Convert dates to MM/DD/YYYY format"""
#         try:
#             # Handle different date formats
#             if '/' in date_str and len(date_str.split('/')) == 2:
#                 # Add current year if missing
#                 return f"{date_str}/2024"
#             elif '/' in date_str and len(date_str.split('/')) == 3:
#                 parts = date_str.split('/')
#                 if len(parts[2]) == 2:  # YY format
#                     return f"{parts[0]}/{parts[1]}/20{parts[2]}"
#             return date_str
#         except:
#             return date_str
    
#     def extract_transactions(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
#         """Main method to extract transactions from PDF"""
#         print(f"Processing: {pdf_path}")
        
#         # Detect bank type
#         bank_type = self.detect_bank_type(pdf_path)
#         print(f"Detected bank: {bank_type}")
        
#         # Extract tables
#         tables = self.extract_tables_tabula(pdf_path, bank_type)
#         print(f"Found {len(tables)} tables")
        
#         # Process based on bank type
#         if bank_type == "wells_fargo":
#             transactions = self.process_wells_fargo(tables)
#         elif bank_type == "bank_of_america":
#             transactions = self.process_bank_of_america(tables)
#         else:
#             # Generic processing
#             transactions = self.process_wells_fargo(tables)  # Default to Wells Fargo logic
        
#         print(f"Extracted {len(transactions)} transactions")
#         return bank_type, transactions
    

#     # Add this method to BankStatementProcessor class

#     # def _clean_description(self, text: str) -> str:
#     #     """Enhanced description cleaning"""
#     #     # Remove extra whitespace
#     #     cleaned = re.sub(r'\s+', ' ', text).strip()
        
#     #     # Remove date patterns
#     #     cleaned = re.sub(r'\d{1,2}/\d{1,2}(/\d{2,4})?', '', cleaned)
        
#     #     # Remove amount patterns
#     #     cleaned = re.sub(r'-?[\d,]+\.\d{2}', '', cleaned)
        
#     #     # Remove common bank codes
#     #     cleaned = re.sub(r'(CKCD|CCD|PPD)\s*\d*', '', cleaned)
#     #     cleaned = re.sub(r'Card\s+\d{4}', '', cleaned)
#     #     cleaned = re.sub(r'XXXXXXXXXXXX\d+', '', cleaned)
        
#     #     # Clean up specific patterns
#     #     cleaned = re.sub(r'DES:[A-Z\s]*', '', cleaned)
#     #     cleaned = re.sub(r'ID:\d+', '', cleaned)
#     #     cleaned = re.sub(r'INDN:[A-Z\s]*', '', cleaned)
        
#     #     # Remove extra spaces and limit length
#     #     cleaned = re.sub(r'\s+', ' ', cleaned).strip()
#     #     return cleaned[:60]  # Reasonable description length

#     def _clean_description(self, text: str) -> str:
#         """Enhanced description cleaning for Bank of America"""
#         # Remove extra whitespace
#         cleaned = re.sub(r'\s+', ' ', text).strip()
        
#         # Remove date patterns
#         cleaned = re.sub(r'\d{1,2}/\d{1,2}(/\d{2,4})?', '', cleaned)
        
#         # Remove amount patterns
#         cleaned = re.sub(r'-?[\d,]+\.\d{2}', '', cleaned)
        
#         # Remove common bank codes
#         cleaned = re.sub(r'(CKCD|CCD|PPD)\s*\d*', '', cleaned)
#         cleaned = re.sub(r'Card\s+\d{4}', '', cleaned)
#         cleaned = re.sub(r'XXXXXXXXXXXX\d+', '', cleaned)
        
#         # Clean up specific patterns for BofA
#         cleaned = re.sub(r'DES:[A-Z\s]*', '', cleaned)
#         cleaned = re.sub(r'ID:\d+', '', cleaned)
#         cleaned = re.sub(r'INDN:[A-Z\s]*', '', cleaned)
        
#         # Remove check number patterns from description
#         cleaned = re.sub(r'Check\s*#?\s*\d{3,4}', 'Check', cleaned, flags=re.IGNORECASE)
        
#         # Remove extra spaces and limit length
#         cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
#         # If description is too short or empty, provide a default
#         if len(cleaned) < 5:
#             if 'CHECKCARD' in text.upper():
#                 cleaned = 'Debit Card Purchase'
#             elif 'CHECK' in text.upper():
#                 cleaned = 'Check'
#             elif 'MERCHANT' in text.upper():
#                 cleaned = 'Merchant Services'
#             else:
#                 cleaned = 'Transaction'
        
#         return cleaned[:60]

#     def export_to_csv(self, transactions: List[Transaction], output_path: str, bank_type: str):
#         """Export transactions to CSV in standardized format"""
#         data = []
        
#         for txn in transactions:
#             # Add month/year if missing from date
#             date_clean = self._standardize_date(txn.date)
            
#             data.append({
#                 'Date': date_clean,
#                 'Check No': txn.check_number if txn.check_number else '',
#                 'Description': txn.description,
#                 'Amount': txn.amount
#             })
        
#         df = pd.DataFrame(data)
#         df.to_csv(output_path, index=False)
#         print(f"Exported {len(data)} transactions to {output_path}")

#     def _standardize_date(self, date_str: str) -> str:
#         """Convert dates to MM/DD/YYYY format"""
#         try:
#             # Handle different date formats
#             if '/' in date_str and len(date_str.split('/')) == 2:
#                 # Add current year if missing
#                 return f"{date_str}/2024"
#             elif '/' in date_str and len(date_str.split('/')) == 3:
#                 parts = date_str.split('/')
#                 if len(parts[2]) == 2:  # YY format
#                     return f"{parts[0]}/{parts[1]}/20{parts[2]}"
#             return date_str
#         except:
#             return date_str


import pandas as pd
import tabula
import pdfplumber
import re
from typing import List, Optional, Tuple
from dataclasses import dataclass

@dataclass
class Transaction:
    date: str
    description: str
    amount: float
    check_number: Optional[str] = None
    balance: Optional[float] = None
    transaction_type: str = "unknown"  # deposit, withdrawal, check

class BankStatementProcessor:
    def __init__(self):
        self.supported_banks = ["wells_fargo", "bank_of_america", "unknown"]

        # Strong withdrawal/debit indicators for BofA rows
        self.bofa_withdrawal_keywords = [
            "CHECKCARD", "PURCHASE", "MERCHANT SRVS", "DEPT REVENUE",
            "VGP DISTRIBUTORS", "CPI SECURITY", "OVERDRAFT", "ADP PAYROLL FEES",
            "COMPORIUM", "DEMAND VAPE", "ALPHA BRANDS", "MOREB WHOLESALE",
            "MIDWEST GOODS", "SNS IMPORTS", "CASH APP", "AMAZON", "AMZN", "DUKE-ENERGY"
        ]

    # --------------------- Detection ---------------------
    def detect_bank_type(self, pdf_path: str) -> str:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                first_page_text = (pdf.pages[0].extract_text() or "").upper()
            if "WELLS FARGO" in first_page_text:
                return "wells_fargo"
            if "BANK OF AMERICA" in first_page_text or "BANKOFAMERICA.COM" in first_page_text:
                return "bank_of_america"
        except Exception as e:
            print(f"Error detecting bank type: {e}")
        return "unknown"

    # --------------------- Extraction ---------------------
    def extract_tables_tabula(self, pdf_path: str, bank_type: str) -> List[pd.DataFrame]:
        """
        Try both lattice (ruled tables) and stream (whitespace tables) because BofA
        often renders different sections in different ways. Deduplicate results.
        """
        frames: List[pd.DataFrame] = []
        seen_signatures = set()

        def _collect(dfs: List[pd.DataFrame]):
            for df in dfs or []:
                # Normalize DataFrame to string signatures for deduping
                sig = (df.shape[0], df.shape[1], tuple(df.head(2).fillna("").astype(str).agg("|".join, axis=1)))
                if sig not in seen_signatures and not df.empty:
                    seen_signatures.add(sig)
                    frames.append(df)

        try:
            # First pass: lattice
            dfs_lat = tabula.read_pdf(
                pdf_path, pages="all", multiple_tables=True, lattice=True, guess=False,
                pandas_options={"header": None}
            )
            _collect(dfs_lat)
        except Exception as e:
            print(f"Tabula lattice error: {e}")

        try:
            # Second pass: stream
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

    # --------------------- Wells Fargo ---------------------
    def process_wells_fargo(self, tables: List[pd.DataFrame]) -> List[Transaction]:
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
                    t = self._parse_wells_fargo_row(row_text)
                    if t:
                        transactions.append(t)
        return transactions

    def _parse_wells_fargo_row(self, row_text: str) -> Optional[Transaction]:
        try:
            date_match = re.search(r"\b(\d{1,2}/\d{1,2})\b", row_text)
            if not date_match:
                return None
            date_str = date_match.group(1)

            # Grab last plausible currency figure in the row (often txn amount)
            amount = self._pick_amount(row_text)
            if amount is None:
                return None

            txn_type = "deposit" if amount > 0 else "withdrawal"

            # Check number (WF “Check” wording appears; also allow bare 4 digits near 'Check')
            check_number = self._extract_check_number(row_text)
            if check_number:
                txn_type = "check"
                if amount > 0:
                    amount = -abs(amount)  # checks should be debits

            desc = self._clean_description(row_text)
            return Transaction(
                date=self._standardize_date(date_str),
                description=desc,
                amount=amount,
                check_number=check_number,
                transaction_type=txn_type
            )
        except Exception as e:
            print(f"Error parsing Wells Fargo row: {e}")
            return None

    # # --------------------- Bank of America ---------------------

    # def process_bank_of_america(self, tables: List[pd.DataFrame]) -> List[Transaction]:
    #     transactions: List[Transaction] = []
    #     print(f"Processing {len(tables)} tables for Bank of America...")

    #     for t_idx, df in enumerate(tables):
    #         if df.empty:
    #             continue
    #         print(f"Table {t_idx}: {df.shape[0]} rows, {df.shape[1]} cols")

    #         for _, row in df.iterrows():
    #             row_text = self._row_to_text(row)
    #             if len(row_text) < 10:
    #                 continue

    #             # Split two-on-a-line "Checks" rows
    #             multi_checks = self._parse_bofa_checks_row_multi(row_text)
    #             if multi_checks:
    #                 transactions.extend(multi_checks)
    #                 continue

    #             # For other rows: require date and amount
    #             date_str = self._extract_date_any(row_text)
    #             if not date_str:
    #                 continue
    #             amount = self._pick_amount(row_text)
    #             if amount is None:
    #                 continue

    #             upper = row_text.upper()

    #             if "CHECK" in upper or re.search(r"\bCHK\b", upper):
    #                 # Only here do we compute check_number
    #                 check_number = self._extract_check_number(row_text)
    #                 amount = -abs(amount)  # checks are debits
    #                 txn_type = "check"
    #                 description = self._clean_description(row_text)
    #                 transactions.append(Transaction(
    #                     date=self._standardize_date(date_str),
    #                     description=description if description else "Check",
    #                     amount=amount,
    #                     check_number=check_number,
    #                     transaction_type=txn_type,
    #                 ))
    #                 continue

    #             # Withdrawals vs deposits (NO check numbers here)
    #             if any(k in upper for k in self.bofa_withdrawal_keywords) or amount < 0:
    #                 txn_type = "withdrawal"
    #                 amount = -abs(amount)
    #             else:
    #                 txn_type = "deposit"
    #                 amount = abs(amount)

    #             description = self._clean_description(row_text)
    #             transactions.append(Transaction(
    #                 date=self._standardize_date(date_str),
    #                 description=description,
    #                 amount=amount,
    #                 check_number=None,  # <- ensured blank
    #                 transaction_type=txn_type,
    #             ))

    #     return transactions
    # def process_bank_of_america(self, tables: List[pd.DataFrame]) -> List[Transaction]:
    #     """
    #     Parse Bank of America statements:
    #     • Deposits/credits
    #     • Withdrawals/debits (CHECKCARD, PURCHASE, ACH, etc.)
    #     • Checks (handles side-by-side 'Checks' section via multi-triplet split)

    #     Guarantees:
    #     • Daily ledger balances are skipped (table + row level).
    #     • Only real Checks get a check_number (not CHECKCARD).
    #     """
    #     transactions: List[Transaction] = []
    #     print(f"Processing {len(tables)} tables for Bank of America...")

    #     # --- precompiled patterns (fast) ---
    #     date_pat = re.compile(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b")
    #     money_pat = re.compile(r"[-+]?\$?\d[\d,]*\.\d{2}")
    #     check_word_pat = re.compile(r"\bCHECK(?!CARD)\b", re.IGNORECASE)
    #     checknum_pat = re.compile(r"\b\d{3,5}\*?\b")
    #     # A row that is ONLY (date amount) pairs, repeated ≥2 times (pure ledger line)
    #     # e.g. "01/01 2,766.86   01/11 6,169.29   01/23 4,094.24"
    #     ledger_row_full_pat = re.compile(
    #         r"^(?:\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?\s+[-+]?\$?\d[\d,]*\.\d{2}\s*){2,}$"
    #     )

    #     def is_ledger_header(text: str) -> bool:
    #         u = text.upper()
    #         return "DAILY LEDGER BALANCES" in u

    #     def row_is_pure_ledger(text: str) -> bool:
    #         """
    #         True ONLY for the ledger section rows (no words, just repeated date+amount).
    #         Never matches Checks (which have a check # between date and amount),
    #         or normal transactions (which include words).
    #         """
    #         t = re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", text)  # (1,234.56) -> -1,234.56
    #         up = t.upper()
    #         # if any of these appear, it's not ledger
    #         if "CHECKCARD" in up or "PURCHASE" in up:
    #             return False
    #         if check_word_pat.search(up) or checknum_pat.search(t):
    #             return False
    #         # pure (date amount){2,}
    #         return ledger_row_full_pat.match(t.strip()) is not None

    #     for t_idx, df in enumerate(tables):
    #         if df.empty:
    #             continue

    #         # ---- strict table-level skip for ledger balances ----
    #         probe_text = " ".join(self._row_to_text(r) for _, r in df.head(6).iterrows())
    #         if is_ledger_header(probe_text):
    #             print(f"Skipping ledger-balance table at index {t_idx} (header detected)")
    #             continue

    #         # If ≥70% of first 10 non-empty rows are pure ledger lines, skip table
    #         non_empty = 0
    #         ledgerish = 0
    #         for _, r in df.head(10).iterrows():
    #             rt = self._row_to_text(r)
    #             if len(rt.strip()) < 6:
    #                 continue
    #             non_empty += 1
    #             if row_is_pure_ledger(rt):
    #                 ledgerish += 1
    #         if non_empty >= 6 and ledgerish / max(non_empty, 1) >= 0.7:
    #             print(f"Skipping ledger-balance table at index {t_idx} (row-shape heuristic)")
    #             continue
    #         # -----------------------------------------------------

    #         print(f"Table {t_idx}: {df.shape[0]} rows, {df.shape[1]} cols")

    #         for _, row in df.iterrows():
    #             row_text = self._row_to_text(row)
    #             if len(row_text.strip()) < 10:
    #                 continue

    #             # Row-level guard (very strict; won't drop Checks)
    #             if row_is_pure_ledger(row_text):
    #                 continue

    #             # 1) Split two-on-a-line "Checks" rows into individual transactions
    #             multi_checks = self._parse_bofa_checks_row_multi(row_text)
    #             if multi_checks:
    #                 transactions.extend(multi_checks)
    #                 continue

    #             # 2) For everything else we need a date and an amount
    #             date_str = self._extract_date_any(row_text)
    #             if not date_str:
    #                 continue
    #             amount = self._pick_amount(row_text)
    #             if amount is None:
    #                 continue

    #             upper = row_text.upper()

    #             # 3) Force CHECKCARD/PURCHASE into withdrawals (no check numbers)
    #             if "CHECKCARD" in upper or "PURCHASE" in upper:
    #                 transactions.append(
    #                     Transaction(
    #                         date=self._standardize_date(date_str),
    #                         description=self._clean_description(row_text),
    #                         amount=-abs(amount),
    #                         check_number=None,
    #                         transaction_type="withdrawal",
    #                     )
    #                 )
    #                 continue

    #             # 4) Strict check detection (exclude CHECKCARD) and require a check number present
    #             is_check_line = (check_word_pat.search(upper) is not None) and (checknum_pat.search(row_text) is not None)
    #             if is_check_line:
    #                 check_number = self._extract_check_number(row_text)
    #                 transactions.append(
    #                     Transaction(
    #                         date=self._standardize_date(date_str),
    #                         description=self._clean_description(row_text) or "Check",
    #                         amount=-abs(amount),  # checks are debits
    #                         check_number=check_number,
    #                         transaction_type="check",
    #                     )
    #                 )
    #                 continue

    #             # 5) Everything else: classify by keywords/sign; NEVER set check_number
    #             if any(k in upper for k in self.bofa_withdrawal_keywords) or amount < 0:
    #                 txn_type = "withdrawal"
    #                 amount = -abs(amount)
    #             else:
    #                 txn_type = "deposit"
    #                 amount = abs(amount)

    #             transactions.append(
    #                 Transaction(
    #                     date=self._standardize_date(date_str),
    #                     description=self._clean_description(row_text),
    #                     amount=amount,
    #                     check_number=None,  # ensure blank for non-checks
    #                     transaction_type=txn_type,
    #                 )
    #             )

    #     return transactions

    def process_bank_of_america(self, tables: List[pd.DataFrame]) -> List[Transaction]:
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

        # Precompiled patterns
        date_pat = re.compile(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b")
        money_pat = re.compile(r"[-+]?\$?\d[\d,]*\.\d{2}")
        check_word_pat = re.compile(r"\bCHECK(?!CARD)\b", re.IGNORECASE)
        checknum_pat = re.compile(r"\b\d{3,5}\*?\b")
        # A row made ONLY of repeated (date amount) pairs (≥2) → pure ledger
        ledger_row_full_pat = re.compile(
            r"^(?:\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?\s+[-+]?\$?\d[\d,]*\.\d{2}\s*){2,}$"
        )

        def norm(s: str) -> str:
            return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

        def is_ledger_header(text: str) -> bool:
            return "DAILY LEDGER BALANCES" in text.upper()

        def row_is_pure_ledger(text: str) -> bool:
            """
            True ONLY for the ledger section rows:
            - no words (just repeated date+amount pairs)
            - never matches checks or normal transactions
            """
            t = norm(re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", text))  # (1,234.56) -> -1,234.56
            up = t.upper()
            # Not ledger if any of these appear
            if "CHECKCARD" in up or "PURCHASE" in up:
                return False
            if check_word_pat.search(up) or checknum_pat.search(t):
                return False
            # Pure (date amount){2,}
            return ledger_row_full_pat.match(t) is not None

        for t_idx, df in enumerate(tables):
            if df.empty:
                continue

            # --- Table-level skip: header says 'Daily ledger balances'
            probe_text = " ".join(norm(self._row_to_text(r)) for _, r in df.head(6).iterrows())
            if is_ledger_header(probe_text):
                print(f"Skipping ledger-balance table at index {t_idx} (header detected)")
                continue

            # --- Secondary table heuristic: mostly pure-ledger rows → skip
            non_empty = 0
            ledgerish = 0
            for _, r in df.head(10).iterrows():
                rt = norm(self._row_to_text(r))
                if len(rt) < 6:
                    continue
                non_empty += 1
                if row_is_pure_ledger(rt):
                    ledgerish += 1
            if non_empty >= 6 and ledgerish / max(non_empty, 1) >= 0.7:
                print(f"Skipping ledger-balance table at index {t_idx} (row-shape heuristic)")
                continue

            print(f"Table {t_idx}: {df.shape[0]} rows, {df.shape[1]} cols")

            for _, row in df.iterrows():
                row_text = norm(self._row_to_text(row))
                if len(row_text) < 10:
                    continue

                # Row-level guard (very strict; won't drop Checks)
                if row_is_pure_ledger(row_text):
                    continue

                # 1) Split two-on-a-line "Checks" rows into individual transactions
                multi_checks = self._parse_bofa_checks_row_multi(row_text)
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
                is_check_line = (check_word_pat.search(upper) is not None) and (checknum_pat.search(row_text) is not None)
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
                # This filters out numeric-only lines that might slip through.
                alpha_count = len(re.findall(r"[A-Za-z]", row_text))
                if alpha_count < 4:  # tweak threshold if needed
                    continue

                # 6) Everything else: classify by keywords/sign; NEVER set check_number
                if any(k in upper for k in self.bofa_withdrawal_keywords) or amount < 0:
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


    def _is_daily_ledger_row(self, text: str) -> bool:
        """
        True only for the 'Daily ledger balances' section rows.
        Explicitly *not* for Checks rows.
        """
        upper = text.upper()

        # Explicit header words for the section
        if "DAILY LEDGER BALANCES" in upper:
            return True

        # If the row looks like a Checks line, don't treat it as ledger
        if re.search(r"\bCHECK(?!CARD)\b", upper) or re.search(r"\b\d{3,5}\*?\b", text):
            return False

        # Normalize parentheses negatives
        t = re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", text)

        # Count date and amount patterns
        dates   = re.findall(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b", t)
        amounts = re.findall(r"[-+]?\$?\d[\d,]*\.\d{2}", t)

        # If the row mentions BALANCE (typical header/column) and has multiple date/amounts,
        # it's very likely part of the ledger section.
        if "BALANCE" in upper and len(dates) >= 1 and len(amounts) >= 1:
            return True

        # Heuristic: three-column ledger often packs multiple date/amount pairs and *no* descriptors.
        # Only treat as ledger if there are multiple pairs AND no strong words besides DATE/BALANCE.
        words = re.findall(r"[A-Z]{3,}", upper)
        words = [w for w in words if w not in {"DATE", "BALANCE", "BALANCES"}]
        if len(dates) >= 2 and len(amounts) >= 2 and not words:
            return True

        return False


    
    def _parse_bofa_checks_row_multi(self, row_text: str) -> list[Transaction]:
        """
        Bank of America 'Checks' section often prints TWO check lines side-by-side
        on one visual row. This function scans the whole row and emits ONE
        Transaction per (date, check#, amount) triplet it finds.

        Examples of what it matches in a single row:
        "01/16/24 228 -1,470.65   01/19/24 230* -1,030.00"
        "01/22/24 231 -967.93     01/26/24 232 -2,869.00"
        """
        txns: list[Transaction] = []

        # Normalize parentheses negatives: (1,234.56) -> -1,234.56
        text = re.sub(r"\(([\d,]+\.\d{2})\)", r"-\1", row_text)

        # Triplet pattern: DATE  CHECKNO  AMOUNT
        # - DATE: 01/16/24 or 01/16/2024 (also allow 1/6/24 just in case)
        # - CHECKNO: 3–5 digits with optional trailing '*'
        # - AMOUNT: signed or unsigned currency, with optional commas or $ sign
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
    def _parse_bofa_row_enhanced(self, row, row_text: str) -> Optional[Transaction]:
        try:
            m = re.search(r'(\d{2}/\d{2}/\d{2,4})', row_text)
            if not m: return None
            date_str = m.group(1)

            amounts = re.findall(r'-?[\d,]+\.\d{2}', row_text)
            if not amounts: return None

            amount = None
            for a in amounts:
                try:
                    v = float(a.replace(',', ''))
                    if abs(v) < 100000: amount = v
                except ValueError:
                    pass
            if amount is None: return None

            txn_type = "deposit" if amount > 0 else "withdrawal"
            return Transaction(
                date=self._standardize_date(date_str),
                description=self._clean_description(row_text),
                amount=amount,
                check_number=None,             # <- force empty
                transaction_type=txn_type
            )
        except Exception as e:
            print(f"Error parsing enhanced BofA row: {e}")
            return None


    def _parse_bofa_withdrawal(self, row, row_text: str) -> Optional[Transaction]:
        try:
            m = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', row_text)
            if not m: return None
            date_str = m.group(1)

            m2 = re.search(r'-?[\d,]+\.\d{2}', row_text)
            if not m2: return None
            amount = float(m2.group().replace(',', ''))
            if amount > 0: amount = -amount

            return Transaction(
                date=self._standardize_date(date_str),
                description=self._clean_description(row_text),
                amount=amount,
                check_number=None,             # <- force empty
                transaction_type="withdrawal"
            )
        except Exception as e:
            print(f"Error parsing BofA withdrawal: {e}")
            return None

    # --------------------- Utilities ---------------------
    def _row_to_text(self, row: pd.Series) -> str:
        # Join cells, preserve signs, drop nans
        parts = [(str(x) if pd.notna(x) else "") for x in row.tolist()]
        txt = " ".join(p.strip() for p in parts if p is not None).strip()
        # Remove repeated internal whitespace
        return re.sub(r"\s+", " ", txt)

    def _extract_date_any(self, text: str) -> Optional[str]:
        # Matches 02/27/24, 02/27/2024, or 2/7
        m = re.search(r"\b(\d{1,2}/\d{1,2}(?:/\d{2,4})?)\b", text)
        return m.group(1) if m else None

    def _extract_check_number(self, text: str) -> Optional[str]:
        # Prefer explicit "Check 1234" then fallback to a 3–4-digit cluster near "Check"
        m = re.search(r"Check\s*#?\s*(\d{3,5})", text, flags=re.IGNORECASE)
        if m:
            return m.group(1)
        # Secondary heuristic: a 3–5 digit number followed by spaces and maybe amount
        m2 = re.search(r"\b(\d{3,5})\b(?=.*-?\$?\d[\d,]*\.\d{2})", text)
        return m2.group(1) if m2 else None

    def _pick_amount(self, text: str) -> Optional[float]:
        """
        Choose the rightmost plausible monetary value.
        Handles negatives and parentheses e.g. (1,234.56)
        Avoids huge numbers that are clearly not amounts.
        """
        # Normalize parentheses negatives
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
        """
        Convert to MM/DD/YYYY when a 2-digit year is present. If no year present,
        leave as MM/DD (upstream can attach statement year if desired).
        """
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

    # --------------------- Main ---------------------
    def extract_transactions(self, pdf_path: str) -> Tuple[str, List[Transaction]]:
        print(f"Processing: {pdf_path}")
        bank_type = self.detect_bank_type(pdf_path)
        print(f"Detected bank: {bank_type}")

        tables = self.extract_tables_tabula(pdf_path, bank_type)
        print(f"Found {len(tables)} tables")

        if bank_type == "wells_fargo":
            txns = self.process_wells_fargo(tables)
        elif bank_type == "bank_of_america":
            txns = self.process_bank_of_america(tables)
        else:
            # Try BofA parser first (more general), then WF as fallback
            txns = self.process_bank_of_america(tables)
            if not txns:
                txns = self.process_wells_fargo(tables)

        print(f"Extracted {len(txns)} transactions")
        return bank_type, txns

    # --------------------- Export ---------------------
    def export_to_csv(self, transactions: List[Transaction], output_path: str):
        data = []
        for t in transactions:
            data.append({
                "Date": t.date,
                "Check No": t.check_number or "",
                "Description": t.description,
                "Amount": t.amount
            })
        pd.DataFrame(data).to_csv(output_path, index=False)
        print(f"Exported {len(data)} transactions to {output_path}")

