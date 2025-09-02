import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pdf_processor import BankStatementProcessor

def test_pdf_extraction():
    processor = BankStatementProcessor()
    
    # Test with your sample PDFs
    test_pdfs = [
        "tests/test_pdfs/wells_fargo_sample.pdf",
        "tests/test_pdfs/bofa_sample.pdf"
    ]
    
    for pdf_path in test_pdfs:
        if os.path.exists(pdf_path):
            print(f"\n=== Testing {pdf_path} ===")
            bank_type, transactions = processor.extract_transactions(pdf_path)
            
            print(f"Bank Type: {bank_type}")
            print(f"Total Transactions: {len(transactions)}")
            
            # Show first 5 transactions
            for i, txn in enumerate(transactions[:5]):
                print(f"{i+1}. {txn.date} | {txn.description[:30]} | ${txn.amount}")
        else:
            print(f"PDF not found: {pdf_path}")

if __name__ == "__main__":
    test_pdf_extraction()