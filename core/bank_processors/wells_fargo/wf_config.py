"""Wells Fargo specific configurations"""

WF_CONFIG = {
    "bank_name": "wells_fargo",
    "detection_keywords": [
        "WELLS FARGO", 
        "NAVIGATE BUSINESS CHECKING",
        "WELLSFARGO.COM/BIZ"
    ],
    "strong_indicators": [
        "1-800-CALL-WELLS", 
        "Portland, OR 97228-6995", 
        "wellsfargo.com/biz"
    ],
    "account_patterns": [
        r"Account number:\s*\d{10}"
    ],
    "header_patterns": [
        "Navigate Business Checking"
    ],
    "edi_companies": [
        "ITG BRANDS", 
        "JAPAN TOBAC", 
        "LIGGETT VECTOR",
        "EDI PYMNTS", 
        "ACH CREDIT"
    ],
    "monthly_summary": False,  # Wells Fargo doesn't need monthly summaries
    "exclude_deposits_from_individual": False,
    "extraction_method": "wf_lattice_stream"  # Wells Fargo specific method
}

def get_wf_config():
    return WF_CONFIG