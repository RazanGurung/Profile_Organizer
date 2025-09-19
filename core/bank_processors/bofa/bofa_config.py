"""Bank of America specific configurations"""

BOA_CONFIG = {
    "bank_name": "bank_of_america",
    "detection_keywords": [
        "BANK OF AMERICA", 
        "BANKOFAMERICA.COM", 
        "BUSINESS ADVANTAGE"
    ],
    "strong_indicators": [
        "P.O. Box 25118", 
        "Tampa, FL 33622-5118", 
        "1.888.BUSINESS"
    ],
    "account_patterns": [
        r"Account number:\s*\d{4}\s*\d{4}\s*\d{4}"
    ],
    "header_patterns": [
        "Your Business Advantage Fundamentals"
    ],
    "edi_companies": [
        "ITG BRANDS", 
        "HELIX PAYMENT", 
        "REYNOLDS", 
        "PM USA",
        "USSMOKELESS", 
        "JAPAN TOBAC", 
        "MECCA PAYMENT"
    ],
    "monthly_summary": True,
    "exclude_deposits_from_individual": True,
    "extraction_method": "boa_specific"  # Your proven method
}

def get_boa_config():
    return BOA_CONFIG