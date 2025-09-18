# Extract the Transaction class from your base_parser.py
from dataclasses import dataclass
from typing import Optional

@dataclass
class Transaction:
    date: str
    description: str
    amount: float
    check_number: Optional[str] = None
    balance: Optional[float] = None
    transaction_type: str = "unknown"  # deposit, withdrawal, check