from .base_parser import BaseParser, Transaction
from .wells_fargo_parser import WellsFargoParser
from .bank_of_america_parser import BankOfAmericaParser

# Registry of available parsers
AVAILABLE_PARSERS = [
    WellsFargoParser,
    BankOfAmericaParser,
]

__all__ = [
    'BaseParser',
    'Transaction', 
    'WellsFargoParser',
    'BankOfAmericaParser',
    'AVAILABLE_PARSERS'
]