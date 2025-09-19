from typing import Optional, Tuple
from .bank_detector import BankDetector
from .bank_processors.bofa.bofa_processor import BankOfAmericaProcessor
from .bank_processors.wells_fargo.wf_processor import WellsFargoProcessor

class ProcessorFactory:
    """Factory to create the appropriate processor for each bank"""
    
    def __init__(self):
        self.detector = BankDetector()
        
        # Registry of bank processors 
        self.processor_classes = {
            "bank_of_america": BankOfAmericaProcessor,
            "wells_fargo": WellsFargoProcessor,
        }
    
    def create_processor(self, pdf_path: str) -> Tuple[Optional[str], Optional[object]]:
        """
        Detect bank and create appropriate processor
        Returns: (bank_name, processor_instance)
        """
        # Step 1: Detect which bank this PDF belongs to
        detected_bank = self.detector.detect_bank(pdf_path)
        
        if not detected_bank:
            print("Could not detect bank type")
            return None, None
        
        # Step 2: Create the appropriate processor for this bank
        processor_class = self.processor_classes.get(detected_bank)
        
        if not processor_class:
            print(f"No processor available for {detected_bank}")
            return detected_bank, None
        
        # Step 3: Create and return the processor instance
        processor = processor_class()
        print(f"Created {detected_bank} processor")
        
        return detected_bank, processor
    
    def get_supported_banks(self) -> list[str]:
        """Get list of banks with available processors"""
        return list(self.processor_classes.keys())