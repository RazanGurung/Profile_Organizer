# Test script - Save as environment_test.py
import sys
import pandas as pd
import tabula
import pdfplumber
from PIL import Image
import openpyxl

print("=== Anaconda Environment Check ===")
print(f"Python Path: {sys.executable}")
print(f"Python Version: {sys.version}")

# Test each package
packages = {
    'pandas': pd.__version__,
    'tabula-py': tabula.__version__,
    'pdfplumber': pdfplumber.__version__,
    'openpyxl': openpyxl.__version__,
    'Pillow': Image.__version__
}

for package, version in packages.items():
    print(f"✓ {package}: {version}")

# Test Java (for tabula-py)
try:
    import subprocess
    result = subprocess.run(['java', '-version'], capture_output=True, text=True)
    print("✓ Java: Available")
except:
    print("✗ Java: Not found")

print("\n🎉 Environment ready for development!")