import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
from core.pdf_processor import BankStatementProcessor

class BankExtractorGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Bank Statement PDF Extractor")
        self.root.geometry("800x600")
        
        self.processor = BankStatementProcessor()
        self.current_transactions = []
        self.current_bank_type = ""
        
        self.setup_ui()
    
    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # File selection
        file_frame = ttk.LabelFrame(main_frame, text="PDF File Selection", padding="10")
        file_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        self.file_path_var = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.file_path_var, width=60).grid(row=0, column=0, padx=(0, 10))
        ttk.Button(file_frame, text="Browse", command=self.browse_file).grid(row=0, column=1)
        ttk.Button(file_frame, text="Process PDF", command=self.process_pdf).grid(row=0, column=2, padx=(10, 0))
        
        # Status
        self.status_var = tk.StringVar(value="Select a PDF file to begin...")
        ttk.Label(main_frame, textvariable=self.status_var).grid(row=1, column=0, columnspan=2, pady=(0, 10))
        
        # Results table
        table_frame = ttk.LabelFrame(main_frame, text="Extracted Transactions", padding="10")
        table_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # Treeview for results
        columns = ('Date', 'Description', 'Amount', 'Check No')
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings', height=15)
        
        # Column headings
        self.tree.heading('Date', text='Date')
        self.tree.heading('Check No', text='Check No')
        self.tree.heading('Description', text='Description')
        self.tree.heading('Amount', text='Amount')
        
        # Column widths
        self.tree.column('Date', width=100)
        self.tree.column('Check No', width=80)
        self.tree.column('Description', width=400)
        self.tree.column('Amount', width=100)
        
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Export buttons
        export_frame = ttk.Frame(main_frame)
        export_frame.grid(row=3, column=0, columnspan=2, pady=(10, 0))
        
        ttk.Button(export_frame, text="Export to CSV", command=self.export_csv).grid(row=0, column=0, padx=(0, 10))
        ttk.Button(export_frame, text="Export to Excel", command=self.export_excel).grid(row=0, column=1)
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)
    
    def browse_file(self):
        filename = filedialog.askopenfilename(
            title="Select Bank Statement PDF",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        if filename:
            self.file_path_var.set(filename)
    
    def process_pdf(self):
        file_path = self.file_path_var.get()
        if not file_path:
            messagebox.showerror("Error", "Please select a PDF file first")
            return
        
        if not os.path.exists(file_path):
            messagebox.showerror("Error", "Selected file does not exist")
            return
        
        try:
            self.status_var.set("Processing PDF... Please wait...")
            self.root.update()
            
            # Process the PDF
            bank_type, transactions = self.processor.extract_transactions(file_path)
            
            self.current_bank_type = bank_type
            self.current_transactions = transactions
            
            # Clear previous results
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            # Add new results
            for txn in transactions:
                self.tree.insert('', 'end', values=(
                    txn.date,
                    txn.description[:50] + "..." if len(txn.description) > 50 else txn.description,
                    f"${txn.amount:.2f}",
                    txn.check_number or ""
                ))
            
            self.status_var.set(f"Extracted {len(transactions)} transactions from {bank_type.replace('_', ' ').title()}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to process PDF:\n{str(e)}")
            self.status_var.set("Error processing PDF")
    
    def export_csv(self):
        if not self.current_transactions:
            messagebox.showwarning("Warning", "No transactions to export")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Save CSV Export"
        )
        
        if filename:
            try:
                self.processor.export_to_csv(self.current_transactions, filename, self.current_bank_type)
                messagebox.showinfo("Success", f"Exported {len(self.current_transactions)} transactions to CSV")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export CSV:\n{str(e)}")
    
    def export_excel(self):
        if not self.current_transactions:
            messagebox.showwarning("Warning", "No transactions to export")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            title="Save Excel Export"
        )
        
        if filename:
            try:
                # Create DataFrame and export to Excel
                data = []
                for txn in self.current_transactions:
                    data.append({
                        'Date': self.processor._standardize_date(txn.date),
                        'Check No': txn.check_number or '',
                        'Description': txn.description,
                        'Amount': txn.amount
                    })
                
                df = pd.DataFrame(data)
                df.to_excel(filename, index=False)
                messagebox.showinfo("Success", f"Exported {len(self.current_transactions)} transactions to Excel")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export Excel:\n{str(e)}")
    
    def run(self):
        self.root.mainloop()

if __name__ == "__main__":
    app = BankExtractorGUI()
    app.run()