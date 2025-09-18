import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import pandas as pd
from core.processor_factory import ProcessorFactory
from core.interfaces.transaction import Transaction

class BankExtractorGUI:
    # def __init__(self):
    #     self.root = tk.Tk()
    #     self.root.title("Bank Statement PDF Extractor")
    #     self.root.geometry("900x700")
        
    #     self.processor = BankStatementProcessor()
    #     self.current_transactions = []
    #     self.current_bank_type = ""
        
    #     self.setup_ui()

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Bank Statement PDF Extractor")
        self.root.geometry("900x700")
        
        # Use factory instead of direct processor
        self.factory = ProcessorFactory()
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
        
        # Supported banks info
        # banks_info = f"Supported Banks: {', '.join(self.processor.get_supported_banks())}"
        # ttk.Label(main_frame, text=banks_info, font=('TkDefaultFont', 8)).grid(row=1, column=0, columnspan=2, pady=(0, 5))

        banks_info = f"Supported Banks: {', '.join(self.factory.get_supported_banks())}"
        ttk.Label(main_frame, text=banks_info, font=('TkDefaultFont', 8)).grid(row=1, column=0, columnspan=2, pady=(0, 5))
        
        # Status
        self.status_var = tk.StringVar(value="Select a PDF file to begin...")
        ttk.Label(main_frame, textvariable=self.status_var).grid(row=2, column=0, columnspan=2, pady=(0, 10))
        
        # NEW: Total displays frame
        totals_frame = ttk.LabelFrame(main_frame, text="Transaction Totals", padding="10")
        totals_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Total Withdrawals display
        withdrawals_frame = ttk.Frame(totals_frame)
        withdrawals_frame.grid(row=0, column=0, padx=(0, 20))
        
        ttk.Label(withdrawals_frame, text="Total Withdrawals:", font=('TkDefaultFont', 10, 'bold')).grid(row=0, column=0, sticky=tk.W)
        self.total_withdrawals_var = tk.StringVar(value="$0.00")
        ttk.Label(withdrawals_frame, textvariable=self.total_withdrawals_var, 
                 font=('TkDefaultFont', 12, 'bold'), foreground='red').grid(row=1, column=0, sticky=tk.W)
        
        # Total Deposits display
        deposits_frame = ttk.Frame(totals_frame)
        deposits_frame.grid(row=0, column=1, padx=(20, 0))
        
        ttk.Label(deposits_frame, text="Total Deposits:", font=('TkDefaultFont', 10, 'bold')).grid(row=0, column=0, sticky=tk.W)
        self.total_deposits_var = tk.StringVar(value="$0.00")
        ttk.Label(deposits_frame, textvariable=self.total_deposits_var, 
                 font=('TkDefaultFont', 12, 'bold'), foreground='green').grid(row=1, column=0, sticky=tk.W)
        
        # Results table
        table_frame = ttk.LabelFrame(main_frame, text="Extracted Transactions", padding="10")
        table_frame.grid(row=4, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # Treeview for results - Updated column order
        columns = ('Date', 'Check No', 'Description', 'Amount')
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
        export_frame.grid(row=5, column=0, columnspan=2, pady=(10, 0))
        
        ttk.Button(export_frame, text="Export to CSV", command=self.export_csv).grid(row=0, column=0, padx=(0, 10))
        ttk.Button(export_frame, text="Export to Excel", command=self.export_excel).grid(row=0, column=1)
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(4, weight=1)  # Updated row index for table
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)
    
    def calculate_totals(self):
        """Calculate total withdrawals and deposits from current transactions"""
        total_withdrawals = 0.0
        total_deposits = 0.0
        
        for txn in self.current_transactions:
            if txn.transaction_type in ["withdrawal", "check"] and txn.amount < 0:
                total_withdrawals += abs(txn.amount)  # Make positive for display
            elif txn.transaction_type in ["deposit", "edi_payment", "deposit_summary"] and txn.amount > 0:
                total_deposits += txn.amount
        
        # Update display
        self.total_withdrawals_var.set(f"${total_withdrawals:,.2f}")
        self.total_deposits_var.set(f"${total_deposits:,.2f}")
        
        print(f"Calculated totals - Withdrawals: ${total_withdrawals:,.2f}, Deposits: ${total_deposits:,.2f}")
    
    def browse_file(self):
        filename = filedialog.askopenfilename(
            title="Select Bank Statement PDF",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        if filename:
            self.file_path_var.set(filename)
    
    # def process_pdf(self):
    #     file_path = self.file_path_var.get()
    #     if not file_path:
    #         messagebox.showerror("Error", "Please select a PDF file first")
    #         return
        
    #     if not os.path.exists(file_path):
    #         messagebox.showerror("Error", "Selected file does not exist")
    #         return
        
    #     try:
    #         self.status_var.set("Processing PDF... Please wait...")
    #         self.root.update()
            
    #         # Process the PDF
    #         bank_type, transactions = self.processor.extract_transactions(file_path)
            
    #         self.current_bank_type = bank_type
    #         self.current_transactions = transactions
            
    #         # Clear previous results
    #         for item in self.tree.get_children():
    #             self.tree.delete(item)
            
    #         # Add new results - Order: Date, Check No, Description, Amount
    #         for txn in transactions:
    #             # Color coding for different transaction types
    #             tags = []
    #             if txn.transaction_type == "deposit_summary":
    #                 tags = ["summary"]
    #             elif txn.transaction_type in ["withdrawal", "check"] and txn.amount < 0:
    #                 tags = ["withdrawal"]
    #             elif txn.transaction_type in ["deposit", "edi_payment"] and txn.amount > 0:
    #                 tags = ["deposit"]
                
    #             self.tree.insert('', 'end', values=(
    #                 txn.date,
    #                 txn.check_number or "",
    #                 txn.description[:50] + "..." if len(txn.description) > 50 else txn.description,
    #                 f"${txn.amount:.2f}"
    #             ), tags=tags)
            
    #         # Configure tag colors
    #         self.tree.tag_configure("summary", background="#E6F3FF", font=('TkDefaultFont', 9, 'bold'))
    #         self.tree.tag_configure("withdrawal", background="#FFE6E6")
    #         self.tree.tag_configure("deposit", background="#E6FFE6")
            
    #         # Calculate and display totals
    #         self.calculate_totals()
            
    #         self.status_var.set(f"Extracted {len(transactions)} transactions from {bank_type.replace('_', ' ').title()}")
            
    #     except Exception as e:
    #         messagebox.showerror("Error", f"Failed to process PDF:\n{str(e)}")
    #         self.status_var.set("Error processing PDF")
    
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
            
            # 🆕 NEW: Use factory to detect bank and create processor
            bank_type, processor = self.factory.create_processor(file_path)
            
            # 🆕 NEW: Check if we got a valid processor
            if processor is None:
                if bank_type:
                    messagebox.showerror("Error", f"Detected {bank_type} but no processor available for this bank yet.")
                else:
                    messagebox.showerror("Error", "Could not detect bank type. Please ensure this is a supported bank statement.")
                self.status_var.set("Error: Unsupported bank or detection failed")
                return
            
            # 🆕 NEW: Process using the bank-specific processor
            bank_type, transactions = processor.extract_transactions(file_path)
            
            self.current_bank_type = bank_type
            self.current_transactions = transactions
            
            # 🔄 SAME: Clear previous results (no change)
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            # 🔄 SAME: Add new results - Order: Date, Check No, Description, Amount (no change)
            for txn in transactions:
                # Color coding for different transaction types
                tags = []
                if txn.transaction_type == "deposit_summary":
                    tags = ["summary"]
                elif txn.transaction_type in ["withdrawal", "check"] and txn.amount < 0:
                    tags = ["withdrawal"]
                elif txn.transaction_type in ["deposit", "edi_payment"] and txn.amount > 0:
                    tags = ["deposit"]
                
                self.tree.insert('', 'end', values=(
                    txn.date,
                    txn.check_number or "",
                    txn.description[:50] + "..." if len(txn.description) > 50 else txn.description,
                    f"${txn.amount:.2f}"
                ), tags=tags)
            
            # 🔄 SAME: Configure tag colors (no change)
            self.tree.tag_configure("summary", background="#E6F3FF", font=('TkDefaultFont', 9, 'bold'))
            self.tree.tag_configure("withdrawal", background="#FFE6E6")
            self.tree.tag_configure("deposit", background="#E6FFE6")
            
            # 🔄 SAME: Calculate and display totals (no change)
            self.calculate_totals()
            
            # 🆕 IMPROVED: Better status message with bank name
            bank_display_name = bank_type.replace('_', ' ').title()
            self.status_var.set(f"Extracted {len(transactions)} transactions from {bank_display_name}")
            
        except Exception as e:
            print(f"Full error details: {e}")  # 🆕 NEW: Debug logging
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
                self.processor.export_to_csv(self.current_transactions, filename)
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
                        'Date': txn.date,
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