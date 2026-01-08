"""
An Query - Main Application
============================
GUI application for exploring and extracting data from PostgreSQL.
READ-ONLY Mode - only performs SELECT operations.
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading

from src.config import (
    COLORS,
    load_database_config, get_available_databases, get_default_database
)
from src.database import DatabaseManager
from src.parser import QueryParser


class AnQueryApp:
    """Main Application Class"""

    def __init__(self, root):
        self.root = root
        self.root.title("An Query")
        self.root.geometry("550x450")
        self.root.minsize(450, 380)
        self.root.configure(bg=COLORS['bg'])

        # State
        self.current_db_key = get_default_database()
        self.db = None
        self.parser = None
        self.current_df = None
        self.is_connected = False
        self.config = None
        self._loading_animation_id = None
        self._loading_frame = 0

        # Setup
        self._setup_styles()
        self._create_main_menu()

        # Auto-connect
        self.root.after(100, self._init_database)

    def _setup_styles(self):
        """Setup ttk styles"""
        style = ttk.Style()
        style.theme_use('clam')

        style.configure('Card.TFrame', background=COLORS['card'])
        style.configure('BG.TFrame', background=COLORS['bg'])
        style.configure('Title.TLabel', font=('Segoe UI', 14, 'bold'),
                       background=COLORS['card'], foreground=COLORS['primary'])
        style.configure('Status.TLabel', font=('Segoe UI', 8), background=COLORS['bg'])
        style.configure('Treeview', font=('Consolas', 9), rowheight=22)
        style.configure('Treeview.Heading', font=('Segoe UI', 9, 'bold'))

    def _create_main_menu(self):
        """Create main menu UI"""
        main = tk.Frame(self.root, bg=COLORS['bg'])
        main.pack(fill=tk.BOTH, expand=True, padx=15, pady=10)

        # Header card
        header = tk.Frame(main, bg=COLORS['card'])
        header.pack(fill=tk.X, pady=(0, 10))

        header_inner = tk.Frame(header, bg=COLORS['card'], padx=15, pady=12)
        header_inner.pack(fill=tk.X)

        # Title with database selector
        title_row = tk.Frame(header_inner, bg=COLORS['card'])
        title_row.pack(fill=tk.X)

        tk.Label(title_row, text="AN QUERY", font=('Segoe UI', 13, 'bold'),
                bg=COLORS['card'], fg=COLORS['primary']).pack(side=tk.LEFT)

        # Database selector
        databases = get_available_databases()
        db_keys = [key for key, _ in databases]
        db_labels = {key: label for key, label in databases}

        self.db_var = tk.StringVar(value=self.current_db_key)

        if databases:
            db_combo = ttk.Combobox(title_row, textvariable=self.db_var,
                                    values=db_keys, state='readonly', width=12)
            db_combo.pack(side=tk.RIGHT)
            db_combo.bind('<<ComboboxSelected>>', self._on_database_change)

            tk.Label(title_row, text="Database:", font=('Segoe UI', 9),
                    bg=COLORS['card'], fg=COLORS['text']).pack(side=tk.RIGHT, padx=(0, 5))

        tk.Label(header_inner, text="Query PostgreSQL & Export to Excel",
                font=('Segoe UI', 9), bg=COLORS['card'],
                fg=COLORS['text']).pack(anchor='w', pady=(2, 0))

        tk.Label(header_inner, text="READ-ONLY Mode",
                font=('Segoe UI', 8), bg=COLORS['card'],
                fg=COLORS['danger']).pack(anchor='w', pady=(2, 0))

        # Menu card
        menu_card = tk.Frame(main, bg=COLORS['card'])
        menu_card.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        menu_inner = tk.Frame(menu_card, bg=COLORS['card'], padx=15, pady=12)
        menu_inner.pack(fill=tk.BOTH, expand=True)

        tk.Label(menu_inner, text="Menu", font=('Segoe UI', 10, 'bold'),
                bg=COLORS['card'], fg=COLORS['text']).pack(anchor='w', pady=(0, 10))

        menu_items = [
            ("1. Check Tables", "View tables & columns", self._open_cek_tabel, COLORS['primary']),
            ("2. Generate Query", "Select columns & filters", self._open_generate_query, COLORS['success']),
            ("3. Preview Data", "View table data", self._open_preview_data, COLORS['warning']),
            ("4. Smart Query", "Natural language query", self._open_smart_query, COLORS['danger']),
        ]

        for text, desc, command, color in menu_items:
            btn_frame = tk.Frame(menu_inner, bg=COLORS['card'])
            btn_frame.pack(fill=tk.X, pady=3)

            indicator = tk.Frame(btn_frame, bg=color, width=4)
            indicator.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 8))

            btn = tk.Button(btn_frame, text=text, font=('Segoe UI', 9),
                           bg=COLORS['card'], fg=COLORS['text'],
                           activebackground=COLORS['bg'], bd=0, padx=8, pady=6,
                           anchor='w', cursor='hand2', command=command)
            btn.pack(side=tk.LEFT, fill=tk.X, expand=True)
            btn.bind('<Enter>', lambda e, b=btn: b.configure(bg=COLORS['bg']))
            btn.bind('<Leave>', lambda e, b=btn: b.configure(bg=COLORS['card']))

            tk.Label(btn_frame, text=desc, font=('Segoe UI', 8),
                    bg=COLORS['card'], fg=COLORS['text_light']).pack(side=tk.RIGHT, padx=5)

        # Exit
        exit_frame = tk.Frame(menu_inner, bg=COLORS['card'])
        exit_frame.pack(fill=tk.X, pady=(15, 0))
        tk.Button(exit_frame, text="Exit", font=('Segoe UI', 9),
                 bg=COLORS['bg'], fg=COLORS['text'], bd=0, padx=12, pady=5,
                 cursor='hand2', command=self.root.quit).pack(side=tk.LEFT)

        # Status bar
        status_frame = tk.Frame(main, bg=COLORS['bg'])
        status_frame.pack(fill=tk.X)

        self.status_var = tk.StringVar(value="Initializing...")
        self.status_label = tk.Label(status_frame, textvariable=self.status_var,
                                     font=('Segoe UI', 8), bg=COLORS['bg'],
                                     fg=COLORS['text_light'])
        self.status_label.pack(side=tk.LEFT)

        self.reconnect_btn = tk.Button(status_frame, text="Reconnect",
                                       font=('Segoe UI', 8), bg=COLORS['bg'],
                                       fg=COLORS['text_light'], bd=0, padx=6, pady=2,
                                       cursor='hand2', command=self._manual_reconnect)
        self.reconnect_btn.pack(side=tk.RIGHT)

        self.conn_var = tk.StringVar(value="Disconnected")
        self.conn_label = tk.Label(status_frame, textvariable=self.conn_var,
                                   font=('Segoe UI', 8), bg=COLORS['bg'],
                                   fg=COLORS['danger'])
        self.conn_label.pack(side=tk.RIGHT, padx=(0, 8))

    def _init_database(self):
        """Initialize database and connect"""
        self._load_database(self.current_db_key)

    def _load_database(self, db_key):
        """Load database configuration and connect"""
        try:
            self.config = load_database_config(db_key)
            self.current_db_key = db_key

            self.db = DatabaseManager(self.config['db_config'])

            self._update_status("Connecting...", connected=False, loading=True)
            self._connect_db()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load database: {e}")
            self._update_status(f"Error: {e}", connected=False)

    def _on_database_change(self, event):
        """Handle database change"""
        new_db = self.db_var.get()
        if new_db != self.current_db_key:
            if self.db:
                self.db.disconnect()
            self._load_database(new_db)

    def _connect_db(self):
        """Connect to database in thread"""
        def connect_thread():
            try:
                if self.db.connect():
                    # Update status: Loading schema
                    self.root.after(0, lambda: self._update_status("Loading schema...", connected=False, loading=True))

                    self.db.get_full_schema()

                    # Update status: Initializing parser
                    self.root.after(0, lambda: self._update_status("Initializing...", connected=False, loading=True))

                    self.parser = QueryParser(
                        self.db.schema_cache,
                        self.db.relations_cache,
                        self.config
                    )

                    self.is_connected = True
                    self.root.after(0, lambda: self._update_status("Ready", connected=True))
                else:
                    self.root.after(0, lambda: self._update_status("Connection failed", connected=False))
            except Exception as e:
                self.root.after(0, lambda: self._update_status("Error", connected=False))

        threading.Thread(target=connect_thread, daemon=True).start()

    def _manual_reconnect(self):
        """Manual reconnect"""
        self._update_status("Reconnecting...", connected=False, loading=True)

        def reconnect_thread():
            try:
                if self.db.reconnect():
                    self.db.get_full_schema()
                    self.parser = QueryParser(
                        self.db.schema_cache,
                        self.db.relations_cache,
                        self.config
                    )
                    self.is_connected = True
                    self.root.after(0, lambda: self._update_status("Reconnected", connected=True))
                    self.root.after(0, lambda: messagebox.showinfo("Success", "Reconnected successfully!"))
                else:
                    self.is_connected = False
                    self.root.after(0, lambda: self._update_status("Failed", connected=False))
                    self.root.after(0, lambda: messagebox.showerror("Error", "Failed to reconnect!"))
            except Exception:
                self.is_connected = False
                self.root.after(0, lambda: self._update_status("Error", connected=False))

        threading.Thread(target=reconnect_thread, daemon=True).start()

    def _start_loading_animation(self):
        """Start loading spinner animation"""
        self._loading_frame = 0
        self._animate_loading()

    def _animate_loading(self):
        """Animate loading spinner"""
        frames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        current_status = self.status_var.get()
        # Remove old spinner if present
        for frame in frames:
            current_status = current_status.replace(f' {frame}', '').replace(f'{frame} ', '')

        # Add new spinner
        spinner = frames[self._loading_frame % len(frames)]
        self.status_var.set(f"{spinner} {current_status.strip()}")

        self._loading_frame += 1
        self._loading_animation_id = self.root.after(100, self._animate_loading)

    def _stop_loading_animation(self):
        """Stop loading spinner animation"""
        if self._loading_animation_id:
            self.root.after_cancel(self._loading_animation_id)
            self._loading_animation_id = None

    def _update_status(self, message, connected=None, loading=False):
        """Update status bar"""
        self._stop_loading_animation()
        self.status_var.set(message)

        if loading:
            self._start_loading_animation()

        if connected is not None:
            if connected:
                self.conn_var.set("Connected")
                self.conn_label.configure(fg=COLORS['success'])
            else:
                self.conn_var.set("Disconnected")
                self.conn_label.configure(fg=COLORS['danger'])

    def _check_connection(self):
        """Check connection before operations"""
        if not self.is_connected:
            messagebox.showwarning("Warning", "Not connected to database.")
            return False

        if not self.db.is_alive():
            self._update_status("Reconnecting...", connected=False, loading=True)
            if self.db.reconnect():
                self.db.get_full_schema()
                self.parser = QueryParser(
                    self.db.schema_cache,
                    self.db.relations_cache,
                    self.config
                )
                self._update_status("Reconnected", connected=True)
                return True
            else:
                messagebox.showerror("Error", "Connection lost!")
                return False
        return True

    def _execute_with_retry(self, func, *args, max_retries=2, **kwargs):
        """Execute function with retry on connection error"""
        last_error = None
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    self.db.rollback()
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                self.db.rollback()

                error_msg = str(e).lower()
                connection_errors = ['connection', 'closed', 'server closed', 'timeout']
                is_connection_error = any(err in error_msg for err in connection_errors)
                is_transaction_error = 'current transaction is aborted' in error_msg

                if (is_connection_error or is_transaction_error) and attempt < max_retries - 1:
                    if is_connection_error:
                        self._update_status("Reconnecting...")
                        if self.db.reconnect():
                            continue
                        else:
                            break
                    elif is_transaction_error:
                        continue
                else:
                    break

        raise last_error if last_error else Exception("Unknown error")

    # =========================================================================
    # MENU 1: Cek Tabel
    # =========================================================================
    def _open_cek_tabel(self):
        """Open Cek Tabel window"""
        if not self._check_connection():
            return

        window = tk.Toplevel(self.root)
        window.title("Check Tables")
        window.geometry("650x450")
        window.minsize(500, 350)
        window.configure(bg=COLORS['bg'])
        window.transient(self.root)

        header = tk.Frame(window, bg=COLORS['card'], padx=12, pady=8)
        header.pack(fill=tk.X)
        tk.Label(header, text="CHECK TABLES", font=('Segoe UI', 11, 'bold'),
                bg=COLORS['card'], fg=COLORS['primary']).pack(side=tk.LEFT)

        content = tk.Frame(window, bg=COLORS['bg'], padx=10, pady=10)
        content.pack(fill=tk.BOTH, expand=True)

        paned = ttk.PanedWindow(content, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)

        # Left
        left = tk.Frame(paned, bg=COLORS['card'], padx=8, pady=8)
        paned.add(left, weight=1)

        tk.Label(left, text="Table List", font=('Segoe UI', 9, 'bold'),
                bg=COLORS['card'], fg=COLORS['text']).pack(anchor='w')

        search_var = tk.StringVar()
        search_entry = ttk.Entry(left, textvariable=search_var)
        search_entry.pack(fill=tk.X, pady=5)

        list_frame = tk.Frame(left, bg=COLORS['card'])
        list_frame.pack(fill=tk.BOTH, expand=True)

        table_listbox = tk.Listbox(list_frame, font=('Consolas', 9),
                                   bg=COLORS['card'], fg=COLORS['text'],
                                   selectbackground=COLORS['primary'],
                                   selectforeground='white', bd=0)
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=table_listbox.yview)
        table_listbox.configure(yscrollcommand=scrollbar.set)
        table_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        tables = list(self.db.schema_cache.keys())
        for table in tables:
            col_count = len(self.db.schema_cache[table]['columns'])
            table_listbox.insert(tk.END, f"{table} ({col_count})")

        def filter_tables(*args):
            search = search_var.get().lower()
            table_listbox.delete(0, tk.END)
            for table in tables:
                if search in table.lower():
                    col_count = len(self.db.schema_cache[table]['columns'])
                    table_listbox.insert(tk.END, f"{table} ({col_count})")

        search_var.trace('w', filter_tables)

        # Right
        right = tk.Frame(paned, bg=COLORS['card'], padx=8, pady=8)
        paned.add(right, weight=2)

        tk.Label(right, text="Columns & Relations", font=('Segoe UI', 9, 'bold'),
                bg=COLORS['card'], fg=COLORS['text']).pack(anchor='w', pady=(0, 5))

        columns_text = scrolledtext.ScrolledText(right, font=('Consolas', 9),
                                                  bg=COLORS['bg'], fg=COLORS['text'],
                                                  bd=0, state='disabled')
        columns_text.pack(fill=tk.BOTH, expand=True)

        def show_columns(event):
            selection = table_listbox.curselection()
            if selection:
                table_name = table_listbox.get(selection[0]).split(' (')[0]
                info = self.db.schema_cache.get(table_name, {})

                columns_text.configure(state='normal')
                columns_text.delete('1.0', tk.END)

                columns_text.insert(tk.END, f"{table_name}\n")
                columns_text.insert(tk.END, "-" * 40 + "\n\n")

                columns_text.insert(tk.END, "Columns:\n")
                for i, col in enumerate(info.get('columns', []), 1):
                    columns_text.insert(tk.END, f"  {i:2}. {col['name']} ({col['type']})\n")

                relations = info.get('relations', [])
                if relations:
                    columns_text.insert(tk.END, "\nRelations:\n")
                    for rel in relations:
                        columns_text.insert(tk.END,
                            f"  -> {rel['from_column']} -> {rel['to_table']}.{rel['to_column']}\n")

                columns_text.configure(state='disabled')

        table_listbox.bind('<<ListboxSelect>>', show_columns)

        footer = tk.Frame(window, bg=COLORS['bg'], pady=8)
        footer.pack(fill=tk.X)
        tk.Button(footer, text="Close", font=('Segoe UI', 9),
                 bg=COLORS['border'], fg=COLORS['text'], bd=0, padx=15, pady=4,
                 cursor='hand2', command=window.destroy).pack()

    # =========================================================================
    # MENU 2: Generate Query
    # =========================================================================
    def _open_generate_query(self):
        """Open Generate Query window"""
        if not self._check_connection():
            return

        window = tk.Toplevel(self.root)
        window.title("Generate Query")
        window.geometry("800x550")
        window.minsize(700, 450)
        window.configure(bg=COLORS['bg'])
        window.transient(self.root)

        # State
        state = {
            'primary_column': None,  # {'table': ..., 'column': ...}
            'selected_columns': [],  # [{'table': ..., 'column': ...}, ...]
            'filters': {},  # {'table.column': {'type': ..., 'value': ..., 'op': ...}, ...}
        }

        # Build all columns list with type info
        all_columns = []
        for tbl, info in self.db.schema_cache.items():
            for col in info['columns']:
                all_columns.append({
                    'table': tbl,
                    'column': col['name'],
                    'type': col['type'],
                    'display': f"{tbl}.{col['name']}"
                })

        header = tk.Frame(window, bg=COLORS['card'], padx=12, pady=8)
        header.pack(fill=tk.X)
        tk.Label(header, text="GENERATE QUERY", font=('Segoe UI', 11, 'bold'),
                bg=COLORS['card'], fg=COLORS['success']).pack(side=tk.LEFT)

        notebook = ttk.Notebook(window)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # =====================================================================
        # Step 1: Pilih Primary Column
        # =====================================================================
        step1 = tk.Frame(notebook, bg=COLORS['bg'], padx=10, pady=10)
        notebook.add(step1, text="1. Primary Column")

        tk.Label(step1, text="Select Primary Column (main column that determines the table):",
                font=('Segoe UI', 9, 'bold'), bg=COLORS['bg']).pack(anchor='w')
        tk.Label(step1, text="Example: job_number, talent_name, company_name",
                font=('Segoe UI', 8), bg=COLORS['bg'], fg=COLORS['text']).pack(anchor='w', pady=(0, 5))

        # Search frame
        search1_frame = tk.Frame(step1, bg=COLORS['bg'])
        search1_frame.pack(fill=tk.X, pady=5)
        tk.Label(search1_frame, text="Search:", font=('Segoe UI', 9), bg=COLORS['bg']).pack(side=tk.LEFT)
        search1_var = tk.StringVar()
        search1_entry = ttk.Entry(search1_frame, textvariable=search1_var, width=30)
        search1_entry.pack(side=tk.LEFT, padx=5)

        # Primary column listbox
        primary_frame = tk.Frame(step1, bg=COLORS['card'])
        primary_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        primary_listbox = tk.Listbox(primary_frame, font=('Consolas', 9), bg=COLORS['card'],
                                     selectbackground=COLORS['success'])
        primary_scroll = ttk.Scrollbar(primary_frame, orient=tk.VERTICAL, command=primary_listbox.yview)
        primary_listbox.configure(yscrollcommand=primary_scroll.set)
        primary_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        primary_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Populate primary listbox
        for col_info in all_columns:
            primary_listbox.insert(tk.END, col_info['display'])

        def filter_primary(*args):
            search = search1_var.get().lower()
            primary_listbox.delete(0, tk.END)
            for col_info in all_columns:
                if search in col_info['display'].lower():
                    primary_listbox.insert(tk.END, col_info['display'])

        search1_var.trace('w', filter_primary)

        # Selected primary display
        primary_selected_var = tk.StringVar(value="Not selected")
        primary_label_frame = tk.Frame(step1, bg=COLORS['bg'])
        primary_label_frame.pack(fill=tk.X, pady=5)
        tk.Label(primary_label_frame, text="Primary:", font=('Segoe UI', 9, 'bold'),
                bg=COLORS['bg']).pack(side=tk.LEFT)
        tk.Label(primary_label_frame, textvariable=primary_selected_var, font=('Consolas', 9),
                bg=COLORS['bg'], fg=COLORS['success']).pack(side=tk.LEFT, padx=5)

        def select_primary():
            selection = primary_listbox.curselection()
            if selection:
                display = primary_listbox.get(selection[0])
                # Find the column info
                for col_info in all_columns:
                    if col_info['display'] == display:
                        state['primary_column'] = {
                            'table': col_info['table'],
                            'column': col_info['column'],
                            'type': col_info.get('type', 'text')
                        }
                        primary_selected_var.set(display)
                        # Auto add to selected columns
                        if col_info not in state['selected_columns']:
                            state['selected_columns'] = [col_info.copy()]
                        notebook.select(1)
                        update_selected_listbox()
                        break

        tk.Button(step1, text="Select Primary >", font=('Segoe UI', 9),
                 bg=COLORS['success'], fg='white', bd=0, padx=12, pady=4,
                 cursor='hand2', command=select_primary).pack(pady=8)

        # =====================================================================
        # Step 2: Select Additional Columns
        # =====================================================================
        step2 = tk.Frame(notebook, bg=COLORS['bg'], padx=10, pady=10)
        notebook.add(step2, text="2. Select Columns")

        # Info label
        step2_info_var = tk.StringVar(value="Primary: Not selected")
        tk.Label(step2, textvariable=step2_info_var, font=('Segoe UI', 9),
                bg=COLORS['bg'], fg=COLORS['success']).pack(anchor='w')

        cols_frame = tk.Frame(step2, bg=COLORS['bg'])
        cols_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        # Left: Available columns
        avail_frame = tk.Frame(cols_frame, bg=COLORS['card'], padx=5, pady=5)
        avail_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))

        tk.Label(avail_frame, text="Available Columns", font=('Segoe UI', 9, 'bold'),
                bg=COLORS['card']).pack(anchor='w')

        # Search for available
        search2_var = tk.StringVar()
        search2_entry = ttk.Entry(avail_frame, textvariable=search2_var, width=25)
        search2_entry.pack(fill=tk.X, pady=3)

        avail_listbox = tk.Listbox(avail_frame, font=('Consolas', 8),
                                   selectmode=tk.EXTENDED, bg=COLORS['card'])
        avail_scroll = ttk.Scrollbar(avail_frame, orient=tk.VERTICAL, command=avail_listbox.yview)
        avail_listbox.configure(yscrollcommand=avail_scroll.set)
        avail_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        avail_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Populate available
        for col_info in all_columns:
            avail_listbox.insert(tk.END, col_info['display'])

        def filter_available(*args):
            search = search2_var.get().lower()
            avail_listbox.delete(0, tk.END)
            for col_info in all_columns:
                if search in col_info['display'].lower():
                    avail_listbox.insert(tk.END, col_info['display'])

        search2_var.trace('w', filter_available)

        # Middle: Buttons
        btn_frame = tk.Frame(cols_frame, bg=COLORS['bg'])
        btn_frame.pack(side=tk.LEFT, padx=5)
        add_btn = tk.Button(btn_frame, text=">>", font=('Segoe UI', 10),
                           bg=COLORS['success'], fg='white', bd=0, padx=8, pady=2)
        add_btn.pack(pady=2)
        rem_btn = tk.Button(btn_frame, text="<<", font=('Segoe UI', 10),
                           bg=COLORS['danger'], fg='white', bd=0, padx=8, pady=2)
        rem_btn.pack(pady=2)
        clear_btn = tk.Button(btn_frame, text="Clear", font=('Segoe UI', 8),
                             bg=COLORS['border'], fg=COLORS['text'], bd=0, padx=6, pady=2)
        clear_btn.pack(pady=10)

        # Right: Selected columns
        sel_frame = tk.Frame(cols_frame, bg=COLORS['card'], padx=5, pady=5)
        sel_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0))

        tk.Label(sel_frame, text="Selected Columns (double-click to filter)", font=('Segoe UI', 9, 'bold'),
                bg=COLORS['card']).pack(anchor='w')

        selected_listbox = tk.Listbox(sel_frame, font=('Consolas', 8), bg=COLORS['card'])
        sel_scroll = ttk.Scrollbar(sel_frame, orient=tk.VERTICAL, command=selected_listbox.yview)
        selected_listbox.configure(yscrollcommand=sel_scroll.set)
        selected_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sel_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        def update_selected_listbox():
            selected_listbox.delete(0, tk.END)
            for col_info in state['selected_columns']:
                key = f"{col_info['table']}.{col_info['column']}"
                marker = " (PRIMARY)" if state['primary_column'] and \
                    col_info['table'] == state['primary_column']['table'] and \
                    col_info['column'] == state['primary_column']['column'] else ""
                # Show filter indicator
                filter_marker = ""
                if key in state['filters'] and state['filters'][key].get('value'):
                    filter_marker = " [F]"
                selected_listbox.insert(tk.END, f"{key}{marker}{filter_marker}")
            if state['primary_column']:
                step2_info_var.set(f"Primary: {state['primary_column']['table']}.{state['primary_column']['column']}")

        def add_columns():
            for idx in avail_listbox.curselection():
                display = avail_listbox.get(idx)
                for col_info in all_columns:
                    if col_info['display'] == display:
                        # Check if already in selected
                        exists = any(c['table'] == col_info['table'] and c['column'] == col_info['column']
                                    for c in state['selected_columns'])
                        if not exists:
                            state['selected_columns'].append(col_info.copy())
                        break
            update_selected_listbox()

        def remove_columns():
            for idx in reversed(selected_listbox.curselection()):
                col_display = selected_listbox.get(idx).replace(" (PRIMARY)", "")
                table, column = col_display.split('.')
                # Don't allow removing primary
                if state['primary_column'] and \
                   state['primary_column']['table'] == table and \
                   state['primary_column']['column'] == column:
                    messagebox.showwarning("Warning", "Cannot remove primary column!")
                    continue
                state['selected_columns'] = [c for c in state['selected_columns']
                                             if not (c['table'] == table and c['column'] == column)]
            update_selected_listbox()

        def clear_columns():
            # Keep only primary
            if state['primary_column']:
                state['selected_columns'] = [{
                    'table': state['primary_column']['table'],
                    'column': state['primary_column']['column'],
                    'type': state['primary_column'].get('type', 'text')
                }]
            else:
                state['selected_columns'] = []
            state['filters'] = {}  # Clear filters too
            update_selected_listbox()
            update_filter_display()

        add_btn.configure(command=add_columns)
        rem_btn.configure(command=remove_columns)
        clear_btn.configure(command=clear_columns)

        # =====================================================================
        # Filter Panel (below column selection)
        # =====================================================================
        filter_section = tk.Frame(step2, bg=COLORS['bg'])
        filter_section.pack(fill=tk.X, pady=(5, 0))

        tk.Label(filter_section, text="Filters:", font=('Segoe UI', 9, 'bold'),
                bg=COLORS['bg']).pack(anchor='w')

        filter_display = tk.Label(filter_section, text="No filters (double-click column to add)",
                                  font=('Segoe UI', 8), bg=COLORS['bg'], fg=COLORS['text_light'])
        filter_display.pack(anchor='w')

        def update_filter_display():
            """Update tampilan filter summary"""
            if not state['filters']:
                filter_display.config(text="No filters (double-click column to add)", fg=COLORS['text_light'])
                return

            filter_texts = []
            for key, flt in state['filters'].items():
                val = flt.get('value')
                # Check if has value (handle boolean False)
                has_value = val is not None and val != ''
                if not has_value and val is not False:
                    continue

                flt_type = flt.get('type', 'string')

                if flt_type == 'boolean':
                    val_str = f"= {'true' if val else 'false'}"
                elif flt_type == 'date':
                    op = flt.get('op', '>=')
                    if op == 'BETWEEN' and flt.get('value_to'):
                        val_str = f"BETWEEN '{val}' AND '{flt['value_to']}'"
                    elif op == '=':
                        val_str = f"= '{val}'"
                    else:
                        val_str = f">= '{val}'"
                elif flt_type == 'numeric':
                    op = flt.get('op', '=')
                    val_str = f"{op} {val}"
                else:  # string
                    op = flt.get('op', 'LIKE')
                    if op == 'LIKE':
                        val_str = f"LIKE '%{val}%'"
                    else:
                        val_str = f"{op} '{val}'"

                filter_texts.append(f"{key} {val_str}")

            if filter_texts:
                filter_display.config(text=" | ".join(filter_texts), fg=COLORS['primary'])
            else:
                filter_display.config(text="No filters (double-click column to add)", fg=COLORS['text_light'])

        def open_filter_dialog(col_key, col_type):
            """Open dialog untuk set filter berdasarkan tipe data"""
            dlg = tk.Toplevel(window)
            dlg.title(f"Filter: {col_key}")
            dlg.configure(bg=COLORS['bg'])
            dlg.transient(window)
            dlg.grab_set()
            dlg.resizable(False, False)

            # Header
            header = tk.Frame(dlg, bg=COLORS['card'], padx=15, pady=10)
            header.pack(fill=tk.X)
            tk.Label(header, text=f"Filter: {col_key}", font=('Segoe UI', 10, 'bold'),
                    bg=COLORS['card'], fg=COLORS['text']).pack(anchor='w')
            tk.Label(header, text=f"Data type: {col_type}", font=('Segoe UI', 8),
                    bg=COLORS['card'], fg=COLORS['text']).pack(anchor='w')

            # Content
            content = tk.Frame(dlg, bg=COLORS['bg'], padx=15, pady=15)
            content.pack(fill=tk.BOTH, expand=True)

            # Get existing filter value
            existing = state['filters'].get(col_key, {})

            # Store save command reference
            save_cmd_ref = [None]

            # Different UI based on type
            if col_type in ('boolean', 'bool'):
                # Boolean: True/False dropdown
                tk.Label(content, text="Value:", font=('Segoe UI', 9), bg=COLORS['bg']).pack(anchor='w')
                bool_var = tk.StringVar()
                if existing.get('value') is True:
                    bool_var.set('true')
                elif existing.get('value') is False:
                    bool_var.set('false')
                else:
                    bool_var.set('')
                bool_combo = ttk.Combobox(content, textvariable=bool_var, width=20, state='readonly')
                bool_combo['values'] = ('', 'true', 'false')
                bool_combo.pack(anchor='w', pady=5)

                def save_bool():
                    val = bool_var.get()
                    if val == '':
                        state['filters'].pop(col_key, None)
                    else:
                        state['filters'][col_key] = {
                            'type': 'boolean',
                            'value': val == 'true',
                            'op': '='
                        }
                    update_selected_listbox()
                    update_filter_display()
                    dlg.destroy()

                save_cmd_ref[0] = save_bool

            elif col_type in ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone'):
                # Date: mode selection + date inputs
                tk.Label(content, text="Mode:", font=('Segoe UI', 9), bg=COLORS['bg']).pack(anchor='w')

                # Determine existing mode
                existing_op = existing.get('op', 'BETWEEN')
                if existing_op == '=':
                    default_mode = 'single'
                elif existing_op == 'BETWEEN':
                    default_mode = 'range'
                else:
                    default_mode = 'gte'  # >= or other

                mode_var = tk.StringVar(value=default_mode)
                mode_frame = tk.Frame(content, bg=COLORS['bg'])
                mode_frame.pack(anchor='w', pady=(2, 8))

                tk.Radiobutton(mode_frame, text="Exact Date", variable=mode_var, value='single',
                              bg=COLORS['bg'], font=('Segoe UI', 8)).pack(side=tk.LEFT)
                tk.Radiobutton(mode_frame, text="Date Range", variable=mode_var, value='range',
                              bg=COLORS['bg'], font=('Segoe UI', 8)).pack(side=tk.LEFT, padx=(10, 0))
                tk.Radiobutton(mode_frame, text=">= Date", variable=mode_var, value='gte',
                              bg=COLORS['bg'], font=('Segoe UI', 8)).pack(side=tk.LEFT, padx=(10, 0))

                # Date input frame
                date_frame = tk.Frame(content, bg=COLORS['bg'])
                date_frame.pack(anchor='w', fill=tk.X)

                from_label = tk.Label(date_frame, text="Date (YYYY-MM-DD):", font=('Segoe UI', 9), bg=COLORS['bg'])
                from_label.pack(anchor='w')
                from_var = tk.StringVar(value=existing.get('value', ''))
                from_entry = ttk.Entry(date_frame, textvariable=from_var, width=25)
                from_entry.pack(anchor='w', pady=(2, 8))

                to_label = tk.Label(date_frame, text="To date (YYYY-MM-DD):", font=('Segoe UI', 9), bg=COLORS['bg'])
                to_label.pack(anchor='w')
                to_var = tk.StringVar(value=existing.get('value_to', ''))
                to_entry = ttk.Entry(date_frame, textvariable=to_var, width=25)
                to_entry.pack(anchor='w', pady=(2, 5))

                def update_date_ui(*args):
                    mode = mode_var.get()
                    if mode == 'single':
                        from_label.config(text="Date (YYYY-MM-DD):")
                        to_label.pack_forget()
                        to_entry.pack_forget()
                    elif mode == 'range':
                        from_label.config(text="From date (YYYY-MM-DD):")
                        to_label.pack(anchor='w')
                        to_entry.pack(anchor='w', pady=(2, 5))
                    else:  # gte
                        from_label.config(text="From date (YYYY-MM-DD):")
                        to_label.pack_forget()
                        to_entry.pack_forget()

                mode_var.trace('w', update_date_ui)
                update_date_ui()  # Initial UI update

                def save_date():
                    from_val = from_var.get().strip()
                    to_val = to_var.get().strip()
                    mode = mode_var.get()

                    if not from_val:
                        state['filters'].pop(col_key, None)
                    else:
                        if mode == 'single':
                            state['filters'][col_key] = {
                                'type': 'date',
                                'value': from_val,
                                'value_to': None,
                                'op': '='
                            }
                        elif mode == 'range' and to_val:
                            state['filters'][col_key] = {
                                'type': 'date',
                                'value': from_val,
                                'value_to': to_val,
                                'op': 'BETWEEN'
                            }
                        else:  # gte or range without to_val
                            state['filters'][col_key] = {
                                'type': 'date',
                                'value': from_val,
                                'value_to': None,
                                'op': '>='
                            }
                    update_selected_listbox()
                    update_filter_display()
                    dlg.destroy()

                save_cmd_ref[0] = save_date

            elif col_type in ('integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision'):
                # Numeric: operator + value
                tk.Label(content, text="Operator:", font=('Segoe UI', 9), bg=COLORS['bg']).pack(anchor='w')
                op_var = tk.StringVar(value=existing.get('op', '='))
                op_combo = ttk.Combobox(content, textvariable=op_var, width=10, state='readonly')
                op_combo['values'] = ('=', '!=', '>', '>=', '<', '<=')
                op_combo.pack(anchor='w', pady=(2, 8))

                tk.Label(content, text="Value:", font=('Segoe UI', 9), bg=COLORS['bg']).pack(anchor='w')
                val_var = tk.StringVar(value=str(existing.get('value', '')))
                val_entry = ttk.Entry(content, textvariable=val_var, width=25)
                val_entry.pack(anchor='w', pady=(2, 5))

                def save_num():
                    val = val_var.get().strip()
                    if not val:
                        state['filters'].pop(col_key, None)
                    else:
                        state['filters'][col_key] = {
                            'type': 'numeric',
                            'value': val,
                            'op': op_var.get()
                        }
                    update_selected_listbox()
                    update_filter_display()
                    dlg.destroy()

                save_cmd_ref[0] = save_num

            else:
                # String/text: LIKE or exact match
                tk.Label(content, text="Mode:", font=('Segoe UI', 9), bg=COLORS['bg']).pack(anchor='w')
                mode_var = tk.StringVar(value=existing.get('op', 'LIKE'))
                mode_combo = ttk.Combobox(content, textvariable=mode_var, width=15, state='readonly')
                mode_combo['values'] = ('LIKE', '=', '!=')
                mode_combo.pack(anchor='w', pady=(2, 8))

                tk.Label(content, text="Value:", font=('Segoe UI', 9), bg=COLORS['bg']).pack(anchor='w')
                val_var = tk.StringVar(value=existing.get('value', ''))
                val_entry = ttk.Entry(content, textvariable=val_var, width=25)
                val_entry.pack(anchor='w', pady=(2, 5))

                tk.Label(content, text="LIKE automatically adds % at start and end", font=('Segoe UI', 7),
                        bg=COLORS['bg'], fg=COLORS['text']).pack(anchor='w')

                def save_str():
                    val = val_var.get().strip()
                    if not val:
                        state['filters'].pop(col_key, None)
                    else:
                        state['filters'][col_key] = {
                            'type': 'string',
                            'value': val,
                            'op': mode_var.get()
                        }
                    update_selected_listbox()
                    update_filter_display()
                    dlg.destroy()

                save_cmd_ref[0] = save_str

            # Separator
            ttk.Separator(dlg, orient='horizontal').pack(fill=tk.X, padx=15)

            # Buttons frame - always at bottom
            btn_frame_dlg = tk.Frame(dlg, bg=COLORS['bg'], padx=15, pady=12)
            btn_frame_dlg.pack(fill=tk.X)

            def do_clear():
                state['filters'].pop(col_key, None)
                update_selected_listbox()
                update_filter_display()
                dlg.destroy()

            tk.Button(btn_frame_dlg, text="Save", font=('Segoe UI', 9),
                     bg=COLORS['success'], fg='white', bd=0, padx=15, pady=5,
                     cursor='hand2', command=lambda: save_cmd_ref[0]()).pack(side=tk.LEFT, padx=(0, 5))
            tk.Button(btn_frame_dlg, text="Clear", font=('Segoe UI', 9),
                     bg=COLORS['danger'], fg='white', bd=0, padx=15, pady=5,
                     cursor='hand2', command=do_clear).pack(side=tk.LEFT, padx=5)
            tk.Button(btn_frame_dlg, text="Cancel", font=('Segoe UI', 9),
                     bg=COLORS['border'], fg=COLORS['text'], bd=0, padx=15, pady=5,
                     cursor='hand2', command=dlg.destroy).pack(side=tk.RIGHT)

            # Set size and center after content is packed
            dlg.update_idletasks()
            dlg.minsize(320, dlg.winfo_reqheight())
            # Center dialog
            x = window.winfo_x() + (window.winfo_width() - 320) // 2
            y = window.winfo_y() + (window.winfo_height() - dlg.winfo_reqheight()) // 2
            dlg.geometry(f"320x{dlg.winfo_reqheight()}+{x}+{y}")

        def on_selected_double_click(event):
            """Handle double click on selected column to open filter"""
            selection = selected_listbox.curselection()
            if not selection:
                return

            item = selected_listbox.get(selection[0])
            # Remove markers like (PRIMARY) and [F]
            col_key = item.replace(" (PRIMARY)", "").replace(" [F]", "").strip()

            # Find column type
            col_type = 'text'
            for col_info in state['selected_columns']:
                if f"{col_info['table']}.{col_info['column']}" == col_key:
                    col_type = col_info.get('type', 'text')
                    break

            open_filter_dialog(col_key, col_type)

        selected_listbox.bind('<Double-1>', on_selected_double_click)

        # Execute button
        btn_row = tk.Frame(step2, bg=COLORS['bg'])
        btn_row.pack(pady=8)
        tk.Button(btn_row, text="Generate & Execute >", font=('Segoe UI', 9),
                 bg=COLORS['success'], fg='white', bd=0, padx=12, pady=4,
                 cursor='hand2', command=lambda: execute_query()).pack(side=tk.LEFT, padx=5)

        # =====================================================================
        # Step 3: Hasil
        # =====================================================================
        step3 = tk.Frame(notebook, bg=COLORS['bg'], padx=10, pady=10)
        notebook.add(step3, text="3. Hasil")

        # SQL display
        tk.Label(step3, text="Generated SQL:", font=('Segoe UI', 9, 'bold'),
                bg=COLORS['bg']).pack(anchor='w')
        sql_text = scrolledtext.ScrolledText(step3, font=('Consolas', 9), bg=COLORS['card'], height=6)
        sql_text.pack(fill=tk.X, pady=5)

        # Results
        tk.Label(step3, text="Results:", font=('Segoe UI', 9, 'bold'),
                bg=COLORS['bg']).pack(anchor='w')

        result_frame = tk.Frame(step3, bg=COLORS['card'])
        result_frame.pack(fill=tk.BOTH, expand=True)

        result_tree = ttk.Treeview(result_frame, show='headings')
        result_vsb = ttk.Scrollbar(result_frame, orient="vertical", command=result_tree.yview)
        result_hsb = ttk.Scrollbar(result_frame, orient="horizontal", command=result_tree.xview)
        result_tree.configure(yscrollcommand=result_vsb.set, xscrollcommand=result_hsb.set)

        result_tree.grid(row=0, column=0, sticky='nsew')
        result_vsb.grid(row=0, column=1, sticky='ns')
        result_hsb.grid(row=1, column=0, sticky='ew')
        result_frame.grid_rowconfigure(0, weight=1)
        result_frame.grid_columnconfigure(0, weight=1)

        result_info_var = tk.StringVar(value="")
        tk.Label(step3, textvariable=result_info_var, font=('Segoe UI', 8),
                bg=COLORS['bg'], fg=COLORS['text']).pack(anchor='w', pady=2)

        def build_where_clause():
            """Build WHERE clause from filters"""
            if not state['filters']:
                return "", []

            conditions = []
            params = []

            for col_key, flt in state['filters'].items():
                # Check if filter has value (handle boolean False)
                has_value = flt.get('value') is not None and flt.get('value') != ''
                if not has_value and flt.get('value') is not False:
                    continue

                table, column = col_key.split('.')
                col_ref = f'"{table}"."{column}"'
                flt_type = flt.get('type', 'string')
                val = flt.get('value')

                if flt_type == 'boolean':
                    conditions.append(f"{col_ref} = %s")
                    params.append(val)

                elif flt_type == 'date':
                    # Date filter - cast to date for proper comparison
                    op = flt.get('op', '>=')
                    if op == 'BETWEEN' and flt.get('value_to'):
                        # Date range
                        conditions.append(f"{col_ref}::date BETWEEN %s::date AND %s::date")
                        params.append(val)
                        params.append(flt['value_to'])
                    elif op == '=':
                        # Single date (exact match)
                        conditions.append(f"{col_ref}::date = %s::date")
                        params.append(val)
                    else:
                        # >= date
                        conditions.append(f"{col_ref}::date >= %s::date")
                        params.append(val)

                elif flt_type == 'numeric':
                    op = flt.get('op', '=')
                    conditions.append(f"{col_ref} {op} %s")
                    params.append(val)

                else:  # string
                    op = flt.get('op', 'LIKE')
                    if op == 'LIKE':
                        # Add wildcards if not present
                        search_val = val if '%' in val else f'%{val}%'
                        conditions.append(f"{col_ref} ILIKE %s")
                        params.append(search_val)
                    else:
                        conditions.append(f"{col_ref} {op} %s")
                        params.append(val)

            if conditions:
                return " AND ".join(conditions), params
            return "", []

        def inject_where_clause(sql, where_clause):
            """
            Inject WHERE clause into SQL properly.
            Strategy: Remove LIMIT, add WHERE, then re-add LIMIT at end.
            """
            import re

            # Extract and remove LIMIT clause
            limit_match = re.search(r'\nLIMIT\s+(\d+)', sql, re.IGNORECASE)
            if not limit_match:
                limit_match = re.search(r'\sLIMIT\s+(\d+)', sql, re.IGNORECASE)

            limit_value = None
            if limit_match:
                limit_value = limit_match.group(1)
                sql = sql[:limit_match.start()] + sql[limit_match.end():]

            # Check if WHERE already exists
            sql_upper = sql.upper()
            where_match = re.search(r'\nWHERE\s', sql_upper)
            if not where_match:
                where_match = re.search(r'\sWHERE\s', sql_upper)

            if where_match:
                # WHERE exists - append with AND before ORDER BY or at end
                order_match = re.search(r'\nORDER BY', sql_upper)
                if not order_match:
                    order_match = re.search(r'\sORDER BY', sql_upper)

                if order_match:
                    insert_pos = order_match.start()
                    sql = sql[:insert_pos] + f" AND {where_clause}" + sql[insert_pos:]
                else:
                    sql = sql + f" AND {where_clause}"
            else:
                # No WHERE - find position after FROM/JOINs but before ORDER BY
                order_match = re.search(r'\nORDER BY', sql_upper)
                if not order_match:
                    order_match = re.search(r'\sORDER BY', sql_upper)

                if order_match:
                    insert_pos = order_match.start()
                    sql = sql[:insert_pos] + f"\nWHERE {where_clause}" + sql[insert_pos:]
                else:
                    sql = sql + f"\nWHERE {where_clause}"

            # Re-add LIMIT at the end
            if limit_value:
                sql = sql + f"\nLIMIT {limit_value}"

            return sql

        def execute_query():
            if not state['primary_column']:
                messagebox.showwarning("Warning", "Please select a primary column first!")
                notebook.select(0)
                return

            if len(state['selected_columns']) == 0:
                messagebox.showwarning("Warning", "Please select at least 1 column!")
                return

            try:
                # Build query using parser
                # Exclude primary column from show list to avoid duplicate
                primary_str = f"{state['primary_column']['table']}.{state['primary_column']['column']}"
                other_columns = [
                    c for c in state['selected_columns']
                    if not (c['table'] == state['primary_column']['table'] and
                            c['column'] == state['primary_column']['column'])
                ]
                if other_columns:
                    columns_str = ', '.join([f"{c['table']}.{c['column']}" for c in other_columns])
                    query_text = f"primary {primary_str} show {columns_str}"
                else:
                    # Only primary column selected
                    query_text = f"show {primary_str}"

                parsed = self.parser.parse(query_text)
                sql, params, applied_filters = self.parser.build_sql(parsed)

                # Add filter WHERE clause
                filter_where, filter_params = build_where_clause()
                if filter_where:
                    sql = inject_where_clause(sql, filter_where)

                # Combine params from build_sql + additional filter params
                all_params = params + filter_params

                # Display SQL with filter info
                display_sql = sql
                if applied_filters:
                    display_sql = f"-- Auto-filters: {', '.join(applied_filters)}\n{sql}"
                sql_text.delete('1.0', tk.END)
                sql_text.insert('1.0', display_sql)

                # Execute
                df = self._execute_with_retry(self.db.execute_query, sql, all_params if all_params else None)
                self.current_df = df

                # Display results
                result_tree.delete(*result_tree.get_children())
                result_tree['columns'] = list(df.columns)
                for col in df.columns:
                    result_tree.heading(col, text=col)
                    result_tree.column(col, width=100, minwidth=50)

                for _, row in df.head(1000).iterrows():
                    values = [str(v) if v is not None else '' for v in row]
                    result_tree.insert('', tk.END, values=values)

                result_info_var.set(f"Result: {len(df)} rows")
                notebook.select(2)

            except Exception as e:
                messagebox.showerror("Error", str(e))

        def export_excel():
            if self.current_df is not None and len(self.current_df) > 0:
                self.db.export_to_excel(self.current_df)
            else:
                messagebox.showwarning("Warning", "No data to export!")

        def copy_sql():
            sql = sql_text.get('1.0', tk.END).strip()
            if sql:
                window.clipboard_clear()
                window.clipboard_append(sql)
                messagebox.showinfo("Copied", "SQL copied to clipboard!")
            else:
                messagebox.showwarning("Warning", "No SQL to copy!")

        def copy_data():
            if self.current_df is not None and len(self.current_df) > 0:
                # Copy as tab-separated values (bisa paste ke Excel)
                text = self.current_df.to_csv(sep='\t', index=False)
                window.clipboard_clear()
                window.clipboard_append(text)
                messagebox.showinfo("Copied", f"Data ({len(self.current_df)} rows) copied to clipboard!\nYou can paste directly into Excel.")
            else:
                messagebox.showwarning("Warning", "No data to export!")

        # Export buttons
        btn_row3 = tk.Frame(step3, bg=COLORS['bg'])
        btn_row3.pack(pady=8)
        tk.Button(btn_row3, text="Export Excel", font=('Segoe UI', 9),
                 bg=COLORS['success'], fg='white', bd=0, padx=12, pady=4,
                 cursor='hand2', command=export_excel).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_row3, text="Copy SQL", font=('Segoe UI', 9),
                 bg=COLORS['primary'], fg='white', bd=0, padx=12, pady=4,
                 cursor='hand2', command=copy_sql).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_row3, text="Copy Data", font=('Segoe UI', 9),
                 bg=COLORS['warning'], fg='white', bd=0, padx=12, pady=4,
                 cursor='hand2', command=copy_data).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_row3, text="< Edit Columns", font=('Segoe UI', 9),
                 bg=COLORS['border'], fg=COLORS['text'], bd=0, padx=12, pady=4,
                 cursor='hand2', command=lambda: notebook.select(1)).pack(side=tk.LEFT, padx=5)

    # =========================================================================
    # MENU 3: Preview Data
    # =========================================================================
    def _open_preview_data(self):
        """Open Preview Data window"""
        if not self._check_connection():
            return

        window = tk.Toplevel(self.root)
        window.title("Preview Data")
        window.geometry("700x450")
        window.minsize(550, 350)
        window.configure(bg=COLORS['bg'])
        window.transient(self.root)

        header = tk.Frame(window, bg=COLORS['card'], padx=12, pady=8)
        header.pack(fill=tk.X)
        tk.Label(header, text="PREVIEW DATA", font=('Segoe UI', 11, 'bold'),
                bg=COLORS['card'], fg=COLORS['warning']).pack(side=tk.LEFT)

        controls = tk.Frame(window, bg=COLORS['bg'], padx=10, pady=8)
        controls.pack(fill=tk.X)

        tk.Label(controls, text="Table:", font=('Segoe UI', 9), bg=COLORS['bg']).pack(side=tk.LEFT)

        table_var = tk.StringVar()
        table_combo = ttk.Combobox(controls, textvariable=table_var, width=30, state='readonly')
        table_combo['values'] = list(self.db.schema_cache.keys())
        table_combo.pack(side=tk.LEFT, padx=5)

        tk.Label(controls, text="Limit:", font=('Segoe UI', 9),
                bg=COLORS['bg']).pack(side=tk.LEFT, padx=(15, 5))
        limit_var = tk.StringVar(value="10")
        limit_entry = ttk.Entry(controls, textvariable=limit_var, width=8)
        limit_entry.pack(side=tk.LEFT)

        results = tk.Frame(window, bg=COLORS['card'], padx=8, pady=8)
        results.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        tree_frame = tk.Frame(results, bg=COLORS['card'])
        tree_frame.pack(fill=tk.BOTH, expand=True)

        tree = ttk.Treeview(tree_frame, show='headings')
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        info_var = tk.StringVar(value="Select table and click Load")
        tk.Label(window, textvariable=info_var, font=('Segoe UI', 8),
                bg=COLORS['bg'], fg=COLORS['text']).pack(pady=5)

        def load_preview():
            table_name = table_var.get()
            if not table_name:
                messagebox.showwarning("Warning", "Please select a table!")
                return

            try:
                limit = int(limit_var.get())
            except:
                limit = 10

            try:
                count = self._execute_with_retry(self.db.get_table_count, table_name)
                info_var.set(f"Total: {count:,} rows | Showing: {min(limit, count)}")

                df = self._execute_with_retry(self.db.preview_table, table_name, limit)
                self.current_df = df

                tree.delete(*tree.get_children())
                tree['columns'] = list(df.columns)
                for col in df.columns:
                    tree.heading(col, text=col)
                    tree.column(col, width=100, minwidth=50)

                for _, row in df.iterrows():
                    values = [str(v) if v is not None else '' for v in row]
                    tree.insert('', tk.END, values=values)

            except Exception as e:
                messagebox.showerror("Error", str(e))

        def export():
            if self.current_df is not None and len(self.current_df) > 0:
                self.db.export_to_excel(self.current_df)
            else:
                messagebox.showwarning("Warning", "No data to export!")

        tk.Button(controls, text="Load", font=('Segoe UI', 9),
                 bg=COLORS['warning'], fg='white', bd=0, padx=10, pady=3,
                 cursor='hand2', command=load_preview).pack(side=tk.LEFT, padx=10)
        tk.Button(controls, text="Export", font=('Segoe UI', 9),
                 bg=COLORS['primary'], fg='white', bd=0, padx=10, pady=3,
                 cursor='hand2', command=export).pack(side=tk.LEFT)

    # =========================================================================
    # MENU 4: Smart Query
    # =========================================================================
    def _build_column_library(self):
        """Build column library dari schema untuk GUI"""
        library = {}

        for table_name, table_info in self.db.schema_cache.items():
            for col in table_info['columns']:
                col_name = col['name']
                col_type = col['type']

                # Skip internal columns
                if col_name in ('id', 'created_at', 'updated_at', 'created_by', 'updated_by'):
                    continue

                # Build display name
                if col_name == 'name':
                    display_name = f"{table_name}_name"
                elif col_name.endswith('_id') and col_name != 'id':
                    continue  # Skip foreign keys
                else:
                    display_name = col_name

                if display_name not in library:
                    library[display_name] = {
                        'tables': [],
                        'type': col_type,
                        'aliases': []
                    }
                library[display_name]['tables'].append(table_name)

        # Add aliases from custom mappings
        custom_mappings = self.config.get('custom_mappings', {})
        for alias, mapping in custom_mappings.items():
            table = mapping.get('table', '')
            column = mapping.get('column', '')

            if column == 'name':
                display_name = f"{table}_name"
            else:
                display_name = column

            if display_name in library:
                if alias not in library[display_name]['aliases'] and alias != display_name:
                    library[display_name]['aliases'].append(alias)

        return library

    def _search_column_library(self, library, keyword):
        """Search column library"""
        if not keyword:
            return list(library.items())

        keyword_lower = keyword.lower()
        results = []

        for col_name, info in library.items():
            score = 0
            # Exact match
            if col_name.lower() == keyword_lower:
                score = 100
            # Starts with
            elif col_name.lower().startswith(keyword_lower):
                score = 80
            # Contains in name
            elif keyword_lower in col_name.lower():
                score = 60
            # Match in alias
            elif any(keyword_lower in a.lower() for a in info['aliases']):
                score = 50
            # Match in table name
            elif any(keyword_lower in t.lower() for t in info['tables']):
                score = 40

            if score > 0:
                results.append((col_name, info, score))

        # Sort by score descending
        results.sort(key=lambda x: (-x[2], x[0]))
        return [(name, info) for name, info, _ in results]

    def _open_smart_query(self):
        """Open Smart Query window with split view"""
        if not self._check_connection():
            return

        # Build column library
        column_library = self._build_column_library()

        window = tk.Toplevel(self.root)
        window.title("Smart Query - An Query")
        window.geometry("1200x700")
        window.minsize(1000, 550)
        window.configure(bg=COLORS['bg'])
        window.transient(self.root)

        # Header
        header = tk.Frame(window, bg=COLORS['card'], padx=12, pady=8)
        header.pack(fill=tk.X)
        tk.Label(header, text="SMART QUERY", font=('Segoe UI', 12, 'bold'),
                bg=COLORS['card'], fg=COLORS['primary']).pack(side=tk.LEFT)

        db_info = f"Database: {self.current_db_key} | Tables: {len(self.db.schema_cache)} | Columns: {len(column_library)}"
        tk.Label(header, text=db_info, font=('Segoe UI', 9),
                bg=COLORS['card'], fg=COLORS['text']).pack(side=tk.RIGHT)

        # Main split container
        main_container = tk.Frame(window, bg=COLORS['bg'])
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Configure grid weights for split
        main_container.grid_columnconfigure(0, weight=3)  # Left panel (query)
        main_container.grid_columnconfigure(1, weight=0)  # Separator
        main_container.grid_columnconfigure(2, weight=2)  # Right panel (library)
        main_container.grid_rowconfigure(0, weight=1)

        # =====================================================================
        # LEFT PANEL - Query
        # =====================================================================
        left_panel = tk.Frame(main_container, bg=COLORS['bg'])
        left_panel.grid(row=0, column=0, sticky='nsew', padx=(0, 5))

        # Help text - EXPANDED with examples
        help_frame = tk.Frame(left_panel, bg=COLORS['card'], padx=10, pady=8)
        help_frame.pack(fill=tk.X, pady=(0, 5))

        tk.Label(help_frame, text="QUERY FORMAT & EXAMPLES", font=('Segoe UI', 9, 'bold'),
                bg=COLORS['card'], fg=COLORS['primary']).pack(anchor='w')

        # Simple examples
        simple_frame = tk.Frame(help_frame, bg=COLORS['card'])
        simple_frame.pack(fill=tk.X, pady=(5, 0))

        tk.Label(simple_frame, text="Simple:", font=('Segoe UI', 8, 'bold'),
                bg=COLORS['card'], fg=COLORS['success']).pack(anchor='w')

        simple_examples = """  show job_number, talent_name
  show job_number, company_name where is_completed=true
  show talent_name, total_fee where status=completed limit 100"""

        tk.Label(simple_frame, text=simple_examples, font=('Consolas', 8),
                bg=COLORS['card'], fg=COLORS['text'], justify=tk.LEFT).pack(anchor='w')

        # Complex examples
        complex_frame = tk.Frame(help_frame, bg=COLORS['card'])
        complex_frame.pack(fill=tk.X, pady=(5, 0))

        tk.Label(complex_frame, text="Advanced:", font=('Segoe UI', 8, 'bold'),
                bg=COLORS['card'], fg=COLORS['warning']).pack(anchor='w')

        complex_examples = """  show job_number, talent_name, company_name, total_fee where start_date=2025-01-01..2025-12-31
  show company_name, count:job_number, sum:total_fee where status=completed
  show job_number, talent_name, schedule_date where is_up=true order by schedule_date desc"""

        tk.Label(complex_frame, text=complex_examples, font=('Consolas', 8),
                bg=COLORS['card'], fg=COLORS['text'], justify=tk.LEFT).pack(anchor='w')

        # Shortcuts
        shortcut_frame = tk.Frame(help_frame, bg=COLORS['card'])
        shortcut_frame.pack(fill=tk.X, pady=(5, 0))

        tk.Label(shortcut_frame, text="Shortcuts:", font=('Segoe UI', 8, 'bold'),
                bg=COLORS['card'], fg=COLORS['danger']).pack(anchor='w')

        shortcuts = """  Date Range: col=2025-01-01..2025-12-31  |  Status: completed, canceled, paid, hold
  Aggregates: count:col, sum:col, avg:col, min:col, max:col"""

        tk.Label(shortcut_frame, text=shortcuts, font=('Consolas', 8),
                bg=COLORS['card'], fg=COLORS['text'], justify=tk.LEFT).pack(anchor='w')

        # Query input
        input_frame = tk.Frame(left_panel, bg=COLORS['bg'])
        input_frame.pack(fill=tk.X, pady=5)

        tk.Label(input_frame, text="Query:", font=('Segoe UI', 9, 'bold'),
                bg=COLORS['bg']).pack(anchor='w')

        query_var = tk.StringVar()
        query_entry = ttk.Entry(input_frame, textvariable=query_var, font=('Consolas', 10))
        query_entry.pack(fill=tk.X, pady=3)
        query_entry.focus()

        # Buttons
        btn_frame = tk.Frame(input_frame, bg=COLORS['bg'])
        btn_frame.pack(fill=tk.X, pady=5)

        # Results notebook
        results_frame = tk.Frame(left_panel, bg=COLORS['bg'])
        results_frame.pack(fill=tk.BOTH, expand=True)

        result_notebook = ttk.Notebook(results_frame)
        result_notebook.pack(fill=tk.BOTH, expand=True)

        sql_frame = tk.Frame(result_notebook, bg=COLORS['card'])
        result_notebook.add(sql_frame, text="SQL")
        sql_text = scrolledtext.ScrolledText(sql_frame, font=('Consolas', 9),
                                              bg=COLORS['card'], height=5)
        sql_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        data_frame = tk.Frame(result_notebook, bg=COLORS['card'])
        result_notebook.add(data_frame, text="Data")

        tree_container = tk.Frame(data_frame, bg=COLORS['card'])
        tree_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        tree = ttk.Treeview(tree_container, show='headings')
        vsb = ttk.Scrollbar(tree_container, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(tree_container, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        # =====================================================================
        # SEPARATOR
        # =====================================================================
        separator = tk.Frame(main_container, bg=COLORS['border'], width=2)
        separator.grid(row=0, column=1, sticky='ns', padx=3)

        # =====================================================================
        # RIGHT PANEL - Column Library
        # =====================================================================
        right_panel = tk.Frame(main_container, bg=COLORS['card'])
        right_panel.grid(row=0, column=2, sticky='nsew', padx=(5, 0))

        # Library header
        lib_header = tk.Frame(right_panel, bg=COLORS['primary'], padx=8, pady=6)
        lib_header.pack(fill=tk.X)

        tk.Label(lib_header, text="COLUMN LIBRARY", font=('Segoe UI', 10, 'bold'),
                bg=COLORS['primary'], fg='white').pack(side=tk.LEFT)

        lib_count_var = tk.StringVar(value=f"{len(column_library)} columns")
        tk.Label(lib_header, textvariable=lib_count_var, font=('Segoe UI', 8),
                bg=COLORS['primary'], fg='white').pack(side=tk.RIGHT)

        # Search box
        search_frame = tk.Frame(right_panel, bg=COLORS['card'], padx=8, pady=6)
        search_frame.pack(fill=tk.X)

        tk.Label(search_frame, text="Search:", font=('Segoe UI', 8),
                bg=COLORS['card'], fg=COLORS['text']).pack(side=tk.LEFT)

        search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=search_var, font=('Segoe UI', 9))
        search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        # Library list
        lib_list_frame = tk.Frame(right_panel, bg=COLORS['card'])
        lib_list_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Treeview for library
        lib_tree = ttk.Treeview(lib_list_frame, columns=('column', 'table', 'type'), show='headings', height=20)
        lib_tree.heading('column', text='Column Name')
        lib_tree.heading('table', text='Table')
        lib_tree.heading('type', text='Type')
        lib_tree.column('column', width=140)
        lib_tree.column('table', width=100)
        lib_tree.column('type', width=70)

        lib_vsb = ttk.Scrollbar(lib_list_frame, orient="vertical", command=lib_tree.yview)
        lib_tree.configure(yscrollcommand=lib_vsb.set)

        lib_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        lib_vsb.pack(side=tk.RIGHT, fill=tk.Y)

        # Tips
        tips_frame = tk.Frame(right_panel, bg=COLORS['card'], padx=8, pady=5)
        tips_frame.pack(fill=tk.X)

        tk.Label(tips_frame, text="Tip: Double-click column to insert into query",
                font=('Segoe UI', 7), bg=COLORS['card'], fg=COLORS['text']).pack(anchor='w')

        # Function to populate library
        def populate_library(search_term=None):
            lib_tree.delete(*lib_tree.get_children())
            results = self._search_column_library(column_library, search_term)

            for col_name, info in results[:100]:  # Limit 100
                tables = ', '.join(info['tables'][:2])
                if len(info['tables']) > 2:
                    tables += f" +{len(info['tables'])-2}"
                col_type = info['type'][:15] if info['type'] else ''
                lib_tree.insert('', tk.END, values=(col_name, tables, col_type))

            if search_term:
                lib_count_var.set(f"{len(results)} results")
            else:
                lib_count_var.set(f"{len(column_library)} columns")

        # Initial populate
        populate_library()

        # Search binding
        def on_search(*args):
            populate_library(search_var.get())

        search_var.trace('w', on_search)

        # Double click to insert column name
        def on_lib_double_click(event):
            selection = lib_tree.selection()
            if selection:
                item = lib_tree.item(selection[0])
                col_name = item['values'][0]
                # Insert at cursor position
                current = query_var.get()
                if current and not current.endswith(' ') and not current.endswith(','):
                    query_var.set(current + ', ' + col_name)
                else:
                    query_var.set(current + col_name)
                query_entry.focus()
                query_entry.icursor(tk.END)

        lib_tree.bind('<Double-1>', on_lib_double_click)

        # =====================================================================
        # Status bar
        # =====================================================================
        status_var = tk.StringVar(value="Enter query and press Execute | Double-click column to insert")
        tk.Label(window, textvariable=status_var, font=('Segoe UI', 8),
                bg=COLORS['bg'], fg=COLORS['text']).pack(pady=5)

        # =====================================================================
        # Query functions
        # =====================================================================
        def execute_query():
            query_text = query_var.get().strip()
            if not query_text:
                messagebox.showwarning("Warning", "Please enter a query!")
                return

            try:
                parsed = self.parser.parse(query_text)
                sql, params, applied_filters = self.parser.build_sql(parsed)

                def run_query():
                    self.db.rollback()
                    return self.db.execute_query(sql, params if params else None)

                df = self._execute_with_retry(run_query)

                # Transform boolean labels
                boolean_labels = self.config.get('boolean_labels', {})
                if boolean_labels:
                    for col in df.columns:
                        col_name = col.split('.')[-1] if '.' in col else col
                        # Try original name, then with underscore instead of space
                        col_underscore = col_name.replace(' ', '_')
                        label_map = None
                        if col_name in boolean_labels:
                            label_map = boolean_labels[col_name]
                        elif col_underscore in boolean_labels:
                            label_map = boolean_labels[col_underscore]
                        if label_map:
                            df[col] = df[col].apply(
                                lambda x, lm=label_map: lm.get(x, lm.get(bool(x) if x is not None else None, x))
                            )

                self.current_df = df

                sql_text.delete('1.0', tk.END)
                display_sql = sql
                for p in (params or []):
                    if isinstance(p, bool):
                        display_sql = display_sql.replace('%s', str(p).upper(), 1)
                    else:
                        display_sql = display_sql.replace('%s', f"'{p}'", 1)
                if applied_filters:
                    display_sql = f"-- Auto-filters: {', '.join(applied_filters)}\n{display_sql}"
                sql_text.insert('1.0', display_sql)

                tree.delete(*tree.get_children())
                tree['columns'] = list(df.columns)
                for col in df.columns:
                    tree.heading(col, text=col)
                    tree.column(col, width=100, minwidth=50)

                for _, row in df.head(1000).iterrows():
                    values = [str(v) if v is not None else '' for v in row]
                    tree.insert('', tk.END, values=values)

                status_var.set(f"Result: {len(df)} rows")
                result_notebook.select(1)

            except Exception as e:
                messagebox.showerror("Error", str(e))
                status_var.set("Error executing query")

        def export():
            if self.current_df is not None and len(self.current_df) > 0:
                self.db.export_to_excel(self.current_df)
            else:
                messagebox.showwarning("Warning", "No data to export!")

        tk.Button(btn_frame, text="Execute", font=('Segoe UI', 9),
                 bg=COLORS['danger'], fg='white', bd=0, padx=12, pady=4,
                 cursor='hand2', command=execute_query).pack(side=tk.LEFT)
        tk.Button(btn_frame, text="Export", font=('Segoe UI', 9),
                 bg=COLORS['primary'], fg='white', bd=0, padx=12, pady=4,
                 cursor='hand2', command=export).pack(side=tk.LEFT, padx=10)
        tk.Button(btn_frame, text="Clear", font=('Segoe UI', 9),
                 bg=COLORS['border'], fg=COLORS['text'], bd=0, padx=12, pady=4,
                 cursor='hand2', command=lambda: query_var.set('')).pack(side=tk.LEFT)

        query_entry.bind('<Return>', lambda e: execute_query())


def main():
    root = tk.Tk()
    app = AnQueryApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
