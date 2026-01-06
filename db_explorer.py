"""
Database Explorer Tool
======================
Tool untuk mengeksplorasi dan mengekstrak data dari PostgreSQL database
tanpa perlu mengetahui relasi antar tabel secara detail.

PENTING: Tool ini HANYA melakukan operasi READ (SELECT) saja!
"""

import psycopg2
import pandas as pd
from typing import List, Dict, Optional, Any
import os
from datetime import datetime
import tkinter as tk
from tkinter import filedialog


class DatabaseExplorer:
    """
    Database Explorer - Tool untuk mengeksplorasi database PostgreSQL
    dan mengekstrak data ke Excel tanpa perlu tahu relasi.
    """

    def __init__(self):
        """Initialize database connection"""
        self.conn_params = {
            'host': 'nexus-db.clvygcekiw3z.ap-southeast-1.rds.amazonaws.com',
            'port': 5432,
            'database': 'mongol',
            'user': 'postgres',
            'password': 'vWN81N364e^ZFJ*fbWt'
        }
        self.conn = None
        self.schema_cache = {}
        self.relations_cache = {}

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            print("✓ Berhasil terhubung ke database!")
            return True
        except Exception as e:
            print(f"✗ Gagal terhubung: {e}")
            return False

    def ensure_connection(self):
        """Check and reconnect if connection is lost"""
        need_reconnect = False

        # Check if connection exists and is open
        if self.conn is None:
            need_reconnect = True
        elif self.conn.closed:
            need_reconnect = True
        else:
            # Test connection with simple query
            try:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1")
            except Exception:
                need_reconnect = True

        if need_reconnect:
            print("⚠ Koneksi terputus, mencoba reconnect...")
            try:
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                self.conn = psycopg2.connect(**self.conn_params)
                print("✓ Berhasil reconnect!")
                return True
            except Exception as e:
                print(f"✗ Gagal reconnect: {e}")
                return False

        return True

    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("✓ Koneksi ditutup")

    def get_all_tables(self) -> List[str]:
        """Get all tables in the database"""
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            tables = [row[0] for row in cur.fetchall()]
        return tables

    def get_table_columns(self, table_name: str) -> List[Dict]:
        """Get all columns for a specific table"""
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (table_name,))
            columns = []
            for row in cur.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2],
                    'default': row[3]
                })
        return columns

    def get_all_relations(self) -> Dict:
        """Get all foreign key relations in the database"""
        query = """
        SELECT
            tc.table_name as from_table,
            kcu.column_name as from_column,
            ccu.table_name AS to_table,
            ccu.column_name AS to_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
        ORDER BY tc.table_name
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            relations = {}
            for row in cur.fetchall():
                from_table, from_col, to_table, to_col = row
                if from_table not in relations:
                    relations[from_table] = []
                relations[from_table].append({
                    'from_column': from_col,
                    'to_table': to_table,
                    'to_column': to_col
                })
        self.relations_cache = relations
        return relations

    def get_full_schema(self) -> Dict:
        """Get complete database schema with tables, columns, and relations"""
        tables = self.get_all_tables()
        relations = self.get_all_relations()

        schema = {}
        for table in tables:
            columns = self.get_table_columns(table)
            schema[table] = {
                'columns': columns,
                'relations': relations.get(table, [])
            }

        self.schema_cache = schema
        return schema

    def print_schema_summary(self):
        """Print a summary of the database schema"""
        if not self.schema_cache:
            self.get_full_schema()

        print("\n" + "="*60)
        print("DATABASE SCHEMA SUMMARY")
        print("="*60)

        for i, (table, info) in enumerate(self.schema_cache.items(), 1):
            print(f"\n{i}. {table}")
            print(f"   Columns: {len(info['columns'])}")
            col_names = [c['name'] for c in info['columns']]
            print(f"   - {', '.join(col_names[:5])}")
            if len(col_names) > 5:
                print(f"     ... dan {len(col_names)-5} kolom lainnya")

            if info['relations']:
                print(f"   Relations:")
                for rel in info['relations']:
                    print(f"   → {rel['from_column']} -> {rel['to_table']}.{rel['to_column']}")

    def find_related_tables(self, base_table: str) -> Dict:
        """Find all tables related to a base table (direct and indirect)"""
        if not self.relations_cache:
            self.get_all_relations()

        related = {'direct': [], 'indirect': []}

        # Direct relations (from base_table)
        if base_table in self.relations_cache:
            for rel in self.relations_cache[base_table]:
                related['direct'].append({
                    'table': rel['to_table'],
                    'join': f"{base_table}.{rel['from_column']} = {rel['to_table']}.{rel['to_column']}"
                })

        # Reverse relations (tables that reference base_table)
        for table, rels in self.relations_cache.items():
            for rel in rels:
                if rel['to_table'] == base_table:
                    related['direct'].append({
                        'table': table,
                        'join': f"{table}.{rel['from_column']} = {base_table}.{rel['to_column']}"
                    })

        return related

    def preview_table(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Preview data from a table"""
        query = f'SELECT * FROM "{table_name}" LIMIT {limit}'
        with self.conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
        return pd.DataFrame(data, columns=columns)

    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table"""
        query = f'SELECT COUNT(*) FROM "{table_name}"'
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]

    def smart_query(self,
                    base_table: str,
                    select_columns: Dict[str, List[str]],
                    filters: Optional[Dict] = None,
                    aggregations: Optional[Dict] = None,
                    limit: Optional[int] = None) -> pd.DataFrame:
        """
        Smart query builder - automatically handles joins based on relations

        Parameters:
        -----------
        base_table : str
            Tabel utama sebagai basis query
        select_columns : Dict[str, List[str]]
            Dictionary dengan format {table_name: [column1, column2, ...]}
        filters : Dict, optional
            Filter conditions {table.column: value}
        aggregations : Dict, optional
            Aggregation functions {alias: 'COUNT(table.column)'}
        limit : int, optional
            Limit hasil

        Returns:
        --------
        pd.DataFrame
            Hasil query dalam bentuk DataFrame
        """
        if not self.relations_cache:
            self.get_all_relations()

        # Build SELECT clause
        select_parts = []

        # Regular columns
        for table, columns in select_columns.items():
            for col in columns:
                select_parts.append(f'"{table}"."{col}" as "{table}_{col}"')

        # Aggregations
        if aggregations:
            for alias, agg_func in aggregations.items():
                select_parts.append(f'{agg_func} as "{alias}"')

        select_clause = ', '.join(select_parts)

        # Build FROM and JOIN clauses
        tables_needed = set(select_columns.keys())
        if filters:
            for key in filters.keys():
                tables_needed.add(key.split('.')[0])

        join_clauses = []
        joined_tables = {base_table}

        # Find and build joins
        for table in tables_needed:
            if table == base_table:
                continue

            join_found = False

            # Check direct relation from base_table
            if base_table in self.relations_cache:
                for rel in self.relations_cache[base_table]:
                    if rel['to_table'] == table:
                        join_clauses.append(
                            f'LEFT JOIN "{table}" ON "{base_table}"."{rel["from_column"]}" = "{table}"."{rel["to_column"]}"'
                        )
                        join_found = True
                        break

            # Check reverse relation
            if not join_found and table in self.relations_cache:
                for rel in self.relations_cache[table]:
                    if rel['to_table'] == base_table:
                        join_clauses.append(
                            f'LEFT JOIN "{table}" ON "{table}"."{rel["from_column"]}" = "{base_table}"."{rel["to_column"]}"'
                        )
                        join_found = True
                        break

            # Check indirect relations through joined tables
            if not join_found:
                for joined in list(joined_tables):
                    if joined in self.relations_cache:
                        for rel in self.relations_cache[joined]:
                            if rel['to_table'] == table:
                                join_clauses.append(
                                    f'LEFT JOIN "{table}" ON "{joined}"."{rel["from_column"]}" = "{table}"."{rel["to_column"]}"'
                                )
                                join_found = True
                                break

                    if not join_found and table in self.relations_cache:
                        for rel in self.relations_cache[table]:
                            if rel['to_table'] == joined:
                                join_clauses.append(
                                    f'LEFT JOIN "{table}" ON "{table}"."{rel["from_column"]}" = "{joined}"."{rel["to_column"]}"'
                                )
                                join_found = True
                                break

                    if join_found:
                        break

            if join_found:
                joined_tables.add(table)
            else:
                print(f"⚠ Warning: Tidak ditemukan relasi untuk tabel '{table}', akan di-cross join")
                join_clauses.append(f'CROSS JOIN "{table}"')

        # Build WHERE clause
        where_clause = ""
        params = []
        if filters:
            conditions = []
            for key, value in filters.items():
                table, col = key.split('.')
                conditions.append(f'"{table}"."{col}" = %s')
                params.append(value)
            where_clause = "WHERE " + " AND ".join(conditions)

        # Build GROUP BY clause (if aggregations exist)
        group_by_clause = ""
        if aggregations:
            group_cols = []
            for table, columns in select_columns.items():
                for col in columns:
                    group_cols.append(f'"{table}"."{col}"')
            if group_cols:
                group_by_clause = "GROUP BY " + ", ".join(group_cols)

        # Build final query
        query = f"""
        SELECT {select_clause}
        FROM "{base_table}"
        {' '.join(join_clauses)}
        {where_clause}
        {group_by_clause}
        """

        if limit:
            query += f" LIMIT {limit}"

        print("\n[Generated Query]")
        print("-" * 40)
        print(query)
        print("-" * 40)

        # Execute query
        with self.conn.cursor() as cur:
            cur.execute(query, params if params else None)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()

        return pd.DataFrame(data, columns=columns)

    def export_to_excel(self, df: pd.DataFrame, filename: str = None, sheet_name: str = "Data", use_dialog: bool = True):
        """Export DataFrame to Excel with optional save dialog"""

        # Default filename with format: export_HH-MM_DD-MM-YYYY.xlsx
        now = datetime.now()
        timestamp = now.strftime("%H-%M_%d-%m-%Y")  # Format: 14-30_05-01-2026
        default_filename = filename if filename else f"export_{timestamp}.xlsx"
        if not default_filename.endswith('.xlsx'):
            default_filename += '.xlsx'

        # Show save dialog if requested
        if use_dialog:
            try:
                # Create hidden root window
                root = tk.Tk()
                root.withdraw()
                root.attributes('-topmost', True)  # Bring dialog to front

                # Open save dialog
                filepath = filedialog.asksaveasfilename(
                    title="Simpan Excel ke...",
                    defaultextension=".xlsx",
                    filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                    initialfile=default_filename
                )

                root.destroy()

                if not filepath:
                    print("\n⚠ Export dibatalkan.")
                    return None

                filename = filepath
            except Exception as e:
                print(f"\n⚠ Dialog tidak tersedia: {e}")
                print(f"  Menggunakan nama file default: {default_filename}")
                filename = default_filename
        else:
            filename = default_filename

        # Export to Excel
        df.to_excel(filename, sheet_name=sheet_name, index=False)
        full_path = os.path.abspath(filename)
        print(f"\n✓ Data berhasil di-export ke: {full_path}")
        print(f"  Total rows: {len(df)}")
        print(f"  Total columns: {len(df.columns)}")
        return full_path


class InteractiveExplorer:
    """Interactive mode untuk eksplorasi database"""

    def __init__(self):
        self.db = DatabaseExplorer()

    def run(self):
        """Run interactive explorer"""
        print("\n" + "="*60)
        print("  DATABASE EXPLORER")
        print("  Tool untuk query database dan export ke Excel")
        print("  PENTING: Tool ini HANYA melakukan operasi READ!")
        print("="*60)

        if not self.db.connect():
            return

        try:
            while True:
                print("\n" + "="*60)
                print("[Menu Utama]")
                print("="*60)
                print("1. Cek Tabel (lihat daftar tabel & kolom)")
                print("2. Generate Smart Query (pilih kolom, filter, export Excel)")
                print("3. Preview Data (lihat isi data tabel)")
                print("4. Automate Generate Data (template siap pakai)")
                print("0. Keluar")

                choice = input("\nPilihan Anda: ").strip()

                if choice == '0':
                    break
                elif choice == '1':
                    self._cek_tabel()
                elif choice == '2':
                    self._smart_query_generator()
                elif choice == '3':
                    self._preview_data()
                elif choice == '4':
                    self._automate_generate()
                else:
                    print("Pilihan tidak valid!")

        finally:
            self.db.disconnect()

    def _cek_tabel(self):
        """Menu 1: Cek Tabel - lihat daftar tabel dan kolom"""
        print("\n" + "="*60)
        print("CEK TABEL")
        print("="*60)

        # Load schema if not loaded
        if not self.db.schema_cache:
            print("Loading schema...")
            self.db.get_full_schema()

        tables = list(self.db.schema_cache.keys())

        while True:
            print("\n[Daftar Tabel]")
            print("-"*60)
            for i, table in enumerate(tables, 1):
                col_count = len(self.db.schema_cache[table]['columns'])
                print(f"  {i:3}. {table} ({col_count} kolom)")

            print("\nKetik nomor/nama tabel untuk lihat kolom, atau 'b' untuk kembali")
            choice = input("Pilihan: ").strip()

            if choice.lower() == 'b':
                return

            # Get table name
            try:
                table_name = tables[int(choice) - 1]
            except:
                table_name = choice

            if table_name not in self.db.schema_cache:
                print(f"✗ Tabel '{table_name}' tidak ditemukan!")
                continue

            # Show columns
            print(f"\n[Kolom di tabel '{table_name}']")
            print("-"*60)
            columns = self.db.schema_cache[table_name]['columns']
            for i, col in enumerate(columns, 1):
                print(f"  {i:3}. {col['name']} ({col['type']})")

            # Show relations
            relations = self.db.schema_cache[table_name]['relations']
            if relations:
                print(f"\n[Relasi]")
                for rel in relations:
                    print(f"  → {rel['from_column']} -> {rel['to_table']}.{rel['to_column']}")

            input("\nTekan Enter untuk lanjut...")

    def _preview_data(self):
        """Menu 3: Preview Data - lihat isi data tabel"""
        print("\n" + "="*60)
        print("PREVIEW DATA")
        print("="*60)

        tables = self.db.get_all_tables()

        while True:
            print("\n[Pilih Tabel]")
            print("-"*60)
            for i, table in enumerate(tables, 1):
                print(f"  {i:3}. {table}")

            choice = input("\nPilih tabel (nomor/nama, 'b' untuk kembali): ").strip()

            if choice.lower() == 'b':
                return

            try:
                table_name = tables[int(choice) - 1]
            except:
                table_name = choice

            if table_name not in tables:
                print(f"✗ Tabel '{table_name}' tidak ditemukan!")
                continue

            # Get row count
            count = self.db.get_table_count(table_name)
            print(f"\nTotal data di '{table_name}': {count:,} baris")

            limit = input("Jumlah baris yang ditampilkan (default 10): ").strip()
            if limit.lower() == 'b':
                continue
            limit = int(limit) if limit else 10

            # Preview data
            print(f"\n[Preview: {table_name}]")
            print("-"*60)
            df = self.db.preview_table(table_name, limit)
            print(df.to_string())

            input("\nTekan Enter untuk lanjut...")

    def _automate_generate(self):
        """Menu 4: Automate Generate Data - Query dengan bahasa sederhana (Enhanced)"""
        print("\n" + "="*60)
        print("AUTOMATE GENERATE DATA (Enhanced)")
        print("Query database dengan format sederhana!")
        print("="*60)

        # Ensure connection and load schema
        if not self.db.ensure_connection():
            print("✗ Tidak dapat terhubung ke database!")
            input("\nTekan Enter untuk kembali...")
            return

        if not self.db.schema_cache:
            print("Loading schema...")
            self.db.get_full_schema()

        # Build column mapping from schema
        column_map = {}  # {'job_number': {'table': 'job', 'column': 'job_number', 'type': '...'}}
        table_columns = {}  # {'job': ['job_number', 'status', ...]}
        boolean_columns = {}  # {'job': ['is_completed', 'is_cancelled', ...]}

        for table, info in self.db.schema_cache.items():
            table_columns[table] = []
            boolean_columns[table] = []
            for col in info['columns']:
                col_name = col['name']
                col_type = col['type']
                table_columns[table].append(col_name)

                # Track boolean columns (is_xxx pattern)
                if col_name.startswith('is_') or col_type.lower() == 'boolean':
                    boolean_columns[table].append(col_name)

                # Map column name and table.column
                key = col_name.lower().replace('_', ' ')
                column_map[key] = {'table': table, 'column': col_name, 'type': col_type}
                # Also map with table prefix
                key_with_table = f"{table} {col_name}".lower().replace('_', ' ')
                column_map[key_with_table] = {'table': table, 'column': col_name, 'type': col_type}

        # ============================================================
        # ENHANCEMENT 3: Dynamic Aliases - Auto-detect from schema
        # ============================================================
        aliases = {}

        # Auto-generate aliases from schema
        for table, cols in table_columns.items():
            for col in cols:
                # Create natural aliases
                # e.g., "job number" for job.job_number
                if col.startswith(table + '_'):
                    alias_key = col.replace('_', ' ')
                    aliases[alias_key] = {'table': table, 'column': col}

                # Create table.column style alias
                # e.g., "job name" -> job.name
                alias_with_table = f"{table} {col}".replace('_', ' ')
                aliases[alias_with_table] = {'table': table, 'column': col}

                # Special handling for common column names
                if col in ['name', 'status', 'email', 'phone', 'created_on', 'updated_on']:
                    # e.g., "talent name", "company name"
                    aliases[f"{table} {col}".replace('_', ' ')] = {'table': table, 'column': col}

        # Add common manual aliases (override auto-generated if exists)
        manual_aliases = {
            'job number': {'table': 'job', 'column': 'job_number'},
            'talent name': {'table': 'talent', 'column': 'name'},
            'company name': {'table': 'company', 'column': 'name'},
            'schedule date': {'table': 'schedule', 'column': 'schedule_date'},
            'bank name': {'table': 'talent_bank_account', 'column': 'bank_name'},
            'account number': {'table': 'talent_bank_account', 'column': 'account_number'},
            'created': {'table': None, 'column': 'created_on'},
            'updated': {'table': None, 'column': 'updated_on'},
            'status': {'table': None, 'column': 'status'},
            'name': {'table': None, 'column': 'name'},
            'email': {'table': None, 'column': 'email'},
            'phone': {'table': None, 'column': 'phone'},
            'job status': {'table': 'job', 'column': 'job_status'},
            'schedule status': {'table': None, 'column': 'schedule_status'},
        }
        aliases.update(manual_aliases)

        # ============================================================
        # ENHANCEMENT 1: Supported Operators
        # ============================================================
        supported_operators = {
            '=': 'sama dengan',
            '!=': 'tidak sama dengan',
            '<>': 'tidak sama dengan',
            '>': 'lebih besar dari',
            '<': 'lebih kecil dari',
            '>=': 'lebih besar atau sama dengan',
            '<=': 'lebih kecil atau sama dengan',
            'like': 'mengandung (partial match)',
            'ilike': 'mengandung (case insensitive)',
            'in': 'salah satu dari (value1, value2, ...)',
            'not in': 'bukan salah satu dari',
            'is null': 'kosong/null',
            'is not null': 'tidak kosong',
            'between': 'antara dua nilai',
        }

        # ============================================================
        # ENHANCEMENT 5: Date Format Patterns
        # ============================================================
        def parse_date_flexible(date_str):
            """Parse date from various formats"""
            import re
            date_str = date_str.strip()

            # Month mapping
            month_map = {
                'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
                'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
            }

            # Try DD-MM-YYYY or DD/MM/YYYY
            match = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$', date_str)
            if match:
                day, month, year = match.groups()
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

            # Try YYYY-MM-DD or YYYY/MM/DD
            match = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', date_str)
            if match:
                year, month, day = match.groups()
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

            # Try DD MMM YYYY (e.g., 25 Dec 2025)
            match = re.match(r'^(\d{1,2})\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{4})$', date_str, re.IGNORECASE)
            if match:
                day, month_str, year = match.groups()
                month = month_map.get(month_str.lower()[:3], '01')
                return f"{year}-{month}-{day.zfill(2)}"

            # Try MMM DD, YYYY (e.g., Dec 25, 2025)
            match = re.match(r'^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2}),?\s+(\d{4})$', date_str, re.IGNORECASE)
            if match:
                month_str, day, year = match.groups()
                month = month_map.get(month_str.lower()[:3], '01')
                return f"{year}-{month}-{day.zfill(2)}"

            # Try DDMMYYYY (no separator)
            match = re.match(r'^(\d{2})(\d{2})(\d{4})$', date_str)
            if match:
                day, month, year = match.groups()
                return f"{year}-{month}-{day}"

            # Try YYYYMMDD (no separator)
            match = re.match(r'^(\d{4})(\d{2})(\d{2})$', date_str)
            if match:
                year, month, day = match.groups()
                return f"{year}-{month}-{day}"

            # If no pattern matches, return as-is (might already be correct format)
            return date_str

        # ============================================================
        # ENHANCEMENT 2: Smart Status Detection Helper
        # ============================================================
        def find_boolean_column_for_status(table, status_value, table_cols, bool_cols):
            """
            Smart detection untuk status -> boolean column mapping
            Returns: (column_name, bool_value) atau None jika tidak ditemukan
            """
            status_lower = status_value.lower().strip()

            # Patterns untuk deteksi status
            # Format: status_keyword -> (column_suffix, boolean_value)
            status_patterns = {
                # Completed variations
                'completed': ('is_completed', True),
                'complete': ('is_completed', True),
                'done': ('is_completed', True),
                'selesai': ('is_completed', True),
                'not completed': ('is_completed', False),
                'incomplete': ('is_completed', False),
                'belum selesai': ('is_completed', False),

                # Cancelled variations
                'cancelled': ('is_cancelled', True),
                'canceled': ('is_cancelled', True),
                'cancel': ('is_cancelled', True),
                'batal': ('is_cancelled', True),
                'not cancelled': ('is_cancelled', False),
                'not canceled': ('is_cancelled', False),

                # Active variations
                'active': ('is_active', True),
                'aktif': ('is_active', True),
                'inactive': ('is_active', False),
                'not active': ('is_active', False),
                'tidak aktif': ('is_active', False),

                # Paid variations
                'paid': ('is_paid', True),
                'lunas': ('is_paid', True),
                'unpaid': ('is_paid', False),
                'not paid': ('is_paid', False),
                'belum bayar': ('is_paid', False),

                # Verified variations
                'verified': ('is_verified', True),
                'terverifikasi': ('is_verified', True),
                'unverified': ('is_verified', False),
                'not verified': ('is_verified', False),

                # Approved variations
                'approved': ('is_approved', True),
                'disetujui': ('is_approved', True),
                'rejected': ('is_approved', False),
                'ditolak': ('is_approved', False),

                # Deleted variations (soft delete)
                'deleted': ('is_deleted', True),
                'dihapus': ('is_deleted', True),
                'not deleted': ('is_deleted', False),

                # Published variations
                'published': ('is_published', True),
                'unpublished': ('is_published', False),
                'draft': ('is_published', False),
            }

            # Check if status matches any pattern
            if status_lower in status_patterns:
                col_name, bool_val = status_patterns[status_lower]
                if col_name in table_cols:
                    return (col_name, bool_val)

            # Try to find is_xxx column matching status
            is_col = f"is_{status_lower.replace(' ', '_')}"
            if is_col in table_cols:
                return (is_col, True)

            # Try negation pattern: "not xxx" -> is_xxx = false
            if status_lower.startswith('not ') or status_lower.startswith('not_'):
                negated = status_lower.replace('not ', '').replace('not_', '').strip()
                neg_col = f"is_{negated.replace(' ', '_')}"
                if neg_col in table_cols:
                    return (neg_col, False)

            return None

        while True:
            print("\n" + "-"*60)
            print("FORMAT: show [kolom] where [kondisi] order by [kolom] [asc/desc]")
            print("-"*60)
            print("Contoh:")
            print("  show job number where status completed")
            print("  show job number where status cancel, created 23-12-2025")
            print("  show talent name, email where status active")
            print("  show job number, company name where status done")
            print("  show job number where created > 01-01-2025 order by created desc")
            print("  show talent name where amount >= 1000000")
            print("  show job number where status in (done, completed)")
            print("  show talent name where email is not null")
            print("  show job number where created between 01-01-2025 and 31-12-2025")
            print("\nOperator yang didukung: =, !=, >, <, >=, <=, like, in, is null, is not null, between")
            print("Ketik 'b' untuk kembali ke menu utama")
            print("Ketik 'help' untuk bantuan lebih lanjut")
            print("-"*60)

            user_input = input("\n> ").strip()

            if user_input.lower() == 'b':
                return

            # Help command
            if user_input.lower() == 'help':
                print("\n" + "="*60)
                print("BANTUAN - AUTOMATE GENERATE DATA")
                print("="*60)
                print("\n[FORMAT DASAR]")
                print("  show [kolom1], [kolom2] where [kondisi] order by [kolom] [asc/desc]")
                print("\n[OPERATOR YANG DIDUKUNG]")
                for op, desc in supported_operators.items():
                    print(f"  {op:12} : {desc}")
                print("\n[CONTOH PENGGUNAAN]")
                print("  show job number")
                print("  show talent name, email where status active")
                print("  show job number where created > 2025-01-01")
                print("  show talent name where amount between 1000000 and 5000000")
                print("  show job number where status in (done, completed, cancelled)")
                print("  show talent name where phone is not null order by name asc")
                print("\n[FORMAT TANGGAL YANG DIDUKUNG]")
                print("  DD-MM-YYYY  : 25-12-2025")
                print("  YYYY-MM-DD  : 2025-12-25")
                print("  DD/MM/YYYY  : 25/12/2025")
                print("  DD MMM YYYY : 25 Dec 2025")
                print("  MMM DD, YYYY: Dec 25, 2025")
                print("="*60)
                input("\nTekan Enter untuk lanjut...")
                continue

            if not user_input.lower().startswith('show '):
                print("✗ Format harus dimulai dengan 'show'")
                continue

            # Parse the input
            try:
                # Remove 'show ' prefix
                query_part = user_input[5:].strip()

                # ============================================================
                # ENHANCEMENT 6: Parse ORDER BY clause
                # ============================================================
                order_by_clause = None
                order_by_column = None
                order_by_direction = 'ASC'

                if ' order by ' in query_part.lower():
                    order_idx = query_part.lower().index(' order by ')
                    order_part = query_part[order_idx + 10:].strip()
                    query_part = query_part[:order_idx].strip()

                    # Parse order by: "column [asc|desc]"
                    order_parts = order_part.split()
                    if order_parts:
                        order_col_str = order_parts[0].lower()
                        if len(order_parts) > 1 and order_parts[-1].lower() in ['asc', 'desc']:
                            order_by_direction = order_parts[-1].upper()
                            # Column might be multi-word
                            order_col_str = ' '.join(order_parts[:-1]).lower()

                        # Resolve order column
                        order_by_column = {'column': order_col_str, 'table': None}

                # Split by 'where'
                if ' where ' in query_part.lower():
                    idx = query_part.lower().index(' where ')
                    columns_part = query_part[:idx].strip()
                    conditions_part = query_part[idx + 7:].strip()
                else:
                    columns_part = query_part
                    conditions_part = None

                # Parse columns (comma separated)
                select_columns = []
                base_table = None

                for col_str in columns_part.split(','):
                    col_str = col_str.strip().lower()

                    # Check aliases first
                    if col_str in aliases:
                        col_info = aliases[col_str].copy()
                    elif col_str in column_map:
                        col_info = column_map[col_str].copy()
                    else:
                        # Try fuzzy match
                        found = False
                        for key, info in column_map.items():
                            if col_str in key or key in col_str:
                                col_info = info.copy()
                                found = True
                                break
                        if not found:
                            print(f"✗ Kolom '{col_str}' tidak ditemukan")
                            raise ValueError(f"Column not found: {col_str}")

                    # Determine table if not set
                    if col_info.get('table') is None:
                        if base_table and col_info['column'] in table_columns.get(base_table, []):
                            col_info['table'] = base_table
                        else:
                            for t, cols in table_columns.items():
                                if col_info['column'] in cols:
                                    col_info['table'] = t
                                    break

                    if col_info.get('table') is None:
                        print(f"✗ Tidak dapat menentukan tabel untuk kolom '{col_str}'")
                        raise ValueError(f"Cannot determine table for: {col_str}")

                    select_columns.append(col_info)

                    if base_table is None:
                        base_table = col_info['table']

                # ============================================================
                # ENHANCED: Parse conditions with full operator support
                # ============================================================
                where_conditions = []
                if conditions_part:
                    import re

                    # Split conditions by comma, but not commas inside parentheses
                    # e.g., "status in (a, b, c), created > 2025" -> ["status in (a, b, c)", "created > 2025"]
                    cond_list = []
                    paren_depth = 0
                    current_cond = ""
                    for char in conditions_part:
                        if char == '(':
                            paren_depth += 1
                            current_cond += char
                        elif char == ')':
                            paren_depth -= 1
                            current_cond += char
                        elif char == ',' and paren_depth == 0:
                            if current_cond.strip():
                                cond_list.append(current_cond.strip())
                            current_cond = ""
                        else:
                            current_cond += char
                    if current_cond.strip():
                        cond_list.append(current_cond.strip())

                    for cond_str in cond_list:
                        cond_str = cond_str.strip()
                        if not cond_str:
                            continue

                        col_part = None
                        operator = '='
                        val_part = None
                        val_part2 = None  # For BETWEEN

                        # Try to detect operator
                        cond_lower = cond_str.lower()

                        # Check for BETWEEN ... AND ...
                        between_match = re.match(r'^(.+?)\s+between\s+(.+?)\s+and\s+(.+)$', cond_str, re.IGNORECASE)
                        if between_match:
                            col_part = between_match.group(1).strip().lower()
                            operator = 'BETWEEN'
                            val_part = between_match.group(2).strip()
                            val_part2 = between_match.group(3).strip()
                        # Check for IS NOT NULL
                        elif ' is not null' in cond_lower:
                            col_part = cond_str[:cond_lower.index(' is not null')].strip().lower()
                            operator = 'IS NOT NULL'
                            val_part = None
                        # Check for IS NULL
                        elif ' is null' in cond_lower:
                            col_part = cond_str[:cond_lower.index(' is null')].strip().lower()
                            operator = 'IS NULL'
                            val_part = None
                        # Check for NOT IN (...)
                        elif ' not in ' in cond_lower or ' not in(' in cond_lower.replace(' ', ''):
                            match = re.match(r'^(.+?)\s+not\s+in\s*\((.+)\)$', cond_str, re.IGNORECASE)
                            if match:
                                col_part = match.group(1).strip().lower()
                                operator = 'NOT IN'
                                val_part = [v.strip().strip("'\"") for v in match.group(2).split(',')]
                        # Check for IN (...)
                        elif ' in ' in cond_lower or ' in(' in cond_lower.replace(' ', ''):
                            match = re.match(r'^(.+?)\s+in\s*\((.+)\)$', cond_str, re.IGNORECASE)
                            if match:
                                col_part = match.group(1).strip().lower()
                                operator = 'IN'
                                val_part = [v.strip().strip("'\"") for v in match.group(2).split(',')]
                        # Check for comparison operators (>=, <=, !=, <>, >, <, =)
                        else:
                            op_match = re.match(r'^(.+?)\s*(>=|<=|!=|<>|>|<|=)\s*(.+)$', cond_str)
                            if op_match:
                                col_part = op_match.group(1).strip().lower()
                                operator = op_match.group(2).strip()
                                val_part = op_match.group(3).strip().strip("'\"")
                            # Check for LIKE / ILIKE
                            elif ' like ' in cond_lower:
                                idx = cond_lower.index(' like ')
                                col_part = cond_str[:idx].strip().lower()
                                operator = 'LIKE'
                                val_part = cond_str[idx + 6:].strip().strip("'\"")
                            elif ' ilike ' in cond_lower:
                                idx = cond_lower.index(' ilike ')
                                col_part = cond_str[:idx].strip().lower()
                                operator = 'ILIKE'
                                val_part = cond_str[idx + 7:].strip().strip("'\"")
                            else:
                                # Fallback: split by space (old behavior)
                                parts = cond_str.split()
                                if len(parts) < 2:
                                    continue

                                # Try 2-word column first
                                if len(parts) >= 3:
                                    two_word = f"{parts[0]} {parts[1]}".lower()
                                    if two_word in aliases or two_word in column_map:
                                        col_part = two_word
                                        val_part = ' '.join(parts[2:])
                                    else:
                                        col_part = parts[0].lower()
                                        val_part = ' '.join(parts[1:])
                                else:
                                    col_part = parts[0].lower()
                                    val_part = ' '.join(parts[1:])

                        if col_part is None:
                            print(f"⚠ Kondisi '{cond_str}' tidak dikenali, dilewati")
                            continue

                        # Resolve column
                        cond_col = None

                        # Check aliases first
                        if col_part in aliases:
                            cond_col = aliases[col_part].copy()
                        elif col_part in column_map:
                            cond_col = column_map[col_part].copy()
                        else:
                            # Try to find table with 'name' column if col_part matches a table name
                            for t in table_columns.keys():
                                if col_part == t.lower() or col_part in t.lower():
                                    if 'name' in table_columns[t]:
                                        cond_col = {'table': t, 'column': 'name'}
                                        break
                                    for c in table_columns[t]:
                                        if c != 'id' and not c.endswith('_id'):
                                            cond_col = {'table': t, 'column': c}
                                            break
                                    break

                            # Fuzzy match in column_map
                            if cond_col is None:
                                for key, info in column_map.items():
                                    if col_part in key or key in col_part:
                                        cond_col = info.copy()
                                        break

                        if cond_col is None:
                            print(f"⚠ Kondisi '{cond_str}' tidak dikenali, dilewati")
                            continue

                        # Determine table for condition column
                        if cond_col.get('table') is None:
                            if base_table and cond_col['column'] in table_columns.get(base_table, []):
                                cond_col['table'] = base_table
                            else:
                                for t, cols in table_columns.items():
                                    if cond_col['column'] in cols:
                                        cond_col['table'] = t
                                        break

                        if cond_col.get('table'):
                            where_conditions.append({
                                'table': cond_col['table'],
                                'column': cond_col['column'],
                                'operator': operator,
                                'value': val_part,
                                'value2': val_part2  # For BETWEEN
                            })

                # Build SQL query
                select_parts = []
                tables_needed = {base_table}

                for col in select_columns:
                    select_parts.append(f'"{col["table"]}"."{col["column"]}"')
                    tables_needed.add(col['table'])

                for cond in where_conditions:
                    tables_needed.add(cond['table'])

                # Build JOINs using BFS for complex paths
                if not self.db.relations_cache:
                    self.db.get_all_relations()

                join_clauses = []
                joined = {base_table}

                def find_relation(t1, t2):
                    """Find direct relation between two tables"""
                    if t1 in self.db.relations_cache:
                        for r in self.db.relations_cache[t1]:
                            if r['to_table'] == t2:
                                return ('fwd', r['from_column'], r['to_column'])
                    if t2 in self.db.relations_cache:
                        for r in self.db.relations_cache[t2]:
                            if r['to_table'] == t1:
                                return ('rev', r['from_column'], r['to_column'])
                    return None

                def build_graph():
                    """Build undirected graph of table relations"""
                    graph = {t: set() for t in self.db.schema_cache.keys()}
                    for t, rels in self.db.relations_cache.items():
                        for r in rels:
                            graph[t].add(r['to_table'])
                            if r['to_table'] in graph:
                                graph[r['to_table']].add(t)
                    return graph

                def bfs_path(start, end, graph):
                    """Find shortest path between two tables using BFS"""
                    if start == end:
                        return [start]
                    visited = {start}
                    queue = [[start]]
                    while queue:
                        path = queue.pop(0)
                        for neighbor in graph.get(path[-1], []):
                            if neighbor == end:
                                return path + [neighbor]
                            if neighbor not in visited:
                                visited.add(neighbor)
                                queue.append(path + [neighbor])
                    return None

                graph = build_graph()

                for table in tables_needed - {base_table}:
                    if table in joined:
                        continue

                    # Try direct relation first
                    added = False
                    for j in list(joined):
                        rel = find_relation(j, table)
                        if rel:
                            if rel[0] == 'fwd':
                                join_clauses.append(f'LEFT JOIN "{table}" ON "{j}"."{rel[1]}" = "{table}"."{rel[2]}"')
                            else:
                                join_clauses.append(f'LEFT JOIN "{table}" ON "{table}"."{rel[1]}" = "{j}"."{rel[2]}"')
                            joined.add(table)
                            added = True
                            break

                    # If no direct relation, use BFS to find path
                    if not added:
                        path = bfs_path(base_table, table, graph)
                        if path and len(path) > 1:
                            for i in range(len(path) - 1):
                                t1, t2 = path[i], path[i + 1]
                                if t2 not in joined:
                                    rel = find_relation(t1, t2)
                                    if rel:
                                        if rel[0] == 'fwd':
                                            join_clauses.append(f'LEFT JOIN "{t2}" ON "{t1}"."{rel[1]}" = "{t2}"."{rel[2]}"')
                                        else:
                                            join_clauses.append(f'LEFT JOIN "{t2}" ON "{t2}"."{rel[1]}" = "{t1}"."{rel[2]}"')
                                        joined.add(t2)
                        else:
                            print(f"⚠ Tidak ditemukan relasi untuk tabel '{table}'")

                # Check if all tables are joined
                missing_tables = tables_needed - joined
                if missing_tables:
                    print(f"⚠ Tabel berikut tidak dapat di-join: {missing_tables}")
                    print("  Query mungkin tidak menghasilkan data yang diharapkan")

                # ============================================================
                # ENHANCED: WHERE clause with full operator support
                # ============================================================
                where_parts = []
                params = []

                for cond in where_conditions:
                    col_type = None
                    table_cols = table_columns.get(cond['table'], [])
                    bool_cols = boolean_columns.get(cond['table'], [])
                    table_schema = self.db.schema_cache.get(cond['table'], {}).get('columns', [])

                    for c in table_schema:
                        if c['name'] == cond['column']:
                            col_type = c['type']
                            break

                    operator = cond.get('operator', '=')
                    value = cond.get('value')
                    value2 = cond.get('value2')

                    # Handle NULL operators first (no value needed)
                    if operator == 'IS NULL':
                        where_parts.append(f'"{cond["table"]}"."{cond["column"]}" IS NULL')
                        continue
                    elif operator == 'IS NOT NULL':
                        where_parts.append(f'"{cond["table"]}"."{cond["column"]}" IS NOT NULL')
                        continue

                    # ENHANCEMENT 2: Smart status detection using helper function
                    if value and isinstance(value, str):
                        value_lower = value.lower().strip()
                        bool_result = find_boolean_column_for_status(
                            cond['table'], value_lower, table_cols, bool_cols
                        )
                        if bool_result:
                            bool_col, bool_val = bool_result
                            where_parts.append(f'"{cond["table"]}"."{bool_col}" = %s')
                            params.append(bool_val)
                            continue

                    # Check if this is a date column
                    is_date_col = (cond['column'] in ['created_on', 'updated_on'] or
                                   'date' in (col_type or '').lower() or
                                   'timestamp' in (col_type or '').lower())

                    # Handle BETWEEN operator
                    if operator == 'BETWEEN':
                        if is_date_col:
                            formatted_val1 = parse_date_flexible(value)
                            formatted_val2 = parse_date_flexible(value2)
                            where_parts.append(f'"{cond["table"]}"."{cond["column"]}"::date BETWEEN %s AND %s')
                            params.append(formatted_val1)
                            params.append(formatted_val2)
                        else:
                            where_parts.append(f'"{cond["table"]}"."{cond["column"]}" BETWEEN %s AND %s')
                            params.append(value)
                            params.append(value2)
                        continue

                    # Handle IN / NOT IN operators
                    if operator in ['IN', 'NOT IN']:
                        if isinstance(value, list):
                            placeholders = ', '.join(['%s'] * len(value))
                            where_parts.append(f'"{cond["table"]}"."{cond["column"]}" {operator} ({placeholders})')
                            params.extend(value)
                        continue

                    # Handle LIKE / ILIKE
                    if operator in ['LIKE', 'ILIKE']:
                        # Add wildcards if not present
                        if '%' not in value:
                            value = f'%{value}%'
                        where_parts.append(f'"{cond["table"]}"."{cond["column"]}" {operator} %s')
                        params.append(value)
                        continue

                    # Handle date columns with comparison operators
                    if is_date_col:
                        formatted_date = parse_date_flexible(value)
                        where_parts.append(f'"{cond["table"]}"."{cond["column"]}"::date {operator} %s')
                        params.append(formatted_date)
                        continue

                    # Handle name column with default ILIKE (unless explicit operator)
                    if cond['column'] == 'name' and operator == '=':
                        where_parts.append(f'"{cond["table"]}"."{cond["column"]}" ILIKE %s')
                        params.append(f"%{value}%")
                        continue

                    # Default: use the operator as-is
                    where_parts.append(f'"{cond["table"]}"."{cond["column"]}" {operator} %s')
                    params.append(value)

                # Build final query
                query = f'SELECT {", ".join(select_parts)}\nFROM "{base_table}"'

                if join_clauses:
                    query += "\n" + "\n".join(join_clauses)

                if where_parts:
                    query += f"\nWHERE {' AND '.join(where_parts)}"

                # ============================================================
                # ENHANCEMENT 6: Add ORDER BY clause
                # ============================================================
                if order_by_column:
                    # Resolve order by column to table.column
                    order_col_str = order_by_column['column']
                    order_table = None
                    order_col = None

                    # Check aliases first
                    if order_col_str in aliases:
                        order_info = aliases[order_col_str]
                        order_table = order_info.get('table')
                        order_col = order_info.get('column')
                    elif order_col_str in column_map:
                        order_info = column_map[order_col_str]
                        order_table = order_info.get('table')
                        order_col = order_info.get('column')
                    else:
                        # Try fuzzy match
                        for key, info in column_map.items():
                            if order_col_str in key or key in order_col_str:
                                order_table = info.get('table')
                                order_col = info.get('column')
                                break

                    # If table not found, try to find in selected columns
                    if order_col and not order_table:
                        for sc in select_columns:
                            if sc['column'] == order_col or order_col_str in sc['column'].lower():
                                order_table = sc['table']
                                order_col = sc['column']
                                break

                    if order_table and order_col:
                        query += f'\nORDER BY "{order_table}"."{order_col}" {order_by_direction}'
                    else:
                        print(f"⚠ Kolom untuk ORDER BY '{order_col_str}' tidak ditemukan")

                # ============================================================
                # ENHANCEMENT 4: Preview and Confirmation before execute
                # ============================================================
                print("\n" + "="*60)
                print("PREVIEW QUERY")
                print("="*60)

                # Show query with parameters replaced for display
                display_query = query
                param_copy = list(params)
                for p in param_copy:
                    if isinstance(p, bool):
                        display_query = display_query.replace('%s', str(p).upper(), 1)
                    elif isinstance(p, (int, float)):
                        display_query = display_query.replace('%s', str(p), 1)
                    else:
                        display_query = display_query.replace('%s', f"'{p}'", 1)

                print("\n[SQL Query]")
                print("-"*60)
                print(display_query)
                print("-"*60)

                # Summary
                print("\n[Ringkasan]")
                print(f"  • Base Table: {base_table}")
                print(f"  • Kolom: {len(select_columns)}")
                for sc in select_columns:
                    print(f"    - {sc['table']}.{sc['column']}")
                if where_conditions:
                    print(f"  • Filter: {len(where_conditions)}")
                    for wc in where_conditions:
                        op = wc.get('operator', '=')
                        val = wc.get('value', '')
                        val2 = wc.get('value2', '')
                        if op in ['IS NULL', 'IS NOT NULL']:
                            print(f"    - {wc['table']}.{wc['column']} {op}")
                        elif op == 'BETWEEN':
                            print(f"    - {wc['table']}.{wc['column']} BETWEEN {val} AND {val2}")
                        elif op in ['IN', 'NOT IN'] and isinstance(val, list):
                            print(f"    - {wc['table']}.{wc['column']} {op} ({', '.join(val)})")
                        else:
                            print(f"    - {wc['table']}.{wc['column']} {op} {val}")
                if order_by_column and order_table and order_col:
                    print(f"  • Order By: {order_table}.{order_col} {order_by_direction}")

                print("-"*60)

                # Confirmation
                confirm = input("\nLanjutkan eksekusi query? (y/n, 'e' untuk edit): ").strip().lower()
                if confirm == 'n':
                    print("Query dibatalkan.")
                    continue
                elif confirm == 'e':
                    # Allow user to modify the query
                    print("\n[Edit Query]")
                    print("Ketik query baru atau tekan Enter untuk menggunakan query di atas:")
                    new_query = input("> ").strip()
                    if new_query:
                        query = new_query
                        params = []  # Clear params for custom query
                        print("✓ Query diubah!")

                # Ask for limit
                limit_input = input("\nLimit hasil (kosong = semua): ").strip()
                if limit_input:
                    try:
                        limit_val = int(limit_input)
                        query += f"\nLIMIT {limit_val}"
                    except ValueError:
                        print("⚠ Limit tidak valid, diabaikan")

                # Execute
                print("\nMenjalankan query...")

                if not self.db.ensure_connection():
                    print("✗ Koneksi database terputus!")
                    continue

                with self.db.conn.cursor() as cur:
                    cur.execute(query, params if params else None)
                    columns = [desc[0] for desc in cur.description]
                    data = cur.fetchall()

                df = pd.DataFrame(data, columns=columns)

                print(f"\n[Hasil: {len(df)} baris]")
                print("-"*60)

                if len(df) == 0:
                    print("Tidak ada data ditemukan.")
                else:
                    print(df.head(20).to_string())
                    if len(df) > 20:
                        print(f"\n... dan {len(df) - 20} baris lainnya")

                # Export options
                while True:
                    print("\n" + "-"*60)
                    print("Pilihan:")
                    print("  1. Export ke Excel")
                    print("  2. Query Baru")
                    print("  3. Kembali ke Menu Utama")
                    print("-"*60)

                    post_choice = input("Pilihan (1/2/3): ").strip()

                    if post_choice == '1':
                        if len(df) == 0:
                            print("\n⚠ Tidak ada data untuk di-export.")
                        else:
                            result = self.db.export_to_excel(df, use_dialog=True)
                            if result:
                                return
                        continue

                    elif post_choice == '2':
                        break

                    elif post_choice == '3':
                        return

                    else:
                        print("Pilihan tidak valid.")

            except ValueError:
                continue
            except Exception as e:
                print(f"\n✗ Error: {e}")
                input("\nTekan Enter untuk lanjut...")

    def _smart_query_generator(self):
        """Menu 2: Generate Smart Query - fitur utama (User Friendly)"""

        # Load schema
        if not self.db.schema_cache:
            print("\nLoading database schema...")
            self.db.get_full_schema()

        tables = list(self.db.schema_cache.keys())

        # Build table_columns_map for later use
        table_columns_map = {}
        for table, info in self.db.schema_cache.items():
            table_columns_map[table] = []
            for col in info['columns']:
                table_columns_map[table].append({'name': col['name'], 'type': col['type']})

        # No fixed options - show all tables from schema
        if not tables:
            print("\n✗ Tidak ada tabel ditemukan di database!")
            input("\nTekan Enter untuk kembali...")
            return

        # State variables
        current_step = 1
        base_table = None
        primary_name = None
        primary_column = None
        primary_column_hint = None
        num_additional = 0
        total_columns = 0
        selected_columns = []
        current_col_num = 2
        current_col_table = None  # Track selected table for column selection

        # Main loop with state machine
        while True:
            # ============================================================
            # STEP 1: Choose primary table from schema
            # ============================================================
            if current_step == 1:
                print("\n" + "="*70)
                print("  GENERATE SMART QUERY")
                print("  Pilih data yang ingin ditampilkan dengan mudah!")
                print("="*70)
                print("\n" + "-"*70)
                print("STEP 1: Pilih PRIMARY TABLE (tabel utama)")
                print("-"*70)

                print("\n[Daftar Tabel]")
                print("-"*50)
                for i, t in enumerate(tables, 1):
                    col_count = len(table_columns_map[t])
                    print(f"  {i:3}. {t} ({col_count} kolom)")

                base_input = input("\nPilih tabel (nomor/nama, 'b' untuk kembali ke menu): ").strip()
                if base_input.lower() == 'b':
                    return  # Back to main menu from step 1

                # Get table name
                try:
                    base_table = tables[int(base_input) - 1]
                except:
                    # Try matching by name
                    found = False
                    for t in tables:
                        if base_input.lower() in t.lower():
                            base_table = t
                            found = True
                            break
                    if not found:
                        print(f"✗ Tabel '{base_input}' tidak ditemukan!")
                        continue

                if base_table not in table_columns_map:
                    print(f"✗ Tabel '{base_table}' tidak ditemukan!")
                    continue

                print(f"\n✓ Primary Table: {base_table}")
                current_step = 1.5  # Go to select primary column

            # ============================================================
            # STEP 1.5: Select primary column from the table
            # ============================================================
            elif current_step == 1.5:
                print("\n" + "-"*70)
                print(f"STEP 1b: Pilih KOLOM PRIMARY dari tabel '{base_table}'")
                print("-"*70)
                print("Kolom primary adalah kolom utama yang akan ditampilkan (misal: job_number, name, dll)")

                print(f"\n[Kolom di tabel '{base_table}']")
                print("-"*50)
                table_cols = table_columns_map[base_table]
                for i, col in enumerate(table_cols, 1):
                    print(f"  {i:3}. {col['name']} ({col['type']})")

                col_input = input("\nPilih kolom primary (nomor, 'b' untuk kembali): ").strip()
                if col_input.lower() == 'b':
                    current_step = 1  # Back to table selection
                    continue

                try:
                    col_idx = int(col_input) - 1
                    if 0 <= col_idx < len(table_cols):
                        chosen_col = table_cols[col_idx]
                        primary_column = {
                            'table': base_table,
                            'column': chosen_col['name'],
                            'display': f"{base_table}.{chosen_col['name']}"
                        }
                        print(f"\n✓ Kolom Primary: {primary_column['display']}")
                        current_step = 2
                    else:
                        print("✗ Nomor tidak valid!")
                except:
                    print("✗ Input tidak valid!")

            # ============================================================
            # STEP 2: Ask how many additional columns
            # ============================================================
            elif current_step == 2:
                print("\n" + "-"*70)
                print("STEP 2: Berapa kolom tambahan yang ingin ditampilkan?")
                print("-"*70)
                print(f"\nKolom 1 (Primary): {primary_column['display']}")
                print("\nContoh: jika input 2, maka total kolom = 3 (1 primary + 2 tambahan)")

                num_input = input("\nJumlah kolom tambahan (1-10, 'b' untuk kembali): ").strip()
                if num_input.lower() == 'b':
                    current_step = 1.5  # Back to select primary column
                    continue

                try:
                    num_additional = int(num_input)
                    if num_additional < 1 or num_additional > 10:
                        print("✗ Jumlah harus antara 1-10!")
                    else:
                        total_columns = 1 + num_additional
                        print(f"\n✓ Total kolom yang akan ditampilkan: {total_columns}")
                        selected_columns = [primary_column]
                        current_col_num = 2
                        current_step = 3
                except:
                    print("✗ Input tidak valid!")

            # ============================================================
            # STEP 3: Select table for column
            # ============================================================
            elif current_step == 3:
                print("\n" + "-"*70)
                print(f"KOLOM {current_col_num}: Pilih tabel")
                print("-"*70)

                print("\n[Kolom yang sudah dipilih]")
                for i, sc in enumerate(selected_columns, 1):
                    print(f"  {i}. {sc['display']}")

                print("\n[Pilih Tabel]")
                print("-"*50)
                for i, t in enumerate(tables, 1):
                    col_count = len(table_columns_map[t])
                    print(f"  {i:3}. {t} ({col_count} kolom)")

                print("\n" + "-"*50)
                print("Ketik 'cari [keyword]' untuk mencari tabel")
                print("-"*50)

                table_input = input(f"\nPilih tabel (nomor/nama, 'cari [keyword]', 'b' kembali): ").strip()
                if table_input.lower() == 'b':
                    if current_col_num == 2:
                        current_step = 2  # Back to step 2
                    else:
                        # Remove last selected column and go back
                        if len(selected_columns) > 1:
                            selected_columns.pop()
                        current_col_num -= 1
                        current_step = 4  # Go to column selection of previous
                    continue

                # Search table feature
                if table_input.lower().startswith('cari '):
                    keyword = table_input[5:].strip().lower()
                    print(f"\n[Hasil pencarian tabel: '{keyword}']")
                    print("-"*50)
                    search_results = []
                    for i, t in enumerate(tables):
                        if keyword in t.lower():
                            search_results.append({'index': i, 'name': t})

                    if search_results:
                        for i, r in enumerate(search_results, 1):
                            col_count = len(table_columns_map[r['name']])
                            print(f"  {i:3}. {r['name']} ({col_count} kolom)")

                        print("\n" + "-"*50)
                        pick = input("Pilih nomor dari hasil pencarian (atau Enter untuk kembali): ").strip()
                        if pick:
                            try:
                                pick_idx = int(pick) - 1
                                if 0 <= pick_idx < len(search_results):
                                    found_table = search_results[pick_idx]['name']
                                    print(f"\n✓ Tabel: {found_table}")
                                    current_col_table = found_table
                                    current_step = 4
                                else:
                                    print("✗ Nomor tidak valid!")
                            except:
                                print("✗ Input tidak valid!")
                    else:
                        print(f"  Tidak ditemukan tabel dengan keyword '{keyword}'")
                    continue

                try:
                    selected_table = tables[int(table_input) - 1]
                except:
                    selected_table = table_input

                found_table = None
                for t in tables:
                    if selected_table.lower() == t.lower():  # Exact match first
                        found_table = t
                        break
                if not found_table:
                    for t in tables:
                        if selected_table.lower() in t.lower():
                            found_table = t
                            break

                if not found_table:
                    print(f"✗ Tabel '{selected_table}' tidak ditemukan!")
                else:
                    print(f"\n✓ Tabel: {found_table}")
                    current_col_table = found_table
                    current_step = 4  # Go to column selection

            # ============================================================
            # STEP 4: Select column from table
            # ============================================================
            elif current_step == 4:
                print(f"\n[Kolom di tabel '{current_col_table}']")
                print("-"*50)
                table_cols = table_columns_map[current_col_table]
                for i, col in enumerate(table_cols, 1):
                    already_selected = any(sc['table'] == current_col_table and sc['column'] == col['name'] for sc in selected_columns)
                    mark = "✓" if already_selected else " "
                    print(f"  {mark} {i:3}. {col['name']} ({col['type']})")

                print("\n" + "-"*50)
                print("Ketik 'cari [keyword]' untuk mencari kolom di semua tabel")
                print("-"*50)

                col_input = input(f"\nPilih kolom (nomor, 'cari [keyword]', 'b' kembali): ").strip()
                if col_input.lower() == 'b':
                    current_step = 3  # Back to table selection
                    continue

                # Search feature - cari kolom di semua tabel
                if col_input.lower().startswith('cari '):
                    keyword = col_input[5:].strip().lower()
                    print(f"\n[Hasil pencarian: '{keyword}']")
                    print("-"*50)
                    search_results = []
                    for t, cols in table_columns_map.items():
                        for col in cols:
                            if keyword in col['name'].lower():
                                search_results.append({'table': t, 'column': col['name'], 'type': col['type']})

                    if search_results:
                        for i, r in enumerate(search_results, 1):
                            already = any(sc['table'] == r['table'] and sc['column'] == r['column'] for sc in selected_columns)
                            mark = "✓" if already else " "
                            print(f"  {mark} {i:3}. {r['table']}.{r['column']} ({r['type']})")

                        print("\n" + "-"*50)
                        pick = input("Pilih nomor dari hasil pencarian (atau Enter untuk kembali): ").strip()
                        if pick:
                            try:
                                pick_idx = int(pick) - 1
                                if 0 <= pick_idx < len(search_results):
                                    chosen = search_results[pick_idx]
                                    new_column = {
                                        'table': chosen['table'],
                                        'column': chosen['column'],
                                        'display': f"{chosen['table']}.{chosen['column']}"
                                    }
                                    if any(sc['table'] == new_column['table'] and sc['column'] == new_column['column'] for sc in selected_columns):
                                        print(f"⚠ Kolom '{new_column['display']}' sudah dipilih sebelumnya!")
                                    else:
                                        selected_columns.append(new_column)
                                        print(f"\n✓ Kolom {current_col_num}: {new_column['display']}")
                                        current_col_num += 1
                                        if current_col_num > total_columns:
                                            current_step = 5
                                        else:
                                            current_step = 3
                                else:
                                    print("✗ Nomor tidak valid!")
                            except:
                                print("✗ Input tidak valid!")
                    else:
                        print(f"  Tidak ditemukan kolom dengan keyword '{keyword}'")
                    continue

                try:
                    col_idx = int(col_input) - 1
                    if 0 <= col_idx < len(table_cols):
                        chosen_col = table_cols[col_idx]
                        new_column = {
                            'table': current_col_table,
                            'column': chosen_col['name'],
                            'display': f"{current_col_table}.{chosen_col['name']}"
                        }

                        if any(sc['table'] == new_column['table'] and sc['column'] == new_column['column'] for sc in selected_columns):
                            print(f"⚠ Kolom '{new_column['display']}' sudah dipilih sebelumnya!")
                        else:
                            selected_columns.append(new_column)
                            print(f"\n✓ Kolom {current_col_num}: {new_column['display']}")
                            current_col_num += 1

                            if current_col_num > total_columns:
                                current_step = 5  # Go to review
                            else:
                                current_step = 3  # Next column - select table
                    else:
                        print("✗ Nomor tidak valid!")
                except:
                    print("✗ Input tidak valid!")

            # ============================================================
            # STEP 5: Review and options
            # ============================================================
            elif current_step == 5:
                print("\n" + "="*70)
                print("REVIEW KOLOM YANG DIPILIH")
                print("="*70)
                for i, sc in enumerate(selected_columns, 1):
                    label = "(PRIMARY)" if i == 1 else ""
                    print(f"  {i}. {sc['display']} {label}")

                print("\n" + "-"*50)
                print("Pilihan:")
                print("  1. Back (kembali pilih kolom)")
                print("  2. Tambah Kolom")
                print("  3. Generate Excel")
                print("  4. Edit Kolom (ganti kolom tertentu)")
                print("-"*50)

                choice = input("\nPilih (1/2/3/4): ").strip()

                if choice == '1' or choice.lower() == 'b':
                    # Remove last column and go back to column selection
                    if len(selected_columns) > 1:
                        selected_columns.pop()
                        current_col_num -= 1
                        total_columns = current_col_num  # Adjust total
                    current_step = 3  # Back to table selection

                elif choice == '2':
                    # Add more columns
                    total_columns += 1
                    current_step = 3  # Go to table selection for new column

                elif choice == '3':
                    # Generate Excel - proceed to filter/query
                    current_step = 6
                    # Don't break, continue to step 6

                elif choice == '4':
                    # Edit specific column
                    print("\n[Edit Kolom]")
                    print("-"*50)
                    for i, sc in enumerate(selected_columns, 1):
                        label = "(PRIMARY)" if i == 1 else ""
                        print(f"  {i}. {sc['display']} {label}")

                    edit_input = input("\nPilih nomor kolom yang ingin diedit (atau Enter untuk batal): ").strip()
                    if edit_input:
                        try:
                            edit_idx = int(edit_input) - 1
                            if 0 <= edit_idx < len(selected_columns):
                                old_col = selected_columns[edit_idx]
                                print(f"\n→ Mengedit kolom {edit_idx + 1}: {old_col['display']}")

                                # Store the index to edit
                                edit_col_index = edit_idx

                                # Go to table selection for replacement
                                print("\n[Pilih tabel baru]")
                                print("-"*50)
                                for i, t in enumerate(tables, 1):
                                    col_count = len(table_columns_map[t])
                                    print(f"  {i:3}. {t} ({col_count} kolom)")

                                print("\n" + "-"*50)
                                print("Ketik 'cari [keyword]' untuk mencari tabel")
                                print("-"*50)

                                table_input = input("\nPilih tabel (nomor/nama, 'cari [keyword]', Enter batal): ").strip()
                                if not table_input:
                                    continue

                                # Search table in edit mode
                                if table_input.lower().startswith('cari '):
                                    keyword = table_input[5:].strip().lower()
                                    print(f"\n[Hasil pencarian tabel: '{keyword}']")
                                    print("-"*50)
                                    search_results = []
                                    for i, t in enumerate(tables):
                                        if keyword in t.lower():
                                            search_results.append({'index': i, 'name': t})

                                    if search_results:
                                        for i, r in enumerate(search_results, 1):
                                            col_count = len(table_columns_map[r['name']])
                                            print(f"  {i:3}. {r['name']} ({col_count} kolom)")

                                        pick = input("\nPilih nomor: ").strip()
                                        if pick:
                                            try:
                                                pick_idx = int(pick) - 1
                                                if 0 <= pick_idx < len(search_results):
                                                    edit_table = search_results[pick_idx]['name']
                                                else:
                                                    print("✗ Nomor tidak valid!")
                                                    continue
                                            except:
                                                print("✗ Input tidak valid!")
                                                continue
                                        else:
                                            continue
                                    else:
                                        print(f"  Tidak ditemukan tabel dengan keyword '{keyword}'")
                                        continue
                                else:
                                    try:
                                        edit_table = tables[int(table_input) - 1]
                                    except:
                                        edit_table = None
                                        for t in tables:
                                            if table_input.lower() == t.lower():
                                                edit_table = t
                                                break
                                        if not edit_table:
                                            for t in tables:
                                                if table_input.lower() in t.lower():
                                                    edit_table = t
                                                    break

                                    if not edit_table:
                                        print(f"✗ Tabel '{table_input}' tidak ditemukan!")
                                        continue

                                print(f"\n✓ Tabel: {edit_table}")

                                # Show columns from selected table
                                print(f"\n[Kolom di tabel '{edit_table}']")
                                print("-"*50)
                                table_cols = table_columns_map[edit_table]
                                for i, col in enumerate(table_cols, 1):
                                    print(f"  {i:3}. {col['name']} ({col['type']})")

                                print("\n" + "-"*50)
                                print("Ketik 'cari [keyword]' untuk mencari kolom di semua tabel")
                                print("-"*50)

                                col_input = input("\nPilih kolom (nomor, 'cari [keyword]', Enter batal): ").strip()
                                if not col_input:
                                    continue

                                # Search column in edit mode
                                if col_input.lower().startswith('cari '):
                                    keyword = col_input[5:].strip().lower()
                                    print(f"\n[Hasil pencarian: '{keyword}']")
                                    print("-"*50)
                                    search_results = []
                                    for t, cols in table_columns_map.items():
                                        for col in cols:
                                            if keyword in col['name'].lower():
                                                search_results.append({'table': t, 'column': col['name'], 'type': col['type']})

                                    if search_results:
                                        for i, r in enumerate(search_results, 1):
                                            print(f"  {i:3}. {r['table']}.{r['column']} ({r['type']})")

                                        pick = input("\nPilih nomor: ").strip()
                                        if pick:
                                            try:
                                                pick_idx = int(pick) - 1
                                                if 0 <= pick_idx < len(search_results):
                                                    chosen = search_results[pick_idx]
                                                    new_column = {
                                                        'table': chosen['table'],
                                                        'column': chosen['column'],
                                                        'display': f"{chosen['table']}.{chosen['column']}"
                                                    }
                                                    selected_columns[edit_col_index] = new_column
                                                    print(f"\n✓ Kolom {edit_col_index + 1} diganti: {old_col['display']} → {new_column['display']}")
                                                else:
                                                    print("✗ Nomor tidak valid!")
                                            except:
                                                print("✗ Input tidak valid!")
                                    else:
                                        print(f"  Tidak ditemukan kolom dengan keyword '{keyword}'")
                                else:
                                    try:
                                        col_idx = int(col_input) - 1
                                        if 0 <= col_idx < len(table_cols):
                                            chosen_col = table_cols[col_idx]
                                            new_column = {
                                                'table': edit_table,
                                                'column': chosen_col['name'],
                                                'display': f"{edit_table}.{chosen_col['name']}"
                                            }
                                            selected_columns[edit_col_index] = new_column
                                            print(f"\n✓ Kolom {edit_col_index + 1} diganti: {old_col['display']} → {new_column['display']}")
                                        else:
                                            print("✗ Nomor tidak valid!")
                                    except:
                                        print("✗ Input tidak valid!")
                            else:
                                print("✗ Nomor kolom tidak valid!")
                        except:
                            print("✗ Input tidak valid!")

                else:
                    print("✗ Pilihan tidak valid!")

            # ============================================================
            # STEP 6: Filter (Optional)
            # ============================================================
            elif current_step == 6:
                print("\n" + "-"*70)
                print("STEP 6: Filter data (opsional)")
                print("-"*70)
                print("Filter memungkinkan menyaring data berdasarkan nilai tertentu")

                # Initialize filters only on first visit
                if not hasattr(self, '_temp_filters'):
                    self._temp_filters = []
                filters = self._temp_filters

                while True:
                    if filters:
                        print("\n[Filter aktif]")
                        for i, f in enumerate(filters, 1):
                            if f.get('value2'):
                                print(f"  {i}. {f['display']}: {f['value']} s/d {f['value2']}")
                            else:
                                print(f"  {i}. {f['display']} {f['operator']} '{f['value']}'")

                    print("\n[Pilih Filter]")
                    print("  1. Created On (tanggal dibuat)")
                    print("  2. Updated On (tanggal diupdate)")
                    print("  3. Status")
                    print("  4. Company")
                    print("  0. Selesai / Lanjut ke export")
                    print("  b. Kembali ke review")

                    fchoice = input("\nPilihan: ").strip().lower()

                    if fchoice == '0' or fchoice == '':
                        current_step = 7
                        break
                    elif fchoice == 'b':
                        current_step = 5
                        break

                    # Handle filter based on choice
                    if fchoice == '1':
                        # Created On filter
                        print("\n[Filter Created On]")
                        print("  1. Single date (tanggal tertentu)")
                        print("  2. Date range (rentang tanggal)")
                        date_type = input("Pilih (1/2): ").strip()

                        if date_type == '1':
                            date_val = input("Masukkan tanggal (YYYY-MM-DD): ").strip()
                            if date_val:
                                filters.append({
                                    'table': base_table,
                                    'column': 'created_on',
                                    'display': 'Created On',
                                    'operator': '::date =',
                                    'value': date_val,
                                    'is_date': True
                                })
                                print(f"✓ Filter Created On = {date_val} ditambahkan!")
                        elif date_type == '2':
                            date_from = input("Dari tanggal (YYYY-MM-DD): ").strip()
                            date_to = input("Sampai tanggal (YYYY-MM-DD): ").strip()
                            if date_from and date_to:
                                filters.append({
                                    'table': base_table,
                                    'column': 'created_on',
                                    'display': 'Created On (range)',
                                    'operator': 'BETWEEN',
                                    'value': date_from,
                                    'value2': date_to,
                                    'is_date_range': True
                                })
                                print(f"✓ Filter Created On {date_from} s/d {date_to} ditambahkan!")

                    elif fchoice == '2':
                        # Updated On filter
                        print("\n[Filter Updated On]")
                        print("  1. Single date (tanggal tertentu)")
                        print("  2. Date range (rentang tanggal)")
                        date_type = input("Pilih (1/2): ").strip()

                        if date_type == '1':
                            date_val = input("Masukkan tanggal (YYYY-MM-DD): ").strip()
                            if date_val:
                                filters.append({
                                    'table': base_table,
                                    'column': 'updated_on',
                                    'display': 'Updated On',
                                    'operator': '::date =',
                                    'value': date_val,
                                    'is_date': True
                                })
                                print(f"✓ Filter Updated On = {date_val} ditambahkan!")
                        elif date_type == '2':
                            date_from = input("Dari tanggal (YYYY-MM-DD): ").strip()
                            date_to = input("Sampai tanggal (YYYY-MM-DD): ").strip()
                            if date_from and date_to:
                                filters.append({
                                    'table': base_table,
                                    'column': 'updated_on',
                                    'display': 'Updated On (range)',
                                    'operator': 'BETWEEN',
                                    'value': date_from,
                                    'value2': date_to,
                                    'is_date_range': True
                                })
                                print(f"✓ Filter Updated On {date_from} s/d {date_to} ditambahkan!")

                    elif fchoice == '3':
                        # Status filter
                        print("\n[Filter Status]")
                        status_val = input("Masukkan nilai status: ").strip()
                        if status_val:
                            filters.append({
                                'table': base_table,
                                'column': 'status',
                                'display': 'Status',
                                'operator': '=',
                                'value': status_val
                            })
                            print(f"✓ Filter Status = '{status_val}' ditambahkan!")

                    elif fchoice == '4':
                        # Company filter - uses company table with name column
                        print("\n[Filter Company]")
                        print("Masukkan nama company (bisa partial, akan menggunakan LIKE)")
                        company_val = input("Nama company: ").strip()
                        if company_val:
                            filters.append({
                                'table': 'company',
                                'column': 'name',
                                'display': 'Company Name',
                                'operator': 'ILIKE',
                                'value': f"%{company_val}%"
                            })
                            print(f"✓ Filter Company Name LIKE '%{company_val}%' ditambahkan!")

                    else:
                        print("✗ Pilihan tidak valid!")

            # ============================================================
            # STEP 7: Generate and Execute Query
            # ============================================================
            elif current_step == 7:
                print("\n" + "-"*70)
                print("STEP 7: Generate & Export ke Excel")
                print("-"*70)

                # Ensure connection before any database operation
                if not self.db.ensure_connection():
                    print("✗ Tidak dapat terhubung ke database!")
                    input("\nTekan Enter untuk kembali ke menu utama...")
                    if hasattr(self, '_temp_filters'):
                        delattr(self, '_temp_filters')
                    return

                # Get filters
                filters = getattr(self, '_temp_filters', [])

                # Load relations
                if not self.db.relations_cache:
                    self.db.get_all_relations()

                # Build SELECT
                select_parts = [f'"{sc["table"]}"."{sc["column"]}" as "{sc["table"]}_{sc["column"]}"' for sc in selected_columns]
                select_clause = ', '.join(select_parts)

                # Tables needed
                tables_needed = set(sc['table'] for sc in selected_columns)
                for f in filters:
                    tables_needed.add(f['table'])

                # Build JOINs using BFS
                def find_relation(t1, t2):
                    if t1 in self.db.relations_cache:
                        for r in self.db.relations_cache[t1]:
                            if r['to_table'] == t2:
                                return ('fwd', r['from_column'], r['to_column'])
                    if t2 in self.db.relations_cache:
                        for r in self.db.relations_cache[t2]:
                            if r['to_table'] == t1:
                                return ('rev', r['from_column'], r['to_column'])
                    return None

                def build_graph():
                    g = {t: set() for t in self.db.schema_cache.keys()}
                    for t, rels in self.db.relations_cache.items():
                        for r in rels:
                            g[t].add(r['to_table'])
                            g[r['to_table']].add(t)
                    return g

                def bfs_path(start, end, graph):
                    if start == end:
                        return [start]
                    visited = {start}
                    queue = [[start]]
                    while queue:
                        path = queue.pop(0)
                        for neighbor in graph.get(path[-1], []):
                            if neighbor == end:
                                return path + [neighbor]
                            if neighbor not in visited:
                                visited.add(neighbor)
                                queue.append(path + [neighbor])
                    return None

                join_clauses = []
                joined = {base_table}
                graph = build_graph()

                for table in tables_needed - {base_table}:
                    if table in joined:
                        continue

                    # Direct relation
                    added = False
                    for j in list(joined):
                        rel = find_relation(j, table)
                        if rel:
                            if rel[0] == 'fwd':
                                join_clauses.append(f'LEFT JOIN "{table}" ON "{j}"."{rel[1]}" = "{table}"."{rel[2]}"')
                            else:
                                join_clauses.append(f'LEFT JOIN "{table}" ON "{table}"."{rel[1]}" = "{j}"."{rel[2]}"')
                            joined.add(table)
                            added = True
                            break

                    # BFS for indirect
                    if not added:
                        path = bfs_path(base_table, table, graph)
                        if path:
                            for i in range(len(path) - 1):
                                t1, t2 = path[i], path[i+1]
                                if t2 not in joined:
                                    rel = find_relation(t1, t2)
                                    if rel:
                                        if rel[0] == 'fwd':
                                            join_clauses.append(f'LEFT JOIN "{t2}" ON "{t1}"."{rel[1]}" = "{t2}"."{rel[2]}"')
                                        else:
                                            join_clauses.append(f'LEFT JOIN "{t2}" ON "{t2}"."{rel[1]}" = "{t1}"."{rel[2]}"')
                                        joined.add(t2)

                # WHERE
                where_parts = []
                params = []
                for f in filters:
                    if f.get('is_date_range'):
                        # Date range with BETWEEN
                        where_parts.append(f'"{f["table"]}"."{f["column"]}"::date BETWEEN %s AND %s')
                        params.append(f['value'])
                        params.append(f['value2'])
                    elif f.get('is_date'):
                        # Single date (cast to date for comparison)
                        where_parts.append(f'"{f["table"]}"."{f["column"]}"::date = %s')
                        params.append(f['value'])
                    else:
                        # Regular filter
                        where_parts.append(f'"{f["table"]}"."{f["column"]}" {f["operator"]} %s')
                        params.append(f['value'])

                where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

                # Build query
                query = f"""SELECT {select_clause}
FROM "{base_table}"
{chr(10).join(join_clauses)}
{where_clause}"""

                print("\n[SQL Query]")
                print("-"*60)
                print(query)
                if params:
                    print(f"Parameters: {params}")
                    # Show query with replaced parameters for manual testing
                    query_preview = query
                    for p in params:
                        query_preview = query_preview.replace('%s', f"'{p}'", 1)
                    print("\n[Query untuk test manual]")
                    print(query_preview)
                print("-"*60)

                # Limit
                limit_input = input("\nLimit hasil (kosong = semua): ").strip()
                if limit_input:
                    query += f"\nLIMIT {int(limit_input)}"

                # Execute
                print("\nMenjalankan query...")

                # Ensure connection is alive before executing
                if not self.db.ensure_connection():
                    print("✗ Tidak dapat terhubung ke database!")
                    input("\nTekan Enter untuk kembali ke menu utama...")
                    if hasattr(self, '_temp_filters'):
                        delattr(self, '_temp_filters')
                    return

                try:
                    with self.db.conn.cursor() as cur:
                        cur.execute(query, params if params else None)
                        columns = [desc[0] for desc in cur.description]
                        data = cur.fetchall()

                    df = pd.DataFrame(data, columns=columns)

                    print(f"\n[Hasil: {len(df)} baris]")
                    print("-"*60)

                    if len(df) == 0:
                        print("Tidak ada data ditemukan.")
                    else:
                        print(df.head(20).to_string())
                        if len(df) > 20:
                            print(f"\n... dan {len(df) - 20} baris lainnya")

                    # Post-query options loop
                    while True:
                        print("\n" + "-"*60)
                        print("Pilihan:")
                        print("  1. Export ke Excel")
                        print("  2. Tambah Filter (kembali ke filter)")
                        print("  3. Kembali ke Menu Utama")
                        print("-"*60)

                        post_choice = input("Pilihan (1/2/3): ").strip()

                        if post_choice == '1':
                            # Export to Excel with save dialog
                            if len(df) == 0:
                                print("\n⚠ Tidak ada data untuk di-export.")
                            else:
                                # Let export_to_excel generate default name automatically
                                result = self.db.export_to_excel(df, use_dialog=True)
                                if result is None:
                                    # User cancelled, don't return to menu
                                    continue
                            # Cleanup and return to main menu
                            if hasattr(self, '_temp_filters'):
                                delattr(self, '_temp_filters')
                            return

                        elif post_choice == '2':
                            # Go back to filter
                            current_step = 6
                            break

                        elif post_choice == '3':
                            # Return to main menu - cleanup and exit
                            if hasattr(self, '_temp_filters'):
                                delattr(self, '_temp_filters')
                            return

                        else:
                            print("Pilihan tidak valid.")

                except Exception as e:
                    print(f"\n✗ Error: {e}")
                    input("\nTekan Enter untuk kembali ke menu utama...")
                    if hasattr(self, '_temp_filters'):
                        delattr(self, '_temp_filters')
                    return

    # Keep old methods for compatibility but they won't be shown in menu
    def _show_tables(self):
        """Show all tables"""
        tables = self.db.get_all_tables()
        print(f"\n[Daftar Tabel ({len(tables)} tabel)]")
        print("-" * 40)
        for i, table in enumerate(tables, 1):
            count = self.db.get_table_count(table)
            print(f"{i:3}. {table:<40} ({count:,} rows)")

        input("\nTekan Enter untuk kembali ke menu utama...")

    def _show_schema(self):
        """Show complete schema"""
        self.db.print_schema_summary()
        input("\nTekan Enter untuk kembali ke menu utama...")

    def _preview_table(self):
        """Preview a table"""
        tables = self.db.get_all_tables()
        print("\nTabel yang tersedia:")
        print("(Ketik 'b' untuk kembali)")
        for i, t in enumerate(tables, 1):
            print(f"  {i}. {t}")

        choice = input("\nMasukkan nama/nomor tabel (atau 'b' untuk kembali): ").strip()

        if choice.lower() == 'b':
            return

        try:
            idx = int(choice) - 1
            table_name = tables[idx]
        except:
            table_name = choice

        limit = input("Jumlah row (default 5, 'b' untuk kembali): ").strip()
        if limit.lower() == 'b':
            return
        limit = int(limit) if limit else 5

        print(f"\n[Preview: {table_name}]")
        df = self.db.preview_table(table_name, limit)
        print(df.to_string())

        # Show columns
        columns = self.db.get_table_columns(table_name)
        print(f"\n[Columns]")
        for col in columns:
            print(f"  - {col['name']} ({col['type']})")

        input("\nTekan Enter untuk kembali ke menu utama...")

    def _find_relations(self):
        """Find table relations"""
        table = input("Masukkan nama tabel (atau 'b' untuk kembali): ").strip()

        if table.lower() == 'b':
            return

        related = self.db.find_related_tables(table)

        print(f"\n[Relasi untuk tabel '{table}']")
        print("-" * 40)

        if related['direct']:
            print("\nDirect Relations:")
            for rel in related['direct']:
                print(f"  → {rel['table']}")
                print(f"    JOIN: {rel['join']}")
        else:
            print("\nTidak ada relasi langsung ditemukan.")

        input("\nTekan Enter untuk kembali ke menu utama...")

    def _smart_query_builder(self):
        """Interactive smart query builder"""
        print("\n" + "="*60)
        print("[Smart Query Builder]")
        print("Tool ini akan otomatis menghubungkan tabel berdasarkan relasi.")
        print("Ketik 'b' kapan saja untuk kembali ke menu utama")
        print("="*60)

        # Get schema
        if not self.db.schema_cache:
            print("Loading schema...")
            self.db.get_full_schema()

        tables = list(self.db.schema_cache.keys())

        # Select base table
        print("\nTabel yang tersedia:")
        for i, t in enumerate(tables, 1):
            print(f"  {i}. {t}")

        base_choice = input("\nPilih tabel utama (nomor/nama, 'b' untuk kembali): ").strip()

        if base_choice.lower() == 'b':
            return

        try:
            base_table = tables[int(base_choice) - 1]
        except:
            base_table = base_choice

        print(f"\nTabel utama: {base_table}")

        # Select columns from multiple tables
        select_columns = {}

        while True:
            print("\n[Pilih Kolom]")
            print("Kolom yang sudah dipilih:", dict(select_columns) if select_columns else "Belum ada")
            table_choice = input("Dari tabel mana? (kosong untuk lanjut, 'b' untuk kembali): ").strip()

            if table_choice.lower() == 'b':
                return

            if not table_choice:
                break

            try:
                table_name = tables[int(table_choice) - 1]
            except:
                table_name = table_choice

            columns = self.db.get_table_columns(table_name)
            print(f"\nKolom di '{table_name}':")
            for i, col in enumerate(columns, 1):
                print(f"  {i}. {col['name']} ({col['type']})")

            col_choices = input("Pilih kolom (pisahkan dengan koma, 'all', atau 'b' untuk kembali): ").strip()

            if col_choices.lower() == 'b':
                return

            if col_choices.lower() == 'all':
                select_columns[table_name] = [c['name'] for c in columns]
            else:
                selected = []
                for c in col_choices.split(','):
                    c = c.strip()
                    try:
                        selected.append(columns[int(c) - 1]['name'])
                    except:
                        selected.append(c)
                select_columns[table_name] = selected

            print(f"✓ Ditambahkan {len(select_columns[table_name])} kolom dari '{table_name}'")

        if not select_columns:
            print("Tidak ada kolom dipilih!")
            input("\nTekan Enter untuk kembali ke menu utama...")
            return

        # Aggregations
        aggregations = {}
        add_agg = input("\nTambah aggregation? (y/n, 'b' untuk kembali): ").strip().lower()
        if add_agg == 'b':
            return
        if add_agg == 'y':
            print("Contoh: COUNT(*), SUM(table.column), AVG(table.column)")
            while True:
                alias = input("Nama alias (kosong untuk lanjut, 'b' untuk kembali): ").strip()
                if alias.lower() == 'b':
                    return
                if not alias:
                    break
                func = input("Aggregation function: ").strip()
                if func.lower() == 'b':
                    return
                aggregations[alias] = func

        # Filters
        filters = {}
        add_filter = input("\nTambah filter? (y/n, 'b' untuk kembali): ").strip().lower()
        if add_filter == 'b':
            return
        if add_filter == 'y':
            print("Format: table.column = value")
            while True:
                key = input("Filter key (table.column, kosong untuk lanjut, 'b' untuk kembali): ").strip()
                if key.lower() == 'b':
                    return
                if not key:
                    break
                value = input("Value: ").strip()
                if value.lower() == 'b':
                    return
                filters[key] = value

        # Limit
        limit_str = input("\nLimit hasil (kosong untuk semua, 'b' untuk kembali): ").strip()
        if limit_str.lower() == 'b':
            return
        limit = int(limit_str) if limit_str else None

        # Execute query
        print("\nMenjalankan query...")
        try:
            df = self.db.smart_query(
                base_table=base_table,
                select_columns=select_columns,
                filters=filters if filters else None,
                aggregations=aggregations if aggregations else None,
                limit=limit
            )

            print(f"\n[Hasil: {len(df)} rows]")
            print(df.head(20).to_string())

            if len(df) > 20:
                print(f"\n... dan {len(df) - 20} row lainnya")

            # Export option
            export = input("\nExport ke Excel? (y/n, 'b' untuk kembali): ").strip().lower()
            if export == 'b':
                return
            if export == 'y':
                filename = input("Nama file (kosong untuk auto): ").strip()
                self.db.export_to_excel(df, filename if filename else None)

        except Exception as e:
            print(f"\n✗ Error: {e}")

        input("\nTekan Enter untuk kembali ke menu utama...")

    def _custom_query(self):
        """Execute custom SELECT query"""
        print("\n[Custom Query]")
        print("PENTING: Hanya SELECT query yang diizinkan!")
        print("Ketik 'b' untuk kembali ke menu utama")

        query = input("\nMasukkan query SQL (atau 'b' untuk kembali):\n").strip()

        if query.lower() == 'b':
            return

        # Security check - only allow SELECT
        if not query.upper().startswith('SELECT'):
            print("✗ ERROR: Hanya SELECT query yang diizinkan!")
            input("\nTekan Enter untuk kembali ke menu utama...")
            return

        # Block dangerous keywords
        dangerous = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE', 'TRUNCATE', 'GRANT', 'REVOKE']
        for keyword in dangerous:
            if keyword in query.upper():
                print(f"✗ ERROR: Keyword '{keyword}' tidak diizinkan!")
                input("\nTekan Enter untuk kembali ke menu utama...")
                return

        try:
            with self.db.conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                data = cur.fetchall()

            df = pd.DataFrame(data, columns=columns)
            print(f"\n[Hasil: {len(df)} rows]")
            print(df.head(20).to_string())

            export = input("\nExport ke Excel? (y/n, 'b' untuk kembali): ").strip().lower()
            if export == 'b':
                return
            if export == 'y':
                filename = input("Nama file (kosong untuk auto): ").strip()
                self.db.export_to_excel(df, filename if filename else None)

        except Exception as e:
            print(f"✗ Error: {e}")

        input("\nTekan Enter untuk kembali ke menu utama...")

    def _export_schema(self):
        """Export database schema to Excel"""
        print("\n[Export Schema ke Excel]")

        confirm = input("Lanjutkan export schema? (y/n, 'b' untuk kembali): ").strip().lower()
        if confirm == 'b' or confirm == 'n':
            return

        print("\nMengexport schema ke Excel...")

        if not self.db.schema_cache:
            self.db.get_full_schema()

        # Create DataFrames for each aspect
        tables_data = []
        columns_data = []
        relations_data = []

        for table, info in self.db.schema_cache.items():
            count = self.db.get_table_count(table)
            tables_data.append({
                'table_name': table,
                'column_count': len(info['columns']),
                'row_count': count,
                'has_relations': len(info['relations']) > 0
            })

            for col in info['columns']:
                columns_data.append({
                    'table_name': table,
                    'column_name': col['name'],
                    'data_type': col['type'],
                    'nullable': col['nullable'],
                    'default': col['default']
                })

            for rel in info['relations']:
                relations_data.append({
                    'from_table': table,
                    'from_column': rel['from_column'],
                    'to_table': rel['to_table'],
                    'to_column': rel['to_column']
                })

        # Export to Excel with multiple sheets
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"db_schema_{timestamp}.xlsx"

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            pd.DataFrame(tables_data).to_excel(writer, sheet_name='Tables', index=False)
            pd.DataFrame(columns_data).to_excel(writer, sheet_name='Columns', index=False)
            pd.DataFrame(relations_data).to_excel(writer, sheet_name='Relations', index=False)

        print(f"\n✓ Schema berhasil di-export ke: {os.path.abspath(filename)}")

        input("\nTekan Enter untuk kembali ke menu utama...")

    def _easy_query_generator(self):
        """Easy Query Generator - User Friendly version"""
        print("\n" + "="*70)
        print("  EASY QUERY GENERATOR v2")
        print("  Pilih kolom dengan nomor, filter data, export ke Excel!")
        print("  Ketik 'b' kapan saja untuk kembali")
        print("="*70)

        # Load schema
        if not self.db.schema_cache:
            print("\nLoading database schema...")
            self.db.get_full_schema()

        # Build global column list with numbering
        all_columns = []  # [{index, table, column, display, type}]
        table_columns_map = {}
        idx = 1

        for table, info in self.db.schema_cache.items():
            table_columns_map[table] = []
            for col in info['columns']:
                all_columns.append({
                    'index': idx,
                    'table': table,
                    'column': col['name'],
                    'display': f"{table}.{col['name']}",
                    'type': col['type']
                })
                table_columns_map[table].append({'index': idx, 'name': col['name'], 'type': col['type']})
                idx += 1

        tables = list(self.db.schema_cache.keys())

        # STEP 0: Choose primary/base table first
        print("\n" + "-"*70)
        print("STEP 0: Pilih PRIMARY TABLE (tabel utama untuk query)")
        print("-"*70)
        print("\nPrimary table menentukan basis data. Pilih sesuai kebutuhan:")
        print("  - Jika ingin data per JOB: pilih tabel job")
        print("  - Jika ingin data per TALENT: pilih tabel talent")
        print("  - Jika ingin data per PAYMENT: pilih tabel payment")
        print("  - Jika ingin data per SCHEDULE: pilih tabel schedule")

        # Show common primary tables first
        common_primary = ['job', 'talent', 'payment', 'schedule', 'campaign', 'project']
        print("\n[Recommended Primary Tables]")
        rec_idx = 1
        recommended = []
        for t in tables:
            t_lower = t.lower()
            for cp in common_primary:
                if cp in t_lower and t not in recommended:
                    recommended.append(t)
                    print(f"  {rec_idx}. {t}")
                    rec_idx += 1
                    break

        print("\n[Semua Tabel]")
        for i, t in enumerate(tables, 1):
            marker = "*" if t in recommended else " "
            print(f"  {marker}{i}. {t}")

        base_input = input("\nPilih primary table (nomor/nama, 'b' untuk kembali): ").strip()
        if base_input.lower() == 'b':
            return

        try:
            base_table = tables[int(base_input) - 1]
        except:
            base_table = base_input

        if base_table not in table_columns_map:
            print(f"✗ Tabel '{base_table}' tidak ditemukan!")
            input("\nTekan Enter untuk kembali...")
            return

        print(f"\n✓ Primary table: {base_table}")

        # STEP 1: Show ALL columns with global numbering
        print("\n" + "-"*70)
        print("STEP 1: Pilih kolom yang ingin ditampilkan")
        print("-"*70)
        print("Ketik nomor kolom dipisah koma (contoh: 21, 43, 68, 32)")
        print("Ketik 'show' untuk melihat daftar kolom")
        print("Ketik 'show [nama_tabel]' untuk melihat kolom tabel tertentu")
        print("Ketik 'search [keyword]' untuk mencari kolom")

        selected_columns = []

        while True:
            print(f"\n[Kolom terpilih: {len(selected_columns)}]")
            if selected_columns:
                for sc in selected_columns[:10]:
                    print(f"  ✓ {sc['index']}. {sc['display']}")
                if len(selected_columns) > 10:
                    print(f"  ... dan {len(selected_columns) - 10} kolom lainnya")

            user_input = input("\nMasukkan nomor kolom (atau 'done'/'show'/'search', 'b' untuk kembali): ").strip()

            if user_input.lower() == 'b':
                return

            if user_input.lower() == 'done':
                if selected_columns:
                    break
                else:
                    print("✗ Pilih minimal 1 kolom!")
                    continue

            # Show all columns
            if user_input.lower() == 'show':
                print("\n" + "="*70)
                print("DAFTAR SEMUA KOLOM")
                print("="*70)
                current_table = ""
                for col in all_columns:
                    if col['table'] != current_table:
                        current_table = col['table']
                        print(f"\n[{current_table}]")
                    selected_mark = "✓" if col in selected_columns else " "
                    print(f"  {selected_mark} {col['index']:4}. {col['column']} ({col['type']})")
                continue

            # Show specific table columns
            if user_input.lower().startswith('show '):
                search_table = user_input[5:].strip()
                found = False
                for t in tables:
                    if search_table.lower() in t.lower():
                        print(f"\n[{t}]")
                        for col_info in table_columns_map[t]:
                            selected_mark = "✓" if any(sc['index'] == col_info['index'] for sc in selected_columns) else " "
                            print(f"  {selected_mark} {col_info['index']:4}. {col_info['name']} ({col_info['type']})")
                        found = True
                if not found:
                    print(f"✗ Tabel '{search_table}' tidak ditemukan!")
                continue

            # Search columns
            if user_input.lower().startswith('search '):
                keyword = user_input[7:].strip().lower()
                print(f"\n[Hasil pencarian: '{keyword}']")
                found_count = 0
                for col in all_columns:
                    if keyword in col['column'].lower() or keyword in col['table'].lower():
                        selected_mark = "✓" if col in selected_columns else " "
                        print(f"  {selected_mark} {col['index']:4}. {col['display']} ({col['type']})")
                        found_count += 1
                if found_count == 0:
                    print("  Tidak ditemukan")
                else:
                    print(f"\n  Total: {found_count} kolom ditemukan")
                continue

            # Parse column numbers
            try:
                col_numbers = [int(x.strip()) for x in user_input.replace(' ', ',').split(',') if x.strip()]
                for num in col_numbers:
                    # Find column by index
                    col_found = None
                    for col in all_columns:
                        if col['index'] == num:
                            col_found = col
                            break

                    if col_found:
                        if col_found not in selected_columns:
                            selected_columns.append(col_found)
                            print(f"  ✓ Ditambahkan: {col_found['display']}")
                        else:
                            print(f"  - Sudah dipilih: {col_found['display']}")
                    else:
                        print(f"  ✗ Nomor {num} tidak valid!")
            except ValueError:
                print("✗ Format tidak valid! Gunakan nomor dipisah koma (contoh: 21, 43, 68)")

        if not selected_columns:
            print("\n✗ Tidak ada kolom yang dipilih!")
            input("\nTekan Enter untuk kembali ke menu utama...")
            return

        # Step 2: Filter data
        print("\n" + "-"*70)
        print("STEP 2: Filter data (opsional)")
        print("-"*70)
        print("Filter memungkinkan Anda menyaring data berdasarkan nilai tertentu")
        print("Contoh: talent.name = 'Khabib Nurmagomedov', company.name LIKE 'Angga'")

        filters = []

        while True:
            print("\n[Filter yang aktif]")
            if filters:
                for i, f in enumerate(filters, 1):
                    val_display = f['value'] if f['value'] else ''
                    print(f"  {i}. {f['display']} {f['operator']} '{val_display}'")
            else:
                print("  Belum ada filter")

            add_filter = input("\nTambah filter? (y/n, 'b' untuk kembali): ").strip().lower()

            if add_filter == 'b':
                return
            if add_filter != 'y':
                break

            # Select column to filter - show selected columns first
            print("\n[Pilih kolom untuk filter]")
            print("Kolom yang sudah dipilih untuk ditampilkan:")
            for i, sc in enumerate(selected_columns, 1):
                print(f"  {i}. {sc['display']}")

            print("\nAtau ketik nomor global kolom, atau 'search [keyword]'")
            filter_col = input("Pilih kolom (nomor 1-{} dari atas, atau nomor global, 'b' untuk kembali): ".format(len(selected_columns))).strip()

            if filter_col.lower() == 'b':
                continue

            # Search in filter
            if filter_col.lower().startswith('search '):
                keyword = filter_col[7:].strip().lower()
                print(f"\n[Hasil pencarian untuk filter: '{keyword}']")
                for col in all_columns:
                    if keyword in col['column'].lower() or keyword in col['table'].lower():
                        print(f"  {col['index']:4}. {col['display']}")
                continue

            # Parse filter column
            filter_table = None
            filter_column = None

            try:
                num = int(filter_col)
                # Check if it's from selected columns (1-N) or global
                if 1 <= num <= len(selected_columns):
                    filter_table = selected_columns[num - 1]['table']
                    filter_column = selected_columns[num - 1]['column']
                else:
                    # Try global index
                    for col in all_columns:
                        if col['index'] == num:
                            filter_table = col['table']
                            filter_column = col['column']
                            break
            except:
                if '.' in filter_col:
                    parts = filter_col.split('.')
                    filter_table = parts[0]
                    filter_column = parts[1]

            if not filter_table or not filter_column:
                print("✗ Kolom tidak ditemukan!")
                continue

            print(f"  Filter pada: {filter_table}.{filter_column}")

            # Select operator
            print("\n[Pilih operator]")
            print("  1. = (sama dengan)")
            print("  2. LIKE (mengandung)")
            print("  3. > (lebih besar)")
            print("  4. < (lebih kecil)")
            print("  5. != (tidak sama dengan)")
            print("  6. IS NULL")
            print("  7. IS NOT NULL")

            op_choice = input("Pilih operator (1-7, default=1, 'b' untuk kembali): ").strip()

            if op_choice.lower() == 'b':
                continue

            operators = {
                '1': '=',
                '2': 'LIKE',
                '3': '>',
                '4': '<',
                '5': '!=',
                '6': 'IS NULL',
                '7': 'IS NOT NULL',
                '': '='
            }

            operator = operators.get(op_choice, '=')

            # Get value (except for NULL operators)
            if operator in ['IS NULL', 'IS NOT NULL']:
                filter_value = None
            else:
                filter_value = input(f"Masukkan nilai (contoh: Khabib Nurmagomedov): ").strip()
                if filter_value.lower() == 'b':
                    continue

                # For LIKE operator, add wildcards if not present
                if operator == 'LIKE' and '%' not in filter_value:
                    filter_value = f"%{filter_value}%"

            filters.append({
                'table': filter_table,
                'column': filter_column,
                'display': f"{filter_table}.{filter_column}",
                'operator': operator,
                'value': filter_value
            })

            val_display = filter_value if filter_value else ''
            print(f"✓ Filter ditambahkan: {filter_table}.{filter_column} {operator} '{val_display}'")

        # Step 3: Generate and execute query
        print("\n" + "-"*70)
        print("STEP 3: Generate Query")
        print("-"*70)
        print(f"Primary Table: {base_table}")

        # Build select columns dict
        select_cols_dict = {}
        for sc in selected_columns:
            if sc['table'] not in select_cols_dict:
                select_cols_dict[sc['table']] = []
            if sc['column'] not in select_cols_dict[sc['table']]:
                select_cols_dict[sc['table']].append(sc['column'])

        # Build query manually with better control
        if not self.db.relations_cache:
            self.db.get_all_relations()

        # SELECT clause
        select_parts = []
        for sc in selected_columns:
            select_parts.append(f'"{sc["table"]}"."{sc["column"]}" as "{sc["table"]}_{sc["column"]}"')

        select_clause = ', '.join(select_parts)

        # Determine all tables needed
        tables_needed = set(sc['table'] for sc in selected_columns)
        for f in filters:
            tables_needed.add(f['table'])

        # Helper function to find relation between two tables
        def find_direct_relation(from_table, to_table):
            """Find direct relation from from_table to to_table"""
            # Check if from_table has FK to to_table
            if from_table in self.db.relations_cache:
                for rel in self.db.relations_cache[from_table]:
                    if rel['to_table'] == to_table:
                        return {
                            'type': 'forward',
                            'from_table': from_table,
                            'from_column': rel['from_column'],
                            'to_table': to_table,
                            'to_column': rel['to_column']
                        }
            # Check if to_table has FK to from_table (reverse)
            if to_table in self.db.relations_cache:
                for rel in self.db.relations_cache[to_table]:
                    if rel['to_table'] == from_table:
                        return {
                            'type': 'reverse',
                            'from_table': to_table,
                            'from_column': rel['from_column'],
                            'to_table': from_table,
                            'to_column': rel['to_column']
                        }
            return None

        # Build adjacency list for BFS
        def build_graph():
            """Build undirected graph of table relations"""
            graph = {}
            all_tables = list(self.db.schema_cache.keys())
            for t in all_tables:
                graph[t] = set()

            for table, rels in self.db.relations_cache.items():
                for rel in rels:
                    graph[table].add(rel['to_table'])
                    graph[rel['to_table']].add(table)
            return graph

        def find_path_bfs(start, end, graph):
            """Find shortest path between two tables using BFS"""
            if start == end:
                return [start]

            visited = {start}
            queue = [[start]]

            while queue:
                path = queue.pop(0)
                node = path[-1]

                for neighbor in graph.get(node, []):
                    if neighbor == end:
                        return path + [neighbor]
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append(path + [neighbor])
            return None

        # Build JOIN clauses using BFS for complex paths
        join_clauses = []
        joined_tables = {base_table}
        graph = build_graph()

        # Sort tables by distance from base_table for better join order
        tables_to_join = list(tables_needed - {base_table})

        for table in tables_to_join:
            if table in joined_tables:
                continue

            # First try direct relation from any joined table
            join_added = False

            for joined in list(joined_tables):
                rel = find_direct_relation(joined, table)
                if rel:
                    if rel['type'] == 'forward':
                        join_clauses.append(
                            f'LEFT JOIN "{table}" ON "{joined}"."{rel["from_column"]}" = "{table}"."{rel["to_column"]}"'
                        )
                    else:
                        join_clauses.append(
                            f'LEFT JOIN "{table}" ON "{table}"."{rel["from_column"]}" = "{joined}"."{rel["to_column"]}"'
                        )
                    joined_tables.add(table)
                    join_added = True
                    break

            # If no direct relation, use BFS to find path
            if not join_added:
                path = find_path_bfs(base_table, table, graph)
                if path and len(path) > 1:
                    # Add intermediate tables if needed
                    for i in range(len(path) - 1):
                        t1, t2 = path[i], path[i + 1]
                        if t2 not in joined_tables:
                            rel = find_direct_relation(t1, t2)
                            if rel:
                                if rel['type'] == 'forward':
                                    join_clauses.append(
                                        f'LEFT JOIN "{t2}" ON "{t1}"."{rel["from_column"]}" = "{t2}"."{rel["to_column"]}"'
                                    )
                                else:
                                    join_clauses.append(
                                        f'LEFT JOIN "{t2}" ON "{t2}"."{rel["from_column"]}" = "{t1}"."{rel["to_column"]}"'
                                    )
                                joined_tables.add(t2)
                    join_added = table in joined_tables

            if not join_added:
                print(f"⚠ Warning: Tidak ditemukan relasi untuk '{table}'")
                print(f"  Tabel ini akan di-skip dari query untuk menghindari CROSS JOIN")
                # Remove columns from this table from selection
                selected_columns = [sc for sc in selected_columns if sc['table'] != table]
                # Update select clause
                select_parts = []
                for sc in selected_columns:
                    select_parts.append(f'"{sc["table"]}"."{sc["column"]}" as "{sc["table"]}_{sc["column"]}"')
                select_clause = ', '.join(select_parts)

        # WHERE clause
        where_parts = []
        params = []

        for f in filters:
            if f['operator'] in ['IS NULL', 'IS NOT NULL']:
                where_parts.append(f'"{f["table"]}"."{f["column"]}" {f["operator"]}')
            else:
                where_parts.append(f'"{f["table"]}"."{f["column"]}" {f["operator"]} %s')
                params.append(f['value'])

        where_clause = ""
        if where_parts:
            where_clause = "WHERE " + " AND ".join(where_parts)

        # Build final query
        query = f"""SELECT {select_clause}
FROM "{base_table}"
{chr(10).join(join_clauses)}
{where_clause}"""

        print("\n[Generated SQL Query]")
        print("-" * 60)
        print(query)
        if params:
            print(f"\nParameters: {params}")
        print("-" * 60)

        # Ask for limit
        limit_input = input("\nLimit hasil? (kosong untuk semua, 'b' untuk kembali): ").strip()
        if limit_input.lower() == 'b':
            return

        if limit_input:
            query += f"\nLIMIT {int(limit_input)}"

        # Execute query
        print("\nMenjalankan query...")
        try:
            with self.db.conn.cursor() as cur:
                cur.execute(query, params if params else None)
                columns = [desc[0] for desc in cur.description]
                data = cur.fetchall()

            df = pd.DataFrame(data, columns=columns)

            print(f"\n[Hasil Query: {len(df)} baris]")
            print("-" * 60)

            if len(df) == 0:
                print("Tidak ada data yang ditemukan dengan filter tersebut.")
            else:
                # Show preview
                print(df.head(20).to_string())
                if len(df) > 20:
                    print(f"\n... dan {len(df) - 20} baris lainnya")

            # Export to Excel
            print("\n" + "-"*60)
            print("STEP 4: Export ke Excel")
            print("-"*60)

            export = input("\nExport hasil ke Excel? (y/n, 'b' untuk kembali): ").strip().lower()
            if export == 'b':
                return

            if export == 'y':
                default_name = f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                filename = input(f"Nama file (Enter untuk '{default_name}'): ").strip()

                if not filename:
                    filename = default_name
                if not filename.endswith('.xlsx'):
                    filename += '.xlsx'

                self.db.export_to_excel(df, filename)

        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()

        input("\nTekan Enter untuk kembali ke menu utama...")


# Quick usage functions
def quick_export(base_table: str, columns: Dict[str, List[str]], filename: str = None, **kwargs):
    """
    Quick function untuk export data ke Excel

    Example:
    --------
    quick_export(
        base_table='talent',
        columns={
            'talent': ['id', 'name', 'email'],
            'talent_bank_account': ['bank_name', 'account_number'],
            'schedule': ['status']
        },
        aggregations={'done_count': "COUNT(CASE WHEN schedule.status = 'done' THEN 1 END)"},
        filename='talent_report.xlsx'
    )
    """
    db = DatabaseExplorer()
    if not db.connect():
        return None

    try:
        df = db.smart_query(
            base_table=base_table,
            select_columns=columns,
            **kwargs
        )
        path = db.export_to_excel(df, filename)
        return df
    finally:
        db.disconnect()


if __name__ == "__main__":
    explorer = InteractiveExplorer()
    explorer.run()
