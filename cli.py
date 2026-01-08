#!/usr/bin/env python3
"""
DB Studio - CLI Runner
======================
Jalankan query dari command line tanpa GUI.

Cara penggunaan:
    python cli.py                           # Mode interaktif
    python cli.py "show job_number"         # Single query
    python cli.py -d neo "show job_number"  # Gunakan database 'neo'
    python cli.py -e output.xlsx "show ..."  # Export ke Excel
    python cli.py --help                    # Bantuan
"""

import sys
import os

# Tambahkan root directory ke path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse

# Try import readline for history support (Unix/Mac)
# or pyreadline3 for Windows
try:
    import readline
    READLINE_AVAILABLE = True
except ImportError:
    try:
        import pyreadline3 as readline
        READLINE_AVAILABLE = True
    except ImportError:
        READLINE_AVAILABLE = False

from src.config import (
    load_database_config,
    get_available_databases,
    get_default_database,
)
from src.database import DatabaseManager
from src.parser import QueryParser


class DBStudioCLI:
    """CLI untuk DB Studio"""

    # Query history (shared across instances)
    query_history = []
    history_index = -1
    MAX_HISTORY = 100

    def __init__(self, db_key=None):
        self.db_key = db_key or get_default_database()
        self.config = None
        self.db = None
        self.parser = None

    def add_to_history(self, query):
        """Add query to history"""
        if query and query.strip():
            # Avoid duplicates of last query
            if not self.query_history or self.query_history[-1] != query:
                self.query_history.append(query)
                if len(self.query_history) > self.MAX_HISTORY:
                    self.query_history.pop(0)
        self.history_index = len(self.query_history)

    def get_history(self, direction):
        """Get query from history. direction: -1 for prev, +1 for next"""
        if not self.query_history:
            return ""

        self.history_index += direction
        self.history_index = max(0, min(self.history_index, len(self.query_history)))

        if self.history_index < len(self.query_history):
            return self.query_history[self.history_index]
        return ""

    def connect(self):
        """Connect ke database"""
        # Get label for display
        databases = dict(get_available_databases())
        label = databases.get(self.db_key, self.db_key)

        print(f"Database: {label} ({self.db_key})")
        self.config = load_database_config(self.db_key)

        self.db = DatabaseManager(self.config['db_config'])

        print("Connecting...")
        if not self.db.connect():
            print("Error: Gagal connect ke database!")
            return False

        print("Loading schema...")
        self.db.get_full_schema()

        # Parser dengan auto-generated mappings dari schema
        self.parser = QueryParser(
            self.db.schema_cache,
            self.db.relations_cache,
            self.config  # Contains custom_mappings, status_mappings, status_keywords
        )

        print(f"Connected! ({len(self.db.schema_cache)} tables)\n")
        return True

    def switch_database(self, db_key):
        """Switch ke database lain"""
        self.disconnect()
        self.db_key = db_key
        return self.connect()

    def _transform_boolean_labels(self, df):
        """Transform boolean/integer columns ke readable labels"""
        if df is None or df.empty:
            return df

        boolean_labels = self.config.get('boolean_labels', {})
        if not boolean_labels:
            return df

        for col in df.columns:
            # Get actual column name (bisa ada prefix table)
            col_name = col.split('.')[-1] if '.' in col else col
            # Try original name, then with underscore instead of space
            col_underscore = col_name.replace(' ', '_')

            label_map = None
            if col_name in boolean_labels:
                label_map = boolean_labels[col_name]
            elif col_underscore in boolean_labels:
                label_map = boolean_labels[col_underscore]

            if label_map:
                # Transform values
                df[col] = df[col].apply(
                    lambda x, lm=label_map: lm.get(x, lm.get(bool(x) if x is not None else None, x))
                )

        return df

    def execute_query(self, query_text, export_file=None):
        """Execute query dan tampilkan hasil"""
        try:
            # Parse query
            parsed = self.parser.parse(query_text)
            sql, params, applied_filters = self.parser.build_sql(parsed)

            print("=" * 60)
            print("SQL Query:")
            print("-" * 60)
            print(sql)
            print("=" * 60)

            # Show applied default filters
            if applied_filters:
                print(f"\n[Auto-filters: {', '.join(applied_filters)}]")

            # Execute
            df = self.db.execute_query(sql, params if params else None)

            # Transform boolean columns ke readable labels
            df = self._transform_boolean_labels(df)

            print(f"\nResult: {len(df)} rows\n")

            if len(df) == 0:
                print("(No data)")
                return df

            # Pagination untuk large results
            page_size = 50
            total_rows = len(df)
            total_pages = (total_rows + page_size - 1) // page_size
            current_page = 0

            while True:
                start_idx = current_page * page_size
                end_idx = min(start_idx + page_size, total_rows)

                # Display current page
                print(df.iloc[start_idx:end_idx].to_string(index=False))

                if total_pages > 1:
                    print(f"\n--- Page {current_page + 1}/{total_pages} (rows {start_idx + 1}-{end_idx} of {total_rows}) ---")
                    print("[n]ext, [p]rev, [f]irst, [l]ast, [e]xport, [q]uit: ", end="")

                    try:
                        choice = input().strip().lower()
                        if choice == 'n' and current_page < total_pages - 1:
                            current_page += 1
                        elif choice == 'p' and current_page > 0:
                            current_page -= 1
                        elif choice == 'f':
                            current_page = 0
                        elif choice == 'l':
                            current_page = total_pages - 1
                        elif choice == 'e':
                            filename = f"query_result_{len(df)}_rows.xlsx"
                            df.to_excel(filename, index=False)
                            print(f"Exported to: {filename}")
                        elif choice == 'q' or choice == '':
                            break
                        else:
                            print("Invalid choice")
                    except (EOFError, KeyboardInterrupt):
                        break
                else:
                    break

            # Export jika diminta
            if export_file:
                df.to_excel(export_file, index=False)
                print(f"\nExported to: {export_file}")

            return df

        except ValueError as e:
            print(f"\nError: {e}")
            return None
        except Exception as e:
            print(f"\nDatabase Error: {e}")
            self.db.rollback()
            return None

    def show_help(self):
        """Tampilkan bantuan"""
        print()
        print("=" * 60)
        print("SMART QUERY - BANTUAN")
        print("=" * 60)
        print()
        print("FORMAT QUERY:")
        print("-" * 60)
        print()
        print("1. Format 'show':")
        print("   show <kolom1>, <kolom2> where <kondisi>")
        print("   Contoh: show job_number, talent_name where status=completed")
        print()
        print("2. Format 'primary':")
        print("   primary <kolom_utama> show <kolom2>, <kolom3>")
        print("   Contoh: primary job_number show company, total_fee")
        print()
        print("3. Format colon ':':")
        print("   <kolom_utama>: <kolom2>, <kolom3> where <kondisi>")
        print("   Contoh: job_number: talent_name, product where status=done")
        print()
        print("-" * 60)
        print("KLAUSA TAMBAHAN:")
        print("-" * 60)
        print()
        print("  where <kondisi>     - Filter data")
        print("  order by <kolom>    - Urutkan hasil (asc/desc)")
        print("  limit <n>           - Batasi jumlah hasil")
        print()
        print("Contoh lengkap:")
        print("  show job_number, talent_name where status=completed order by job_number desc limit 100")
        print()
        print("-" * 60)
        print("DATE RANGE FILTER:")
        print("-" * 60)
        print()
        print("  Format: kolom=tanggal_awal..tanggal_akhir")
        print("  Atau:   kolom=tanggal_awal to tanggal_akhir")
        print()
        print("  Contoh:")
        print("    show job_number, talent_name where start_date=2025-01-01..2025-12-31")
        print("    show job_number, company where schedule_date=2025-10-01 to 2025-12-29")
        print()
        print("-" * 60)
        print("STATUS SHORTCUTS:")
        print("-" * 60)
        print()
        print("  completed, done     -> is_completed = true")
        print("  cancelled, canceled -> is_canceled = true")
        print("  paid, transferred   -> is_transferred = true")
        print("  hold, onhold        -> is_hold = true")
        print("  not_completed       -> is_completed = false")
        print()
        print("-" * 60)
        print("COMMANDS:")
        print("-" * 60)
        print()
        print("  help       - Tampilkan bantuan ini")
        print("  tables     - Daftar semua tabel")
        print("  schema     - Tampilkan schema lengkap")
        print("  cols <key> - Cari kolom (contoh: cols fee)")
        print("  use <db>   - Switch database (contoh: use neo)")
        print("  databases  - Daftar database yang tersedia")
        print("  back       - Kembali ke main menu")
        print()

    def show_databases(self):
        """Tampilkan daftar database"""
        print("\nAvailable Databases:")
        print("-" * 40)
        for key, label in get_available_databases():
            marker = " (active)" if key == self.db_key else ""
            print(f"  {key:15} - {label}{marker}")
        print()

    def show_tables(self):
        """Tampilkan daftar tabel"""
        print("\nTables:")
        print("-" * 40)
        for i, table in enumerate(sorted(self.db.schema_cache.keys()), 1):
            count = self.db.get_table_count(table)
            print(f"  {i:3}. {table} ({count} rows)")
        print()

    def show_schema(self, table_filter=None):
        """Tampilkan schema"""
        tables = sorted(self.db.schema_cache.keys())

        if table_filter:
            tables = [t for t in tables if table_filter.lower() in t.lower()]

        for table in tables:
            info = self.db.schema_cache[table]
            print(f"\n{table}")
            print("-" * len(table))

            for col in info['columns']:
                print(f"  {col['name']:30} {col['type']}")

            if info.get('relations'):
                print("  Relations:")
                for rel in info['relations']:
                    print(f"    {rel['from_column']} -> {rel['to_table']}.{rel['to_column']}")

    def search_columns(self, search_term):
        """Cari kolom berdasarkan nama"""
        print(f"\nSearching for: {search_term}")
        print("-" * 50)

        matches = self.parser.find_all_columns(search_term)

        if matches:
            for m in matches[:20]:  # Max 20 results
                print(f"  {m['table']}.{m['column']}")
            if len(matches) > 20:
                print(f"  ... ({len(matches) - 20} more)")
        else:
            print("  No matches found")
        print()

    def show_main_menu(self):
        """Tampilkan main menu"""
        while True:
            # Get database info
            databases = dict(get_available_databases())
            db_label = databases.get(self.db_key, self.db_key)
            table_count = len(self.db.schema_cache)

            print()
            print("=" * 60)
            print("DB STUDIO")
            print("=" * 60)
            print(f"Database: {db_label} ({self.db_key})")
            print(f"Tables: {table_count} | Mode: READ-ONLY")
            print("-" * 60)
            print()
            print("Menu:")
            print()
            print("  [1] Cek Tabel")
            print("      Lihat daftar tabel, kolom, tipe data, dan relasi")
            print()
            print("  [2] Generate Query")
            print("      Cari kolom berdasarkan keyword untuk membantu query")
            print()
            print("  [3] Preview Data")
            print("      Lihat isi data dari tabel dengan limit tertentu")
            print()
            print("  [4] Smart Query")
            print("      Jalankan query dengan format sederhana:")
            print("      - show job_number, talent_name where status=completed")
            print("      - primary job_number show company, total_fee")
            print()
            print("-" * 60)
            print("  [d] Ganti Database    [0] Keluar")
            print("-" * 60)

            try:
                choice = input("Pilih menu [1-4, d, 0]: ").strip().lower()

                if choice == '1':
                    self.menu_cek_tabel()
                elif choice == '2':
                    self.menu_generate_query()
                elif choice == '3':
                    self.menu_preview_data()
                elif choice == '4':
                    self.menu_smart_query()
                elif choice == 'd':
                    new_db = select_database_interactive()
                    if new_db != self.db_key:
                        self.switch_database(new_db)
                elif choice in ('0', 'exit', 'quit', 'q'):
                    print("\nBye!")
                    break
                else:
                    print("Pilihan tidak valid.")

            except KeyboardInterrupt:
                print("\n\nBye!")
                break
            except EOFError:
                break

    def menu_cek_tabel(self):
        """Menu 1: Cek Tabel - Lihat daftar tabel & kolom"""
        table_count = len(self.db.schema_cache)

        print()
        print("=" * 60)
        print("CEK TABEL")
        print("=" * 60)
        print(f"Total: {table_count} tabel tersedia")
        print("-" * 60)
        print()
        print("Commands:")
        print("  tables          - Daftar semua tabel dengan jumlah kolom")
        print("  schema          - Tampilkan seluruh schema (kolom & relasi)")
        print("  schema <nama>   - Tampilkan schema tabel tertentu")
        print("  cols <keyword>  - Cari kolom berdasarkan keyword")
        print("  back            - Kembali ke main menu")
        print()
        print("Contoh:")
        print("  schema job      - Lihat schema tabel yang mengandung 'job'")
        print("  cols fee        - Cari semua kolom yang mengandung 'fee'")
        print()

        while True:
            try:
                cmd = input("[cek-tabel] (b=back)> ").strip().lower()

                if not cmd:
                    continue
                elif cmd in ('back', 'b', 'exit', 'q'):
                    break
                elif cmd == 'tables':
                    self.show_tables()
                elif cmd == 'schema':
                    self.show_schema()
                elif cmd.startswith('schema '):
                    self.show_schema(cmd[7:].strip())
                elif cmd.startswith('cols '):
                    self.search_columns(cmd[5:].strip())
                else:
                    print("Command tidak dikenal. Ketik 'tables', 'schema <nama>', atau 'cols <keyword>'")

            except KeyboardInterrupt:
                print()
                break

    def menu_generate_query(self):
        """Menu 2: Generate Query - Cari kolom & filter"""
        print()
        print("=" * 60)
        print("GENERATE QUERY - Pencarian Kolom")
        print("=" * 60)
        print()
        print("Cari kolom berdasarkan keyword untuk membantu membuat query.")
        print("Hasil pencarian menampilkan: tabel.kolom")
        print()
        print("-" * 60)
        print()
        print("Tips untuk Smart Query:")
        print("  - Gunakan hasil pencarian sebagai referensi kolom")
        print("  - Format: show <kolom1>, <kolom2> where <kondisi>")
        print("  - Kolom pertama menentukan tabel utama (primary)")
        print()

        while True:
            try:
                keyword = input("[generate-query] (b=back)> ").strip()

                if not keyword:
                    continue
                elif keyword.lower() in ('back', 'b', 'exit', 'q'):
                    break
                else:
                    self.search_columns(keyword)

            except KeyboardInterrupt:
                print()
                break

    def menu_preview_data(self):
        """Menu 3: Preview Data - Lihat isi data tabel"""
        tables = sorted(self.db.schema_cache.keys())

        print()
        print("=" * 60)
        print("PREVIEW DATA")
        print("=" * 60)
        print()
        print("Lihat isi data dari tabel. Pilih tabel dengan nomor atau nama.")
        print("Anda dapat mengatur jumlah baris yang ditampilkan (limit).")
        print()
        print("-" * 60)
        print()
        print("Daftar Tabel:")
        print()

        # Tampilkan dalam 2 kolom
        half = (len(tables) + 1) // 2
        for i in range(half):
            left = f"  {i+1:3}. {tables[i][:25]:<25}"
            if i + half < len(tables):
                right = f"  {i+half+1:3}. {tables[i+half]}"
            else:
                right = ""
            print(f"{left}  {right}")

        print()
        print("-" * 60)
        print("Ketik nomor/nama tabel, 'list' untuk daftar, 'b' untuk kembali")
        print()

        while True:
            try:
                choice = input("[preview] (b=back)> ").strip()

                if not choice:
                    continue
                elif choice.lower() in ('back', 'b', 'exit', 'q'):
                    break
                elif choice.lower() == 'list':
                    # Tampilkan ulang daftar tabel
                    print()
                    for i in range(half):
                        left = f"  {i+1:3}. {tables[i][:25]:<25}"
                        if i + half < len(tables):
                            right = f"  {i+half+1:3}. {tables[i+half]}"
                        else:
                            right = ""
                        print(f"{left}  {right}")
                    print()
                    continue

                # Cek apakah input adalah nomor
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(tables):
                        table_name = tables[idx]
                    else:
                        print(f"Nomor tidak valid (1-{len(tables)}). Ketik 'list' untuk daftar tabel.")
                        continue
                except ValueError:
                    # Input adalah nama tabel
                    table_name = choice

                # Validasi tabel
                if table_name not in self.db.schema_cache:
                    # Coba cari tabel yang mirip
                    matches = [t for t in tables if choice.lower() in t.lower()]
                    if matches:
                        print(f"Tabel '{choice}' tidak ditemukan. Mungkin maksud Anda:")
                        for m in matches[:5]:
                            print(f"  - {m}")
                    else:
                        print(f"Tabel '{choice}' tidak ditemukan. Ketik 'list' untuk daftar tabel.")
                    continue

                # Preview data
                limit_input = input(f"Jumlah baris untuk '{table_name}' (default 10): ").strip()
                try:
                    limit = int(limit_input) if limit_input else 10
                    if limit <= 0:
                        print("Limit harus lebih dari 0, menggunakan default 10")
                        limit = 10
                    elif limit > 10000:
                        print("Limit maksimal 10000, menggunakan 10000")
                        limit = 10000
                except ValueError:
                    print(f"'{limit_input}' bukan angka valid, menggunakan default 10")
                    limit = 10

                try:
                    count = self.db.get_table_count(table_name)
                    col_count = len(self.db.schema_cache[table_name]['columns'])

                    print()
                    print(f"Tabel: {table_name}")
                    print(f"Total: {count:,} rows | Kolom: {col_count} | Showing: {min(limit, count)} rows")
                    print("-" * 60)

                    df = self.db.preview_table(table_name, limit)
                    print(df.to_string(index=False))

                    # Export option
                    print()
                    export = input("Export ke Excel? (y/n): ").strip().lower()
                    if export == 'y':
                        filename = f"{table_name}_preview.xlsx"
                        df.to_excel(filename, index=False)
                        print(f"Exported to: {filename}")

                    print()

                except Exception as e:
                    print(f"Error: {e}")
                    self.db.rollback()

            except KeyboardInterrupt:
                print()
                break

    def _setup_autocomplete(self):
        """Setup tab completion untuk column names"""
        if not READLINE_AVAILABLE:
            return

        # Collect all column names for autocomplete
        completions = ['show', 'primary', 'where', 'order', 'by', 'limit', 'and',
                       'help', 'tables', 'schema', 'cols', 'history', 'back', 'lib', 'library']

        # Add column names from mappings
        if self.parser and hasattr(self.parser, 'column_map'):
            completions.extend(self.parser.column_map.keys())

        # Add table names
        if self.db and self.db.schema_cache:
            completions.extend(self.db.schema_cache.keys())

        self._completions = sorted(set(completions))

        def completer(text, state):
            """Tab completion function"""
            options = [c for c in self._completions if c.startswith(text.lower())]
            if state < len(options):
                return options[state]
            return None

        readline.set_completer(completer)
        readline.parse_and_bind('tab: complete')

    def _build_column_library(self):
        """Build column library dengan info relasi dan alias"""
        library = {}

        # Get all tables and their columns
        for table_name, table_info in self.db.schema_cache.items():
            for col in table_info['columns']:
                col_name = col['name']
                col_type = col['type']

                # Skip internal columns
                if col_name in ('id', 'created_at', 'updated_at', 'created_by', 'updated_by'):
                    continue

                # Build display name untuk kolom
                if col_name == 'name':
                    # name di tabel talent -> talent_name
                    display_name = f"{table_name}_name"
                elif col_name.endswith('_id') and col_name != 'id':
                    # job_id di job_detail -> skip (foreign key)
                    continue
                else:
                    display_name = col_name

                # Group by display name
                if display_name not in library:
                    library[display_name] = {
                        'tables': [],
                        'type': col_type,
                        'aliases': [],
                        'relations': []
                    }

                library[display_name]['tables'].append(table_name)

        # Add aliases from custom mappings
        custom_mappings = self.config.get('custom_mappings', {})
        for alias, mapping in custom_mappings.items():
            table = mapping.get('table', '')
            column = mapping.get('column', '')

            # Find the display name for this column
            if column == 'name':
                display_name = f"{table}_name"
            else:
                display_name = column

            if display_name in library:
                if alias not in library[display_name]['aliases'] and alias != display_name:
                    library[display_name]['aliases'].append(alias)

        # Add relation info
        for table_name, table_info in self.db.schema_cache.items():
            for rel in table_info.get('relations', []):
                from_col = rel['from_column']
                to_table = rel['to_table']

                # Find columns yang bisa di-join via relasi ini
                if to_table in self.db.schema_cache:
                    for col in self.db.schema_cache[to_table]['columns']:
                        if col['name'] == 'name':
                            display_name = f"{to_table}_name"
                        else:
                            display_name = col['name']

                        if display_name in library:
                            rel_info = f"{table_name} -> {to_table}"
                            if rel_info not in library[display_name]['relations']:
                                library[display_name]['relations'].append(rel_info)

        return library

    def _search_column_library(self, keyword):
        """Search column library dan return formatted results"""
        if not hasattr(self, '_column_library'):
            self._column_library = self._build_column_library()

        keyword_lower = keyword.lower()
        results = []

        for col_name, info in self._column_library.items():
            # Match by column name
            if keyword_lower in col_name.lower():
                results.append((col_name, info, 'name'))
                continue

            # Match by alias
            for alias in info['aliases']:
                if keyword_lower in alias.lower():
                    results.append((col_name, info, f'alias:{alias}'))
                    break

            # Match by table name
            for table in info['tables']:
                if keyword_lower in table.lower():
                    results.append((col_name, info, f'table:{table}'))
                    break

        # Sort by relevance: exact match first, then name match, then others
        def sort_key(item):
            col_name, info, match_type = item
            if col_name.lower() == keyword_lower:
                return (0, col_name)
            elif col_name.lower().startswith(keyword_lower):
                return (1, col_name)
            elif match_type == 'name':
                return (2, col_name)
            else:
                return (3, col_name)

        results.sort(key=sort_key)
        return results

    def show_column_library(self, search_term=None):
        """Show column library dengan optional search"""
        print()
        print("=" * 100)
        print("COLUMN LIBRARY - Perpustakaan Kolom")
        print("=" * 100)

        if search_term:
            results = self._search_column_library(search_term)
            if not results:
                print(f"\nTidak ada kolom yang cocok dengan '{search_term}'")
                print("Tips: Coba keyword yang lebih umum seperti 'job', 'talent', 'fee', 'date'")
                return

            print(f"\nHasil pencarian untuk '{search_term}': {len(results)} kolom ditemukan")
            print("-" * 100)
            print()

            # Format: Column Name | Tables | Type | Aliases | How to use
            print(f"{'COLUMN NAME':<30} {'TABLE(S)':<25} {'TYPE':<15} {'ALIASES':<20}")
            print("-" * 100)

            for col_name, info, match_type in results[:30]:  # Max 30 results
                tables_str = ', '.join(info['tables'][:2])
                if len(info['tables']) > 2:
                    tables_str += f" (+{len(info['tables'])-2})"

                aliases_str = ', '.join(info['aliases'][:2]) if info['aliases'] else '-'
                if len(info['aliases']) > 2:
                    aliases_str += '...'

                type_str = info['type'][:12] if info['type'] else 'unknown'

                print(f"{col_name:<30} {tables_str:<25} {type_str:<15} {aliases_str:<20}")

            if len(results) > 30:
                print(f"\n... dan {len(results) - 30} kolom lainnya")

            # Show usage tips
            print()
            print("-" * 100)
            print("CARA PENGGUNAAN:")
            print("-" * 100)

            # Pick first result as example
            if results:
                example_col, example_info, _ = results[0]
                example_table = example_info['tables'][0] if example_info['tables'] else 'table'

                print(f"  Kolom '{example_col}' ada di tabel: {', '.join(example_info['tables'])}")
                if example_info['aliases']:
                    print(f"  Alias yang bisa digunakan: {', '.join(example_info['aliases'])}")
                print()
                print(f"  Contoh query:")
                print(f"    show {example_col} where ...")
                print(f"    show job_number, {example_col} where status=completed")

        else:
            # Show categories/groups
            if not hasattr(self, '_column_library'):
                self._column_library = self._build_column_library()

            # Group columns by category (based on prefix or table)
            categories = {
                'Job': [],
                'Talent': [],
                'Company': [],
                'Payment': [],
                'Product': [],
                'Schedule': [],
                'Other': []
            }

            for col_name, info in self._column_library.items():
                col_lower = col_name.lower()
                tables_lower = [t.lower() for t in info['tables']]

                if 'job' in col_lower or any('job' in t for t in tables_lower):
                    categories['Job'].append(col_name)
                elif 'talent' in col_lower or any('talent' in t for t in tables_lower):
                    categories['Talent'].append(col_name)
                elif 'company' in col_lower or any('company' in t for t in tables_lower):
                    categories['Company'].append(col_name)
                elif 'payment' in col_lower or any('payment' in t for t in tables_lower):
                    categories['Payment'].append(col_name)
                elif 'product' in col_lower or any('product' in t for t in tables_lower):
                    categories['Product'].append(col_name)
                elif 'schedule' in col_lower or any('schedule' in t for t in tables_lower):
                    categories['Schedule'].append(col_name)
                else:
                    categories['Other'].append(col_name)

            print()
            print("Ketik 'lib <keyword>' untuk mencari kolom. Contoh: lib job, lib fee, lib talent")
            print()
            print("-" * 100)
            print("KATEGORI KOLOM:")
            print("-" * 100)

            for category, columns in categories.items():
                if columns:
                    # Sort and limit display
                    columns_sorted = sorted(columns)[:10]
                    columns_str = ', '.join(columns_sorted)
                    if len(columns) > 10:
                        columns_str += f' ... (+{len(columns)-10} more)'

                    print(f"\n  [{category}] ({len(columns)} kolom)")
                    print(f"    {columns_str}")

            print()
            print("-" * 100)
            print("TIPS:")
            print("  - Ketik 'lib job' untuk melihat semua kolom terkait job")
            print("  - Ketik 'lib fee' untuk melihat kolom fee/payment")
            print("  - Ketik 'lib name' untuk melihat kolom nama (talent, company, etc)")
            print("  - Gunakan nama kolom langsung di query, misal: show job_number, talent_name")
            print()

    def _get_library_preview(self, search_term=None, max_items=15):
        """Get library items untuk preview di sidebar"""
        if not hasattr(self, '_column_library'):
            self._column_library = self._build_column_library()

        if search_term:
            results = self._search_column_library(search_term)
            items = []
            for col_name, info, _ in results[:max_items]:
                table = info['tables'][0] if info['tables'] else ''
                items.append((col_name, table))
            return items, len(results)
        else:
            # Show popular/common columns
            popular = [
                'job_number', 'talent_name', 'company_name', 'product_name',
                'total_fee', 'fee', 'start_date', 'end_date', 'schedule_date',
                'is_completed', 'is_canceled', 'is_hold', 'payment_number',
                'team_name', 'campaign_name'
            ]
            items = []
            for col in popular:
                if col in self._column_library:
                    table = self._column_library[col]['tables'][0]
                    items.append((col, table))
            return items[:max_items], len(self._column_library)

    def show_smart_query_split_view(self, library_search=None):
        """Show Smart Query dengan split view - Query kiri, Library kanan"""
        # Get database info
        databases = dict(get_available_databases())
        db_label = databases.get(self.db_key, self.db_key)
        table_count = len(self.db.schema_cache)
        total_columns = sum(len(t['columns']) for t in self.db.schema_cache.values())

        # Get library items
        lib_items, lib_total = self._get_library_preview(library_search, max_items=18)

        # Layout dimensions
        left_width = 58
        right_width = 58
        total_width = left_width + 3 + right_width  # 3 for separator

        print()
        print("=" * total_width)
        print(f"{'SMART QUERY':^{total_width}}")
        print(f"Database: {db_label} ({self.db_key}) | Tables: {table_count} | Columns: {total_columns}".center(total_width))
        print("=" * total_width)

        # Split header
        left_header = "QUERY PANEL"
        right_header = f"COLUMN LIBRARY ({lib_total} kolom)"
        if library_search:
            right_header = f"LIBRARY: '{library_search}' ({len(lib_items)} hasil)"

        print(f"{left_header:^{left_width}} | {right_header:^{right_width}}")
        print("-" * left_width + "-+-" + "-" * right_width)

        # Content rows - Query format on left, Library on right
        left_lines = [
            "FORMAT:",
            "  show <col1>, <col2> where <cond>",
            "  show col1, count:col2, sum:col3",
            "  ... order by <col> limit N",
            "",
            "DATE RANGE:",
            "  col=2025-01-01..2025-12-31",
            "",
            "STATUS SHORTCUTS:",
            "  completed, canceled, paid, hold",
            "",
            "COMMANDS:",
            "  lib <keyword> - Cari kolom",
            "  tables        - List tabel",
            "  history       - Query history",
            "  help          - Bantuan lengkap",
            "  clear         - Refresh tampilan",
            "  back (b)      - Kembali",
        ]

        # Build right side (library)
        right_lines = []
        if library_search:
            right_lines.append(f"Hasil pencarian '{library_search}':")
            right_lines.append("")
        else:
            right_lines.append("Kolom populer (ketik 'lib <keyword>'):")
            right_lines.append("")

        for col_name, table in lib_items:
            # Format: column_name (table)
            display = f"  {col_name}"
            if table:
                display += f" ({table})"
            if len(display) > right_width - 2:
                display = display[:right_width - 5] + "..."
            right_lines.append(display)

        if len(lib_items) < lib_total:
            right_lines.append("")
            right_lines.append(f"  ... +{lib_total - len(lib_items)} more")
            right_lines.append("")
            right_lines.append("Ketik: lib job, lib fee, lib name")

        # Pad lines to same length
        max_lines = max(len(left_lines), len(right_lines))
        while len(left_lines) < max_lines:
            left_lines.append("")
        while len(right_lines) < max_lines:
            right_lines.append("")

        # Print side by side
        for left, right in zip(left_lines, right_lines):
            print(f"{left:<{left_width}} | {right:<{right_width}}")

        print("-" * left_width + "-+-" + "-" * right_width)

        # Footer
        if READLINE_AVAILABLE:
            footer = "[Tab] Autocomplete | Ketik query atau command"
        else:
            footer = "Ketik query atau command"
        print(f"{footer:^{total_width}}")
        print("=" * total_width)
        print()

    def show_smart_query_header(self):
        """Show enhanced Smart Query header dengan info singkat"""
        # Use split view instead
        self.show_smart_query_split_view()

    def menu_smart_query(self):
        """Menu 4: Smart Query - Query format sederhana (interactive mode)"""
        # Setup autocomplete
        self._setup_autocomplete()

        # Build column library on first load
        self._column_library = self._build_column_library()

        # Track current library search for split view
        current_lib_search = None

        # Show enhanced split view header
        self.show_smart_query_split_view(current_lib_search)

        while True:
            try:
                query = input(f"[{self.db_key}]> ").strip()

                if not query:
                    continue

                cmd = query.lower()

                if cmd in ('back', 'b', 'exit', 'quit', 'q'):
                    break
                elif cmd == 'help':
                    self.show_help()
                elif cmd == 'tables':
                    self.show_tables()
                elif cmd == 'schema':
                    self.show_schema()
                elif cmd.startswith('schema '):
                    self.show_schema(query[7:].strip())
                elif cmd.startswith('cols '):
                    self.search_columns(query[5:].strip())
                elif cmd == 'lib' or cmd == 'library':
                    # Reset library search dan show full split view
                    current_lib_search = None
                    self.show_smart_query_split_view(current_lib_search)
                elif cmd.startswith('lib ') or cmd.startswith('library '):
                    # Search column library dan refresh split view
                    current_lib_search = query.split(' ', 1)[1].strip()
                    self.show_smart_query_split_view(current_lib_search)
                elif cmd == 'databases' or cmd == 'dbs':
                    self.show_databases()
                elif cmd.startswith('use '):
                    new_db = query[4:].strip()
                    available = dict(get_available_databases())
                    if new_db in available:
                        print()
                        self.switch_database(new_db)
                        # Rebuild column library for new database
                        self._column_library = self._build_column_library()
                        current_lib_search = None
                        self.show_smart_query_split_view(current_lib_search)
                    else:
                        print(f"Database '{new_db}' tidak ditemukan.")
                        self.show_databases()
                elif cmd == 'history':
                    # Show query history
                    if self.query_history:
                        print("\nQuery History:")
                        print("-" * 80)
                        for i, q in enumerate(self.query_history[-20:], 1):
                            print(f"  {i:2}. {q[:70]}{'...' if len(q) > 70 else ''}")
                        print()
                    else:
                        print("No query history yet")
                elif cmd == 'header' or cmd == 'clear':
                    # Refresh split view dengan current search
                    self.show_smart_query_split_view(current_lib_search)
                elif cmd == 'reset':
                    # Reset library search
                    current_lib_search = None
                    self.show_smart_query_split_view(current_lib_search)
                else:
                    # Add to history dan execute
                    self.add_to_history(query)
                    self.execute_query(query)

            except KeyboardInterrupt:
                print()
                break
            except EOFError:
                break

    def interactive_mode(self):
        """Mode interaktif (legacy - langsung ke smart query)"""
        self.menu_smart_query()

    def disconnect(self):
        """Disconnect dari database"""
        if self.db:
            self.db.disconnect()


def select_database_interactive():
    """Pilih database secara interaktif"""
    databases = get_available_databases()
    default = get_default_database()

    print()
    print("=" * 50)
    print("DB STUDIO - SELECT DATABASE")
    print("=" * 50)
    print()

    for i, (key, label) in enumerate(databases, 1):
        marker = " (default)" if key == default else ""
        print(f"  [{i}] {label}{marker}")
        print(f"      Key: {key}")
        print()

    print("-" * 50)
    choice = input(f"Pilih database [1-{len(databases)}] atau Enter untuk default: ").strip()

    if not choice:
        return default

    try:
        idx = int(choice) - 1
        if 0 <= idx < len(databases):
            return databases[idx][0]
    except ValueError:
        # Mungkin user ketik nama langsung
        for key, _ in databases:
            if key == choice:
                return key

    print(f"Pilihan tidak valid, menggunakan default: {default}")
    return default


def main():
    parser = argparse.ArgumentParser(
        description='DB Studio CLI - Query database dari command line',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py                             # Mode interaktif
  python cli.py "show job_number"           # Execute query langsung
  python cli.py -d neo "show job_number"    # Gunakan database 'neo'
  python cli.py -e out.xlsx "show ..."      # Export ke Excel
  python cli.py --list                      # List database yang tersedia
  python cli.py --select                    # Pilih database interaktif
        """
    )

    parser.add_argument('query', nargs='?', help='Query to execute')
    parser.add_argument('-d', '--database', help='Database key to use')
    parser.add_argument('-e', '--export', help='Export result to Excel file')
    parser.add_argument('--list', action='store_true', help='List available databases')
    parser.add_argument('--select', action='store_true', help='Select database interactively')

    args = parser.parse_args()

    # List databases
    if args.list:
        print("Available databases:")
        default = get_default_database()
        for key, label in get_available_databases():
            marker = " (default)" if key == default else ""
            print(f"  {key:15} - {label}{marker}")
        return 0

    # Determine database to use
    db_key = args.database

    # Jika tidak ada argument sama sekali (interactive mode), tampilkan pilihan database
    if not args.query and not args.database and not args.select:
        db_key = select_database_interactive()
    elif args.select:
        db_key = select_database_interactive()

    # Create CLI instance
    cli = DBStudioCLI(db_key=db_key)

    try:
        if not cli.connect():
            return 1

        if args.query:
            # Execute single query
            result = cli.execute_query(args.query, export_file=args.export)
            return 0 if result is not None else 1
        else:
            # Show main menu
            cli.show_main_menu()
            return 0

    finally:
        cli.disconnect()


if __name__ == "__main__":
    sys.exit(main())
