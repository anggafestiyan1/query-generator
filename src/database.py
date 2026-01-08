"""
DB Studio - Database Manager
============================
Mengelola koneksi dan operasi database PostgreSQL.
"""

import psycopg2
import pandas as pd
from datetime import datetime
from tkinter import filedialog, messagebox


class DatabaseManager:
    """Mengelola koneksi dan operasi database"""

    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.schema_cache = {}
        self.relations_cache = {}

    def connect(self):
        """Connect ke database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False

    def disconnect(self):
        """Disconnect dari database"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None

    def is_alive(self):
        """Cek apakah koneksi masih aktif"""
        try:
            if self.conn is None or self.conn.closed:
                return False
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except:
            return False

    def reconnect(self):
        """Reconnect ke database"""
        self.disconnect()
        return self.connect()

    def rollback(self):
        """Rollback transaction"""
        if self.conn:
            try:
                self.conn.rollback()
            except:
                pass

    def get_full_schema(self):
        """Get schema lengkap dari database"""
        if not self.conn:
            return {}

        self.schema_cache = {}

        with self.conn.cursor() as cur:
            # Get tables
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cur.fetchall()]

            for table in tables:
                # Get columns
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    ORDER BY ordinal_position
                """, (table,))
                columns = [{'name': row[0], 'type': row[1]} for row in cur.fetchall()]

                # Get relations
                cur.execute("""
                    SELECT
                        kcu.column_name as from_column,
                        ccu.table_name as to_table,
                        ccu.column_name as to_column
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_name = %s
                """, (table,))
                relations = [
                    {'from_column': row[0], 'to_table': row[1], 'to_column': row[2]}
                    for row in cur.fetchall()
                ]

                self.schema_cache[table] = {
                    'columns': columns,
                    'relations': relations
                }

                if relations:
                    self.relations_cache[table] = relations

        return self.schema_cache

    def execute_query(self, sql, params=None):
        """Execute query dan return DataFrame"""
        self.rollback()

        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()

        return pd.DataFrame(data, columns=columns)

    def get_table_count(self, table_name):
        """Get jumlah row dalam tabel"""
        self.rollback()
        with self.conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            return cur.fetchone()[0]

    def preview_table(self, table_name, limit=10):
        """Preview data dari tabel"""
        sql = f'SELECT * FROM "{table_name}" LIMIT {limit}'
        return self.execute_query(sql)

    def export_to_excel(self, df, filename=None):
        """Export DataFrame ke Excel"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = filedialog.asksaveasfilename(
                defaultextension='.xlsx',
                filetypes=[('Excel files', '*.xlsx')],
                initialfile=f'export_{timestamp}.xlsx'
            )

        if filename:
            df.to_excel(filename, index=False)
            messagebox.showinfo("Success", f"Data exported ke:\n{filename}")
            return filename
        return None
