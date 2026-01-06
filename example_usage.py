"""
Contoh Penggunaan Database Explorer
===================================

File ini berisi contoh-contoh cara menggunakan Database Explorer
untuk berbagai kebutuhan data extraction.
"""

from db_explorer import DatabaseExplorer, quick_export

# =============================================================================
# CONTOH 1: Lihat Schema Database
# =============================================================================
def contoh_lihat_schema():
    """Melihat struktur database secara keseluruhan"""
    db = DatabaseExplorer()
    if db.connect():
        # Print semua tabel
        tables = db.get_all_tables()
        print("Daftar Tabel:")
        for t in tables:
            print(f"  - {t}")

        # Print schema lengkap
        db.print_schema_summary()
        db.disconnect()


# =============================================================================
# CONTOH 2: Preview Data dari Tabel
# =============================================================================
def contoh_preview_data():
    """Preview beberapa row dari tabel tertentu"""
    db = DatabaseExplorer()
    if db.connect():
        # Ganti 'nama_tabel' dengan nama tabel yang ingin dilihat
        df = db.preview_table('nama_tabel', limit=10)
        print(df)
        db.disconnect()


# =============================================================================
# CONTOH 3: Smart Query - Ambil Data dari Multiple Tabel
# =============================================================================
def contoh_smart_query():
    """
    Contoh: Ambil data talent dengan bank account dan status schedule
    Tool akan otomatis handle JOIN berdasarkan foreign key
    """
    db = DatabaseExplorer()
    if db.connect():
        # Definisikan kolom yang ingin diambil dari setiap tabel
        # Format: {nama_tabel: [kolom1, kolom2, ...]}

        df = db.smart_query(
            base_table='talent',  # Tabel utama
            select_columns={
                'talent': ['id', 'name', 'email', 'phone'],
                'talent_bank_account': ['bank_name', 'account_number'],
                'schedule': ['status', 'schedule_date']
            },
            limit=100  # Optional: batasi hasil
        )

        print(df)
        db.export_to_excel(df, 'talent_data.xlsx')
        db.disconnect()


# =============================================================================
# CONTOH 4: Query dengan Aggregation (COUNT, SUM, dll)
# =============================================================================
def contoh_dengan_aggregation():
    """
    Contoh: Hitung berapa schedule dengan status 'done' per talent
    """
    db = DatabaseExplorer()
    if db.connect():
        df = db.smart_query(
            base_table='talent',
            select_columns={
                'talent': ['id', 'name'],
            },
            aggregations={
                'total_schedule': "COUNT(schedule.id)",
                'done_count': "COUNT(CASE WHEN schedule.status = 'done' THEN 1 END)",
                'pending_count': "COUNT(CASE WHEN schedule.status = 'pending' THEN 1 END)"
            }
        )

        print(df)
        db.export_to_excel(df, 'talent_schedule_summary.xlsx')
        db.disconnect()


# =============================================================================
# CONTOH 5: Query dengan Filter
# =============================================================================
def contoh_dengan_filter():
    """
    Contoh: Ambil data talent dengan filter tertentu
    """
    db = DatabaseExplorer()
    if db.connect():
        df = db.smart_query(
            base_table='talent',
            select_columns={
                'talent': ['id', 'name', 'email'],
                'schedule': ['status', 'job_number']
            },
            filters={
                'schedule.status': 'done'  # Hanya yang status = 'done'
            }
        )

        print(df)
        db.export_to_excel(df, 'talent_done_schedules.xlsx')
        db.disconnect()


# =============================================================================
# CONTOH 6: Quick Export - Cara Cepat
# =============================================================================
def contoh_quick_export():
    """
    Menggunakan fungsi quick_export untuk export cepat
    """
    # Cara singkat tanpa perlu manage connection manual
    df = quick_export(
        base_table='talent',
        columns={
            'talent': ['id', 'name'],
            'talent_bank_account': ['bank_name', 'account_number']
        },
        filename='quick_talent_export.xlsx'
    )

    if df is not None:
        print(f"Berhasil export {len(df)} rows")


# =============================================================================
# CONTOH 7: Export Schema ke Excel
# =============================================================================
def contoh_export_schema():
    """
    Export struktur database ke Excel untuk dokumentasi
    """
    db = DatabaseExplorer()
    if db.connect():
        db.get_full_schema()

        # Export akan membuat file Excel dengan 3 sheet:
        # 1. Tables - daftar tabel dengan jumlah kolom dan row
        # 2. Columns - detail semua kolom di setiap tabel
        # 3. Relations - foreign key relationships

        # (Gunakan menu interaktif option 7)
        db.disconnect()


# =============================================================================
# CONTOH 8: Cari Relasi Tabel
# =============================================================================
def contoh_cari_relasi():
    """
    Cari tabel apa saja yang berelasi dengan tabel tertentu
    """
    db = DatabaseExplorer()
    if db.connect():
        # Cari relasi untuk tabel 'talent'
        related = db.find_related_tables('talent')

        print("Tabel yang berelasi dengan 'talent':")
        for rel in related['direct']:
            print(f"  - {rel['table']}")
            print(f"    JOIN: {rel['join']}")

        db.disconnect()


# =============================================================================
# MAIN - Jalankan contoh yang diinginkan
# =============================================================================
if __name__ == "__main__":
    print("="*60)
    print("DATABASE EXPLORER - Contoh Penggunaan")
    print("="*60)
    print("""
Pilih contoh yang ingin dijalankan:
1. Lihat Schema Database
2. Preview Data Tabel
3. Smart Query (Multiple Tables)
4. Query dengan Aggregation
5. Query dengan Filter
6. Quick Export
7. Export Schema ke Excel
8. Cari Relasi Tabel
0. Jalankan Mode Interaktif

Atau langsung edit file ini sesuai kebutuhan!
""")

    choice = input("Pilihan: ").strip()

    if choice == '1':
        contoh_lihat_schema()
    elif choice == '2':
        contoh_preview_data()
    elif choice == '3':
        contoh_smart_query()
    elif choice == '4':
        contoh_dengan_aggregation()
    elif choice == '5':
        contoh_dengan_filter()
    elif choice == '6':
        contoh_quick_export()
    elif choice == '7':
        contoh_export_schema()
    elif choice == '8':
        contoh_cari_relasi()
    elif choice == '0':
        from db_explorer import InteractiveExplorer
        explorer = InteractiveExplorer()
        explorer.run()
    else:
        print("Pilihan tidak valid")
