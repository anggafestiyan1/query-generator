"""
DB Studio - Column Mappings (EXAMPLE)
=====================================
Copy file ini ke mappings.py dan sesuaikan dengan schema database Anda.

Mapping nama kolom untuk mempermudah query.
Setiap database punya mappings sendiri.

Format mapping:
  'alias': {'table': 'nama_tabel', 'column': 'nama_kolom'}
"""

# =============================================================================
# YOUR DATABASE COLUMNS
# =============================================================================
MYDB_COLUMNS = {
    # Contoh mapping - sesuaikan dengan tabel Anda
    'user_name': {'table': 'users', 'column': 'name'},
    'username': {'table': 'users', 'column': 'name'},

    # Tambahkan mapping lain sesuai kebutuhan
}

MYDB_STATUS = {
    # Mapping status ke kolom boolean
    # 'active': ('is_active', True),
    # 'inactive': ('is_active', False),
}

MYDB_KEYWORDS = ['status', 'state']

MYDB_BOOLEAN_LABELS = {
    # Label untuk kolom boolean
    # 'is_active': {True: 'Active', False: 'Inactive'},
}

MYDB_DEFAULT_FILTERS = {
    # Filter default per tabel
    # 'users': [
    #     {'column': 'is_deleted', 'op': '=', 'value': False},
    # ],
}

MYDB_PREFERRED_PATHS = {
    # Path JOIN yang dipreferensikan
    # ('table_a', 'table_c'): ['table_b'],  # a -> b -> c
}


# =============================================================================
# REGISTRY
# =============================================================================
DATABASE_MAPPINGS = {
    'mydb': {
        'columns': MYDB_COLUMNS,
        'status': MYDB_STATUS,
        'keywords': MYDB_KEYWORDS,
        'default_filters': MYDB_DEFAULT_FILTERS,
        'preferred_paths': MYDB_PREFERRED_PATHS,
        'boolean_labels': MYDB_BOOLEAN_LABELS,
    },
}


# =============================================================================
# HELPER FUNCTION
# =============================================================================
def get_mappings(db_key):
    """Get mappings untuk database tertentu."""
    config = DATABASE_MAPPINGS.get(db_key, {})

    return {
        'columns': config.get('columns', {}),
        'status': config.get('status', {}),
        'keywords': config.get('keywords', []),
        'default_filters': config.get('default_filters', {}),
        'preferred_paths': config.get('preferred_paths', {}),
        'boolean_labels': config.get('boolean_labels', {}),
    }
