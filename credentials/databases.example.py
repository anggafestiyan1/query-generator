"""
DB Studio - Database Configurations (EXAMPLE)
==============================================
Copy file ini ke databases.py dan isi dengan credentials yang benar.

Untuk menambah database baru:
1. Tambahkan entry baru di DATABASES dict
2. Restart aplikasi
"""

# =============================================================================
# DATABASE CONFIGURATIONS
# =============================================================================
DATABASES = {
    'mydb': {
        'label': 'My Database',
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'user': 'postgres',
        'password': 'your_password_here',
    },

    # Tambahkan database lain jika perlu
    # 'otherdb': {
    #     'label': 'Other Database',
    #     'host': 'db.example.com',
    #     'port': 5432,
    #     'database': 'otherdb',
    #     'user': 'user',
    #     'password': 'password',
    # },
}


# =============================================================================
# DEFAULT DATABASE
# =============================================================================
DEFAULT_DATABASE = 'mydb'
