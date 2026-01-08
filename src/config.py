"""
DB Studio - Configuration
=========================
Konfigurasi global dan loader.

- config/databases.py: Konfigurasi koneksi database
- config/mappings.py: Column mappings dan status mappings per database
"""

import os
import sys

# =============================================================================
# IMPORT CONFIGURATIONS
# =============================================================================
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(ROOT_DIR, 'config')

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
if CONFIG_DIR not in sys.path:
    sys.path.insert(0, CONFIG_DIR)

from databases import DATABASES, DEFAULT_DATABASE
from mappings import get_mappings


# =============================================================================
# COLOR SCHEME
# =============================================================================
COLORS = {
    'bg': '#f5f6fa',
    'card': '#ffffff',
    'primary': '#3498db',
    'primary_dark': '#2980b9',
    'success': '#27ae60',
    'danger': '#e74c3c',
    'warning': '#f39c12',
    'text': '#2c3e50',
    'text_light': '#7f8c8d',
    'border': '#dcdde1',
}


# =============================================================================
# FUNCTIONS
# =============================================================================
def get_base_path():
    """Get base path of the application"""
    return ROOT_DIR


def get_available_databases():
    """
    Get list of available databases.

    Returns:
        list: [(key, label), ...]
    """
    return [(key, db.get('label', key)) for key, db in DATABASES.items()]


def get_default_database():
    """Get default database key"""
    return DEFAULT_DATABASE


def load_database_config(db_key=None):
    """
    Load konfigurasi database.

    Args:
        db_key: Key database dari DATABASES dict

    Returns:
        dict: {
            'db_config': {host, port, database, user, password},
            'custom_mappings': {...},
            'status_mappings': {...},
            'status_keywords': [...],
        }
    """
    key = db_key or DEFAULT_DATABASE

    if key not in DATABASES:
        available = ', '.join(DATABASES.keys())
        raise ValueError(f"Database '{key}' tidak ditemukan. Available: {available}")

    db = DATABASES[key]

    # Extract connection config (exclude 'label')
    db_config = {k: v for k, v in db.items() if k != 'label'}

    # Get mappings for this database
    mappings = get_mappings(key)

    return {
        'db_config': db_config,
        'custom_mappings': mappings['columns'],
        'status_mappings': mappings['status'],
        'status_keywords': mappings['keywords'],
        'default_filters': mappings['default_filters'],
        'preferred_paths': mappings['preferred_paths'],
        'boolean_labels': mappings['boolean_labels'],
    }
