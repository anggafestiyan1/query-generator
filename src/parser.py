"""
DB Studio - Query Parser
========================
Parser untuk format query sederhana seperti:
  show job_number where status_job=completed

Kolom PERTAMA adalah PRIMARY - menentukan base table.
Semua kolom lain akan di-JOIN berdasarkan relasi ke base table.

Mappings otomatis dibuat dari schema database:
  - table.column -> exact match
  - column_name -> auto-detect table
  - table_column (e.g., talent_name) -> talent.name
"""

import re
from datetime import datetime

# Valid identifier pattern untuk SQL (letters, numbers, underscore)
VALID_IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# Supported date formats untuk parsing
DATE_FORMATS = [
    '%Y-%m-%d',      # 2025-01-15
    '%d-%m-%Y',      # 15-01-2025
    '%d/%m/%Y',      # 15/01/2025
    '%Y/%m/%d',      # 2025/01/15
    '%d.%m.%Y',      # 15.01.2025
    '%Y%m%d',        # 20250115
]


def parse_date(date_str):
    """
    Parse date string dengan berbagai format.
    Returns: string dalam format YYYY-MM-DD atau original jika gagal parse.
    """
    date_str = date_str.strip()

    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    # Return original jika tidak bisa parse (biarkan database handle)
    return date_str


def sanitize_identifier(name):
    """
    Sanitize SQL identifier (table/column name) untuk prevent SQL injection.
    Hanya izinkan karakter alphanumeric dan underscore.
    """
    if not name:
        raise ValueError("Identifier tidak boleh kosong")
    if not VALID_IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"Invalid identifier: '{name}'. Hanya huruf, angka, dan underscore yang diizinkan.")
    return name


class QueryParser:
    """Parser untuk query format sederhana"""

    def __init__(self, schema_cache, relations_cache=None, config=None):
        """
        Args:
            schema_cache: Dict schema dari database
            relations_cache: Dict relasi antar tabel
            config: Dict konfigurasi {custom_mappings, status_mappings, status_keywords}
        """
        self.schema_cache = schema_cache
        self.relations_cache = relations_cache or {}
        self.config = config or {}

        # Load from config
        self.custom_mappings = self.config.get('custom_mappings', {})
        self.status_mappings = self.config.get('status_mappings', {})
        self.status_keywords = self.config.get('status_keywords', ['status', 'state', 'kondisi'])
        self.default_filters = self.config.get('default_filters', {})
        self.preferred_paths = self.config.get('preferred_paths', {})

        # Auto-generated mappings from schema
        # column_map: key -> list of {table, column}
        self.column_map = {}
        self.table_columns = {}

        # Reverse relations: to_table -> [{from_table, from_column, to_column}]
        self.reverse_relations = {}

        # Lazy-loaded caches
        self._fuzzy_cache = {}  # Cache untuk fuzzy search results
        self._fuzzy_index_built = False  # Flag untuk lazy build

        self._build_column_map()
        self._build_reverse_relations()

    def _build_column_map(self):
        """
        Build mapping dari nama kolom ke table.column

        Auto-generated mappings dari schema:
        1. column_name -> semua tabel yang punya kolom ini
        2. table.column -> exact match (unique)
        3. table_column -> jika kolom 'name' ada di tabel 'talent', buat 'talent_name' -> talent.name

        Custom mappings dari config akan override auto-generated.
        """
        self.column_map = {}
        self.table_columns = {}

        # Auto-map semua kolom dari schema
        for table, info in self.schema_cache.items():
            self.table_columns[table] = [col['name'] for col in info['columns']]

            for col in info['columns']:
                col_name = col['name'].lower()
                col_info = {'table': table, 'column': col['name']}

                # 1. Key as-is (e.g., 'job_number', 'name', 'id')
                self._add_to_map(col_name, col_info)

                # 2. Key table.column format (unique, exact match)
                key_dot = f"{table}.{col_name}"
                self._add_to_map(key_dot, col_info, unique=True)

                # 3. Key with table prefix untuk kolom umum (name, id, etc)
                #    Contoh: talent.name -> 'talent_name', company.name -> 'company_name'
                common_cols = ['name', 'id', 'code', 'type', 'status', 'date', 'description', 'title']
                if col_name in common_cols:
                    key_prefixed = f"{table}_{col_name}"
                    self._add_to_map(key_prefixed, col_info, unique=True)

                # 4. Key tanpa underscore untuk fuzzy match
                key_no_underscore = col_name.replace('_', '')
                if key_no_underscore != col_name:
                    self._add_to_map(key_no_underscore, col_info)

        # Add custom mappings from config - prioritas tertinggi, override semua
        for alias, info in self.custom_mappings.items():
            alias_lower = alias.lower()
            self.column_map[alias_lower] = [info]

    def _build_reverse_relations(self):
        """
        Build reverse relations map.
        Untuk setiap tabel, simpan tabel mana yang punya FK ke tabel ini.

        Contoh: job punya FK ke company
        - relations_cache['job'] = [{from_column: 'company_id', to_table: 'company', to_column: 'id'}]
        - reverse_relations['company'] = [{from_table: 'job', from_column: 'company_id', to_column: 'id'}]
        """
        self.reverse_relations = {}

        for from_table, rels in self.relations_cache.items():
            for rel in rels:
                to_table = rel['to_table']
                if to_table not in self.reverse_relations:
                    self.reverse_relations[to_table] = []

                self.reverse_relations[to_table].append({
                    'from_table': from_table,
                    'from_column': rel['from_column'],
                    'to_column': rel['to_column']
                })

    def get_related_tables(self, base_table):
        """
        Get semua tabel yang bisa di-JOIN dari base_table.

        Returns:
            dict: {table_name: join_info}
        """
        related = {}

        # 1. Tabel yang base_table punya FK ke sana (base -> other)
        if base_table in self.relations_cache:
            for rel in self.relations_cache[base_table]:
                to_table = rel['to_table']
                related[to_table] = {
                    'join_type': 'outgoing',  # base punya FK ke tabel ini
                    'base_column': rel['from_column'],
                    'target_column': rel['to_column']
                }

        # 2. Tabel yang punya FK ke base_table (other -> base)
        if base_table in self.reverse_relations:
            for rel in self.reverse_relations[base_table]:
                from_table = rel['from_table']
                if from_table not in related:  # Jangan override jika sudah ada
                    related[from_table] = {
                        'join_type': 'incoming',  # tabel ini punya FK ke base
                        'base_column': rel['to_column'],
                        'target_column': rel['from_column']
                    }

        return related

    def find_join_path(self, base_table, target_table, visited=None):
        """
        Cari path JOIN dari base_table ke target_table.
        Prioritas: preferred_paths > scored BFS (prioritas relasi yang relevan).

        Returns:
            list: [(table, join_info), ...] atau None jika tidak ada path
        """
        import heapq

        if base_table == target_table:
            return []

        # 1. Cek preferred path terlebih dahulu
        preferred = self.preferred_paths.get((base_table, target_table))
        if preferred is not None:
            path = self._build_preferred_path(base_table, target_table, preferred)
            if path:
                return path

        # 2. Scored BFS - prioritaskan relasi yang relevan berdasarkan nama
        # Heap: (score, counter, current_table, path)
        # Lower score = better path
        counter = 0
        heap = []
        visited = {base_table}

        related = self.get_related_tables(base_table)
        for next_table, join_info in related.items():
            score = self._score_relation(base_table, next_table, join_info)
            if next_table == target_table:
                return [(target_table, join_info)]
            if next_table not in visited:
                visited.add(next_table)
                heapq.heappush(heap, (score, counter, next_table, [(next_table, join_info)]))
                counter += 1

        while heap:
            current_score, _, current_table, path = heapq.heappop(heap)

            related = self.get_related_tables(current_table)
            for next_table, join_info in related.items():
                if next_table == target_table:
                    return path + [(target_table, join_info)]

                if next_table not in visited:
                    visited.add(next_table)
                    new_score = current_score + self._score_relation(current_table, next_table, join_info)
                    heapq.heappush(heap, (new_score, counter, next_table, path + [(next_table, join_info)]))
                    counter += 1

        return None

    def _score_relation(self, from_table, to_table, join_info):
        """
        Score a relation - lower is better.
        Prioritas:
        1. Relasi dengan nama kolom yang matching (job_id ke job) = 0
        2. Relasi incoming (child punya FK ke parent) = 1
        3. Relasi outgoing biasa = 2
        4. Relasi via file/content = 10 (hindari)
        """
        base_col = join_info.get('base_column', '')
        target_col = join_info.get('target_column', '')
        join_type = join_info.get('join_type', '')

        # Relasi via file_content, attachment, dll biasanya tidak relevan untuk bisnis
        irrelevant_tables = {'file_content', 'attachment', 'document_history', 'file_history'}
        if to_table in irrelevant_tables or from_table in irrelevant_tables:
            return 10

        # Cek apakah kolom FK mengandung nama tabel tujuan
        if join_type == 'incoming':
            # incoming: to_table punya FK ke from_table
            # Contoh: job_detail.job_id -> job.id
            expected_fk = f"{from_table}_id"
            if target_col == expected_fk or target_col == 'id':
                return 0
            if target_col.endswith('_id') and from_table in target_col:
                return 0
            return 1
        else:
            # outgoing: from_table punya FK ke to_table
            # Contoh: job.company_id -> company.id
            expected_fk = f"{to_table}_id"
            if base_col == expected_fk:
                return 0
            if base_col.endswith('_id') and to_table in base_col:
                return 1
            return 2

    def _build_preferred_path(self, base_table, target_table, intermediates):
        """
        Build path berdasarkan preferred path config.

        Args:
            base_table: Tabel awal
            target_table: Tabel tujuan
            intermediates: List tabel perantara

        Returns:
            list: [(table, join_info), ...] atau None jika path invalid
        """
        path = []
        current = base_table

        # Build path through intermediates
        for next_table in intermediates + [target_table]:
            related = self.get_related_tables(current)
            if next_table not in related:
                # Preferred path invalid, fallback ke BFS
                return None
            path.append((next_table, related[next_table]))
            current = next_table

        return path

    def _add_to_map(self, key, col_info, unique=False):
        """Add column info to map"""
        if unique or key not in self.column_map:
            self.column_map[key] = [col_info]
        else:
            # Cek apakah sudah ada entry yang sama
            existing = self.column_map[key]
            if not any(e['table'] == col_info['table'] and e['column'] == col_info['column'] for e in existing):
                self.column_map[key].append(col_info)

    def _detect_optimal_base_table(self, select_columns, current_base):
        """
        Detect optimal base table berdasarkan kolom yang dipilih.
        Prioritas: job > job_detail > job_schedule > current_base

        Ini memastikan JOIN path yang paling efisien untuk query bisnis.
        """
        # Tabel prioritas tinggi untuk base table
        priority_tables = ['job', 'job_detail', 'job_schedule']

        # Kumpulkan semua tabel dari select columns
        tables_in_query = set(c['table'] for c in select_columns)

        # Pilih base table berdasarkan prioritas
        for table in priority_tables:
            if table in tables_in_query:
                return table

        # Fallback ke current base
        return current_base

    def find_column(self, col_part, prefer_table=None):
        """
        Find column info by name.

        Args:
            col_part: Column name (e.g., 'job_number', 'job.job_number')
            prefer_table: Prefer column from this table if ambiguous

        Returns:
            dict: {'table': 'table_name', 'column': 'column_name'} or None
        """
        col_part = col_part.strip().lower()

        # 1. Cek format table.column atau table_column (explicit)
        if '.' in col_part:
            parts = col_part.split('.', 1)
            table_hint = parts[0]
            col_name = parts[1]

            # Cari di tabel yang dimaksud
            for table, cols in self.table_columns.items():
                if table.lower() == table_hint or table.lower().startswith(table_hint):
                    for c in cols:
                        if c.lower() == col_name or c.lower().replace('_', '') == col_name.replace('_', ''):
                            return {'table': table, 'column': c}

        # 2. Exact match di column_map
        matches = self._get_matches(col_part)

        # 3. Coba dengan underscore diganti spasi
        if not matches:
            col_part_space = col_part.replace('_', ' ')
            if col_part_space != col_part:
                matches = self._get_matches(col_part_space)

        # 4. Coba tanpa underscore
        if not matches:
            col_part_no_underscore = col_part.replace('_', '')
            matches = self._get_matches(col_part_no_underscore)

        # 5. Fuzzy search - cari kolom yang mengandung search term
        if not matches:
            matches = self._fuzzy_search(col_part)

        if not matches:
            return None

        # Jika hanya 1 match, return langsung
        if len(matches) == 1:
            return matches[0].copy()

        # Multiple matches - pilih berdasarkan prioritas
        return self._select_best_match(matches, col_part, prefer_table)

    def _get_matches(self, key):
        """Get matches from column_map"""
        if key in self.column_map:
            return self.column_map[key]
        return []

    def _fuzzy_search(self, search_term):
        """Fuzzy search untuk kolom dengan caching"""
        # Check cache first
        if search_term in self._fuzzy_cache:
            return self._fuzzy_cache[search_term]

        matches = []
        search_clean = search_term.replace('_', '').replace(' ', '')

        for table, cols in self.table_columns.items():
            for col in cols:
                col_lower = col.lower()
                col_clean = col_lower.replace('_', '')

                # Exact column name match
                if col_lower == search_term:
                    matches.append({'table': table, 'column': col, 'score': 100})
                # Column contains search term
                elif search_term in col_lower:
                    matches.append({'table': table, 'column': col, 'score': 80})
                # Search term contains column
                elif col_lower in search_term:
                    matches.append({'table': table, 'column': col, 'score': 70})
                # Clean match (tanpa underscore)
                elif col_clean == search_clean:
                    matches.append({'table': table, 'column': col, 'score': 90})
                elif search_clean in col_clean:
                    matches.append({'table': table, 'column': col, 'score': 60})

        # Sort by score descending
        matches.sort(key=lambda x: x.get('score', 0), reverse=True)

        # Remove score dan return
        result = [{'table': m['table'], 'column': m['column']} for m in matches]

        # Cache result (limit cache size to prevent memory bloat)
        if len(self._fuzzy_cache) < 1000:
            self._fuzzy_cache[search_term] = result

        return result

    def _select_best_match(self, matches, search_term, prefer_table=None):
        """
        Pilih match terbaik dari multiple matches dengan scoring.

        Scoring criteria:
        - prefer_table match: +100
        - Table name exact match prefix: +50
        - Table name partial match: +30
        - Core business table (job, talent, company): +20
        - Table with relations: +10
        """
        if not matches:
            return None

        # Core business tables yang sering digunakan
        core_tables = {'job', 'job_detail', 'job_schedule', 'talent', 'company', 'payment'}

        scored_matches = []
        search_parts = search_term.lower().split('_')

        for m in matches:
            score = 0
            table = m['table'].lower()

            # Prefer table yang diminta
            if prefer_table and table == prefer_table.lower():
                score += 100

            # Table name exact match dengan prefix search term
            # e.g., job_number -> table 'job' (exact)
            if search_parts and table == search_parts[0]:
                score += 50
            elif search_parts and table.startswith(search_parts[0]):
                score += 30

            # Core business table bonus
            if table in core_tables:
                score += 20

            # Table dengan relasi (lebih penting)
            if table in self.relations_cache:
                score += 10

            # Bonus jika column name sama dengan search term
            if m['column'].lower() == search_term:
                score += 5

            scored_matches.append((score, m))

        # Sort by score descending
        scored_matches.sort(key=lambda x: x[0], reverse=True)

        return scored_matches[0][1].copy()

    def find_all_columns(self, col_part):
        """Find all matching columns (untuk debugging)"""
        col_part = col_part.strip().lower()

        matches = self._get_matches(col_part)
        if not matches:
            col_part_space = col_part.replace('_', ' ')
            matches = self._get_matches(col_part_space)
        if not matches:
            matches = self._fuzzy_search(col_part)

        return matches

    def find_boolean_column_for_status(self, table, status_values):
        """Find boolean column for status values"""
        results = []
        table_info = self.schema_cache.get(table, {})
        columns = table_info.get('columns', [])

        bool_cols = [c['name'] for c in columns if c['type'] in ('boolean', 'bool')]
        is_cols = [c for c in bool_cols if c.lower().startswith('is_')]

        for status_val in status_values:
            status_val = status_val.strip().lower()
            found = False

            # Check in status_mappings
            if status_val in self.status_mappings:
                bool_col_name, bool_value = self.status_mappings[status_val]
                if bool_col_name in bool_cols:
                    results.append(({'table': table, 'column': bool_col_name}, bool_value))
                    found = True
                    continue

            # Handle "not_xxx" pattern
            if not found and status_val.startswith('not_'):
                base_status = status_val[4:]
                if base_status in self.status_mappings:
                    bool_col_name, _ = self.status_mappings[base_status]
                    if bool_col_name in bool_cols:
                        results.append(({'table': table, 'column': bool_col_name}, False))
                        found = True
                        continue

                target_col = f'is_{base_status}'
                for is_col in is_cols:
                    if is_col.lower() == target_col:
                        results.append(({'table': table, 'column': is_col}, False))
                        found = True
                        break

            # Try is_{status_val}
            if not found:
                target_col = f'is_{status_val}'
                for is_col in is_cols:
                    if is_col.lower() == target_col:
                        results.append(({'table': table, 'column': is_col}, True))
                        found = True
                        break

            # Partial match
            if not found:
                for is_col in is_cols:
                    col_suffix = is_col.lower().replace('is_', '')
                    if status_val in col_suffix or col_suffix.startswith(status_val):
                        results.append(({'table': table, 'column': is_col}, True))
                        found = True
                        break

        return results if results else None

    def _parse_order_where(self, query_part):
        """
        Extract ORDER BY dan WHERE dari query part.
        Returns: (columns_part, conditions_part, order_by, order_dir)
        """
        order_by = None
        order_dir = 'ASC'
        conditions_part = None

        # Parse ORDER BY
        if ' order by ' in query_part.lower():
            idx = query_part.lower().index(' order by ')
            order_part = query_part[idx + 10:].strip()
            query_part = query_part[:idx].strip()
            order_parts = order_part.split()
            if order_parts:
                order_by = order_parts[0]
                if len(order_parts) > 1 and order_parts[-1].upper() in ['ASC', 'DESC']:
                    order_dir = order_parts[-1].upper()

        # Parse WHERE (also support "when" as alias)
        where_keywords = [' where ', ' when ']
        for where_kw in where_keywords:
            if where_kw in query_part.lower():
                idx = query_part.lower().index(where_kw)
                columns_part = query_part[:idx].strip()
                conditions_part = query_part[idx + len(where_kw):].strip()
                return columns_part, conditions_part, order_by, order_dir

        columns_part = query_part
        return columns_part, conditions_part, order_by, order_dir

    def parse(self, query_text):
        """
        Parse query text and return SQL components.

        Format yang didukung:
        1. primary [col] show [cols] where [cond]
           Contoh: primary job_number show talent_name,company where status=completed

        2. [primary_col]: [cols] where [cond]
           Contoh: job_number: talent_name,company where status=completed

        3. show [cols] where [cond]  (legacy, kolom pertama = primary)
           Contoh: show job_number,talent_name where status=completed
        """
        query_text = query_text.strip()
        query_lower = query_text.lower()

        primary_column = None
        columns_part = None
        conditions_part = None
        order_by = None
        order_dir = 'ASC'

        # Format 1: primary [col] show [cols] ...
        if query_lower.startswith('primary '):
            query_part = query_text[8:].strip()

            # Cari "show"
            show_idx = query_part.lower().find(' show ')
            if show_idx == -1:
                raise ValueError("Format: primary [kolom] show [kolom1,kolom2] where [kondisi]")

            primary_column = query_part[:show_idx].strip()
            query_part = query_part[show_idx + 6:].strip()
            columns_part, conditions_part, order_by, order_dir = self._parse_order_where(query_part)

        # Format 2: [col]: [cols] ...
        elif ':' in query_text and not query_lower.startswith('show '):
            colon_idx = query_text.index(':')
            primary_column = query_text[:colon_idx].strip()
            query_part = query_text[colon_idx + 1:].strip()
            columns_part, conditions_part, order_by, order_dir = self._parse_order_where(query_part)

        # Format 3: show [cols] ... (legacy)
        elif query_lower.startswith('show '):
            query_part = query_text[5:].strip()
            columns_part, conditions_part, order_by, order_dir = self._parse_order_where(query_part)

        else:
            raise ValueError(
                "Format yang didukung:\n"
                "1. primary job_number show talent_name,company where status=completed\n"
                "2. job_number: talent_name,company where status=completed\n"
                "3. show job_number,talent_name where status=completed"
            )

        # Resolve primary column -> base table
        base_table = None
        select_columns = []

        if primary_column:
            primary_info = self.find_column(primary_column)
            if primary_info is None:
                raise ValueError(f"Primary kolom '{primary_column}' tidak ditemukan")

            base_table = primary_info['table']
            select_columns.append(primary_info)

        # Parse SELECT columns (support aggregate functions)
        # Format: count:kolom, sum:kolom, avg:kolom, min:kolom, max:kolom
        aggregate_funcs = {'count', 'sum', 'avg', 'min', 'max'}

        if columns_part:
            for col_str in columns_part.split(','):
                col_str = col_str.strip()
                if not col_str:
                    continue

                # Check for aggregate function prefix
                agg_func = None
                if ':' in col_str:
                    parts = col_str.split(':', 1)
                    if parts[0].lower() in aggregate_funcs:
                        agg_func = parts[0].upper()
                        col_str = parts[1].strip()

                # Prefer base_table, atau cari tabel yang relate ke base_table
                col_info = self.find_column_for_base(col_str, base_table)

                if col_info is None:
                    all_matches = self.find_all_columns(col_str)
                    if all_matches:
                        tables = list(set(m['table'] for m in all_matches))
                        raise ValueError(f"Kolom '{col_str}' ambigu, ada di tabel: {', '.join(tables)}. Gunakan format table.column")
                    else:
                        raise ValueError(f"Kolom '{col_str}' tidak ditemukan")

                # Add aggregate function info
                if agg_func:
                    col_info = col_info.copy()
                    col_info['aggregate'] = agg_func

                select_columns.append(col_info)
                if base_table is None:
                    base_table = col_info['table']

        if not select_columns:
            raise ValueError("Tidak ada kolom yang dipilih")

        # Auto-detect optimal base table jika tidak di-set explicit
        # Prioritas: job > job_detail > job_schedule > lainnya
        if primary_column is None:
            base_table = self._detect_optimal_base_table(select_columns, base_table)

        # Parse WHERE conditions
        where_parts = []
        params = []
        where_tables = set()  # Track tables used in WHERE clause

        if conditions_part:
            conditions = re.split(r'\s+and\s+|,', conditions_part, flags=re.IGNORECASE)

            for cond in conditions:
                cond = cond.strip()
                if not cond:
                    continue
                self._process_condition(cond, base_table, where_parts, params, where_tables)

        return {
            'select_columns': select_columns,
            'base_table': base_table,
            'where_parts': where_parts,
            'params': params,
            'where_tables': where_tables,  # Include tables from WHERE
            'order_by': order_by,
            'order_dir': order_dir,
        }

    def find_column_for_base(self, col_part, base_table):
        """
        Find column, preferring tables that relate to base_table.
        """
        col_part = col_part.strip().lower()

        # 1. Cek exact match dengan table prefix
        if '.' in col_part:
            return self.find_column(col_part)

        # 2. Cari semua matches
        matches = self._get_matches(col_part)
        if not matches:
            # Try with underscore replaced by space
            col_part_space = col_part.replace('_', ' ')
            if col_part_space != col_part:
                matches = self._get_matches(col_part_space)
        if not matches:
            # Try with space replaced by underscore
            col_part_underscore = col_part.replace(' ', '_')
            if col_part_underscore != col_part:
                matches = self._get_matches(col_part_underscore)
        if not matches:
            matches = self._fuzzy_search(col_part)

        if not matches:
            return None

        if len(matches) == 1:
            return matches[0].copy()

        # Multiple matches - pilih berdasarkan relasi ke base_table
        if base_table:
            # Prioritas 1: Kolom dari base_table sendiri
            for m in matches:
                if m['table'] == base_table:
                    return m.copy()

            # Prioritas 2: Kolom dari tabel yang directly relate ke base_table
            related = self.get_related_tables(base_table)
            for m in matches:
                if m['table'] in related:
                    return m.copy()

            # Prioritas 3: Kolom dari tabel yang bisa di-join (indirect)
            for m in matches:
                path = self.find_join_path(base_table, m['table'])
                if path is not None:
                    return m.copy()

        # Fallback ke smart selection biasa
        return self._select_best_match(matches, col_part, base_table)

    def _process_condition(self, cond, base_table, where_parts, params, where_tables=None):
        """Process a single condition"""
        cond_lower = cond.lower()
        if where_tables is None:
            where_tables = set()

        # Date range: daterange=YYYY-MM-DD..YYYY-MM-DD or daterange=YYYY-MM-DD to YYYY-MM-DD
        # Also supports: schedule_date=2025-01-01..2025-12-31
        if '..' in cond or ' to ' in cond_lower:
            # Parse format: column=start..end or column=start to end
            for op in ['=']:
                if op in cond:
                    parts = cond.split(op, 1)
                    col_part = parts[0].strip()
                    range_part = parts[1].strip()

                    # Split by '..' or ' to '
                    if '..' in range_part:
                        date_parts = range_part.split('..', 1)
                    else:
                        date_parts = re.split(r'\s+to\s+', range_part, flags=re.IGNORECASE)

                    if len(date_parts) == 2:
                        start_date = parse_date(date_parts[0].strip().strip("'\""))
                        end_date = parse_date(date_parts[1].strip().strip("'\""))

                        col_info = self.find_column(col_part, prefer_table=base_table)
                        if col_info:
                            where_parts.append(f'"{col_info["table"]}"."{col_info["column"]}"::date BETWEEN %s::date AND %s::date')
                            params.append(start_date)
                            params.append(end_date)
                            where_tables.add(col_info['table'])  # Track table
                            return

        # IS NOT NULL
        if ' is not null' in cond_lower:
            col_part = cond[:cond_lower.index(' is not null')].strip()
            col_info = self.find_column(col_part, prefer_table=base_table)
            if col_info:
                where_parts.append(f'"{col_info["table"]}"."{col_info["column"]}" IS NOT NULL')
                where_tables.add(col_info['table'])  # Track table
            return

        # IS NULL
        if ' is null' in cond_lower:
            col_part = cond[:cond_lower.index(' is null')].strip()
            col_info = self.find_column(col_part, prefer_table=base_table)
            if col_info:
                where_parts.append(f'"{col_info["table"]}"."{col_info["column"]}" IS NULL')
                where_tables.add(col_info['table'])  # Track table
            return

        # Parse with operators
        for op in ['>=', '<=', '!=', '<>', '>', '<', '=']:
            if op in cond:
                parts = cond.split(op, 1)
                col_part = parts[0].strip()
                val_part = parts[1].strip().strip("'\"")
                self._add_condition(col_part, val_part, op, base_table, where_parts, params, where_tables)
                return

        # Space-separated
        parts = cond.split(None, 1)
        if len(parts) == 2:
            col_part = parts[0]
            val_part = parts[1].strip()
            self._add_condition(col_part, val_part, '=', base_table, where_parts, params, where_tables)

    def _add_condition(self, col_part, val_part, op, base_table, where_parts, params, where_tables=None):
        """Add a condition to where_parts and params"""
        col_part_lower = col_part.lower()
        if where_tables is None:
            where_tables = set()

        # Parse multiple values
        if '/' in val_part or '|' in val_part:
            values = re.split(r'[/|]', val_part)
            values = [v.strip().lower() for v in values if v.strip()]
        else:
            values = [val_part.lower()]

        # Check if status condition
        is_status_condition = any(kw in col_part_lower for kw in self.status_keywords)

        if is_status_condition and base_table:
            bool_results = self.find_boolean_column_for_status(base_table, values)
            if bool_results:
                or_conditions = []
                for col_info, bool_value in bool_results:
                    or_conditions.append(f'"{col_info["table"]}"."{col_info["column"]}" = %s')
                    params.append(bool_value)
                    where_tables.add(col_info['table'])  # Track table

                if len(or_conditions) == 1:
                    where_parts.append(or_conditions[0])
                else:
                    where_parts.append(f'({" OR ".join(or_conditions)})')
                return

            raise ValueError(f"Status '{'/'.join(values)}' tidak dikenali.")

        # Regular column condition
        col_info = self.find_column(col_part, prefer_table=base_table)
        if col_info:
            where_tables.add(col_info['table'])  # Track table
            col_ref = f'"{col_info["table"]}"."{col_info["column"]}"'
            col_name = col_info['column'].lower()

            # Get column type from schema
            col_type = self._get_column_type(col_info['table'], col_info['column'])

            # Handle date/timestamp columns with status-like values
            # e.g., completed_on=completed -> is_completed=TRUE or completed_on IS NOT NULL
            if col_type in ('timestamp', 'timestamptz', 'date', 'timestamp with time zone', 'timestamp without time zone'):
                status_values = {'completed', 'done', 'finished', 'yes', 'true', 'ada'}
                if len(values) == 1 and values[0] in status_values:
                    # Check if there's a corresponding boolean column
                    # e.g., completed_on -> is_completed
                    base_col_name = col_name.replace('_on', '').replace('_date', '').replace('_at', '')
                    bool_col_candidates = [f'is_{base_col_name}', f'is_{base_col_name}ed']

                    for bool_col in bool_col_candidates:
                        if self._column_exists(col_info['table'], bool_col):
                            where_parts.append(f'"{col_info["table"]}"."{bool_col}" = %s')
                            params.append(True)
                            return

                    # Fallback: use IS NOT NULL
                    where_parts.append(f'{col_ref} IS NOT NULL')
                    return

                # Handle "not completed", "none", "empty" etc.
                empty_values = {'none', 'null', 'empty', 'kosong', 'belum', 'no', 'false', 'tidak'}
                if len(values) == 1 and values[0] in empty_values:
                    where_parts.append(f'{col_ref} IS NULL')
                    return

            # Handle string columns with ILIKE for partial matching
            if col_type in ('text', 'varchar', 'character varying', 'char', 'character', 'name'):
                if len(values) > 1:
                    # Multiple values: use OR with ILIKE
                    or_conditions = []
                    for v in values:
                        or_conditions.append(f'{col_ref} ILIKE %s')
                        params.append(f'%{v}%')
                    where_parts.append(f'({" OR ".join(or_conditions)})')
                else:
                    # Single value: use ILIKE for partial match
                    where_parts.append(f'{col_ref} ILIKE %s')
                    params.append(f'%{values[0]}%')
                return

            # Default handling for other types (integer, boolean, etc.)
            if len(values) > 1:
                placeholders = ', '.join(['%s'] * len(values))
                where_parts.append(f'{col_ref} IN ({placeholders})')
                params.extend(values)
            else:
                where_parts.append(f'{col_ref} {op} %s')
                params.append(values[0])

    def _get_column_type(self, table, column):
        """Get column type from schema cache"""
        table_info = self.schema_cache.get(table, {})
        columns = table_info.get('columns', [])
        for col in columns:
            if col['name'].lower() == column.lower():
                return col.get('type', '').lower()
        return ''

    def _column_exists(self, table, column):
        """Check if column exists in table"""
        table_info = self.schema_cache.get(table, {})
        columns = table_info.get('columns', [])
        for col in columns:
            if col['name'].lower() == column.lower():
                return True
        return False

    def _build_column_alias(self, table, column, used_aliases=None, user_input=None):
        """
        Build readable alias for a column.

        Rules:
        - Use user's input format if provided (e.g., "product name" -> "product name")
        - If column is 'name', use table_name format (e.g., talent.name -> "talent name")
        - Replace underscores with spaces for readability
        - If alias already used, prefix with table name
        """
        col_lower = column.lower()
        table_lower = table.lower()

        # Common columns that ALWAYS need table prefix to avoid ambiguity
        always_prefix_cols = {'name', 'id', 'code', 'type', 'status', 'description', 'title', 'date'}

        if col_lower in always_prefix_cols:
            alias = f"{table_lower} {col_lower}"
        elif col_lower.startswith(table_lower):
            # Column already includes table name (e.g., job_number -> "job number")
            alias = col_lower.replace('_', ' ')
        else:
            alias = col_lower.replace('_', ' ')

        # Check for duplicate aliases - if used, add table prefix
        if used_aliases is not None and alias in used_aliases:
            alias = f"{table_lower} {col_lower.replace('_', ' ')}"

        return alias

    def build_sql(self, parsed, limit=1000, apply_default_filters=True):
        """Build SQL query from parsed components"""
        select_columns = parsed['select_columns']
        base_table = parsed['base_table']
        where_parts = list(parsed['where_parts'])  # Copy to avoid modifying original
        params = list(parsed['params'])  # Copy params too
        where_tables = parsed.get('where_tables', set())  # Tables from WHERE clause
        order_by = parsed['order_by']
        order_dir = parsed['order_dir']

        # Sanitize all identifiers untuk prevent SQL injection
        sanitize_identifier(base_table)
        for col in select_columns:
            sanitize_identifier(col['table'])
            sanitize_identifier(col['column'])

        # Build SELECT parts with aggregate support and readable aliases
        select_parts = []
        group_by_cols = []
        has_aggregate = any(c.get('aggregate') for c in select_columns)
        used_aliases = set()  # Track used aliases to avoid duplicates

        for c in select_columns:
            col_ref = f'"{c["table"]}"."{c["column"]}"'
            # Build readable alias: table_column or just column for common patterns
            alias = self._build_column_alias(c['table'], c['column'], used_aliases)
            used_aliases.add(alias)

            if c.get('aggregate'):
                # Aggregate function: COUNT, SUM, AVG, MIN, MAX
                agg = c['aggregate']
                agg_alias = f"{agg.lower()}_{alias}"
                select_parts.append(f'{agg}({col_ref}) AS "{agg_alias}"')
            else:
                select_parts.append(f'{col_ref} AS "{alias}"')
                if has_aggregate:
                    # Non-aggregate columns need GROUP BY
                    group_by_cols.append(col_ref)

        sql = f'SELECT {", ".join(select_parts)}\nFROM "{base_table}"'

        # JOINs - include both SELECT and WHERE tables
        tables_needed = set(c['table'] for c in select_columns)
        tables_needed.update(where_tables)  # Add tables from WHERE clause
        join_clauses = self._build_joins(base_table, tables_needed)
        if join_clauses:
            sql += '\n' + '\n'.join(join_clauses)

        # Apply default filters for all joined tables
        applied_default_filters = []
        if apply_default_filters and self.default_filters:
            all_tables = {base_table} | tables_needed
            default_where, default_params, applied_default_filters = self._build_default_filters(all_tables, where_parts)
            where_parts.extend(default_where)
            params.extend(default_params)

        # WHERE
        if where_parts:
            sql += f'\nWHERE {" AND ".join(where_parts)}'

        # GROUP BY (for aggregate queries)
        if group_by_cols:
            sql += f'\nGROUP BY {", ".join(group_by_cols)}'

        # ORDER BY
        if order_by:
            col_info = self.find_column(order_by, prefer_table=base_table)
            if col_info:
                sql += f'\nORDER BY "{col_info["table"]}"."{col_info["column"]}" {order_dir}'

        sql += f'\nLIMIT {limit}'

        # Return sql, params, dan info filter yang diterapkan
        return sql, params, applied_default_filters

    def _build_default_filters(self, tables, existing_where_parts):
        """
        Build default filter conditions for tables.
        Only adds filters if the column isn't already filtered.

        Returns:
            tuple: (where_parts, params, applied_filters_info)
        """
        where_parts = []
        params = []
        applied_filters = []  # List of filter descriptions

        # Extract columns that are already filtered
        existing_filters = set()
        for part in existing_where_parts:
            # Extract table.column from WHERE part like '"table"."column"'
            matches = re.findall(r'"(\w+)"."(\w+)"', part)
            for table, col in matches:
                existing_filters.add(f'{table}.{col}')

        for table in tables:
            if table not in self.default_filters:
                continue

            for flt in self.default_filters[table]:
                col = flt['column']
                key = f'{table}.{col}'

                # Skip if already filtered
                if key in existing_filters:
                    continue

                op = flt.get('op', '=')
                value = flt['value']

                where_parts.append(f'"{table}"."{col}" {op} %s')
                params.append(value)
                existing_filters.add(key)  # Mark as filtered

                # Track applied filter for reporting
                applied_filters.append(f"{table}.{col}{op}{value}")

        return where_parts, params, applied_filters

    def _build_joins(self, base_table, tables_needed):
        """
        Build JOIN clauses menggunakan path finding.
        Mendukung multi-level joins (A -> B -> C).
        """
        join_clauses = []
        joined = {base_table}
        remaining = tables_needed - joined

        for target_table in list(remaining):
            if target_table in joined:
                continue

            # Cari path dari base_table ke target_table
            path = self.find_join_path(base_table, target_table)

            if path:
                # Build joins untuk setiap step di path
                current = base_table
                for next_table, join_info in path:
                    if next_table not in joined:
                        if join_info['join_type'] == 'outgoing':
                            # current punya FK ke next_table
                            join_clauses.append(
                                f'LEFT JOIN "{next_table}" ON "{current}"."{join_info["base_column"]}" = "{next_table}"."{join_info["target_column"]}"'
                            )
                        else:
                            # next_table punya FK ke current
                            join_clauses.append(
                                f'LEFT JOIN "{next_table}" ON "{next_table}"."{join_info["target_column"]}" = "{current}"."{join_info["base_column"]}"'
                            )
                        joined.add(next_table)
                    current = next_table
            else:
                # Fallback: coba direct relation
                # Check if base_table has FK to target
                if base_table in self.relations_cache:
                    for rel in self.relations_cache[base_table]:
                        if rel['to_table'] == target_table:
                            join_clauses.append(
                                f'LEFT JOIN "{target_table}" ON "{base_table}"."{rel["from_column"]}" = "{target_table}"."{rel["to_column"]}"'
                            )
                            joined.add(target_table)
                            break

                # Check if target has FK to base_table
                if target_table not in joined and target_table in self.relations_cache:
                    for rel in self.relations_cache[target_table]:
                        if rel['to_table'] == base_table:
                            join_clauses.append(
                                f'LEFT JOIN "{target_table}" ON "{target_table}"."{rel["from_column"]}" = "{base_table}"."{rel["to_column"]}"'
                            )
                            joined.add(target_table)
                            break

        return join_clauses
