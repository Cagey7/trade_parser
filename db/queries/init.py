create_tables_sql = """
    CREATE TABLE IF NOT EXISTS tn_veds (
        id SERIAL PRIMARY KEY,
        name VARCHAR,
        code VARCHAR(10) NOT NULL UNIQUE,
        digit INTEGER NOT NULL CHECK (digit IN (2, 4, 6, 10)),
        measure VARCHAR(127)
    );

    CREATE TABLE IF NOT EXISTS countries (
        id SERIAL PRIMARY KEY,
        iso_code VARCHAR(3) NOT NULL UNIQUE,
        name_ru VARCHAR(255),
        name_eng VARCHAR(255)
    );

    CREATE TABLE IF NOT EXISTS country_aliases (
        id SERIAL PRIMARY KEY,
        alias VARCHAR(255),
        country_id INTEGER REFERENCES countries(id),
        UNIQUE (country_id, alias)
    );

    CREATE TABLE IF NOT EXISTS regions (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        kgd_code VARCHAR(2) UNIQUE,
        statgov_code VARCHAR(5) UNIQUE,
        statgov_name VARCHAR(255) UNIQUE
    );

    CREATE TABLE IF NOT EXISTS okeds (
        id SERIAL PRIMARY KEY,
        name VARCHAR(1023) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS tn_ved_oked (
        id SERIAL PRIMARY KEY,
        oked_id INTEGER REFERENCES okeds(id),
        tn_ved_id INTEGER REFERENCES tn_veds(id)
    );

    CREATE TABLE IF NOT EXISTS data (
        id SERIAL PRIMARY KEY,
        export_tonn DOUBLE PRECISION,
        export_units DOUBLE PRECISION,
        export_value DOUBLE PRECISION,
        import_tonn DOUBLE PRECISION,
        import_units DOUBLE PRECISION,
        import_value DOUBLE PRECISION,
        country_id INTEGER REFERENCES countries(id),
        region_id INTEGER REFERENCES regions(id),
        tn_ved_id INTEGER REFERENCES tn_veds(id),
        year INTEGER NOT NULL CHECK (year BETWEEN 1970 AND 2200),
        month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
        updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS data_stats (
        id SERIAL PRIMARY KEY,
        region_id INTEGER REFERENCES regions(id),
        year INTEGER NOT NULL,
        month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
        digit INTEGER NOT NULL CHECK (digit IN (2, 4, 6, 10)),
        source_type VARCHAR(10) NOT NULL CHECK (source_type IN ('statgov', 'kgd')),
        updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (region_id, year, month, digit, source_type)
    );
"""

check_database_sql = "SELECT to_regclass('public.tn_veds');"