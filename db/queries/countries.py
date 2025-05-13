get_country_names_all_sql = """
SELECT name_ru AS name, id AS country_id
FROM countries
WHERE name_ru IS NOT NULL

UNION

SELECT name_eng AS name, id AS country_id
FROM countries
WHERE name_eng IS NOT NULL

UNION

SELECT alias AS name, country_id
FROM country_aliases
WHERE alias IS NOT NULL;
"""

insert_country_sql = """
INSERT INTO countries (iso_code, name_ru, name_eng)
VALUES (%s, %s, %s)
ON CONFLICT (iso_code) DO NOTHING;
"""

get_country_id_by_iso_sql = """
SELECT id FROM countries WHERE iso_code = %s;
"""

insert_country_alias_sql = """
INSERT INTO country_aliases (country_id, alias)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
"""


get_get_country_id_by_name_or_alias_sql = """
    SELECT c.id
    FROM countries c
    LEFT JOIN country_aliases a ON a.country_id = c.id
    WHERE c.name_ru = %s OR a.alias = %s
    LIMIT 1;
""" 