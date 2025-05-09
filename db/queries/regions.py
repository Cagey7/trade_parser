get_region_dict_sql = "SELECT name, id FROM regions;"

insert_region_sql = """
INSERT INTO regions (name, kgd_code, statgov_code, statgov_name)
VALUES (%s, %s, %s, %s)
ON CONFLICT (name) DO NOTHING;
"""

get_region_by_id_sql = """
SELECT id, name, kgd_code, statgov_code, statgov_name
FROM regions
WHERE id = %s;
"""