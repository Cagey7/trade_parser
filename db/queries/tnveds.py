get_tn_ved_dict_sql = "SELECT code, id FROM tn_veds;"

insert_tn_ved_sql = """
INSERT INTO tn_veds (name, code, digit)
VALUES (%s, %s, %s)
ON CONFLICT (code) DO NOTHING
RETURNING id;
"""

update_measure_sql = """
UPDATE tn_veds
SET measure = %s
WHERE code = %s AND measure IS NULL;
"""