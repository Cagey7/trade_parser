check_data_stats_exists_sql = """
SELECT 1 FROM data_stats
WHERE region_id = %s AND year = %s AND month = %s AND digit = %s AND source_type = %s
LIMIT 1;
"""

insert_or_update_data_stats_sql = """
INSERT INTO data_stats (region_id, year, month, digit, source_type, updated)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (region_id, year, month, digit, source_type)
DO UPDATE SET updated = EXCLUDED.updated;
"""