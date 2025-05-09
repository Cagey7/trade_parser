insert_data_sql = """
INSERT INTO data (
    export_tonn, export_units, export_value,
    import_tonn, import_units, import_value,
    country_id, region_id, tn_ved_id,
    year, month, updated
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""