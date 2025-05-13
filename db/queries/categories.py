insert_country_groups_sql = """
    INSERT INTO country_groups (name)
    VALUES (%s)
    ON CONFLICT (name) DO NOTHING
    RETURNING id;
"""

insert_country_group_membership_sql = """
    INSERT INTO country_group_membership (country_id, country_group_id)
    VALUES (%s, %s)
    ON CONFLICT (country_id, country_group_id) DO NOTHING;
"""

insert_tn_ved_categories_sql = """
    INSERT INTO tn_ved_categories (name, parent_id)
    VALUES (%s, %s)
    ON CONFLICT (name) DO NOTHING
    RETURNING id;
"""

insert_tn_ved_category_map_sql = """
    INSERT INTO tn_ved_category_map (tn_ved_id, tn_ved_category_id)
    VALUES (%s, %s)
    ON CONFLICT (tn_ved_id, tn_ved_category_id) DO NOTHING;
"""