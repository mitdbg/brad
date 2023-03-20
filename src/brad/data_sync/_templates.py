from brad.config.strings import AURORA_EXTRACT_PROGRESS_TABLE_NAME

# Used to get the state of data synchronization (i.e., where did we "stop" last time the sync job ran?)
GET_NEXT_EXTRACT = "SELECT next_extract_seq, next_shadow_extract_seq FROM {} WHERE table_name = ?".format(
    AURORA_EXTRACT_PROGRESS_TABLE_NAME
)
GET_MAX_EXTRACT_TEMPLATE = "SELECT MAX(brad_seq) FROM {table_name}"


# Used to export data from Aurora to S3.
EXTRACT_S3_TEMPLATE = """
SELECT * from aws_s3.query_export_to_s3(
    '{query}',
    aws_commons.create_s3_uri('{s3_bucket}', '{s3_file_path}', '{s3_region}'),
    options :='FORMAT text, DELIMITER ''|'''
);"""
EXTRACT_FROM_MAIN_TEMPLATE = "SELECT {table_cols} FROM {main_table} WHERE brad_seq >= {lower_bound} AND brad_seq <= {upper_bound}"
EXTRACT_FROM_SHADOW_TEMPLATE = "SELECT {pkey_cols} FROM {shadow_table} WHERE brad_seq >= {lower_bound} AND brad_seq <= {upper_bound}"


# Used to import data from S3 into Redshift.
REDSHIFT_CREATE_STAGING_TABLE = (
    "CREATE TEMPORARY TABLE {staging_table} (LIKE {base_table})"
)
REDSHIFT_CREATE_SHADOW_STAGING_TABLE = (
    "CREATE TEMPORARY TABLE {shadow_staging_table} ({pkey_cols})"
)
REDSHIFT_IMPORT_COMMAND = "COPY {dest_table} FROM '{s3_file_path}' IAM_ROLE '{s3_iam_role}' REGION '{s3_region}'"
REDSHIFT_DELETE_COMMAND = (
    "DELETE FROM {main_table} USING {staging_table} WHERE {conditions}"
)
REDSHIFT_INSERT_COMMAND = "INSERT INTO {dest_table} SELECT * FROM {staging_table}"


# Used to merge in writes from S3 into Redshift.
ATHENA_CREATE_STAGING_TABLE = """
    CREATE EXTERNAL TABLE {staging_table} ({columns})
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION '{s3_location}'"""
ATHENA_MERGE_COMMAND = """
    MERGE INTO {main_table} AS t
    USING (
        SELECT
        {pkey_cols},
        {other_cols},
        0 AS brad_is_delete
        FROM {staging_table}
        UNION ALL
        SELECT
        {pkey_cols},
        {other_cols_as_null},
        1 AS brad_is_delete
        FROM {shadow_staging_table}
    ) AS s
    ON {merge_cond}
    WHEN MATCHED AND s.brad_is_delete = 1
        THEN DELETE
    WHEN MATCHED AND s.brad_is_delete != 1
        THEN UPDATE SET {update_cols}
    WHEN NOT MATCHED AND s.brad_is_delete != 1
        THEN INSERT VALUES ({insert_cols});"""


# Used to update the state of data synchronization (so later syncs are incremental).
UPDATE_EXTRACT_PROGRESS_BOTH = "UPDATE {} SET next_extract_seq = ?, next_shadow_extract_seq = ? WHERE table_name = ?".format(
    AURORA_EXTRACT_PROGRESS_TABLE_NAME
)
UPDATE_EXTRACT_PROGRESS_SHADOW = (
    "UPDATE {} SET next_shadow_extract_seq = ? WHERE table_name = ?".format(
        AURORA_EXTRACT_PROGRESS_TABLE_NAME
    )
)
UPDATE_EXTRACT_PROGRESS_NON_SHADOW = (
    "UPDATE {} SET next_extract_seq = ? WHERE table_name = ?".format(
        AURORA_EXTRACT_PROGRESS_TABLE_NAME
    )
)
DELETE_FROM_SHADOW_STAGING = "DELETE FROM {shadow_staging_table} WHERE brad_seq >= {lower_bound} AND brad_seq <= {upper_bound}"
