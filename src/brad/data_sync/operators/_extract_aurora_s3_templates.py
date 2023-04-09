from brad.config.strings import AURORA_EXTRACT_PROGRESS_TABLE_NAME, AURORA_SEQ_COLUMN


# Used to export data from Aurora to S3.
EXTRACT_S3_TEMPLATE = """
SELECT * from aws_s3.query_export_to_s3(
    '{extract_query}',
    aws_commons.create_s3_uri('{s3_bucket}', '{s3_file_path}', '{s3_region}'),
    options :='FORMAT text, DELIMITER ''|'''
);"""
EXTRACT_FROM_MAIN_TEMPLATE = (
    "SELECT {table_cols} FROM {main_table_name} WHERE "
    + AURORA_SEQ_COLUMN
    + " >= {lower_bound} AND "
    + AURORA_SEQ_COLUMN
    + " <= {upper_bound}"
)
EXTRACT_FROM_SHADOW_TEMPLATE = (
    "SELECT {pkey_cols} FROM {shadow_table_name} WHERE "
    + AURORA_SEQ_COLUMN
    + " >= {lower_bound} AND "
    + AURORA_SEQ_COLUMN
    + " <= {upper_bound}"
)


# Used to update the state of data synchronization (so later syncs are incremental).
UPDATE_EXTRACT_PROGRESS_BOTH = (
    "UPDATE "
    + AURORA_EXTRACT_PROGRESS_TABLE_NAME
    + " SET next_extract_seq = {next_main}, next_shadow_extract_seq = '{next_shadow}' WHERE table_name = '{table_name}'"
)
UPDATE_EXTRACT_PROGRESS_SHADOW = (
    "UPDATE "
    + AURORA_EXTRACT_PROGRESS_TABLE_NAME
    + " SET next_shadow_extract_seq = {next_shadow} WHERE table_name = '{table_name}'"
)
UPDATE_EXTRACT_PROGRESS_NON_SHADOW = (
    "UPDATE "
    + AURORA_EXTRACT_PROGRESS_TABLE_NAME
    + " SET next_extract_seq = {next_main} WHERE table_name = '{table_name}'"
)
DELETE_FROM_SHADOW = (
    "DELETE FROM {shadow_table} WHERE "
    + AURORA_SEQ_COLUMN
    + " >= {lower_bound} AND "
    + AURORA_SEQ_COLUMN
    + " <= {upper_bound}"
)
