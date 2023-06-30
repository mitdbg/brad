from brad.config.strings import AURORA_SEQ_COLUMN


AURORA_SEQ_CREATE_TABLE_TEMPLATE = (
    "CREATE TABLE {table_name} ({columns}, "
    + AURORA_SEQ_COLUMN
    + " BIGSERIAL, PRIMARY KEY ({pkey_columns}));"
)

AURORA_CREATE_SOURCE_VIEW_TEMPLATE = (
    "CREATE VIEW {view_name} AS SELECT {columns} FROM {source_table_name}"
)

AURORA_DELETE_TRIGGER_FN_TEMPLATE = """
    CREATE OR REPLACE FUNCTION {trigger_fn_name}()
        RETURNS trigger AS
    $BODY$
    BEGIN
    INSERT INTO {shadow_table_name} ({pkey_cols}) VALUES ({pkey_vals})
        ON CONFLICT DO NOTHING;
    RETURN NULL;
    END;
    $BODY$
    LANGUAGE plpgsql VOLATILE;
"""

AURORA_UPDATE_TRIGGER_FN_TEMPLATE = """
    CREATE OR REPLACE FUNCTION {trigger_fn_name}()
        RETURNS trigger AS
    $BODY$
    BEGIN
        NEW.{seq_col} = nextval('{seq_name}');
        RETURN NEW;
    END;
    $BODY$
    LANGUAGE plpgsql VOLATILE;
"""

AURORA_TRIGGER_TEMPLATE = """
    CREATE TRIGGER {trigger_name}
    {trigger_cond} ON {table_name}
    FOR EACH ROW
    EXECUTE PROCEDURE {trigger_fn_name}();
"""

AURORA_CREATE_BTREE_INDEX_TEMPLATE = (
    "CREATE INDEX {index_name} ON {table_name} USING btree ({columns});"
)

AURORA_SEQ_COL_INDEX_TEMPLATE = (
    "CREATE INDEX {index_name} ON {table_name} USING btree (" + AURORA_SEQ_COLUMN + ");"
)

AURORA_BARE_OR_REDSHIFT_CREATE_TABLE_TEMPLATE = (
    "CREATE TABLE {table_name} ({columns}, PRIMARY KEY ({pkey_columns}));"
)

ATHENA_CREATE_TABLE_TEMPLATE = "CREATE TABLE {table_name} ({columns}) LOCATION '{s3_path}' TBLPROPERTIES ('table_type' = 'ICEBERG');"
