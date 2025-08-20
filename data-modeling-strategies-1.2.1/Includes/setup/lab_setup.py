# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup Notebook
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Derives a unique user identifier from the user’s email.
# MAGIC 2. Creates a catalog named as lab user's user_id.
# MAGIC 3. Creates `bronze`, `silver`, and `gold` schemas in that catalog.
# MAGIC 4. Copies TPC-H tables into the `bronze` schema (CTAS) from the built-in `samples.tpch` tables.
# MAGIC 5. Grants privileges so that only this user can access these schemas.
# MAGIC 6. Validates the setup by confirming the catalog, schemas, and tables have been created successfully.
# MAGIC
# MAGIC **Important**: If any validation step fails, it raises an exception, ensuring that your environment is fully set up.

# COMMAND ----------

import re

def get_username():
    """
    Retrieves the current user’s email from the SQL function `current_user()`,
    then extracts the portion before the "@" to use as a user ID.
    Returns the user ID as a string.
    """
    user_email = spark.sql("SELECT current_user() AS user").collect()[0]["user"]
    user_id = user_email.split("@")[0]
    user_id_cleaned = re.sub(r'[^a-zA-Z0-9]', '_', user_id) # New Code
    # return user_id
    return user_id_cleaned


def validate_catalog_exists(catalog_name):
    """
    Checks whether the specified catalog exists in Unity Catalog.
    Raises an exception if not found.
    """
    existing_catalogs = [row["catalog"] for row in spark.sql("SHOW CATALOGS").collect()]
    if catalog_name not in existing_catalogs:
        raise Exception(f"Catalog {catalog_name} not found. Validation failed.")


def validate_schema_exists(catalog_name, schema_name):
    """
    Checks whether the specified schema exists within the given catalog.
    Raises an exception if not found.
    """
    full_schema_name = f"{catalog_name}.{schema_name}"
    existing_schemas = [
        row["databaseName"]
        for row in spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
    ]
    if schema_name not in existing_schemas:
        raise Exception(f"Schema {full_schema_name} not found. Validation failed.")


def validate_table_exists(full_table_name):
    """
    Checks whether the specified table exists.
    Expects a string of the form <catalog>.<schema>.<table>.
    Raises an exception if the table is not found.
    """
    parts = full_table_name.split(".")
    if len(parts) != 3:
        raise Exception(
            f"Table name {full_table_name} should include catalog.schema.table"
        )

    catalog_name, schema_name, table_name = parts
    existing_tables = [
        row["tableName"]
        for row in spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
    ]
    if table_name not in existing_tables:
        raise Exception(f"Table {full_table_name} not found. Validation failed.")


def validate_setup(catalog_name, schema_names, table_list):
    """
    Overall validation function to confirm:
      1. The catalog exists.
      2. Each schema in schema_names exists.
      3. Each table in table_list (within the 'bronze' schema) exists.
    Raises exceptions if any validation fails.
    """
    # 1. Validate the catalog
    validate_catalog_exists(catalog_name)

    # 2. Validate each schema
    for s_name in schema_names:
        validate_schema_exists(catalog_name, s_name)

    # 3. Validate each table in the 'bronze' schema
    for tbl in table_list:
        bronze_table_fqn = f"{catalog_name}.bronze.{tbl}"
        validate_table_exists(bronze_table_fqn)

# COMMAND ----------


def setup_environment():
    """
    Creates a user-specific catalog and the schemas bronze/silver/gold,
    copies TPC-H tables from samples.tpch to the new catalog's bronze schema,
    and configures privileges so that only the user can access them.

    If everything goes well, it runs final validation. Otherwise, it raises exceptions.
    """
    user_id = get_username()
    catalog_name = f"{user_id}"
    schema_names = ["bronze", "silver", "gold"]

    # Create catalog
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

    # Create schemas (bronze, silver, gold)
    for s_name in schema_names:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{s_name}")

    # Grant usage on the catalog only to the current user (optional, adjust to your security requirements).
    # If your environment requires more fine-grained privileges, adapt as needed.
    current_user_email = spark.sql("SELECT current_user() AS user").collect()[0]["user"]
    spark.sql(f"GRANT USAGE ON CATALOG {catalog_name} TO `{current_user_email}`")
    spark.sql(f"ALTER CATALOG {catalog_name} OWNER TO `{current_user_email}`")

    for s_name in schema_names:
        spark.sql(
            f"GRANT CREATE, USAGE, SELECT, MODIFY ON SCHEMA {catalog_name}.{s_name} TO `{current_user_email}`"
        )
        spark.sql(
            f"ALTER SCHEMA {catalog_name}.{s_name} OWNER TO `{current_user_email}`"
        )

    # List of TPC-H tables to copy into the bronze schema
    tpch_tables = [
        "nation",
        "region",
        "orders",
        "customer",
        "lineitem",
        "part",
        "partsupp",
        "supplier",
    ]

    # Copy TPC-H tables into the user’s bronze schema
    # This uses CREATE TABLE AS SELECT (CTAS)
    for tbl in tpch_tables:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.bronze.{tbl}")
        spark.sql(
            f"""
            CREATE TABLE {catalog_name}.bronze.{tbl} AS 
            SELECT * FROM samples.tpch.{tbl}
        """
        )

    # Validate everything
    validate_setup(catalog_name, schema_names, tpch_tables)
    print(
        f"Setup successful for user {user_id}. Catalog created: {catalog_name}, with schemas: {schema_names}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute the Setup
# MAGIC Run the following cell to perform the setup. If all validations pass, you'll see a success message.
# MAGIC If anything fails, the script raises an exception.

# COMMAND ----------

setup_environment()
