# Databricks notebook source
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

# COMMAND ----------

def validate_catalog_exists(catalog_name):
    """
    Checks whether the specified catalog exists in Unity Catalog.
    Raises an exception if not found.
    """
    existing_catalogs = [row["catalog"] for row in spark.sql("SHOW CATALOGS").collect()]
    if catalog_name not in existing_catalogs:
        raise Exception(f"Catalog {catalog_name} not found. Validation failed.")

# COMMAND ----------

def teardown_environment():
    """
    Drops the user-specific catalog created during setup (including all objects).
    """
    user_id = get_username()
    catalog_name = f"{user_id}"

    # Validate the catalog before dropping
    validate_catalog_exists(catalog_name)

    # Drop the catalog along with all objects (schemas, tables) inside
    spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")
    print(f"Teardown successful for user {user_id}. Dropped catalog: {catalog_name}")
