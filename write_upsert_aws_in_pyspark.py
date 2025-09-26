from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def update_and_load_to_aurora(
    spark: SparkSession,
    current_data: DataFrame,
    new_data: DataFrame,
    primary_keys: list,
    aurora_jdbc_url: str,
    aurora_table_name: str,
    aurora_properties: dict
):
    """
    Updates rows in an existing DataFrame with values from a new DataFrame
    if values have changed, then loads the updated data to an AWS Aurora table.

    Args:
        spark: The SparkSession object.
        current_data: The DataFrame representing the existing data
        new_data: The DataFrame containing the new/updated data.
        primary_keys: A list of column names that form the primary key for joining.
        aurora_jdbc_url: The JDBC URL for the Aurora database.
        aurora_table_name: The name of the table in Aurora.
        aurora_properties: A dictionary containing JDBC connection properties
                           (e.g., user, password, driver).
    """

        # -------------------------------------------------------------------------------
    # Identify rows to update (primary keys exist in both, and values have changed)
    # and new rows to insert (primary keys exist only in new_data)

    # Join source and target DFs on primary keys
    joined_df = new_data.alias("nd").join(
        current_data.alias("cd"),
        on=primary_keys,
        how="fullouter"
    )

    # Identify rows that need updating (primary keys match, but other columns differ)
    # This requires comparing all non-primary key columns.
    # For simplicity, this example assumes a full replacement if any non-PK column changes.
    # More granular comparison can be added if needed.
    updated_rows = joined_df.filter(
        (col("nd." + primary_keys[0]).isNotNull()) & (col("cd." + primary_keys[0]).isNotNull()) &
        (
            # Add conditions to check if non-primary key columns have changed; this is example for 2 cols
            (col("nd.value_col") != col("cd.value_col")) |
            (col("nd.another_col") != col("cd.another_col")) # Add more for all relevant columns
        )
    ).select([col(f"nd.{pk}") for pk in primary_keys] + [col(f"nd.{c}") for c in new_data.columns if c not in primary_keys])

    # Identify new rows (primary keys exist only in new_data)
    new_rows = joined_df.filter(col("cd." + primary_keys[0]).isNull()).select("nd.*")

    # Identify rows to keep from current_data (primary keys exist only in current_data or no changes)
    # Do we need to do this?  We could filter new and changed out
    unchanged_rows = joined_df.filter(
        (col("nd." + primary_keys[0]).isNotNull()) & (col("cd." + primary_keys[0]).isNotNull()) &
        (
            # Add conditions to check if non-primary key columns have NOT changed
            # This is the inverse of the updated_rows condition and seems unwieldly
            ~((col("nd.value_col") != col("cd.value_col")) |
              (col("nd.another_col") != col("cd.another_col"))) # Add more for all relevant columns
        )
    ).select("cd.*")

    # Combine all dataframes: new, updated, and unchanged rows
    final_df = new_rows.unionByName(updated_rows).unionByName(unchanged_rows)

    # Write the final DataFrame to Aurora
    final_df.write \
        .format("jdbc") \
        .option("url", aurora_jdbc_url) \
        .option("dbtable", aurora_table_name) \
        .option("user", aurora_properties["user"]) \
        .option("password", aurora_properties["password"]) \
        .option("driver", aurora_properties["driver"]) \
        .mode("overwrite") \
        .save()

    print(f"Data successfully updated and loaded to Aurora table: {aurora_table_name}")
