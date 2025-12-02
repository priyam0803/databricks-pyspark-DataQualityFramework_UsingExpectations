
from pyspark import pipelines as dp
from pyspark.sql.functions import expr, col, lit, array, when, array_union,current_timestamp
from pyspark.sql import functions as F
import dlt

#Parameterized variables
catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("schema_name")
bronze_table_name = spark.conf.get("bronze_table_name")
quarantine_table_name = spark.conf.get("quarantine_table_name")
quarantine_history_table_name = spark.conf.get("quarantine_history_table_name")

########################################################################
##Uncomment to create materialized views for valid and invalid data
#valid_data_table_name = spark.conf.get("valid_data_table_name")   
#invalid_data_table_name = spark.conf.get("invalid_data_table_name")
#######################################################################




# Read the rules table dynamically
rules_df = spark.table("dq_rules")

#Filter rules for the current table
def get_rules_for_table(catalog_name: str,schema_name: str,bronze_table_name: str):
    return (rules_df
            .filter(F.col("catalog_name") == catalog_name)
            .filter(F.col("schema_name") == schema_name)
            .filter(F.col("table_name") == bronze_table_name)
            .filter(F.col("active") == True)
            .collect())

rules = {row.rule_name: row.rule_condition for row in rules_df.collect()}



#Iterate through the rules and apply them to the bronze table. The table also captures the failed rules for each record. It contains a column named is_quarantine which is set to true if any of the rules fail.
@dp.table(name=quarantine_table_name)
@dp.expect_all(rules)
def quarantine():
    df = spark.readStream.table(bronze_table_name)

    # Initialize empty array to store failing rule names
    df = df.withColumn("failed_rules", array())

    # Evaluate each rule dynamically
    for rule_name, rule_expr in rules.items():
        df = df.withColumn(
            rule_name,
            expr(rule_expr)
        ).withColumn(
            "failed_rules",
            when(
                col(rule_name) == False,
                array_union(col("failed_rules"), array(lit(rule_name)))
            ).otherwise(col("failed_rules"))
        )

    # Mark row as quarantined if ANY rule failed
    df = df.withColumn(
        "is_quarantine",
        (col("failed_rules").getItem(0).isNotNull())
    )

    return df

###########################################################################################
###Uncomment the below lines to create materialized views for valid and invalid data###
#@dp.materialized_view(name=valid_data_table_name)
#def valid_data():
#  return spark.read.table(quarantine_table_name).filter("is_quarantined=false")

#@dp.materialized_view(name=invalid_data_table_name)
#def invalid_data():
#  return spark.read.table(quarantine_table_name).filter("is_quarantined=true")
############################################################################################

#Load errored records to quarantine history table 
@dlt.table(
    name=quarantine_history_table_name,
    comment="Append-only historical table for all error rows"
)

def quarantine_history():
    # Use STREAM READ to append new errors forever
    return (
    dlt.read_stream(quarantine_table_name)
    .filter("is_quarantine=true")
    
    .withColumn("error_load_timestamp", current_timestamp())
    .withColumn("table_name", lit(bronze_table_name))
    )
    
