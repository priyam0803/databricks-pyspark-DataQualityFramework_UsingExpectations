# databricks-pyspark-DataQualityFramework_UsingExpectations

This is a config driven data quality framework built in databricks pyspark using expectations. 

1.The notebook accepts the following parameters:

  a.catalog_name - Name of the catalog where the tables reside
  
  b.schema_name - Name of the schema where the tables reside
  
  c.bronze_table_name - Name of the bronze table
  
  d.quarantine_table_name - Name of the silver table which will hold both valid and invalid data
  
  e.quarantine_history_table_name - Name of the history table which will hold all historical invalid data for all tables
  
2.The table DQ_RULES contains all the rules for the tables

3.Each rule is validated for every row in the bronze table and the results are captured in the quarantine_table_name. The table also captures the failed rules for each record. It contains a column named is_quarantine which is set to true if any of the rules fail.

4.All the invalid records are also stored in a historical table  quarantine_history_table_name
