As a pre-requisite , we have to create the following:

**1. Create a data quality table  in databricks**


CREATE OR REPLACE TABLE workspace.default.dq_rules (
    rule_id STRING,
    catalog_name STRING,
    schema_name STRING,
    table_name STRING,
    column_name STRING,
    rule_name STRING,
    rule_condition STRING,     -- example: "amount >= 0"
    rule_description STRING,
    severity STRING,           -- "error" or "warning"
    active BOOLEAN DEFAULT true,
    insert_timestamp TIMESTAMP DEFAULT current_timestamp,
    update_timestamp TIMESTAMP DEFAULT current_timestamp,
    inserted_by STRING DEFAULT current_user(),
    updated_by STRING DEFAULT current_user()
) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

INSERT INTO workspace.default.dq_rules VALUES
('1', 'workspace','default','stg_person', 'email','valid_email_not_null' ,'email is not null','email is not null', 'error', true,current_timestamp,current_timestamp,current_user(),current_user()),
('2', 'workspace','default','stg_person', 'email', 'valid_email_format',"email RLIKE '^[A-Za-z0-9._%+-]+'",'email is properly formatted', 'error', true,current_timestamp,current_timestamp,current_user(),current_user()),
('3', 'workspace','default','stg_person', 'postalcode', 'valid_postalcode_not_null','postalcode is not null','postalcode is not null', 'warning', true,current_timestamp,current_timestamp,current_user(),current_user()),
('4', 'workspace','default','stg_person', 'address_unit', 'valid_address_unit','address_unit is not null','address_unit is not null', 'error', true,current_timestamp,current_timestamp,current_user(),current_user());
