alter table stg_person 
add column filename varchar(200);


alter table stg_person
add column load_timestamp timestamp;

update stg_person
set filename = 'person_10000.csv',
load_timestamp = current_timestamp

