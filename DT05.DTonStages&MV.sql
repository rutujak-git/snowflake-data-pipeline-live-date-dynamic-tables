use role sysadmin;
use warehouse compute_wh;
use schema dt_db.clean_sch;

----------------------- is it possible to create a dynamic table on stage - NO ---------------------
create or replace dynamic table dt_db.clean_sch.employees_01_clean_dt
    target_lag='5 minutes'
    warehouse=dt_01_wh
AS
select 
        t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,t.$11,
        current_timestamp(),
        metadata$file_row_number,
        metadata$filename
    from @dt_db.source_sch.dynamic_tbl_stage/employees/ as t;

-- Dynamic tables don't support stages as sources. You can only use tables, views(which are not based on stages like below exampl, Iceberg tables, and other dynamic tables.

------------------------- what if we create a view and then create a dynamic table - NO  -------------------

create or replace view dt_db.raw_sch.employee_raw_vw 
(   emp_id,first_name,last_name,date_of_birth,date_of_joining,email_address,department,designation,level,office_location,active,
    load_ts,
    load_timestamp,
    load_filename
 
)
as
select 
    t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,t.$11,
    current_timestamp(),
    metadata$file_row_number,
    metadata$filename
from @dt_db.source_sch.dynamic_tbl_stage/employees/ as t;


-- get the data from view
select * from dt_db.raw_sch.employee_raw_vw limit 10;

create or replace dynamic table dt_db.clean_sch.employees_02_clean_dt
    target_lag='5 minutes'
    warehouse=dt_01_wh
AS
select 
    emp_id,first_name,last_name,date_of_birth,date_of_joining,email_address,department,designation,level,office_location,active
from dt_db.raw_sch.employee_raw_vw where active = 'Yes';


--------------------------------- Materialized view not supported over a stage --------------

create or replace materialized view dt_db.raw_sch.employee_raw_mvw 
(
    emp_id,first_name,last_name,date_of_birth,date_of_joining,email_address,department,designation,level,office_location,active,
    load_ts,
    load_timestamp,
    load_filename
    
)
as
select 
        t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,t.$11,
        current_timestamp(),
        metadata$file_row_number,
        metadata$filename
    from @dt_db.source_sch.dynamic_tbl_stage/employees/ as t;

/*
    SQL compilation error: error line 1 at position 0
Invalid materialized view definition. Materialized view not supported over a stage.
     */
-- 002236 (0A000): SQL compilation error: error line 1 at position 0
-- Invalid materialized view definition. Materialized view not supported over a stage.
