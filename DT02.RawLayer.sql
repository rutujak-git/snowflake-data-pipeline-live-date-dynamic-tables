use role sysadmin;
use warehouse compute_wh;
use schema dt_db.raw_sch;

--------------------- Step 6: Create an employee table -------------------
create or replace table dt_db.raw_sch.employee_raw(
    emp_id text primary key,
    first_name text,
    last_name text,
    date_of_birth date,
    date_of_joining date,
    email_address text,
    department text,
    designation text,
    level text,
    office_location text,
    active text,
    load_ts timestamp,
    load_row_number number,
    load_file_name text 
) comment = 'this is customer raw table where raw data will be stored';

select count(*) from dt_db.raw_sch.employee_raw;

--------------------- Step 7: Create a Warehouse  -------------------
create or replace warehouse dt_01_wh
    warehouse_size = 'x-small'
    warehouse_type = 'standard'
    min_cluster_count = 1
    max_cluster_count = 1
    initially_suspended = true,
    auto_suspend = 60
    auto_resume = true;

--------------------- Step 8: Create Task to copy data from stage to employee raw table  -------------------
create or replace task dt_db.raw_sch.copy_emp_to_raw_task
    warehouse = dt_01_wh
    schedule = '2 minute'
    as 
        copy into dt_db.raw_sch.employee_raw from
            (
                select 
                t.$1,
                t.$2,
                t.$3,
                t.$4,
                t.$5,
                t.$6,
                t.$7,
                t.$8,
                t.$9,
                t.$10,
                t.$11,
                current_timestamp(),
                metadata$file_row_number,
                metadata$filename
            from @dt_db.source_sch.dynamic_tbl_stage/employees/ as t
            ) file_format = (format_name = 'dt_db.source_sch.csv_format')
              on_error = continue;

--------------------- Step 9: check the objects + task graph home home page = error  -------------------
--------------------- Step 10: Make sure that your user must have necessary privileges -------------------
use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;

--------------------- Step 11: Resume the task -------------------
alter task  dt_db.raw_sch.copy_emp_to_raw_task resume;
alter task  dt_db.raw_sch.copy_emp_to_raw_task suspend;

select * from dt_db.raw_sch.employee_raw;

select count(*) from dt_db.raw_sch.employee_raw;