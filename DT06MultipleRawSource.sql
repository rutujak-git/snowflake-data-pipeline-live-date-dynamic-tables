use role sysadmin;
use warehouse compute_wh;
use schema dt_db.raw_sch;

-- create a new employee raw table.
create or replace table dt_db.raw_sch.employee_raw (
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
);

select * from employee_raw;

-- leave raw table
create or replace table dt_db.raw_sch.emp_leave_raw (
    emp_id TEXT,
    leave_type TEXT,
    leave_applied_date DATE,
    leave_start_date DATE,
    leave_end_date DATE,
    leave_days INTEGER,
    status TEXT,
    load_ts timestamp,
    load_row_number number,
    load_file_name text
);

select * from emp_leave_raw;

-- create a new virtual warehouse
create warehouse dt_emp_leave_wh 
    with 
    warehouse_size = 'xsmall' 
    warehouse_type = 'standard' 
    auto_suspend = 60 
    auto_resume = true 
    min_cluster_count = 1
    max_cluster_count = 1 
    scaling_policy = 'standard'
    initially_suspended = True;

create or replace task dt_db.raw_sch.copy_emp02_to_raw
    warehouse = dt_emp_leave_wh
    schedule = '1 minute'
    as
        copy into dt_db.raw_sch.employee_raw
            from (
                select 
                    t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,t.$11,
                    current_timestamp(),
                    metadata$file_row_number,
                    metadata$filename
                from @DT_DB.SOURCE_SCH.DYNAMIC_TBL_STAGE/emp_leave_context/as t
            ) file_format = ( format_name ='dt_db.source_sch.csv_format' );

create or replace task dt_db.raw_sch.copy_leave02_to_raw
    warehouse = dt_emp_leave_wh
    after dt_db.raw_sch.copy_emp02_to_raw
    as
        copy into dt_db.raw_sch.emp_leave_raw from 
        (
                select 
                    t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,
                    current_timestamp(),
                    metadata$file_row_number,
                    metadata$filename
                from @dt_db.source_sch.dynamic_tbl_stage/emp_leave_context/leave_data/ as t
        ) file_format = (format_name = 'dt_db.source_sch.csv_format');


alter task dt_db.raw_sch.copy_leave02_to_raw resume;
alter task dt_db.raw_sch.copy_emp02_to_raw resume;

select * from dt_db.raw_sch.emp_leave_raw;
select * from dt_db.raw_sch.employee_raw;

-- truncate table dt_db.raw_sch.employee_raw;
