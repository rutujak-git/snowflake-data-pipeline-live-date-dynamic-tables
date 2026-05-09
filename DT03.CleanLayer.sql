use role sysadmin;
use warehouse compute_wh;
use schema dt_db.clean_sch;

--------------------- Step 12: Create Dynamic Table  -------------------
create or replace dynamic table dt_db.clean_sch.employees_clean_dt(
    emp_id,
    first_name,
    last_name,
    dob comment 'Date of Birth',
    doj comment 'Date of Joining',
    email comment 'Email Address',
    department,
    designation,
    emp_level,
    offica_location,
    active_flag comment 'Employee Status'
)
target_lag = '5 minutes'
warehouse = dt_01_wh
as
select 
    emp_id,
    first_name,
    last_name,
    date_of_birth,
    date_of_joining,
    email_address,
    department,
    designation,
    level,
    office_location,
    active
from 
    dt_db.raw_sch.employee_raw 
    where active = 'Yes';

select count(*) from employees_clean_dt;

