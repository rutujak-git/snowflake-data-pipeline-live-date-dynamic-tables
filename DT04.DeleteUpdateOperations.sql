use role sysadmin;
use warehouse compute_wh;
use schema dt_db.clean_sch;

select * from dt_db.raw_sch.employee_raw where emp_id between 1 and 10; 

alter task  dt_db.raw_sch.copy_emp_to_raw_task resume;

delete from dt_db.raw_sch.employee_raw where emp_id between 1 and 10; 

select * from dt_db.clean_sch.employees_clean_dt order by emp_id;

select * from dt_db.raw_sch.employee_raw where 
    level = 'L1' and 
    emp_id between 11 and 20;

update dt_db.raw_sch.employee_raw set level = 'L0'
where 
    level = 'L1' and 
    emp_id between 11 and 20;