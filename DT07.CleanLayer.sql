use role sysadmin;
use warehouse compute_wh;
use schema dt_db.clean_sch;

create or replace dynamic table dt_db.clean_sch.emp_leave_clean_dt
    warehouse = dt_emp_leave_wh
    target_lag = '2 minutes'
    as 
        select 
            e.emp_id,
            e.first_name,
            e.last_name,
            e.date_of_birth,
            e.date_of_joining,
            e.email_address,
            e.department,
            e.designation,
            e.level,
            e.office_location,
            e.active,
            count(l.leave_type) as total_leave_count
        from dt_db.raw_sch.employee_raw as e 
        left outer join dt_db.raw_sch.emp_leave_raw as l 
        on e.emp_id = l.emp_id
        where 
            e.active = 'Yes' and
            l.status = 'Approved'
        group by 
            e.emp_id,
            e.first_name,
            e.last_name,
            e.date_of_birth,
            e.date_of_joining,
            e.email_address,
            e.department,
            e.designation,
            e.level,
            e.office_location,
            e.active ;
            

    select * from dt_db.clean_sch.emp_leave_clean_dt;