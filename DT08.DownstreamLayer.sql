use role sysadmin;
use warehouse compute_wh;
use schema dt_db.clean_sch;


create or replace dynamic table dt_db.clean_sch.leave_by_cat_dt
    target_lag = '2 minutes'
    warehouse = dt_emp_leave_wh
    as 
        select leave_type,
               status,
               sum(1) as leave_count
        from dt_db.raw_sch.emp_leave_raw
        group by leave_type,
                 status;

select * from dt_db.clean_sch.leave_by_cat_dt;

create or replace dynamic table dt_db.clean_sch.leave_by_category
    target_lag = '2 minutes'
    warehouse = dt_emp_leave_wh
    as
        with MonthlyLeave as (
            select 
                month(leave_start_date) AS leave_month,
                year(leave_start_date) AS leave_year,
                leave_type,
                SUM(leave_days) AS total_leave_days
            from dt_db.raw_sch.emp_leave_raw
            group by leave_month,
                     leave_year,
                     leave_type
        )
        select 
            leave_month,
            leave_year,
            leave_type,
            total_leave_days,
            AVG(total_leave_days) OVER (PARTITION BY leave_type, leave_year ORDER BY leave_month) AS avg_leave_per_month,
            month(current_timestamp()) as current_month
        from MonthlyLeave 
        order by leave_year,
                 leave_month,
                 leave_type;

select * from dt_db.clean_sch.leave_by_category;

alter dynamic table dt_db.clean_sch.emp_leave_clean_dt set target_lag = 'downstream';

create or replace dynamic table dt_db.clean_sch.total_leave_dt
    target_lag = '2 minutes'
    warehouse = dt_emp_leave_wh
as 
    select department,
           sum(total_leave_count) as total_leave_sum
           from dt_db.clean_sch.emp_leave_clean_dt
            group by department
            order by department;

select * from dt_db.clean_sch.total_leave_dt;

