use role sysadmin;
use warehouse compute_wh;
use schema dt_dbv2.raw;

create or replace table dt_dbv2.raw.customer_raw (
    cust_key number,
    name text,
    address text,
    nation_name text,
    phone text,
    acct_bal number,
    mkt_segment text,
    load_ts timestamp,
    load_row_number number,
    load_file_name text 
);

create or replace table dt_dbv2.raw.order_raw (
    order_key number,
    cust_key number,
    order_status text(1),
    total_price number,
    order_date date,
    order_priority text,
    clerk text,
    ship_priority number(1),
    load_ts timestamp,
    load_row_number number,
    load_file_name text 
);

create or replace warehouse dt_task_load_wh
    warehouse_size = 'x-small'
    warehouse_type = 'standard'
    initially_suspended = true
    auto_suspend = 60
    auto_resume = true
    min_cluster_count = 1
    max_cluster_count = 1
    scaling_policy = 'standard';

create or replace task dt_dbv2.raw.copy_to_customer_raw_task
    warehouse = dt_task_load_wh
    schedule = '2 minute'
    as
    copy into dt_dbv2.raw.customer_raw from
    (
        select 
            t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,
            current_timestamp(),
            metadata$file_row_number,
            metadata$filename
        from @dt_dbv2.source.my_stage/customer/ as t
    ) file_format = (format_name = 'dt_dbv2.source.csv_format');


create or replace task dt_dbv2.raw.copy_to_order_raw_task
    warehouse = dt_task_load_wh
    schedule = '2 minute'
    as
    copy into dt_dbv2.raw.order_raw from
    (
        select 
            t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,
            current_timestamp(),
            metadata$file_row_number,
            metadata$filename
        from @dt_dbv2.source.my_stage/order/ as t
    ) file_format = (format_name = 'dt_dbv2.source.csv_format');


alter task dt_dbv2.raw.copy_to_order_raw_task resume;
alter task dt_dbv2.raw.copy_to_customer_raw_task resume;

select * from customer_raw;
select * from order_raw;