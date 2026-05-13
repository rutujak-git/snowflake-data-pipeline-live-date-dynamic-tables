use role sysadmin;
use warehouse compute_wh;
use schema dt_dbv2.clean;

create warehouse dt_transform_wh 
    with 
    warehouse_size = 'xsmall' 
    warehouse_type = 'standard' 
    auto_suspend = 60 
    auto_resume = true 
    min_cluster_count = 1
    max_cluster_count = 1 
    scaling_policy = 'standard'
    initially_suspended = True;

create or replace dynamic table dt_dbv2.clean.customer_clean_dt
    target_lag = '2 minutes'
    warehouse = dt_transform_wh
    as 
    with RankedCustomer as (
        select 
            cust_key,
            name,
            address,
            nation_name,
            phone,
            acct_bal,
            mkt_segment,
            load_ts,
            load_row_number,
            load_file_name,
            row_number() over (partition by cust_key order by load_ts desc) as row_rank
        from 
            dt_dbv2.raw.customer_raw 
    )
    select 
            cust_key,
            name,
            address,
            nation_name,
            phone,
            acct_bal,
            mkt_segment,
            load_ts,
            load_row_number,
            load_file_name
    from RankedCustomer
    where row_rank = 1;


create or replace dynamic table dt_dbv2.clean.order_clean_dt
    TARGET_LAG ='2 minutes'
    WAREHOUSE = dt_transform_wh
    as 
        select order_key,
                cust_key,
                order_status,
                total_price,
                order_date,
                order_priority,
                clerk,
                ship_priority,
                load_ts,
                load_row_number,
                load_file_name
        from (
            select order_key,
                    cust_key,
                    order_status,
                    total_price,
                    order_date,
                    order_priority,
                    clerk,
                    ship_priority,
                    load_ts,
                    load_row_number,
                    load_file_name,
                    ROW_NUMBER() OVER (PARTITION BY order_key ORDER BY load_ts DESC) AS row_rank
                FROM 
                    dt_dbv2.raw.order_raw
        ) as RankedOrders
        where row_rank = 1;

select * from dt_dbv2.clean.order_clean_dt;
                