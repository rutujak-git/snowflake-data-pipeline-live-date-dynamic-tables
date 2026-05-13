-- changing the context
use role sysadmin;
use warehouse compute_wh;
use schema dt_dbv2.consumption;

-- customer dim sequence number
CREATE OR REPLACE SEQUENCE dt_dbv2.consumption.cust_dim_seq START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE dt_dbv2.consumption.date_dim_seq START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE dt_dbv2.consumption.priority_dim_seq START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE dt_dbv2.consumption.order_fact_seq START = 1 INCREMENT = 1;


create or replace dynamic table dt_dbv2.consumption.customer_dim_dt
    target_lag = downstream
    warehouse = dt_transform_wh
as 
    select
     dt_dbv2.consumption.cust_dim_seq.nextval as c_dim_id,
    cust_key,
    name,
    address,
    nation_name,
    phone,
    acct_bal,
    mkt_segment
from 
    dt_dbv2.clean.customer_clean_dt;

create or replace dynamic table dt_dbv2.consumption.date_dim_dt
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE=dt_transform_wh
AS
with unique_date_cte as (
select 
    order_date,
    year(order_date) as order_year,
    quarter(order_date) as order_quarter,
    month(order_date) as order_month,
    week(order_date) as order_week,
    dayofmonth(order_date) as order_day
from 
    dt_dbv2.clean.order_clean_dt 
group by 
    order_date,
    order_year,
    order_quarter,
    order_month,
    order_week,
    order_day
)
select 
dt_dbv2.consumption.date_dim_seq.nextval as d_dim_id,
* 
from unique_date_cte;


create or replace dynamic table dt_dbv2.consumption.priority_dim_dt
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE=dt_transform_wh
AS 
with unique_priority_cte as (
select 
    order_priority
from 
    dt_dbv2.clean.order_clean_dt 
    group by order_priority
) 
select 
    dt_dbv2.consumption.priority_dim_seq.nextval as p_dim_id,
    *
from unique_priority_cte;

alter dynamic table dt_dbv2.clean.customer_clean_dt set target_lag = 'DOWNSTREAM';
alter dynamic table dt_dbv2.clean.order_clean_dt set target_lag = 'DOWNSTREAM';

create or replace dynamic table dt_dbv2.consumption.order_fact_dt
    TARGET_LAG='3 Mins'
    WAREHOUSE=dt_transform_wh
AS
select  
    dt_dbv2.consumption.order_fact_seq.nextval as o_fact_id,
    cd.c_dim_id,
    dd.d_dim_id,
    pd.p_dim_id,
    oc.order_key,
    oc.total_price
from 
    dt_dbv2.clean.order_clean_dt as oc
    join dt_dbv2.consumption.customer_dim_dt as cd on cd.cust_key = oc.CUST_KEY
    join dt_dbv2.consumption.date_dim_dt as dd on dd.order_date = oc.order_date
    join dt_dbv2.consumption.priority_dim_dt as pd on pd.order_priority = oc.order_priority;

select 'customer_raw',count(*) from DT_DBV2.RAW.CUSTOMER_RAW 
union all
select 'order_raw', count(*) from DT_DBV2.RAW.order_raw
union all
select 'customer_clean',count(*) from DT_DBV2.CLEAN.CUSTOMER_CLEAN_DT
union all
select 'order_raw', count(*) from DT_DBV2.CLEAN.order_CLEAN_DT
union all 
select 'cusotomer_dim',count(*) from DT_DBV2.CONSUMPTION.CUSTOMER_DIM_DT
union all 
select 'date_dim', count(*) from DT_DBV2.CONSUMPTION.DATE_DIM_DT
union all 
select 'order_fact' ,count(*) from DT_DBV2.CONSUMPTION.ORDER_FACT_DT
union all 
select 'priority_dim',count(*) from DT_DBV2.CONSUMPTION.PRIORITY_DIM_DT;

ALTER TASK DT_DBV2.RAW.COPY_TO_CUSTOMER_RAW_TASK RESUME;
ALTER TASK DT_DBV2.RAW.COPY_TO_ORDER_RAW_TASK RESUME;

