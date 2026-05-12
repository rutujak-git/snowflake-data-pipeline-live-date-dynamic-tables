use role sysadmin;
use warehouse compute_wh;

create or replace database dt_dbv2
comment = 'this is dt_dbv2 database for pipeline using dynamic tables';

use database dt_dbv2;

create or replace schema dt_dbv2.source;
create or replace schema dt_dbv2.raw;
create or replace schema dt_dbv2.clean;
create or replace schema dt_dbv2.consumption;

create or replace file format dt_dbv2.source.csv_format
    type = 'csv'
    compression = 'auto'
    field_delimiter = ','
    record_delimiter = '\n'
    field_optionally_enclosed_by = '"'
    skip_header = 1;

create or replace stage dt_dbv2.source.my_stage
file_format = (format_name = 'dt_dbv2.source.csv_format')
comment = 'this is snowflake internal stage to stage the data files under the dt_dbv2/source schema';

select 
    t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,
    current_timestamp()
    metadata$file_row_number,
    metadata$filename
from @dt_dbv2.source.my_stage/customer/ as t;

select 
    t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,
    current_timestamp()
    metadata$file_row_number,
    metadata$filename
from @dt_dbv2.source.my_stage/order/ as t;