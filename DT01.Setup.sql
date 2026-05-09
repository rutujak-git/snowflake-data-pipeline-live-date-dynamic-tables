use role sysadmin;
use warehouse compute_wh;

---------------------- Step 1: Create Database ----------------------------
create or replace database dt_db
    comment = 'this is dt_db database for dynamic tables';

---------------------- Step 2: Create Schemas -----------------------------
create or replace schema dt_db.source_sch
    comment =  'this is stage schema in dt_db database';
create or replace schema dt_db.raw_sch
    comment = 'this is raw schema in dt_db database';
create or replace schema dt_db.clean_sch
    comment = 'this is clean schema in dt_db database';
create or replace schema dt_db.consumption_sch
    comment = 'this is consumption schema in dt_db database';

use schema dt_db.source_sch;

----------------------- Step 3: Create File Format ---------------------------
create or replace file format dt_db.source_sch.csv_format
    type = 'csv'
    field_delimiter = ','
    record_delimiter = '\n'
    field_optionally_enclosed_by = '"'
    skip_header = 1
    compression = 'auto';

describe file format dt_db.source_sch.csv_format;

alter file format dt_db.source_sch.csv_format
    set null_if = ('','null','NULL')
        date_format = 'YYYY-MM-DD';

----------------------- Step 4: Create Stage ----------------------------------
create or replace stage dt_db.source_sch.dynamic_tbl_stage
    file_format = (format_name = 'dt_db.source_sch.csv_format')
    comment = 'this is a new stage location to store employee data';

----------------------- Step 5: Load Data from snowsight into stage & verify here --------------
list @dt_db.source_sch.dynamic_tbl_stage/employees/;

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
    metadata$filename,
    metadata$file_row_number
from @dt_db.source_sch.dynamic_tbl_stage/employees/ as t;