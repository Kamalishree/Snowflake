select current_role();
select current_user();

-----------------------------------
create database project_db;
show databases;

create table my_table (name varchar);

insert into my_table values 
('Snowflake User');

select * from my_table;

drop table my_table;

-----------------------------------------
--creating in proper way

use role developer_role;
use database project_db;
use schema public;
use warehouse dev_warehouse;

create or replace stage my_azure_stage

url = 'azure://hexstorageshree.blob.core.windows.net/mycontainer1'

credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-31T17:07:47Z&st=2025-10-08T08:52:47Z&spr=https&sig=v%2FcOGJ9CDPwfwNtqbvGYe25BQYEdAWD9%2BeIevltmRCk%3D');

--see what is in the stage??

list @my_azure_stage;

-----------------------------------------------
------------------------------------------------
select $1,$2,$3,$4 from @my_azure_stage limit 100;
select $1,$2,$3,$4,metadata$filename from @my_azure_stage limit 100;
 
------
create table my_lineitems
like
SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;
 
--------------------
select * from my_lineitems;
 
--data loading from AZURE to our table in snowflake ()
copy into my_lineitems
from @my_azure_stage;
------------------------
select count(*) from my_lineitems;
-------------------------
create stage my_aws_public_stage
url = 's3://general-bkt-snfdata/datalake/';
 
list @my_aws_public_stage;
---------------------------
create table my_lineitems2
like
SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;
 
copy into my_lineitems2
from @my_aws_public_stage;
---------------------------
select * from PROJECT_DB.INFORMATION_SCHEMA.LOAD_HISTORY;
select * from PROJECT_DB.INFORMATION_SCHEMA.STAGES;
select * from PROJECT_DB.INFORMATION_SCHEMA.TABLES;
---------------------------
-------------------------- data loading with csv file delimited with header
create or replace stage my_azure_stage2

url = 'azure://hexstorageshree.blob.core.windows.net/mycontainer2'

credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-31T17:07:47Z&st=2025-10-08T08:52:47Z&spr=https&sig=v%2FcOGJ9CDPwfwNtqbvGYe25BQYEdAWD9%2BeIevltmRCk%3D');

list @my_azure_stage2;
-----------------------------
select $1,$2,$3,$4 from @my_azure_stage2 limit 100;
-----------------------------
create table my_customer (
Customer_ID number,
Customer_Name varchar,
Customer_Email varchar,
Customer_City varchar,
Customer_State varchar,
Customer_DOB date
);
---
copy into my_customer
from @my_azure_stage2;

desc stage my_azure_stage2;
--
create file format my_custom_csv1
type = csv
FIELD_DELIMITER = '|'
SKIP_HEADER = 1;

copy into my_customer
from @my_azure_stage2
file_format = my_custom_csv1;

select * from my_customer;

-------------------------------------
create table my_customer_subset (
Customer_ID number,
Customer_Name varchar,
Customer_Email varchar 
);

copy into my_customer_subset
from @my_azure_stage2
file_format = my_custom_csv1;

desc stage my_azure_stage2;

create file format my_custom_csv2
type = csv
FIELD_DELIMITER = '|'
SKIP_HEADER = 1
ERROR_ON_COLUMN_COUNT_MISMATCH = false;

copy into my_customer_subset
from @my_azure_stage2
file_format = my_custom_csv2;

select * from my_customer_subset;
----------
--Using force and purge file format options 
desc stage MY_AZURE_STAGE2;
 
create or replace table my_customer (
Customer_ID number,
Customer_Name varchar,
Customer_Email varchar,
Customer_City varchar,
Customer_State varchar,
Customer_DOB date
);
 
select * from my_customer; -- no data
 
copy into my_customer
from @my_azure_stage2
file_format = my_custom_csv1;
 
select count(*) from my_customer; --436, 872, 1308
 
--lets reload again
copy into my_customer
from @my_azure_stage2
file_format = my_custom_csv1
force = true;
---------------------
-- purge optn
create or replace table my_customer (
Customer_ID number,
Customer_Name varchar,
Customer_Email varchar,
Customer_City varchar,
Customer_State varchar,
Customer_DOB date
);
copy into my_customer
from @my_azure_stage2
file_format = my_custom_csv1
purge = true;
-----------------------
--column re-ordering /specific columns we need...
create table my_customer_subset2 (
Customer_ID number,
Customer_Name varchar,
DOB date
);
 
copy into my_customer_subset2
from @my_azure_stage2
file_format = my_custom_csv2; --fail
 
copy into my_customer_subset2
from 
(
select $1, $2, $6 from @my_azure_stage2
)
file_format = my_custom_csv2;
 
select * from my_customer_subset2;
------------------------------------