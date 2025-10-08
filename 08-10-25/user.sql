use role securityadmin;
create role developer_role;
show roles;

--create a user for assuming the developer role

create user dev_user2
password = 'doe'
default_role = developer_role
must_change_password= false;

grant role developer_role to user dev_user2;

------------------------------------------------

--creating warehouse

use role sysadmin;

create or replace warehouse dev_warehouse
warehouse_size = 'SMALL'
auto_suspend = 60;

grant usage on warehouse dev_warehouse to role developer_role;

---------------------------------------------

--giving access to creating database

grant create database on account to role developer_role;