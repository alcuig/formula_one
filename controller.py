import csv
import psycopg2
import uuid
from pprint import pprint
from merge_winner_fastest_lap_time_table import merged_winner_fastest_lap_records
from tools import read_data
from db_connection import db_connect
from db_init import (
    drop_all_tables,
    create_table_f1_data,
    create_table_drivers,
    create_table_drivers_per_race,
    create_table_drivers_pii,
    create_table_races,  
    create_table_race_fastest_laps_and_winners
)
from tools import read_data, get_drivers_from_data, get_race_id_from_data
from records_insert import (
    insert_records_f1_data_table,
    insert_records_drivers_table,
    insert_records_drivers_pii_table,
    insert_records_races_table,
    insert_records_drivers_per_race,
    insert_records_race_fastest_laps_and_winners_table
)

###
# Global values
table_schema = "data"
data = read_data()
conn, cur = db_connect()

# Drop all tables
drop_all_tables(conn, cur)

# F1_DATA TABLE CREATION AND POPULATION
table_name = create_table_f1_data(conn, cur, table_schema)
insert_records_f1_data_table(table_schema, table_name, data, conn, cur)

# #DRIVERS TABLE  CREATION AND POPULATION
table_name = create_table_drivers(conn, cur, table_schema)
driver_data = get_drivers_from_data(data)
insert_records_drivers_table(table_schema, table_name, driver_data, conn, cur)

# #DRIVERS PII TABLE CREATION AND POPULATION
table_name = create_table_drivers_pii(conn, cur, table_schema)
driver_data = get_drivers_from_data(data)
insert_records_drivers_pii_table(table_schema, table_name, driver_data, conn, cur)

# TABLE WITH RACE INFO
# Create a table with all the race ids : name should be ‘races’ and PK should be ‘race_id’
table_name = create_table_races(conn, cur, table_schema)
race_id = get_race_id_from_data(data)
insert_records_races_table(table_schema, table_name, race_id, conn, cur)

# TABLE WITH ALL RACES AND DRIVERS
# The columns should be ‘race_id’, ‘driver_surname’, ‘driver_forename’
# ‘race_id’ needs to be a FK referencing the races table.
# Forename and surname need to be FKs and reference the drivers tables

table_name = create_table_drivers_per_race(conn, cur, table_schema)
insert_records_drivers_per_race(table_schema, table_name, data, conn, cur)

# +++++++
# CREATE TABLE FASTEST LAP, FASTEST LAP DRIVERS, WINNING DRIVERS PER RACE
# Create a table with race details. 
# Columns should be race_id referencing races table, 
# the fastest_lap_time, the fastest_lap, the fastest_lap’s driver forename and surname, 
# and the winner driver’s forename and surname. 
# Who won can be different from who made the fastest map time

table_name = create_table_race_fastest_laps_and_winners(conn, cur, table_schema)
merged_data = merged_winner_fastest_lap_records()
insert_records_race_fastest_laps_and_winners_table(table_schema, table_name, merged_data, conn, cur)


# ----------------------

# insert new column age

# find nationality, date of birth and name of person with fastest lap time
# select fastestLapTime, driver_surname, driver_nationality from f1.f1_data order by fastestLapTime tb limit 1;
# selects columns I want to see from the table, ranks them according to the order of fastestlaptime, slices the 1st result (which will be smallest)

# find out who is the youngest driver
# (select driver_dob, driver_surname, driver_forename from f1.f1_data order by driver_dob desc)limit 1;

# select distinct driver names, distinct driver births dates, distinct driver codes

# find all british drivers (where nationality = 'British')


# use NOT to show drivers who aren't british (where not nationality = 'british')
# select distinct driver_nationality from f1.f1_data where not driver_nationality = 'British' order by driver_nationality;

# select all records where driver is British AND is older than 30
# select distinct driver_nationality, driver_dob from f1.f1_data where not driver_nationality = 'British' and driver_dob ~ '19[0-9][012]' order by driver_dob desc;

# select all records where the driver is japanese or french
# select distinct driver_surname, driver_nationality from f1.f1_data where driver_nationality = 'French'
# or driver_nationality = 'Japanese' order by driver_nationality, driver_surname desc;

# select all records where the driver is older than 31 or younger than 25
# ???

# Order the records alphabetically by nationality
# select distinct driver_surname, driver_nationality from f1.f1_data order by driver_nationality, driver_surname;

# order records by youngest to eldest
# select distinct driver_surname, driver_forename, driver_dob from f1.f1_data order by driver_dob desc;


# Order records alphabetically by nationality then by race lap
# select distinct driver_surname, driver_forename, driver_nationality, fastestLapTime from f1.f1_data order by driver_nationality, fastestLapTime;

# insert a new record into the table , if you are inserting values into all columns in the table you do not need to specify column headers
# INSERT INTO table_name
# VALUES (value1, value2, value3, ...);
# insert into f1.f1_data values ('000', 'DriverName', 0001, 'TEST', 'Imnot', 'Arealdriver', '2022-01-01', 'Avatarian', 900, '01:27.09');

# select all records where the driver number is empty where X IS NULL, and then all those where the driver number is not empty
# select distinct driver_number, driver_forename, driver_surname from f1.f1_data where driver_number is null;
# select distinct driver_number, driver_forename, driver_surname from f1.f1_data where driver_number is not null order by driver_number;


# find driver code of oldest driver
# select distinct driver_code, driver_dob from f1.f1_data where driver_code not like '%\N%'  order by driver_dob;


# UPDATE/ SET command : update the Nationality column of all drivers who have a P in their surname to french
# update f1.f1_data set driver_nationality = 'French' where driver_surname like '%p%';

# count the number of drivers who now have the french nationality
# select count (distinct driver_dob) from f1.f1_data where driver_nationality = 'French';

# set the value of all surname columns to NotFrench, but only where the nationality column is not french (update, set where)
# update f1.f1_data set driver_surname = 'NotFrench' where driver_nationality != 'French';

# where nationality is british and firstname is George, set surname to "Orwell"
# update f1.f1_data set driver_surname = 'Orwell' where driver_forename = 'George' and driver_nationality = 'British';

# delete all records whose driver code is "HAM" (delete from table where X = X)


# delete all the recrods from the table (delete from table_name)

# Use the MIN function (Min(ColumnName)) to select the minimum value from the lap time


# Use Max function to select max value from lap time

# use count function to return the number of records that have the code value set to HAM
# select count(driver_code) from f1.f1_data where driver_code = 'HAM';

# calculate average of all points AVG(column)


# calculate sum of all points select SUM(Column) from TableName;


# like function to find records where last name column starts with "R"


# Select * from Table where Column LIKE 'letter%';
# select sum(points) as SumPoints from f1.f1_data;


# select all records where first name sztarts with G and ends with e
# select distinct driver_surname, driver_forename from f1.f1_data where driver_forename like 'G%e';


# select all records where first name does not start with L
# select distinct driver_surname, driver_forename from f1.f1_data where driver_forename not like 'L%';


# select all records where second letter first name is E ('_e%)


# select all records where the driver_forename begins with the letters A, S or L
# select distinct driver_surname, driver_forename from f1.f1_data where driver_forename ~ '^[ASL]';

# select all records where last name begins with anything from L to Y '[a-f]
# select distinct driver_surname, driver_forename from f1.f1_data where driver_surname ~* '^[l-y]';

# #select all records where first letter of nationality is not B or F or C '[!bfc]
# select distinct driver_surname, driver_forename, driver_nationality from f1.f1_data where driver_nationality !~* '^[bfc]';

# ----
# use IN operator to select all records where name is either George or Mick
# The IN operator allows you to specify multiple values in a WHERE clause.
# The IN operator is a shorthand for multiple OR conditions.

# IN Syntax
# SELECT column_name(s)
# FROM table_name
# WHERE column_name IN (value1, value2, ...);

# select distinct driver_forename, driver_surname, driver_code, driver_dob from f1.f1_data where driver_forename in ('George', 'Mick') order by driver_dob;
# ---

# use IN operator to select all records where name is neither Paul nor Pierre nor Lewis, nor Max;
# select from TableName WHERE ColumnName NOT IN('Condition1','Condition2');
# select distinct driver_surname, driver_forename, driver_dob, driver_nationality from f1.f1_data where driver_forename not in ('Paul', 'Lewis', 'Pierre', 'Max') order by driver_dob;

# -----
# BETWEEN

# find people whose DOBS are beteween 1989 and 1999
# #syntax : (select from Table WHERE column BETWEEN x AND x)

# select * from f1.f1_data where driver_dob between '1989-01-01' and '1999-01-01';


# -----
# show records where values of first name are alphabetically between Geoorge and Mick
# Select * from TableName where ColumnName BETWEEN 'x' AND 'x'
# select driver_forename, driver_surname, driver_dob from f1.f1_data as NewDriverTable where driver_forename between 'George' and 'Mick';

# ---
# IS NOT BETWEEN
##find all records where points are NOT in between 10 and 8;
# select * from f1.f1_data where points not between 8 and 10 order by points desc;

# ---
# AVG
# find which driver has highest average points
# ???

# ---
# SQL alias (to give temporary name to table column)
# Select ColumnName, ColumnName2, ColumnName3 AS AliasName, FROM TableName;
# select driverref, driver_dob, driver_nationality from f1.f1_data as NewDriverTable;

# -----


# transform racetime into seconds
# average fastest seconds per driver
# insert other columns
# find out who was in first position most often


# when displaying the f1 table, refer to the table as formula 1 instead of f1
# Select * from TableName AS AliasName,
# select * from f1.f1_data as formula_1_data

# ---
# JOIN

# create table with all drivers - name, surname, and create a surrogate key  (i.e. UUID). Make UUID the primary key.
# create table with all the PII(DOB, nationality etc) of the driver, from the first task get the surrogate key  as a foreign key in this table
# create table with all the races and drivers in the race.

# TO LEFT JOIN : Select * FROM TableName1 LEFT JOIN Table2 ON TableName1.PrimaryKey = TableName2.SameKey;

# Select all records from the two tables where there is a match in both tables.

# SELECT * FROM Table1 INNER JOIN Table2 ON TableName1.PrimaryKey = TableName2.SameKey


# RIGHT JOIN clause to select all the records from one table plus all the matches in a second table.

# Group By
# List the numbers of drivers of each nationality
# Select COUNT(DriverName), Nationality FROM TableName GROUP BY Nationality;

# --
# CREATE DATABASE
# Create a new database
# CREATE DATABASE newDB;

# Delete database
# DROP DATABASE newDB;

# create table (will need to indicate column headers plus datatype)
# drop the table

# delete all data in table but not table itself

# add a column of type DATE called Birthday (ALTER TABLE)

# drop driver code column from table
