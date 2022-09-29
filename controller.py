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
