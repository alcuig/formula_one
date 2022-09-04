def drop_all_tables(conn, cur):
    """Function used at beginning of controller.py file to drop all tables and prevent script for throwing errors
    because of table dependencies.
    i.e. "Cannot drop table 1 because table 2 depends on it."
    """
    print(f"Dropping all tables.")

    table_schema = "data"
    table_names = [
        "race_fastest_laps_and_winners",
        "drivers_per_race",
        "races",
        "drivers_pii",
        "drivers",
        "f1_data"
    ]
    for table_name in table_names:
        drop_table_sql = f"drop table if exists {table_schema}.{table_name}"
        cur.execute(drop_table_sql)
    conn.commit()

def create_table_f1_data(conn, cur, table_schema):
    """
    For each race, createes dataframe contaiing columns for the race_id, the reference code, driver code, forename, surname, dob,
    nationality, points and fastest laptime of each driver.

    """
    table_name = "f1_data"

    create_table_sql = f"""
        create table {table_schema}.{table_name} (
            id varchar(50) primary key, 
            driverRef varchar(50),
            driver_number int, 
            driver_code varchar(50), 
            driver_forename varchar(50), 
            driver_surname varchar(50), 
            driver_dob varchar(50), 
            driver_nationality varchar(50),
            points numeric,
            fastestLapTime varchar(50)
        )
    """
    
    drop_table_sql = f"drop table if exists {table_schema}.{table_name}"
    cur.execute(drop_table_sql)
    cur.execute(create_table_sql)
    conn.commit()
    print(f"Step 2/3 complete. Your empty table {table_name} has been created.")
    return table_name

def create_table_drivers(conn, cur, table_schema):
    """
    Creates table to store information about each driver : forename, surname.
    Compound primar key composed of driver forename and surname.
    Drivers_pii table depends on it.

    """
    table_name = "drivers"
    create_table_sql = f"""
        create table {table_schema}.{table_name} (
            driver_forename varchar(50), 
            driver_surname varchar(50),
            primary key (driver_forename, driver_surname)
        )
    """
    
    drop_table_sql = f"drop table if exists {table_schema}.{table_name}"
    cur.execute(drop_table_sql)
    cur.execute(create_table_sql)
    conn.commit()
    print(f"Step 2/3 complete. Your empty table {table_name} has been created.")
    return table_name

def create_table_drivers_pii(conn, cur, table_schema):
    """
    Creates table with information about each driver : forename, surname, dob, nationality.
    Foreign key driver surname and forname referencing data.drivers table.
    """
    table_name = "drivers_pii"
    create_table_sql = f"""
        create table {table_schema}.{table_name} (
            driver_forename varchar(50),
            driver_surname varchar(50),
            driver_dob varchar(50), 
            driver_nationality varchar(50),
            FOREIGN KEY (driver_forename, driver_surname)
                REFERENCES {table_schema}.drivers(driver_forename, driver_surname)
            );
    """
    
    drop_table_sql = f"drop table if exists {table_schema}.{table_name}"
    cur.execute(drop_table_sql)
    cur.execute(create_table_sql)
    conn.commit()
    print(f"Step 2/3 complete. Your empty table {table_name} has been created.")
    return table_name

def create_table_races(conn, cur, table_schema):
    """
    Creates empty table to store race ids for each race. 
    """
    table_name = "races"

    create_table_sql = f"""
        create table {table_schema}.{table_name} (
            race_id varchar(50) primary key
        )
    """
    
    drop_table_sql = f"drop table if exists {table_schema}.{table_name}"
    cur.execute(drop_table_sql)
    cur.execute(create_table_sql)
    conn.commit()
    print(f"Step 2/3 complete. Your empty table {table_name} has been created.")
    return table_name

def create_table_drivers_per_race (conn, cur, table_schema):
    """Creates a table containing drivers competing in each race."""
    table_name = "drivers_per_race"

    create_table_sql = f"""
        create table {table_schema}.{table_name} (
            race_id varchar(50),
            driver_forename varchar(50),
            driver_surname varchar(50),
            FOREIGN KEY (race_id)
                REFERENCES data.races(race_id),
            FOREIGN KEY (driver_forename, driver_surname)
                REFERENCES {table_schema}.drivers(driver_forename, driver_surname)
            );
    """

    drop_table_sql = f"drop table if exists {table_schema}.{table_name}"
    cur.execute(drop_table_sql)
    cur.execute(create_table_sql)
    conn.commit()
    print(f"Step 2/3 complete. Your empty table {table_name} has been created.")
    return table_name

def create_table_race_fastest_laps_and_winners(conn, cur, table_schema):
    """
    Creates table to store, for each race, the fastest laptime, 
    forename and surname of driver who achieved fastest laptime, 
    and forename and lastname of driver who won race.
    """
    table_name = "race_fastest_laps_and_winners"
    create_table_sql = f"""
        create table {table_schema}.{table_name} (
            race_id varchar(50),
            fastest_lap_time varchar(50),
            fastest_lap_driver_forename varchar(50),
            fastest_lap_driver_surname varchar(50),
            winning_driver_forename varchar(50),
            winning_driver_surname varchar(50),
            FOREIGN KEY (race_id)
                REFERENCES {table_schema}.races(race_id)
            );
    """
    
    drop_table_sql = f"drop table if exists {table_schema}.{table_name}"
    cur.execute(drop_table_sql)
    cur.execute(create_table_sql)
    conn.commit()
    print(f"Step 2/3 complete. Your empty table {table_name} has been created.")
    return table_name