import uuid
import psycopg2

def insert_records_f1_data_table(table_schema, table_name, data, conn, cur):
    """
    For each race, inserts into dataframe the reference code, driver code, forename, surname, dob,
    nationality, points and fastest laptime of each driver.
    
    Bug solved: N was input as defualt null value for driver numbers. Issue as driver_number must be integer in destination table.
    To resolve issue, replaced 'N' with None value.
    """
    for record in data:
        postgres_insert_query = f""" 
            INSERT INTO {table_schema}.{table_name} (
                id, 
                driverRef,
                driver_number, 
                driver_code, 
                driver_forename, 
                driver_surname, 
                driver_dob, 
                driver_nationality,
                points,
                fastestLapTime)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        if "N" in record["driver_number"]:
            record["driver_number"] = None

        record_to_insert = (
            str(uuid.uuid4()),
            record["driverRef"],
            record["driver_number"],
            record["driver_code"],
            record["driver_forename"],
            record["driver_surname"],
            record["driver_dob"],
            record["driver_nationality"],
            record["points"],
            record["fastestLapTime"],
        )

        cur.execute(postgres_insert_query, record_to_insert)

    conn.commit()
    print(f"Step 3/3 complete. Data inserted succesfully inyo table '{table_name}'.")

def insert_records_drivers_table(table_schema, table_name, data, conn, cur):
    """
    Inserts into dataframe forename and surname of each formula 1 driver.
    """
    for record in data:
        postgres_insert_query = f""" 
            INSERT INTO {table_schema}.{table_name} (
                driver_forename, 
                driver_surname
                )
                VALUES (%s,%s)
        """
        record_to_insert = (record["driver_forename"], record["driver_surname"])
        cur.execute(postgres_insert_query, record_to_insert)

    conn.commit()
    print(
        f"Step 3/3 complete. Driver names inserted successfully into '{table_schema}.{table_name}'."
    )

def insert_records_drivers_pii_table(table_schema, table_name, data, conn, cur):
    """
    Inserts into dataframe information about each driver : forename, surname, dob, nationality.
    """
    for record in data:

        postgres_insert_query = f""" 
            INSERT INTO {table_schema}.{table_name} (
                driver_forename, 
                driver_surname, 
                driver_dob, 
                driver_nationality
                )
            VALUES(%s, %s, %s, %s)
        """

        record_to_insert = (
            record["driver_forename"],
            record["driver_surname"],
            record["driver_dob"],
            record["driver_nationality"],
        )

        cur.execute(postgres_insert_query, record_to_insert)

    conn.commit()
    print(
        f"Step 3/3 complete. Driver pii data inserted successfully into '{table_schema}.{table_name}'."
    )

def insert_records_races_table(table_schema, table_name, data, conn, cur):
    """
    Inserts into empty dataframe the ids for each race. 
    """
    for record in data:

        postgres_insert_query = f""" 
            INSERT INTO {table_schema}.{table_name} (
                race_id
                )
            VALUES(%s)
        """

        record_to_insert = (record["race_id"],)

        cur.execute(postgres_insert_query, record_to_insert)

        conn.commit()
    print(
        f"Step 3/3 complete. Driver pii data inserted successfully into '{table_schema}.{table_name}'."
    )

def insert_records_drivers_per_race(table_schema, table_name, data, conn, cur):
    """Inserts into empty dataframe the forename and surname of drivers performing in each race. """
    for record in data:
        postgres_insert_query_2 = f""" 
                INSERT INTO {table_schema}.{table_name} (
                    race_id,
                    driver_forename, 
                    driver_surname 
                    )
                VALUES(%s, %s, %s)
            """
        record_to_insert_2 = (
            record["raceId"],
            record["driver_forename"],
            record["driver_surname"],
        )
    
        cur.execute(postgres_insert_query_2, record_to_insert_2)

    conn.commit()
    print(
        f"Step 3/3 complete. Driver and races data inserted successfully into '{table_schema}.{table_name}'."
    )

def insert_records_race_fastest_laps_and_winners_table(table_schema, table_name, data, conn, cur): 
    """
    Insert records into table. Records must contain for each race, the fastest laptime, 
    forename and surname of driver who achieved fastest laptime, 
    and forename and lastname of driver who won race.
"""
    for record in data:
        postgres_insert_query_2 = f""" 
                INSERT INTO {table_schema}.{table_name} (
                    race_id,
                    fastest_lap_time,
                    fastest_lap_driver_forename,
                    fastest_lap_driver_surname,
                    winning_driver_forename,
                    winning_driver_surname
                    )
                VALUES(%s, %s, %s, %s, %s, %s)
            """
        record_to_insert_2 = (
            record["race_id"],
            record["fastest_lap_time"],
            record["fastest_lap_driver_forename"],
            record["fastest_lap_driver_surname"],
            record["winning_driver_forename"],
            record["winning_driver_surname"],
        )
        cur.execute(postgres_insert_query_2, record_to_insert_2)
    conn.commit()
    print(
        f"Step 3/3 complete. Fastest laptime, laptime driver, winning driver per race data inserted successfully into '{table_schema}.{table_name}'."
    )
    