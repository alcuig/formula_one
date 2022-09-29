from db_connection import db_connect

###
# Global values
table_schema = "analytics"
conn, cur = db_connect()

def count_french_and_american_drivers():
    """
    Counts number of french and american drivers, compares to total number of drivers.
    """
    table_name = "french_drivers_and_american"
    drop_table = f"""
        drop table if exists {table_schema}.{table_name}
    """
    cur.execute(drop_table)
    print(f"Step 1/3 complete. Dropped pre-existing table {table_schema}.{table_name}. ")

    create_table = f"""
        create table {table_schema}.{table_name} (
            number_french_drivers int,
            number_american_drivers int,
            total_number_drivers int
        )
    """
    print(f"Step 2/3 complete. Created table {table_schema}.{table_name}. ")

    insert_statement = f"""
        insert into {table_schema}.{table_name}
        select
    french_count,
    american_count,
    total_count
from (
    select
        count(concat(driver_forename, driver_surname)) as french_count
    from data.drivers_pii data_french
    where driver_nationality = 'French'
) as a
join (
    select
        count(concat(driver_forename, driver_surname)) as total_count
    from data.drivers_pii data_french
) as b
    on True
join (
    select
        count(concat(driver_forename, driver_surname)) as american_count
    from data.drivers_pii data_french
    where driver_nationality = 'American'
) as c
    on True
    """
    cur.execute(drop_table)
    cur.execute(create_table)
    cur.execute(insert_statement)

    conn.commit()

    print(f"Step 3/3 complete. Table {table_schema}.{table_name} filled with SQL query result. ")

def count_driver_nationalities():
    """
    Counts nationalites of drivers.
    """
    table_name = "count_driver_nationalities"
    drop_table = f"""
        drop table if exists {table_schema}.{table_name}
    """
    cur.execute(drop_table)
    print(f"Step 1/3 complete. Dropped pre-existing table {table_schema}.{table_name}. ")

    create_table = f"""
        create table {table_schema}.{table_name} (
            nationality varchar(50),
            nationality_count int
        )
    """
    print(f"Step 2/3 complete. Created table {table_schema}.{table_name}. ")

    insert_statement = f"""
        insert into {table_schema}.{table_name}
    select driver_nationality, 
    COUNT(*) as nationality_count 
    from data.drivers_pii 
    group by driver_nationality;
    """
    cur.execute(drop_table)
    cur.execute(create_table)
    # conn.commit()
    cur.execute(insert_statement)

    conn.commit()

    print(f"Step 3/3 complete. Table {table_schema}.{table_name} filled with SQL query result. ")

def sum_points_per_nationality():
    """
    Compares sum points per nationality.    
    """
    table_name = "sum_points_per_nationality"
    drop_table = f"""
        drop table if exists {table_schema}.{table_name}
    """
    cur.execute(drop_table)
    print(f"Step 1/3 complete. Dropped pre-existing table {table_schema}.{table_name}. ")

    create_table = f"""
        create table {table_schema}.{table_name} (
            nationality varchar(50),
            max_points int
        )
    """
    print(f"Step 2/3 complete. Created table {table_schema}.{table_name}. ")

    insert_statement = f"""
        insert into {table_schema}.{table_name}
        select driver_nationality, 
        sum(points) from data.f1_data 
        group by driver_nationality 
        order by sum(points) desc;
    """
    cur.execute(drop_table)
    cur.execute(create_table)
    cur.execute(insert_statement)

    conn.commit()

    print(f"Step 3/3 complete. Table {table_schema}.{table_name} filled with SQL query result. ")

def sum_points_per_driver():
    """
    Sum points per driver.
    """
    table_name = "sum_points_per_driver"
    drop_table = f"""
        drop table if exists {table_schema}.{table_name}
    """
    cur.execute(drop_table)
    print(f"Step 1/3 complete. Dropped pre-existing table {table_schema}.{table_name}. ")

    create_table = f"""
        create table {table_schema}.{table_name} (
            driver_nationality varchar(50),
            sum_of_points int,
            driver_forename varchar(50),
            driver_surname varchar(50) 
        )
    """
    print(f"Step 2/3 complete. Created table {table_schema}.{table_name}. ")

    insert_statement = f"""
        insert into {table_schema}.{table_name} (driver_nationality, driver_forename, sum_of_points, driver_surname)
        select driver_nationality, driver_forename, sum(points) as sum_of_points, driver_surname
        from data.f1_data 
        group by driver_nationality, driver_forename, driver_surname 
        order by sum(points) desc;
    """
    cur.execute(drop_table)
    cur.execute(create_table)
    cur.execute(insert_statement)

    conn.commit()

    print(f"Step 3/3 complete. Table {table_schema}.{table_name} filled with SQL query result. ")


count_french_and_american_drivers()
count_driver_nationalities()
sum_points_per_nationality()
sum_points_per_driver()
