import psycopg2


def db_connect():
    conn = psycopg2.connect("host=localhost dbname=f1 user=alyssacuignet")
    cur = conn.cursor()
    print(f"Step 1/3 complete. Connection to f1 database established, cursor created.")
    return conn, cur
