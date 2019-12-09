import sqlite3
from sqlite3 import Error
import fileinput
import re

db_path = None
db_conn = None

def set_db_path(path):
    global db_path
    if db_path:
        return db_path
    db_path = path
    return db_path

def get_db_path():
    global db_path
    return db_path

def connect_sqlite_db(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def set_db_conn():
    global db_conn
    if db_conn:
        return db_conn
    db_conn = connect_sqlite_db(get_db_path())
    return db_conn

def get_db():
    global db_conn
    if not db_conn:
        return set_db_conn()
    return db_conn

def create_sqlite_table(conn):
    sql_create_table_statement = \
    """
     CREATE TABLE IF NOT EXISTS people_data (
     last_name text NOT NULL,
     first_name text NOT NULL,
     gender text NOT NULL,
     favorite_color text NOT NULL,
     date_of_birth text NOT NULL
     ); """
    c = conn.cursor()
    c.execute(sql_create_table_statement)

def insert_into_sqlite_table(conn, record):
    sql_insert_statement = \
    """ 
    INSERT INTO people_data(last_name, first_name, gender, favorite_color,
    date_of_birth)
    VALUES(?, ?, ?, ?, ?)
    ; """
    cur = conn.cursor()
    cur.execute(sql_insert_statement, record)

def readin_file_to_db(conn):
    with conn:
        data = fileinput.input()
        for line in data:
            if fileinput.isfirstline(): # skip first line of files
                continue
            parsed_line = parse_data_line(line)
            insert_into_sqlite_table(conn, parsed_line)

def parse_data_line(line):
    line_vals = re.split(', | \| | ', line)
    last_name, first_name, gender, favorite_color, date_of_birth = \
        line_vals
    date_of_birth = date_of_birth.replace('\n', '')
    new_line_vals = (last_name, first_name, gender, favorite_color,
        date_of_birth)
    return new_line_vals

def transform_date_string(date):
    """
    Converts date from YYYY-MM-DD to M/D/YYYY
    """
    year, month, date = date.split('-')
    new_month_str = str(int(month))
    new_date_str = str(int(date))
    return new_month_str + '/' + new_date_str + '/' + year

def transform_row(row):
    last_name, first_name, gender, favorite_color, date_of_birth = row
    return (last_name, first_name, gender, favorite_color,
        transform_date_string(date_of_birth))

def get_by_gender(conn):
    sql_get_by_gender_sorted = \
    """
    SELECT * FROM people_data
    ORDER BY gender ASC;
    """
    cur = conn.cursor()
    data = cur.execute(sql_get_by_gender_sorted)
    rows = data.fetchall()
    return rows

def get_birth_date_ascending(conn):
    """
    Dates are assumed to be in the format YYYY-MM-DD
    """
    sql_get_birth_date_ascending = \
    """
    SELECT * FROM people_data
    ORDER BY date_of_birth ASC;
    """
    cur = conn.cursor()
    data = cur.execute(sql_get_birth_date_ascending)
    rows = data.fetchall()
    return rows

def get_last_name_descending(conn):
    sql_get_last_name_descending = \
    """
    SELECT * FROM people_data
    ORDER BY last_name DESC;
    """
    cur = conn.cursor()
    data = cur.execute(sql_get_last_name_descending)
    rows = data.fetchall()
    return rows

def display_data(conn):
    print("Show By Gender")
    for row in get_by_gender(conn):
        print(transform_row(row))
    print("Show Birth Date ASC")
    for row in get_birth_date_ascending(conn):
        print(transform_row(row))
    print("Show Last Name DESC")
    for row in get_last_name_descending(conn):
        print(transform_row(row))

def main():
    set_db_path("./people.db")
    conn = set_db_conn()
    create_sqlite_table(conn)
    readin_file_to_db(conn)
    display_data(conn)

if __name__ == "__main__":
    main()