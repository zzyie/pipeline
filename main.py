import mysql.connector
import os
import csv


def read_db_config(file_path):

    if not os.path.exists(file_path):
        
        raise FileNotFoundError(f"Error: The file {file_path} does not exist.")

    config = {}

    with open(file_path, 'r') as file:
            
            for line in file:

                key, value = line.strip().split('=')
                config[key] = value

    print('config read in from file')

    return config

def create_database_if_not_exists(config):

    temp_config = config.copy()

    temp_config.pop('database')

    try: 

        conn = mysql.connector.connect(**temp_config)

        cursor = conn.cursor()

        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['database']}")

        cursor.close()

        print('ticket sale db created')

        conn.close()

    except Exception as error:

        print("Error while creating db if not exists", error)

def get_db_connection(config):

    conn = None

    try:

        conn = mysql.connector.connect(user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
            database=config['database'])
        
        print('mysql db connected')

    except Exception as error:

        print("Error while connecting to database for job tracker", error)

    return conn

def create_table_if_not_exists(conn):

    create_tbl_sql = """

    CREATE TABLE ticket_sale (
        ticket_id int not null,
        trans_date date not null,
        event_id int not null,
        event_name varchar(50) not null,
        event_date date not null,
        event_type varchar(10) not null,
        event_city varchar(20) not null,
        customer_id int not null,
        price decimal not null,
        num_tickets int not null
    )

    """

    try: 

        cursor = conn.cursor()

        tbl_name = 'ticket_sale'

        cursor.execute(f"DROP TABLE IF EXISTS {tbl_name}") 

        print(f"Table {tbl_name} deleted successfully.")

        cursor.execute(create_tbl_sql)

        cursor.close()

        print('ticket sale table created')

    except Exception as error:

        print("Error while creating ticket sale tbl", error)

def load_third_party(conn, csv_file_path):

    cursor=conn.cursor()

    insert_sql = """ 
            
        INSERT INTO ticket_sale 
        values (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s)
                
        """
    
    with open(csv_file_path, newline='') as f:

        reader = csv.reader(f)

        row_ct = 0

        for row in reader:

            cursor.execute(insert_sql,(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))

            # print(f"Inserted row {row_ct}")

            # print(f"Inserted event {row[3]} with tix count {row[9]}")

            row_ct += 1

    conn.commit()

    print(f"Inserted {row_ct} rows")

    cursor.close()
    
    return

def query_popular_tickets(conn):

    popular_sql= """

        SELECT event_name FROM ticket_sale ORDER BY num_tickets DESC LIMIT 3
        
        """

    cursor=conn.cursor()

    cursor.execute(popular_sql)

    records=cursor.fetchall()

    cursor.close()
    
    print(f"Qry returned {len(records)} row(s)")

    print(f"Qry returned {len(records[0])} col(s)")

    # for r in records:

    #     print(f"- {r[0]}")

    return records

def print_popular(records): 

    print('Here are the most popular tickets:')

    for r in records:

        print(f"- {r[0]}")


if __name__ == "__main__":

    try: 

        config = read_db_config('config.txt')

        create_database_if_not_exists(config)

        conn = get_db_connection(config)

        # create table if does not exist

        create_table_if_not_exists(conn)

        csv_file_path = 'third_party_sales_1.csv'

        load_third_party(conn, csv_file_path)

        popular = query_popular_tickets(conn)

        print_popular(popular)
    
    except:

        print("Aborted.")

