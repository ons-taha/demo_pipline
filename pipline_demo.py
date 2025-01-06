import configparser  # To read database configuration files (ini files)
import pickle        # To save and load Python objects (e.g., last run date)
import pandas as pd  # For data manipulation and analysis
from sqlalchemy import create_engine  # For SQLAlchemy database connections
import psycopg2      # For interacting with PostgreSQL databases
import pandahouse as ph  # For interacting with ClickHouse databases
import requests      # For making HTTP requests (used to fetch IP-based data)
from datetime import datetime, timedelta  # For working with dates and times
import os            # For working with file paths

def readdbinfo(path, name, section):
    """
    Reads database connection details from an ini configuration file.
    """
    config = configparser.ConfigParser()
    file = os.path.join(path, name)  # Construct the full path to the config file
    config.read(file)                # Read the configuration file
    db_info = config[section]        # Extract the relevant section
    return db_info['username'], db_info['password'], db_info['host'], db_info['port'], db_info['database']

def read_from_postgres(host, port, user, password, database, sql_query):
    """
    Executes a query on a PostgreSQL database and returns the results as a DataFrame.
    """
    with psycopg2.connect(host=host, port=port, user=user, password=password, database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)                      # Execute the SQL query
            columns = [desc[0] for desc in cursor.description]  # Extract column names
            results = cursor.fetchall()                    # Fetch all results
    return pd.DataFrame(results, columns=columns)          # Convert to DataFrame

def check_ip(ip_address):                                         # that function here just for meet requiremnt for used api to get data but for optimizing that should handling completely in different way 
    """
    Fetches city information for a given IP address using the IPInfo API.
    """
    API_TOKEN = '5959a672ca47d4'  # Token for the IPInfo API
    url = f'https://ipinfo.io/{ip_address}?token={API_TOKEN}'  # API endpoint
    response = requests.get(url)        # Send HTTP request
    data = response.json()              # Parse JSON response
    return data.get('city', 'unknown')  # Return city if available, else 'unknown'

def FromTClickhouse(server, port, schema, username, password, query, chunksize=1000000):
    """
    Fetches data from a ClickHouse database in chunks and combines them into a single DataFrame.
    """
    connection = {
        'host': f'http://{server}:{port}',
        'database': schema,
        'user': username,
        'password': password
    }
    chunks = [chunk for chunk in ph.read_clickhouse(query=query, connection=connection, chunksize=chunksize)]
    return pd.concat(chunks, ignore_index=True)  # Combine chunks into a single DataFrame

def ToClickhouse(df, server, port, schema, username, password, clickhouse_table):
    """
    Writes a DataFrame to a ClickHouse table.
    """
    connection = {
        'host': f'http://{server}:{port}',
        'database': schema,
        'user': username,
        'password': password
    }
    ph.to_clickhouse(df, clickhouse_table, index=False, connection=connection)

def main():
    """
    Main ETL pipeline to fetch, process, and store transaction data.
    """
    # Define paths
    base_path = r'C:\Users\Administrator\Desktop'
    last_session_path = r'C:\Users\otaha\Desktop\load_time.pkl'

    # Read database configurations
    clickUserName, clickPassword, clickHost, clickPort, clickSchema = readdbinfo(
        path=base_path, name='supporthrms7.ini', section='clickhouse'
    )
    MVUserName, MVPassword, MVHost, MVPort, MVSchema = readdbinfo(
        path=base_path, name='TBCInfo.ini', section='mv'
    )

    # Load last run date
    with open(last_session_path, 'rb') as f:
        run_date = pickle.load(f)

    # Query data from ClickHouse
    query = f"""
        SELECT session_id, request_header, ip_ AS ip_address, CAST(e2.time_local AS datetime) AS request_time,
               amount,
               CASE
                   WHEN e2.http_ REGEXP '(?i)okhttp/4.11.0' THEN 'Android'
                   WHEN e2.http_ REGEXP '(?i)CFNetwork.*Darwin|Darwin.*CFNetwork' THEN 'IOS'
                   ELSE 'website'
               END AS type,
               json AS info, type, card_num
        FROM table e2
        WHERE toDate(time_local) = toDate('{run_date}')
          AND host = 'api-ms-v2.almanasa.tv'
          AND request_header REGEXP 'trans/'
          AND status = 200
          AND request_method = 'GET'
    """
    transactions = FromTClickhouse(clickHost, '9000', clickSchema, clickUserName, clickPassword, query)

    # Query data from PostgreSQL
    services_query = """
        SELECT service_id, number AS card_num, name
        FROM services
    """
    services = read_from_postgres(MVHost, MVPort, MVUserName, MVPassword, MVSchema, services_query)

    # Process transactions
    pattern = r'([a-f0-9]{8}(?:-[a-z0-9]{4}){3}-[a-z0-9]{12})'
    transactions['transaction_id'] = transactions['request_header'].str.extract(pattern)
    transactions = transactions[transactions['transaction_id'].notnull()].drop_duplicates()

    transactions['customer_id'] = transactions['info'].apply(lambda x: x.get('customer_id'))
    transactions['vender_id'] = transactions['info'].apply(lambda x: x.get('vender_id'))
    transactions['city'] = transactions['ip_address'].apply(check_ip)  # from ip_address get city

    # Merge with services data
    merged_df = transactions.merge(services, on='card_num', how='inner') #detect_card_type
    merged_df.drop(columns=['card_num', 'service_id'], inplace=True)
    merged_df.rename(columns={'type': 'transaction_type', 'request_time': 'date_time'}, inplace=True)

    # Select and organize final columns
    col = ['transaction_id', 'customer_id', 'card_type', 'amount', 'transaction_type',
           'vender_id', 'date_time', 'session_id', 'ip_address', 'city']
    final_df = merged_df[col]
    final_df['snapshot'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))

    # Write data to ClickHouse
    ToClickhouse(final_df, clickHost, clickPort, 'datawarehouse_production', clickUserName, clickPassword, 'transaction')

    # Update last run date
    new_run_date = (datetime.strptime(run_date, "%Y-%m-%d %H:%M:%S") + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    with open(last_session_path, 'wb') as f:
        pickle.dump(new_run_date, f)

    print('Good', len(final_df))  #  for me monitoring power shell for fast 

if __name__ == "__main__":
    main()
