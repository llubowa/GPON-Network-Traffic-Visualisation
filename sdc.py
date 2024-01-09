# Import necessary libraries
import os
import pandas as pd
import psycopg2
import schedule
import time
from sqlalchemy import create_engine
import tarfile
from sqlalchemy.exc import ProgrammingError

# PON wrangle function
def pon_wrangle(path):
    """
    Perform data wrangling on PON-related data from the specified CSV file path.

    Parameters:
    - path (str): The path to the CSV file containing PON-related data.

    Returns:
    - df (DataFrame): A pandas DataFrame containing the wrangled PON data.

    Note:
    - The function reads the CSV file, extracts relevant columns, and performs necessary transformations.
    - It creates separate columns for ONTs, Utilization (%), Rx (Received Bytes), RxDropped (Dropped Received Bytes),
      Tx (Transmitted Bytes), and TxDropped (Dropped Transmitted Bytes).
    - The function converts missing values to zero and changes the datatype of numeric columns to float.
    - Bytes are converted to Mega bits per second for relevant numeric columns.
    - The 'datetime' column is formatted to '%Y-%m-%d %H:%M:%S'.

    """
    date = pd.read_csv(path,nrows=2)
    k=date.columns
    date = k[1]
    datetime = pd.to_datetime(date)
    t1 = datetime
    t2 = t1 + pd.Timedelta(minutes=5)
    t3 = t1 + pd.Timedelta(minutes=10)

    df = pd.read_csv(path,skiprows=5)
    
    df[['ONTs1', 'ONTs2', 'ONTs3']] = df['gponOltSidePonUtilPmIntervalNumActiveOnts'].str.extract(r'{(\d+), (\d+), (\d+)}')
    df[['Util1', 'Util2', 'Util3']] = df['gponOltSidePonUtilRxPmIntervalTotalUtil'].str.extract(r'{(\d+), (\d+), (\d+)}')
    
    df[['Rx1', 'Rx2', 'Rx3']] = df['gponOltSidePonUtilRxPmIntervalTotalBytes'].str.extract(r'{(\d+), (\d+), (\d+)}')
    df[['RxDroped1', 'RxDroped2', 'RxDroped3']] = df['gponOltSidePonUtilRxPmIntervalTotalDropBytes'].str.extract(r'{(\d+), (\d+), (\d+)}')
    df[['Tx1', 'Tx2', 'Tx3']] = df['gponOltSidePonUtilTxPmIntervalTotalBytes'].str.extract(r'{(\d+), (\d+), (\d+)}')
    df[['TxDroped1', 'TxDroped2', 'TxDroped3']] = df['gponOltSidePonUtilTxPmIntervalTotalDropBytes'].str.extract(r'{(\d+), (\d+), (\d+)}')
    df.drop(columns=['gponOltSidePonUtilRxPmIntervalTotalBytes','gponOltSidePonUtilRxPmIntervalTotalDropBytes','gponOltSidePonUtilTxPmIntervalTotalDropBytes','gponOltSidePonUtilPmIntervalNumActiveOnts','gponOltSidePonUtilRxPmIntervalTotalUtil'],inplace=True)

    dfONTs = df.melt(id_vars='Object ID', value_vars=['ONTs1', 'ONTs2', 'ONTs3'],var_name='ONTs',value_name='ONT(count)')
    dfUtil = df.melt(id_vars='Object ID', value_vars=['Util1', 'Util2', 'Util3'],var_name='Util',value_name='Util(%)')
    dfRx = df.melt(id_vars='Object ID', value_vars=['Rx1','Rx2','Rx3'],var_name='Rx',value_name='Rx(Mbs)')
    dfRxDroped = df.melt(id_vars='Object ID', value_vars=['RxDroped1','RxDroped2','RxDroped3'],var_name='Rxdropped',value_name='RxDropped(Mbs)')
    dfTx = df.melt(id_vars='Object ID', value_vars=['Tx1','Tx2','Tx3'],var_name='Tx',value_name='Tx(Mbs)')
    dfTxDroped = df.melt(id_vars='Object ID', value_vars=['TxDroped1','TxDroped2','TxDroped3'],var_name='TxDroped',value_name='TxDroped(Mbs)')
    df = pd.concat([dfONTs,dfUtil,dfRx,dfRxDroped,dfTx,dfTxDroped],axis=1)
    df = df.loc[:, ~df.columns.duplicated()]

    df.loc[df['ONTs'] == 'ONTs1', 'datetime'] = pd.to_datetime(t1).replace(tzinfo=None)
    df.loc[df['ONTs'] == 'ONTs2', 'datetime'] = pd.to_datetime(t2).replace(tzinfo=None)
    df.loc[df['ONTs'] == 'ONTs3', 'datetime'] = pd.to_datetime(t3).replace(tzinfo=None)
    df.drop(columns=['Util','ONTs','Rx','Tx','Rxdropped','TxDroped'],inplace=True)
    cols = ['Util(%)','ONT(count)','Rx(Mbs)','RxDropped(Mbs)','Tx(Mbs)','TxDroped(Mbs)']

    for col in cols:
        # For every column, replace missing values with zero and change datatype to float
        df[col].fillna(0,inplace=True)
        df[col] = df[col].astype(float)
    #return df
    for column in df.columns.drop(['Util(%)','ONT(count)']):
        if pd.api.types.is_numeric_dtype(df[column]):
            # Apply the formula to convert bytes into Mega bits per second
            df[column] = (df[column] * 8) / (5 * 60 * (10 ** 6))
    
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df
    

# ONT wrangle function
def ont_wrangle(path):
    """
    Perform data wrangling on ONT-related data from the specified CSV file path.

    Parameters:
    - path (str): The path to the CSV file containing ONT-related data.

    Returns:
    - df (DataFrame): A pandas DataFrame containing the wrangled ONT data.

    Note:
    - The function reads the CSV file, extracts relevant columns, and performs necessary transformations.
    - It creates separate columns for Rx (Received Bytes), RxDropped (Dropped Received Bytes),
      Tx (Transmitted Bytes), and TxDropped (Dropped Transmitted Bytes).
    - The function converts missing values to zero and changes the datatype of numeric columns to float.
    - Bytes are converted to Mega bits per second for relevant numeric columns.
    - The 'datetime' column is formatted to '%Y-%m-%d %H:%M:%S'.

    """
    date = pd.read_csv(path,nrows=2)
    k = date.columns
    date = k[1]
    datetime = pd.to_datetime(date)
    t1 = datetime
    t2 = t1 + pd.Timedelta(minutes=5)
    t3 = t1 + pd.Timedelta(minutes=10)

    df = pd.read_csv(path,skiprows=5)
    
    df[['Rx1', 'Rx2', 'Rx3']] = df['gponOltSideOntUtilBulkPmIntervalRxUcastBytes'].str.extract(r'{(\d+), (\d+), (\d+)}')
    df[['RxDroped1', 'RxDroped2', 'RxDroped3']] = df['gponOltSideOntUtilBulkPmIntervalRxUcastDropBytes'].str.extract(r'{(\d+), (\d+), (\d+)}')
    df[['Tx1', 'Tx2', 'Tx3']] = df['gponOltSideOntUtilBulkPmIntervalTxUcastBytes'].str.extract(r'{(\d+), (\d+), (\d+)}')
    df[['TxDroped1', 'TxDroped2', 'TxDroped3']] = df['gponOltSideOntUtilBulkPmIntervalTxUcastDropBytes'].str.extract(r'{(\d+), (\d+), (\d+)}')
    df.drop(columns=['gponOltSideOntUtilBulkPmIntervalRxUcastBytes','gponOltSideOntUtilBulkPmIntervalRxUcastDropBytes','gponOltSideOntUtilBulkPmIntervalTxUcastBytes','gponOltSideOntUtilBulkPmIntervalTxUcastDropBytes'],inplace=True)

    dfRx = df.melt(id_vars='Object ID', value_vars=['Rx1','Rx2','Rx3'],var_name='Rx',value_name='Rx(Mbs)')
    dfRxDroped = df.melt(id_vars='Object ID', value_vars=['RxDroped1','RxDroped2','RxDroped3'],var_name='Rxdropped',value_name='RxDropped(Mbs)')
    dfTx = df.melt(id_vars='Object ID', value_vars=['Tx1','Tx2','Tx3'],var_name='Tx',value_name='Tx(Mbs)')
    dfTxDroped = df.melt(id_vars='Object ID', value_vars=['TxDroped1','TxDroped2','TxDroped3'],var_name='TxDroped',value_name='TxDroped(Mbs)')
    df = pd.concat([dfRx,dfRxDroped,dfTx,dfTxDroped],axis=1)
    df = df.loc[:, ~df.columns.duplicated()]

    df.loc[df['Rx'] == 'Rx1', 'datetime'] = pd.to_datetime(t1).replace(tzinfo=None)
    df.loc[df['Rx'] == 'Rx2', 'datetime'] = pd.to_datetime(t2).replace(tzinfo=None)
    df.loc[df['Rx'] == 'Rx3', 'datetime'] = pd.to_datetime(t3).replace(tzinfo=None)
    df.drop(columns=['Rx','Tx','Rxdropped','TxDroped'],inplace=True)
    cols = ['Rx(Mbs)','RxDropped(Mbs)','Tx(Mbs)','TxDroped(Mbs)']

    for col in cols:
        df[col].fillna(0,inplace=True)
        df[col] = df[col].astype(float)
    
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            # Apply the formula to the numeric column
            df[column] = (df[column] * 8) / (5 * 60 * (10 ** 6))
    
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df

# Define the function to process new files
def process_new_files(folder_path, ont_db_connection_params,pon_db_connection_params, processed_file_record_set):
    """
    Continuously process new files in the specified folder, wrangle data, and update PostgreSQL tables.

    Parameters:
    - folder_path (str): The path to the folder containing the files to be processed.
    - ont_db_connection_params (dict): Connection parameters for the ONT PostgreSQL database.
    - pon_db_connection_params (dict): Connection parameters for the PON PostgreSQL database.
    - processed_file_record_set (set): A set containing the names of files that have already been processed.

    Note:
    - The function continuously checks for new files in the specified folder and processes them.
    - It extracts CSV files from zip files, wrangles the data, and updates corresponding tables in PostgreSQL databases.
    - The function uses an infinite loop with a sleep time of 5 minutes between iterations for continuity.
    - Processed file records are stored in a 'processed_files.txt' file.

    """
    while True: # Infinite looping for continuity of the process

        # List for OLT ip addresses
        olts = ['p.p.k.a','p.p.k.b','p.p.k.c','p.p.k.d','p.p.k.e','.....','p.p.k.z']
        
        for olt in olts:
            pon_dataframes =[]
            ont_dataframes =[]
            # List for new processed files
            records =[]
            # Check for new files by comparing with the list of processed files
            new_files = {f for f in os.listdir(folder_path) if "H" in f} - processed_file_record_set
            # Process each new file
            for file_name in new_files:
                # Check if the file is a zip file
                if olt in file_name: 
                    file_path = os.path.join(folder_path, file_name)
                    # Using context manager to extract CSV files in the zipped folder and keeping them in the temp folder
                    with tarfile.open(file_path,'r:gz') as tar:
                        tar.extractall(r"./temp") # Path to be changed
                    # Path to the desired CSV file in the temp folder
                    ont_csv_path=r"./temp/iSAM_ontOltUtilBulkHistoryData.csv" # Path to be changed
                    pon_csv_path=r"./temp/iSAM_ponOltUtilHistoryData.csv"       # Path to be changed
                    # Using the wrangle function to manipulate the data into the desired dataframe format
                    ont_data = ont_wrangle(ont_csv_path)
                    pon_data = pon_wrangle(pon_csv_path)
                    
                   #Appending wrangled data into dataframes list
                    ont_dataframes.append(ont_data)
                    pon_dataframes.append(pon_data)
                    # Updating the processed file record the processed file
                    processed_file_record_set.add(file_name)
                    records.append(file_name)
                
            try:  
                ont_df = pd.concat(ont_dataframes,ignore_index=True)
                pon_df = pd.concat(pon_dataframes,ignore_index=True)
                print(ont_df.head())
                # Create or replace the table in PostgreSQL
                table_name = tablename(names,olt)
                PON_database_tables = existing_tables(pon_db_connection_params)
                ONT_database_tables = existing_tables(ont_db_connection_params)
                # Push the data to PostgreSQL
                write_dataframe_to_postgres(ont_df,table_name,ont_db_connection_params)
                write_dataframe_to_postgres(pon_df,table_name,pon_db_connection_params)
                
                if table_name not in PON_database_tables:
                    alter_datetime_column(pon_db_connection_params,table_name)
                if table_name not in ONT_database_tables:
                    alter_datetime_column(ont_db_connection_params,table_name)  
            except Exception as e:
                print(f"Error: {e}")
            # Update the record of processed files
            with open('processed_files.txt', 'a') as file:
                for processed_file in records:
                    file.write(processed_file + '\n')
        # Process to sleep for 5 minutes before another loop            
        time.sleep(300)
    

# Path to the folder where sdc files are deposited
folder_path = r"/path/to/folder" # Path to be changed

# Dictionary for ont database access parameter
ont_db_connection_params = {
    'user': 'xxxx',
    'password': 'xxxx',
    'host': 'x.x.x.x',
    'port': 'xxxx',
    'database': 'onts',   
}

# Dictionary for pon database access parameter
pon_db_connection_params = {
    'user': 'xxxx',
    'password': 'xxxx',
    'host': 'x.x.x.x',
    'port': 'xxxx',
    'database': 'pons',
}

# Dictionary which maps OLT ip addresses to names
names ={
    'p.p.k.a':'olt1',
    'p.p.k.b':'olt2',
    'p.p.k.c':'olt3',
    'p.p.k.d':'olt4',
    'p.p.k.e':'olt2',
           '.':'.',
           '.':'.',
           '.':'.',
    'p.p.k.z':'CSQ'
}

def write_dataframe_to_postgres(dataframe, table_name, db_params):
    """
    Write a Pandas DataFrame to a PostgreSQL table.

    Parameters:
    - dataframe: Pandas DataFrame to be written to the table.
    - table_name: Name of the PostgreSQL table.
    - dbname: Name of the PostgreSQL database.
    - db_params: database parameters.

    Returns:
    - None
    """
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(**db_params)

        # Use SQLAlchemy's create_engine to simplify writing to PostgreSQL
        engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

        # Write the DataFrame to the PostgreSQL table
        dataframe.to_sql(table_name, engine, if_exists='append', index=False,chunksize=1000)
        
        # Commit the changes
        conn.commit()
        print(f"Data has been pushed to the data base")
    except Exception as e:
        print(f"Error: {e}")
    finally:
       # Close the connection
       if conn:
            conn.close()

# Getting the table name
def tablename(dictionary, ip):
    """
    Map an IP address to a corresponding table name using a provided dictionary.

    Parameters:
    - dictionary (dict): A dictionary containing mapping between specific keywords and corresponding table names.
    - ip (str): The IP address to be used for mapping to a table name.

    Returns:
    - name (str): The resulting table name. If the IP address contains a keyword from the dictionary,
                 the corresponding table name is returned; otherwise, the original IP address is returned.
    """
    for key, value in dictionary.items():
        if key in ip:
            name = value.lower()
            break
        else:
            name = ip
    return name

# Updated alter_datetime_column function
def alter_datetime_column(connection_params, table):
    """
    Alter the datatype of the 'datetime' column in the specified PostgreSQL table to TIMESTAMP WITH TIME ZONE.

    Parameters:
    - connection_params (dict): A dictionary containing parameters for establishing a connection to the PostgreSQL database.
                               Typically includes keys such as 'host', 'user', 'password', 'dbname', etc.
    - table (str): The name of the table in which the 'datetime' column needs to be altered.

    Note:
    - The alteration is specifically designed for converting the 'datetime' column to TIMESTAMP WITH TIME ZONE.
      The USING clause includes a conversion to the 'Africa/Nairobi' time zone.

    Raises:
    - Exception: If an error occurs during the execution of the ALTER TABLE query, an exception is caught and
                 an error message is printed.
    """
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**connection_params)

        # Create a cursor
        cursor = conn.cursor()

        # Define the SQL statement to alter the datetime column for the specified table
        alter_query = f"""
        ALTER TABLE {table}
        ALTER COLUMN "datetime" TYPE TIMESTAMP WITH TIME ZONE 
        USING "datetime"::TIMESTAMP WITH TIME ZONE AT TIME ZONE 'Africa/Nairobi';
        """

        # Execute the ALTER TABLE query
        cursor.execute(alter_query)

        # Commit the changes
        conn.commit()

        print(f"Datetime column for {table} altered successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
def existing_tables(connection_params):
    """
    Retrieve the names of all tables in the 'public' schema of a PostgreSQL database.

    Parameters:
    - connection_params (dict): A dictionary containing parameters for establishing a connection to the database.
                               Typically includes keys such as 'host', 'user', 'password', 'dbname', etc.

    Returns:
    - table_names (list): A list of strings representing the names of tables in the 'public' schema.
    """
    
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(**connection_params)

    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    # Use a SQL query to retrieve the names of all tables in the current schema
    table_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';  -- You can adjust the schema if needed
    """

    # Execute the query
    cursor.execute(table_query)

    # Fetch all the results into a list
    table_names = [row[0] for row in cursor.fetchall()]

    # Close the cursor and connection
    cursor.close()
    conn.close()
    return table_names

# Read the record of database tables from the file
processed_file_record_set = set()

# Read the record of processed files from the file
if os.path.exists('processed_files.txt'):
    with open('processed_files.txt', 'r') as file:
        processed_file_record_set = set(file.read().splitlines())
#if __name__ == "__main__":
# Call the function with the provided parameters
#    process_new_files(folder_path, db_connection_params, processed_file_record)

# Define the job to be scheduled
def scheduled_job():
    process_new_files(folder_path, ont_db_connection_params,pon_db_connection_params, processed_file_record_set)
# Schedule the job every 15 minutes
schedule.every(1).minutes.do(scheduled_job)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)  # Sleep for 1 second to avoid high CPU usage

