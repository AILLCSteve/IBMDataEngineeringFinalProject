# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import sqlite3
import pandas as pd
import numpy as np

# Initializing values 
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name','MC_USD_Billion']
csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = './code_log.txt'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = "%Y-%b-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp+":"+message+"\n")

log_progress('Preliminaries complete. Initiating ETL process.')
    
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if col:  
            name = col[1].find_all('a')[-1].get_text(strip=True)
            mc_raw = col[2].get_text(strip=True)
            if any(ch.isdigit() for ch in mc_raw):
                df = pd.concat(
                    [df, pd.DataFrame({"Name": [name], "MC_USD_Billion": [mc_raw]})],
                    ignore_index=True
                )

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    dfcsv = pd.read_csv(csv_path)
    rdic = dfcsv.set_index('Currency')['Rate'].to_dict()

    df["MC_USD_Billion"] = (
        df["MC_USD_Billion"]
        .astype(str)
        .str.replace(r"[^0-9.\-]", "", regex=True)
        .astype(float)
    )

    df["MC_GBP_Billion"] = (df["MC_USD_Billion"] * rdic["GBP"]).round(2)
    df["MC_EUR_Billion"] = (df["MC_USD_Billion"] * rdic["EUR"]).round(2)
    df["MC_INR_Billion"] = (df["MC_USD_Billion"] * rdic["INR"]).round(2)

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


df = extract(url, table_attribs)
#print(df)
log_progress('Data extraction complete, Initiating Transformation process.')
transform(df,csv_path)
print(df)
log_progress('Data transformation complete. Initiating Loading process.')
load_to_csv(df, output_path)
log_progress('Data saved to CSV file.')
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries.')
query_statement = 'SELECT * FROM Largest_banks'
run_query(query_statement, sql_connection)
query_statement = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_statement, sql_connection)
query_statement = 'SELECT Name FROM Largest_banks LIMIT 5'
run_query(query_statement,sql_connection)
log_progress('Process Complete.')
sql_connection.close()
log_progress('Server Connection closed')
