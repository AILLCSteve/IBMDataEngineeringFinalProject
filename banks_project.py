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
table_attribs = "['Name','MC_USD_Billion']"
csv_path = './Largest_banks_data.csv'
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
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    row = table[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if col:  
            name = col[1].a.get_text(strip=True) if col[1].find('a') else col[1].get_text(strip=True)
            mc_raw = col[2].get_text(strip=True)
            if any(ch.isdigit() for ch in mc_raw):
                df = pd.concat(
                    [df, pd.DataFrame({"Name": [name], "MC_USD_Billion": [mc_raw]})],
                    ignore_index=True
                )
    return df


    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Data extraction complete, Initiating Transformation process.')
log_progress('Data transformation complete. Initiating Loading process.')
log_progress('Data saved to CSV file.')
log_progress('SQL Connection initiated.')
log_progress('Data loaded to Database as a table, Executing queries.')
log_progress('Process Complete.')
log_progress('Server Connection closed')
