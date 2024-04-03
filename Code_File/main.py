import pandas as pd 
import requests 
from bs4 import BeautifulSoup
import numpy as np
import sqlite3
from datetime import datetime
 
# get current date and time


def log_progress(message):
    current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    m1=str(current_datetime)+":"+message
    f = open("code_log.txt", "a")
    f.write(m1)
    f.close()

def extract(url, table_attribs):
    wikiurl=url
    table_class=table_attribs
    response=requests.get(wikiurl)
    print(response.status_code)
    
    soup = BeautifulSoup(response.text, 'html.parser')
    bank_data=soup.find('table',{'class':table_attribs})
    df=pd.read_html(str(bank_data))
    df=pd.DataFrame(df[0])
    df2=pd.DataFrame({'Name':df['Bank name'] , 'MC_USD_Billion':df['Market cap (US$ billion)']})
    df2.set_index('Name', inplace=True)
    return df2  

def Transform(d,csv_path):
    er=pd.read_csv(csv_path)
    data = {
        'Name': d.index,
        'MC_USD_Billion': d['MC_USD_Billion'],
        'MC_GBP_Billion': round(d['MC_USD_Billion'] * er['Rate'][1], 2),
        'MC_EUR_Billion': round(d['MC_USD_Billion'] * er['Rate'][0], 2),
        'MC_INR_Billion': round(d['MC_USD_Billion'] * er['Rate'][2], 2),
        }

    d3=pd.DataFrame(data)
    d3.set_index('Name', inplace=True)
    return d3


#print(d4)
# print(d4['MC_EUR_Billion'][4])

def load_to_csv(d,output_path):
    d.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    conn = sqlite3.connect(sql_connection)
    
    # Insert the DataFrame into the SQLite database
    df.to_sql(table_name, conn, if_exists='replace', index=True)
    
    # Commit the changes and close the connection
    conn.commit()
    # conn.close()
    log_progress("SQL Connection initiated\n")




def run_query(query_statement, sql_connection):
    print("Executing Query:",query_statement)
    sqliteConnection = sqlite3.connect(sql_connection)
    cursor = sqliteConnection.cursor()
    cursor.execute(query_statement)
    result = cursor.fetchall()
    sqliteConnection.close()
    log_progress("Server Connection closed\n")
    print(result)
    print("\n\n")



log_progress("Preliminaries complete. Initiating ETL process\n")

d2=extract('https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks','wikitable sortable mw-collapsible')
log_progress("Data extraction complete. Initiating Transformation process\n")
# print(d2)

d4=Transform(d2,'exchange_rate.csv')
log_progress("Data transformation complete. Initiating Loading process\n")
# print(d4['MC_EUR_Billion'][4])

load_to_csv(d4,'Largest_banks_data.csv')
log_progress("Data saved to CSV file\n")

load_to_db(d4, 'Banks.db', 'Largest_banks')
log_progress("Data loaded to Database as a table, Executing queries\n")

query_statement="SELECT * FROM Largest_banks"
run_query(query_statement, "Banks.db")
log_progress("Process Complete\n")

query_statement="SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, "Banks.db")
log_progress("Process Complete\n")

query_statement="SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, "Banks.db")
log_progress("Process Complete\n")
