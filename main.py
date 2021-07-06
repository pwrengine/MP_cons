import csv
# import pandas as pd
import tkinter as tk
from tkinter import filedialog
# from pandas_datareader import data as pdr
# import pandas_datareader as pdr
import sqlite3
import datetime
from datetime import date
import yahoo_fin.stock_info as si

def create_ticker_list():
    root = tk.Tk()
    root.withdraw()
    print('Choose a txt file to convert to csv')
    file_path = filedialog.askopenfilename()
    csv_file_name = input('\nType a name for the CSV file including the extension\n')


    # successfully converts TXT with tab delimiter into CSV for processing to DB
    with open(file_path) as infile, open(csv_file_name, 'w') as outfile:
        for line in infile:
            outfile.write(line.replace('\t', ','))

    # Take the CSV and convert it to a SQLite DB
    connection = sqlite3.connect('stock_list.db')
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE if not exists stocks (
        Symbol TEXT)
    """)

    with open(csv_file_name) as g:
        reader = csv.reader(g)
    #    print('This is READER')
    #    print(reader)
    #    print(type(reader))
        for t in reader:
            if t != []:
                cursor.execute('INSERT INTO Stocks VALUES (?)', t[0:1])
    # SQL to remove the extraneous header row and leave only the symbols
    cursor.execute("DELETE FROM Stocks WHERE Symbol = 'Symbol'")
    connection.commit()
    # now we have a list (db) of all currently listed tickers on the exchange
    # next step is to compare the existing full DB (the one that includes all years' data) to this list and add any
    # new ones.  This is done using the Yahoo data grab that will collect the data for all stocks

def update_to_current_date():
    # prior to this script, convert the text file to a DB with the full list of stocks

    conn = sqlite3.connect('../../DataBase/first_try.db')
    list_con = sqlite3.connect('stock_list.db')
    cur = conn.cursor()
    cur2 = list_con.cursor()
    cur.execute('SELECT MAX(date) FROM Stocks')
    latest_date = cur.fetchone()
    print('The last DB entry is on:')
    print(latest_date)
    current_date = date.today()
    latest_year = int(latest_date[0][0:4])
    latest_month = int(latest_date[0][5:7])
    latest_day = int(latest_date[0][8:10])
    print('The current date is:')
    print(current_date)
    working_date = date(latest_year, latest_month, latest_day)
    working_date = working_date + datetime.timedelta(days=1)
    print('The date to start Yahoo data grab is:')
    print(working_date)

    cur2.execute('SELECT * FROM Stocks')
    symbol_list = cur2.fetchall()
    for ticker in symbol_list:
        symbol = ticker[0]
        try:

            data = si.get_data(symbol, start_date=working_date, end_date=date.today())
    #        data = si.get_data(symbol, start_date="2021-06-25", end_date="2021-07-04")
    #            pdr.get_data_enigma()
    #        print(data)
            dict = {'ticker': 'Symbol',
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'adjclose': 'Adj Close',
                    'volume': 'Volume',
                    }
            data.rename(columns=dict, inplace=True)
            data.reset_index(inplace=True)
            data = data.rename(columns={'index': 'Date'})
            print(symbol)
            print(data)
            data.to_sql('stocks', con=conn, schema='none', if_exists=""'append'"", index=False)
        except Exception as exc:
            print('Data grab failed for ' + symbol + "\n")
            print('The exception is\n'), exc
#        quit()
print('Menu:')
print('1. Convert TXT list of tickers to Database')
print('2. Update main database from Yahoo to current date')
print('3. Exit')
choice = input('Enter your selection:')
running = True
while running:
    if choice == '1':
        create_ticker_list()
        choice == ''
        break
    elif choice == '2':
        update_to_current_date()
        choice = ''
        break
    elif choice == '3':
        running = False
        choice = ''
        break
    else:
        print('Invalid selection, please try again')


