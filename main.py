import csv
import tkinter as tk
from tkinter import filedialog
import datetime
from datetime import date
import yahoo_fin.stock_info as si
import sqlite3
import inflect


def str_create_table(x):
    correct_number = ''
    p = inflect.engine()
    number = p.number_to_words(x)
    for w in number:
        u = w
        if w == ' ':
            u = '_'
        elif w == '-':
            u = '_'
        correct_number = correct_number + u

    table_name = correct_number + '_day_avg'
    header = 'Volume'
    sql1 = 'CREATE TABLE IF NOT EXISTS ' + table_name
    sql1 = sql1 + '(Date TEXT,'
    sql1 = sql1 + 'Symbol TEXT,'
    sql1 = sql1 + header
    sql1 = sql1 + ', Close'
    sql1 = sql1 + ' FLOAT)'
    return sql1, header, table_name


def get_averages():
    conn = sqlite3.connect('../../DataBase/first_try.db')
    cur = conn.cursor()
    list_of_avg_lengths = [4]
    total_close = 0
    for r in list_of_avg_lengths:
        tracer = 0
        row_str = []
        ticker_averages = []
        counter = r
        print('Collecting rolling averages for ' + str(counter))
        sql_tup = str_create_table(counter)
        sql = sql_tup[0]
        header = sql_tup[1]
        table_name = sql_tup[2]
        stored_rows = []

        cur.execute(sql)
        conn.commit()

        avg_len = counter
        total_volume = 0
        cur.execute("SELECT DISTINCT SYMBOL FROM Stocks")
        all_unique_stocks = cur.fetchall()

        for v in all_unique_stocks:
            cur.execute("SELECT Date, Close, Volume FROM Stocks WHERE Symbol=?", v) #added 'close' to the querie
            ind_ticker_list = cur.fetchall()
            total_lines = int(len(ind_ticker_list))
            total_volume = 0
    # Note - close is [0][1], volume is [0][2], date is [0][0]

            if total_lines > r:
                while counter < total_lines:

                    ticker = ''.join(str(v))
                    ticker_l = len(ticker) - 2
                    ticker = str(ticker[1:ticker_l])

                    for i in range(avg_len, 0, -1):
                        full_row = ''.join(str(ind_ticker_list[counter - i]))
                        total_close = total_close + ind_ticker_list[counter - i + 1][1]
                        total_volume = total_volume + ind_ticker_list[counter - i +1][2]
                    average = total_volume/avg_len
                    close_avg = total_close/avg_len
                    total_volume = 0
                    total_close = 0
                    date_holder = ind_ticker_list[counter][0]
                    stored_rows.append([date_holder, ticker, average, close_avg])
                    counter = counter + 1
                    conn.commit()

                insert_sql = 'INSERT INTO '
                insert_sql = insert_sql + table_name + ' VALUES(?,?,?,?)'
                cur.executemany(insert_sql, stored_rows)
                stored_rows = []
                tracer = tracer + 1
                counter = 0
                average = 0
                close_avg = 0
                total_close = 0
                total_volume = 0
                conn.commit()
            print(ticker)
#            if tracer >= 5:
#                print('if triggered')
#                quit()


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


print('Menu:')
print('1. Convert TXT list of tickers to Database')
print('2. Update main database from Yahoo to current date')
print('3. Get averages and update database, created tables if they don\'t exist')
print('4. Exit')
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
        choice = ''
        get_averages()
        break
    elif choice == '4':
        choice = ''
        running = False
        break
    else:
        choice = ''
        print('Invalid selection, please try again')


