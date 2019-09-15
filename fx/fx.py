#!/usr/bin/env python

import urllib2

import datetime
import itertools as it
import pandas as pd
import os
import os.path
import datetime
import re
import calendar

DATA_DIR_XLS = "xls"
DATA_DIR_CSV = "csv"
DATA_START_MONTH = 5
DATA_START_YEAR = 2016
DATA_XLS_HEADER_ROWS = [5, 3, 2, 1, 4, 0, 6]

URL_PREFIX = "https://www.ato.gov.au/uploadedFiles/Content/TPALS/downloads/"
URL_SUFFIX = [
    "{}_{}_daily_rates.xlsx",
    "{}_{}_daily_input.xlsx",
    "{}{}dailyinput.xlsx",
    "{}-{}-daily-input.xlsx",
    "{1}_{0}_daily_input.xlsx",
    "{1}_{0}_Daily_%20input.xlsx",
    "{}%20{}%20daily%20input.xlsx",
    "{}%20{}%20daily%20input.xls.xlsx"
]

HTTP_HEADER = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
               'Accept-Encoding': 'none',
               'Accept-Language': 'en-US,en;q=0.8',
               'Connection': 'keep-alive'}

if not os.path.exists(DATA_DIR_XLS):
    os.makedirs(DATA_DIR_XLS)
if not os.path.exists(DATA_DIR_CSV):
    os.makedirs(DATA_DIR_CSV)

all_df = pd.DataFrame()

for year in range(DATA_START_YEAR, datetime.datetime.now().year + 1):
    for month in range(1 if year != DATA_START_YEAR else DATA_START_MONTH,
                       13 if year < datetime.datetime.now().year else datetime.datetime.now().month):
        month_string = datetime.date(2000, month, 1).strftime('%B')
        year_month_file = os.path.join(DATA_DIR_XLS, "ato_fx_{}-{}.xls".format(year, str(month).zfill(2)))
        available = False
        if os.path.isfile(year_month_file):
            print("{}-{} cached [{}]".format(year, str(month).zfill(2), "TRUE"))
            available = True
        else:
            print("{}-{} cached [{}]".format(year, str(month).zfill(2), "FALSE"))
            for suffix in URL_SUFFIX:
                url = (URL_PREFIX + suffix).format(month_string, year)
                try:
                    month_xls = urllib2.urlopen(urllib2.Request(url, headers=HTTP_HEADER))
                    with open(year_month_file, 'wb') as output:
                        output.write(month_xls.read())
                    print("{}-{} downloaded [{}] from [{}]".format(year, str(month).zfill(2), "TRUE", url))
                    available = True
                    break
                except:
                    print("{}-{} downloaded [{}] from [{}]".format(year, str(month).zfill(2), "FALSE", url))
                    continue
        print("{}-{} available [{}]".format(year, str(month).zfill(2), "TRUE" if available else "FALSE"))
        month_df = None
        for header_rows in DATA_XLS_HEADER_ROWS:
            month_df = pd.read_excel(year_month_file, skiprows=header_rows)
            if month_df.columns[0] == "Country":
                month_df = month_df[month_df['Country'].isin(['USA', 'UK'])]
                for column in month_df.columns:
                    if isinstance(column, basestring) and column != 'Country':
                        if column[0].isdigit():
                            match = re.compile("(.*)-(.*)").match(column)
                            month_df.rename(columns={column: "{}-{}-{}".format(
                                year, str(list(calendar.month_abbr).index(match.group(2))).zfill(2), match.group(1).zfill(2)
                            )}, inplace=True)
                        else:
                            month_df.drop(column, axis=1, inplace=True)
                    elif isinstance(column, datetime.datetime):
                        month_df.rename(columns={column: column.strftime('%Y-%m-%d')}, inplace=True)
                month_df = month_df.melt('Country', var_name='Date', value_name='Rate'). \
                    pivot_table('Rate', ['Date'], 'Country', aggfunc='first'). \
                    fillna(method='ffill').fillna(method='bfill').reset_index()
                month_df.rename(columns={"USA": "USD/AUD", "UK": "GBP/AUD"}, inplace=True)
                month_df.index.name = None
                month_df.columns.name = None
                all_df = all_df.append(month_df, ignore_index=True, verify_integrity=True)
                print("{}-{} parsed [{}] with header rows [{}] and data points [{}]".
                      format(year, str(month).zfill(2), "TRUE", header_rows, len(month_df)))
                break
        if month_df is not None:
            print("{}-{} processed [{}]".format(year, str(month).zfill(2), "TRUE"))
        else:
            print("{}-{} processed [{}]".format(year, str(month).zfill(2), "FALSE"))

all_df['Date'] = pd.to_datetime(all_df['Date'])
all_df = all_df.set_index('Date').reindex(pd.date_range(start=all_df['Date'].iloc[0], end=all_df['Date'].iloc[-1])). \
    fillna(method='ffill').fillna(method='bfill').rename_axis('Date').reset_index()
date_start = "{}-{}".format(all_df['Date'].iloc[0].year, str(all_df['Date'].iloc[0].month).zfill(2))
date_finish = "{}-{}".format(all_df['Date'].iloc[-1].year, str(all_df['Date'].iloc[-1].month).zfill(2))
all_df['Date'] = all_df['Date'].dt.strftime('%Y-%m-%d')
all_df.to_csv(os.path.join(DATA_DIR_CSV, "ato_fx_{}_{}.csv".format(date_start, date_finish)), index=False)
print("{} to {} processed with rows [{}]".format(date_start, date_finish, len(all_df)))
