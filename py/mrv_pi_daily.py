# -*- coding: utf-8 -*-

# PI server connectivity related library
from win32com.client.dynamic import Dispatch

# Library for handling datetime data
from datetime import datetime, timedelta
import pandas as pd
from os import path
import smtplib
import math
import pyodbc
import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
from datetime import timedelta
from email.mime.text import MIMEText
from dateutil import relativedelta
from dateutil.relativedelta import relativedelta

def conn_sql_server(server, db, user, pwd, sql_string):
    """
    Establish connection to SQL Server and return data as DataFrame
    Parameters
    ----------
    server : TYPE
        DESCRIPTION.
    db : TYPE
        DESCRIPTION.
    user : TYPE
        DESCRIPTION.
    pwd : TYPE
        DESCRIPTION.
    sql_string : TYPE
        DESCRIPTION.

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    """
    df = pd.DataFrame()
    try:
        conn = pyodbc.connect(Driver="{SQL Server}", Server=server, Database=db, Trusted_Connection="NO", User=user, Password=pwd)
        print("connection with {} successful".format(db))
        df = pd.read_sql_query(sql_string, conn)
        success = True
    except Exception as e:
        print("connection with {} failed: ->{}".format(db, str(e)))   
        success = False
    return df, success

def insert_SQL(server,db,user,pwd,tbl,cols,vals):
    conn = pyodbc.connect(Driver="{SQL Server}", Server=server, Database=db, Trusted_Connection="NO", User=user, Password=pwd)
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO """ + tbl + """ (""" + cols + """)""" + """ VALUES(""" + vals + """);""")
    conn.commit()
    
def clear_SQL(server,db,user,pwd,tbl):
    conn = pyodbc.connect(Driver="{SQL Server}", Server=server, Database=db, Trusted_Connection="NO", User=user, Password=pwd)
    cursor = conn.cursor()
    cursor.execute("""TRUNCATE TABLE """ + tbl + """;""")
    conn.commit()

def iferror(success, failure, *exceptions):
    try:
        return success()
    except Exception as e:
        return failure    
    
try:
    txt = 'getting GHG_Live_Data from Datatbase'
    df_GHG_Live_Data, success = conn_sql_server(
            server = "BNY-S-560",
            db = "dataEntryDB",
            user = "Abimbola.Salami",
            pwd = "NLNG@3070",
            sql_string = """SELECT * FROM dbo.GHG_Live_Data"""
            )

    txt = 'converting dataframe to numbers'
    for col in df_GHG_Live_Data.columns:
        if col != 'RecordDate' and col != 'UpdatedDate' and col != 'UpdatedBy':
            df_GHG_Live_Data[col] = df_GHG_Live_Data[col].apply(pd.to_numeric, errors='coerce')
    df_GHG_Live_Data.fillna(0)
    
    txt = 'creating new dataframe'
    GHG_Daily_Data = pd.DataFrame(columns = ['RecordDate', 'UpdatedDate', 'UpdatedBy', 'Acid_Gas_T1_CO2',
        'Acid_Gas_T2_CO2', 'Acid_Gas_T3_CO2', 'Acid_Gas_T4_CO2',
        'Acid_Gas_T5_CO2', 'Acid_Gas_T6_CO2', 'Acid_Gas_CO2', 'T1_CO2',
        'T2_CO2', 'T3_CO2', 'T4_CO2', 'T5_CO2', 'T6_CO2', 'Trains_CO2',
        'T1_N2O', 'T2_N2O', 'T3_N2O', 'T4_N2O', 'T5_N2O', 'T6_N2O',
        'Trains_N2O', 'T1_CH4', 'T2_CH4', 'T3_CH4', 'T4_CH4', 'T5_CH4',
        'T6_CH4', 'Trains_CH4', 'T1_CO2e', 'T2_CO2e', 'T3_CO2e', 'T4_CO2e',
        'T5_CO2e', 'T6_CO2e', 'Trains_CO2e', 'LHU_CO2', 'LHU_N2O', 'LHU_CH4',
        'LHU_CO2e', 'GTG_CO2', 'GTG_N2O', 'GTG_CH4', 'GTG_CO2e'])
            
    txt = 'updating constant column in new dataframe'
    GHG_Daily_Data.loc[0, "RecordDate"] = datetime.today().strftime('%Y-%m-%d')
    GHG_Daily_Data.loc[0, "UpdatedDate"] = datetime.today().strftime('%Y-%m-%d')
    GHG_Daily_Data.loc[0, "UpdatedBy"] = 'Admin'
    
    txt = 'getting mean of all data columns and posting in new dataframe'
    for col in df_GHG_Live_Data.columns:
        if col != 'RecordDate' and col != 'UpdatedDate' and col != 'UpdatedBy' and col != 'id':
            GHG_Daily_Data.at[0, col] = df_GHG_Live_Data[col].mean()
        
    txt = 'posting output to database'
    vals = ''
    cols = ''
    for col in GHG_Daily_Data.columns:
        cols = cols + col + ","
        vals = vals + "'" + str(GHG_Daily_Data.at[0, col]) + "',"
    vals=vals[:-1]
    cols=cols[:-1]
    insert_SQL(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        tbl = "GHG_Daily_Data",
        cols = cols,
        vals = vals
    )

    txt = 'clearing Live database'
    clear_SQL(server = "BNY-S-560",db = "dataEntryDB",user = "Abimbola.Salami",pwd = "NLNG@3070",tbl = "GHG_Live_Data")
except Exception as e:
    success = False
    error = "Error in " + txt + ". " + str(e)
    error = error.replace('"', '')
    error = error.replace("'", "")
    
    to='abimbola.salami@nlng.com'          
    s = smtplib.SMTP('webmail.nlng.com', 25)
    msg = MIMEText("""Dear Admin\n\nPlease note that mrv_pi_daily.py failed while running for the following reasons:\n\n""" + error + """\n\nRegards\nData Entry Portal""")
    sender = 'noreply-dataentry@nlng.com'
    msg['Subject'] = "Error Running Script mrv_pi_daily.py"
    msg['From'] = sender
    msg['To'] = to
    s.sendmail(sender, to, msg.as_string()) 