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
    
def update_SQL(server,db,user,pwd,tbl,cols_vals,row_id):
    conn = pyodbc.connect(Driver="{SQL Server}", Server=server, Database=db, Trusted_Connection="NO", User=user, Password=pwd)
    cursor = conn.cursor()
    cursor.execute("""UPDATE """ + tbl + """ SET """ + cols_vals + """ WHERE id = """ + row_id + """;""")
    conn.commit()

def iferror(success, failure, *exceptions):
    try:
        return success()
    except Exception as e:
        return failure    
    
# Getting GHG Factors from Datatbase
df_Factors, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Factors"""
        )
df_Factors = df_Factors.set_index('Factor_Name')

# Getting Non-Bonny Offices data from Datatbase
df_Non_Bny_Off, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.Non_Bonny_Offices"""
        )

# Getting Non-Bonny Offices data from Datatbase
df_GHG_Offices_Monthly, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Offices_Monthly"""
        )

# Creating a new dataframe to store GHG output
GHG_Offices_Monthly = pd.DataFrame(columns = ['RecordDate', 'UpdatedDate', 'UpdatedBy', 'PHC_NG_CO2e','PHC_Diesel_CO2e', 'PHC_CO2e', 'ABJ_NG_CO2e', 'ABJ_Diesel_CO2e','ABJ_CO2e', 'LTO_NG_CO2e', 'LTO_Diesel_CO2e', 'LTO_CO2e','LON_NG_CO2e'])

# Calculating emissions for PHC_NG_CO2e and storing to dataframe for all months
for i in range(df_Non_Bny_Off.shape[0]):
    NG_Conversion = iferror(lambda: float(df_Factors['Factor_Value']['NG_Conversion']),0)
    NG_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['NG_CO2']),0)
    PHC_NG = iferror(lambda: float(df_Non_Bny_Off['PHC_NG'][i]),0)
    PHC_NG_CO2e = iferror(lambda: PHC_NG * NG_Conversion * NG_CO2,0)
    GHG_Offices_Monthly.loc[i, 'PHC_NG_CO2e'] = PHC_NG_CO2e
    GHG_Offices_Monthly.loc[i, "RecordDate"] = df_Non_Bny_Off['RecordDate'][i]
    GHG_Offices_Monthly.loc[i, "UpdatedDate"] = datetime.today().strftime('%Y-%m-%d')
    GHG_Offices_Monthly.loc[i, "UpdatedBy"] = 'Admin'
    
# Calculating emissions for PHC_Diesel_CO2e and storing to dataframe for all months
for i in range(df_Non_Bny_Off.shape[0]):
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CO2']),0)
    PHC_Diesel = iferror(lambda: float(df_Non_Bny_Off['PHC_Diesel'][i]),0)
    PHC_Diesel_CO2e = iferror(lambda: PHC_Diesel/1000 * Diesel_Density/1000 * Diesel_CO2,0)
    GHG_Offices_Monthly.loc[i, 'PHC_Diesel_CO2e'] = PHC_Diesel_CO2e
    
# Calculating emissions for total PHC_CO2e and storing to dataframe for all months
for i in range(df_Non_Bny_Off.shape[0]):
    GHG_Offices_Monthly.loc[i, 'PHC_CO2e'] = GHG_Offices_Monthly.loc[i, 'PHC_NG_CO2e'] + GHG_Offices_Monthly.loc[i, 'PHC_Diesel_CO2e']
    
# Calculating emissions for ABJ_NG_CO2e and storing to dataframe for all months
for i in range(df_Non_Bny_Off.shape[0]):
    GT_Efficiency = iferror(lambda: float(df_Factors['Factor_Value']['GT_Efficiency']),0)
    NG_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['NG_CO2']),0)
    ABJ_NG = iferror(lambda: float(df_Non_Bny_Off['ABJ_NG'][i]),0)
    ABJ_NG_CO2e = iferror(lambda: ABJ_NG/24 * GT_Efficiency * NG_CO2,0)
    GHG_Offices_Monthly.loc[i, 'ABJ_NG_CO2e'] = ABJ_NG_CO2e
    
# Calculating emissions for ABJ_Diesel_CO2e and storing to dataframe for all months
for i in range(df_Non_Bny_Off.shape[0]):
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CO2']),0)
    ABJ_Diesel = iferror(lambda: float(df_Non_Bny_Off['ABJ_Diesel'][i]),0)
    ABJ_Diesel_CO2e = iferror(lambda: ABJ_Diesel/1000 * Diesel_Density/1000 * Diesel_CO2,0)
    GHG_Offices_Monthly.loc[i, 'ABJ_Diesel_CO2e'] = ABJ_Diesel_CO2e
    
# Calculating emissions for total ABJ_CO2e and storing to dataframe for all months
for i in range(df_Non_Bny_Off.shape[0]):
    GHG_Offices_Monthly.loc[i, 'ABJ_CO2e'] = GHG_Offices_Monthly.loc[i, 'ABJ_NG_CO2e'] + GHG_Offices_Monthly.loc[i, 'ABJ_Diesel_CO2e']
    
# Calculating emissions for LTO_NG_CO2e and storing to dataframe for all months
for i in range(df_Non_Bny_Off.shape[0]):
    NG_Conversion = iferror(lambda: float(df_Factors['Factor_Value']['NG_Conversion']),0)
    NG_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['NG_CO2']),0)
    LTO_NG = iferror(lambda: float(df_Non_Bny_Off['LTO_NG'][i]),0)
    LTO_NG_CO2e = iferror(lambda: LTO_NG * NG_Conversion * NG_CO2,0)
    GHG_Offices_Monthly.loc[i, 'LTO_NG_CO2e'] = LTO_NG_CO2e
    
# Calculating emissions for LTO_Diesel_CO2e and storing to dataframe for all months
for i in range(df_Non_Bny_Off.shape[0]):
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CO2']),0)
    LTO_Diesel = iferror(lambda: float(df_Non_Bny_Off['LTO_Diesel'][i]),0)
    LTO_Diesel_CO2e = iferror(lambda: LTO_Diesel/1000 * Diesel_Density/1000 * Diesel_CO2,0)
    GHG_Offices_Monthly.loc[i, 'LTO_Diesel_CO2e'] = LTO_Diesel_CO2e
    
# Calculating emissions for total LTO_CO2e and storing to dataframe for all months
for i in range(df_Non_Bny_Off.shape[0]):
    GHG_Offices_Monthly.loc[i, 'LTO_CO2e'] = GHG_Offices_Monthly.loc[i, 'LTO_NG_CO2e'] + GHG_Offices_Monthly.loc[i, 'LTO_Diesel_CO2e']
    
# Calculating emissions for LON_NG_CO2e and storing to dataframe for all months
for i in range(df_Non_Bny_Off.shape[0]):
    GT_Efficiency = iferror(lambda: float(df_Factors['Factor_Value']['GT_Efficiency']),0)
    NG_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['NG_CO2']),0)
    LON_NG = iferror(lambda: float(df_Non_Bny_Off['LON_NG'][i]),0)
    LON_NG_CO2e = iferror(lambda: LON_NG/24 * GT_Efficiency * NG_CO2,0)
    GHG_Offices_Monthly.loc[i, 'LON_NG_CO2e'] = LON_NG_CO2e
    
# Posting all GHG entries to database
col_list = GHG_Offices_Monthly.columns
for i, row in GHG_Offices_Monthly.iterrows():
    record_date = GHG_Offices_Monthly.at[i, 'RecordDate']
    if df_GHG_Offices_Monthly.shape[0] == 0:
        record_found = "NO"
    else:
        for j, row in df_GHG_Offices_Monthly.iterrows():
            new_date = df_GHG_Offices_Monthly.at[j, 'RecordDate'] 
            if record_date.month==new_date.month and record_date.year==new_date.year:
                record_found = "YES"
                break
            else:
                record_found = "NO"
    if record_found == "YES":
        cols_vals = ''
        cols_vals = ''
        for col in col_list:
            cols_vals = cols_vals + col + "=" + "'" + str(GHG_Offices_Monthly.at[i, col]) + "',"
        cols_vals=cols_vals[:-1]
        row_id = str(df_GHG_Offices_Monthly.at[j, 'id'])
        update_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Offices_Monthly",cols_vals=cols_vals,row_id = row_id)
    else:
        vals = ''
        cols = ''
        for col in col_list:
            cols = cols + col + ","
            vals = vals + "'" + str(GHG_Offices_Monthly.at[i, col]) + "',"
        vals=vals[:-1]
        cols=cols[:-1]
        insert_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Offices_Monthly",cols=cols,vals=vals)


# Processing LSS GHG data

# Getting Aviation data from Datatbase
GHG_Aviation, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Aviation"""
        )

# Getting Passenger boats data from Datatbase
GHG_Passenger_Boats, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Passenger_Boats"""
        )

# Getting Tug boats data from Datatbase
GHG_Tug_Boats, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Tug_Boats"""
        )

# Getting Escort boats data from Datatbase
GHG_Escort_Boats, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Escort_Boats"""
        )

# Getting Bonny Fleet boats data from Datatbase
GHG_Bonny_Fleet, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Bonny_Fleet"""
        )

# Getting Non-Bonny Fleet boats data from Datatbase
GHG_Non_Bonny_Fleet, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Non_Bonny_Fleet"""
        )

# Getting Non-Bonny Logistics data from Datatbase
df_GHG_Logistics_Monthly, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Logistics_Monthly"""
        )

# Creating a new dataframe to store GHG output
GHG_Logistics_Monthly = pd.DataFrame(columns = ['RecordDate', 'UpdatedDate', 'UpdatedBy', 'Aviation_CO2e',
       'Av_Per_Passenger', 'Av_Per_Distance', 'Passenger_Boats_CO2e',
       'P_Boats_Per_Passenger', 'P_Boats_Per_Distance', 'Tug_Boats_CO2e',
       'T_Boats_Per_Passenger', 'T_Boats_Per_Distance', 'Long_Escort_CO2e',
       'L_Escort_Per_Passenger', 'L_Escort_Per_Distance',
       'Passenger_Escort_CO2e', 'P_Escort_Per_Passenger',
       'P_Escort_Per_Distance', 'Escort_CO2e', 'Bny_Fleet_Diesel_CO2e',
       'Bny_Fleet_Petrol_CO2e', 'Bny_Fleet_CO2e', 'Bny_Fleet_Per_Passenger',
       'Bny_Fleet_Per_Distance', 'Non_Bny_Fleet_Diesel_CO2e',
       'Non_Bny_Fleet_Petrol_CO2e', 'Non_Bny_Fleet_CO2e',
       'Non_Bny_Fleet_Per_Passenger', 'Non_Bny_Fleet_Per_Distance'])

# Calculating emissions for Aviation_CO2e and storing to dataframe for all months
for i, row in GHG_Aviation.iterrows():
    Jet_Fuel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Jet_Fuel_Density']),0)
    Jet_Fuel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Jet_Fuel_CO2']),0)
    Jet_Fuel = iferror(lambda: float(GHG_Aviation['Jet_Fuel'][i]),0)
    Tot_Passengers = iferror(lambda: float(GHG_Aviation['Tot_Passengers'][i]),0)
    Tot_Distance = iferror(lambda: float(GHG_Aviation['Tot_Distance'][i]),0)
    Aviation_CO2e = iferror(lambda: Jet_Fuel/1000 * Jet_Fuel_Density/1000 * Jet_Fuel_CO2,0)
    GHG_Logistics_Monthly.loc[i, 'Aviation_CO2e'] = Aviation_CO2e
    GHG_Logistics_Monthly.loc[i, 'Av_Per_Passenger'] = iferror(lambda: Aviation_CO2e/Tot_Passengers,0)
    GHG_Logistics_Monthly.loc[i, 'Av_Per_Distance'] = iferror(lambda: Aviation_CO2e/Tot_Distance,0)
    GHG_Logistics_Monthly.loc[i, "RecordDate"] = GHG_Aviation['RecordDate'][i]
    GHG_Logistics_Monthly.loc[i, "UpdatedDate"] = datetime.today().strftime('%Y-%m-%d')
    GHG_Logistics_Monthly.loc[i, "UpdatedBy"] = 'Admin'
    
# Calculating emissions for Passenger_Boats_CO2e and storing to dataframe for all months
for i, row in GHG_Passenger_Boats.iterrows():
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CO2']),0)
    Diesel = iferror(lambda: float(GHG_Passenger_Boats['Diesel'][i]),0)
    Tot_Passengers = iferror(lambda: float(GHG_Passenger_Boats['Tot_Passengers'][i]),0)
    Tot_Distance = iferror(lambda: float(GHG_Passenger_Boats['Tot_Distance'][i]),0)
    Passenger_Boats_CO2e = iferror(lambda: Diesel/1000 * Diesel_Density/1000 * Diesel_CO2,0)
    GHG_Logistics_Monthly.loc[i, 'Passenger_Boats_CO2e'] = Passenger_Boats_CO2e
    GHG_Logistics_Monthly.loc[i, 'P_Boats_Per_Passenger'] = iferror(lambda: Passenger_Boats_CO2e/Tot_Passengers,0)
    GHG_Logistics_Monthly.loc[i, 'P_Boats_Per_Distance'] = iferror(lambda: Passenger_Boats_CO2e/Tot_Distance,0)
    
# Calculating emissions for Tug_Boats_CO2e and storing to dataframe for all months
for i, row in GHG_Tug_Boats.iterrows():
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CO2']),0)
    Diesel = iferror(lambda: float(GHG_Tug_Boats['Diesel'][i]),0)
    Tot_Passengers = iferror(lambda: float(GHG_Tug_Boats['Tot_Passengers'][i]),0)
    Tot_Distance = iferror(lambda: float(GHG_Tug_Boats['Tot_Distance'][i]),0)
    Tug_Boats_CO2e = iferror(lambda: Diesel/1000 * Diesel_Density/1000 * Diesel_CO2,0)
    GHG_Logistics_Monthly.loc[i, 'Tug_Boats_CO2e'] = Tug_Boats_CO2e
    GHG_Logistics_Monthly.loc[i, 'T_Boats_Per_Passenger'] = iferror(lambda: Tug_Boats_CO2e/Tot_Passengers,0)
    GHG_Logistics_Monthly.loc[i, 'T_Boats_Per_Distance'] = iferror(lambda: Tug_Boats_CO2e/Tot_Distance,0)
    
# Calculating emissions for Escort_CO2e and storing to dataframe for all months
for i, row in GHG_Escort_Boats.iterrows():
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CO2']),0)
    Diesel = iferror(lambda: float(GHG_Escort_Boats['Long_Escort_Diesel'][i]),0)
    Tot_Passengers = iferror(lambda: float(GHG_Escort_Boats['Long_Escort_Tot_Passengers	'][i]),0)
    Tot_Distance = iferror(lambda: float(GHG_Escort_Boats['Long_Escort_Tot_Distance'][i]),0)
    Long_Escort_CO2e = iferror(lambda: Diesel/1000 * Diesel_Density/1000 * Diesel_CO2,0)
    GHG_Logistics_Monthly.loc[i, 'Long_Escort_CO2e'] = Long_Escort_CO2e
    GHG_Logistics_Monthly.loc[i, 'L_Escort_Per_Passenger'] = iferror(lambda: Long_Escort_CO2e/Tot_Passengers,0)
    GHG_Logistics_Monthly.loc[i, 'L_Escort_Per_Distance'] = iferror(lambda: Long_Escort_CO2e/Tot_Distance,0)
    
    # Calculating emissions for Passenger_Escort_CO2e and storing to dataframe for all months
    Gasoline_Density = iferror(lambda: float(df_Factors['Factor_Value']['Gasoline_Density']),0)
    Petrol_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Petrol_CO2']),0)
    Petrol = iferror(lambda: float(GHG_Escort_Boats['Passenger_Escort_Petrol'][i]),0)
    Tot_Passengers = iferror(lambda: float(GHG_Escort_Boats['Passenger_Escort_Tot_Passengers'][i]),0)
    Tot_Distance = iferror(lambda: float(GHG_Escort_Boats['Passenger_Escort_Tot_Distance'][i]),0)
    Passenger_Escort_CO2e = iferror(lambda: Petrol/1000 * Gasoline_Density/1000 * Petrol_CO2,0)
    GHG_Logistics_Monthly.loc[i, 'Passenger_Escort_CO2e'] = Passenger_Escort_CO2e
    GHG_Logistics_Monthly.loc[i, 'P_Escort_Per_Passenger'] = iferror(lambda: Passenger_Escort_CO2e/Tot_Passengers,0)
    GHG_Logistics_Monthly.loc[i, 'P_Escort_Per_Distance'] = iferror(lambda: Passenger_Escort_CO2e/Tot_Distance,0)
    
    GHG_Logistics_Monthly.loc[i, 'Escort_CO2e'] = Long_Escort_CO2e + Passenger_Escort_CO2e
    
# Calculating emissions for Bny_Fleet_CO2e and storing to dataframe for all months
for i, row in GHG_Bonny_Fleet.iterrows():
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CO2']),0)
    Diesel = iferror(lambda: float(GHG_Bonny_Fleet['Diesel'][i]),0)
    Bny_Fleet_Diesel_CO2e = iferror(lambda: Diesel/1000 * Diesel_Density/1000 * Diesel_CO2,0)
    GHG_Logistics_Monthly.loc[i, 'Bny_Fleet_Diesel_CO2e'] = Bny_Fleet_Diesel_CO2e
    
    # Calculating emissions for Bny_Fleet_Petrol_CO2e and storing to dataframe for all months
    Gasoline_Density = iferror(lambda: float(df_Factors['Factor_Value']['Gasoline_Density']),0)
    Petrol_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Petrol_CO2']),0)
    Petrol = iferror(lambda: float(GHG_Bonny_Fleet['Petrol'][i]),0)
    Bny_Fleet_Petrol_CO2e = iferror(lambda: Petrol/1000 * Gasoline_Density/1000 * Petrol_CO2,0)
    GHG_Logistics_Monthly.loc[i, 'Bny_Fleet_Petrol_CO2e'] = Bny_Fleet_Petrol_CO2e
    
    GHG_Logistics_Monthly.loc[i, 'Bny_Fleet_CO2e'] = Bny_Fleet_Diesel_CO2e + Bny_Fleet_Petrol_CO2e
    
    Tot_Passengers = iferror(lambda: float(GHG_Bonny_Fleet['Tot_Passengers'][i]),0)
    Tot_Distance = iferror(lambda: float(GHG_Bonny_Fleet['Tot_Distance'][i]),0)    
    GHG_Logistics_Monthly.loc[i, 'Bny_Fleet_Per_Passenger'] = iferror(lambda: Bny_Fleet_CO2e/Tot_Passengers,0)
    GHG_Logistics_Monthly.loc[i, 'Bny_Fleet_Per_Distance'] = iferror(lambda: Bny_Fleet_CO2e/Tot_Distance,0)
    
# Calculating emissions for Non_Bny_Fleet_CO2e and storing to dataframe for all months
for i, row in GHG_Bonny_Fleet.iterrows():
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CO2']),0)
    Diesel = iferror(lambda: float(GHG_Non_Bonny_Fleet['Diesel'][i]),0)
    Non_Bny_Fleet_Diesel_CO2e = iferror(lambda: Diesel/1000 * Diesel_Density/1000 * Diesel_CO2,0)
    GHG_Logistics_Monthly.loc[i, 'Non_Bny_Fleet_Diesel_CO2e'] = Non_Bny_Fleet_Diesel_CO2e
    
    # Calculating emissions for Non_Bny_Fleet_Petrol_CO2e and storing to dataframe for all months
    Gasoline_Density = iferror(lambda: float(df_Factors['Factor_Value']['Gasoline_Density']),0)
    Petrol_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Petrol_CO2']),0)
    Petrol = iferror(lambda: float(GHG_Non_Bonny_Fleet['Petrol'][i]),0)
    Non_Bny_Fleet_Petrol_CO2e = iferror(lambda: Petrol/1000 * Gasoline_Density/1000 * Petrol_CO2,0)
    GHG_Logistics_Monthly.loc[i, 'Non_Bny_Fleet_Petrol_CO2e'] = Non_Bny_Fleet_Petrol_CO2e
    
    GHG_Logistics_Monthly.loc[i, 'Non_Bny_Fleet_CO2e'] = Non_Bny_Fleet_Diesel_CO2e + Non_Bny_Fleet_Petrol_CO2e
    
    Tot_Passengers = iferror(lambda: float(GHG_Non_Bonny_Fleet['Tot_Passengers'][i]),0)
    Tot_Distance = iferror(lambda: float(GHG_Non_Bonny_Fleet['Tot_Distance'][i]),0)    
    GHG_Logistics_Monthly.loc[i, 'Non_Bny_Fleet_Per_Passenger'] = iferror(lambda: Non_Bny_Fleet_CO2e/Tot_Passengers,0)
    GHG_Logistics_Monthly.loc[i, 'Non_Bny_Fleet_Per_Distance'] = iferror(lambda: Non_Bny_Fleet_CO2e/Tot_Distance,0)
    
# Posting all GHG entries to database
col_list = GHG_Logistics_Monthly.columns
for i, row in GHG_Logistics_Monthly.iterrows():
    record_date = GHG_Logistics_Monthly.at[i, 'RecordDate']
    if df_GHG_Logistics_Monthly.shape[0] == 0:
        record_found = "NO"
    else:
        for j, row in df_GHG_Logistics_Monthly.iterrows():
            new_date = df_GHG_Logistics_Monthly.at[j, 'RecordDate'] 
            if record_date.month==new_date.month and record_date.year==new_date.year:
                record_found = "YES"
                break
            else:
                record_found = "NO"
    if record_found == "YES":
        cols_vals = ''
        cols_vals = ''
        for col in col_list:
            cols_vals = cols_vals + col + "=" + "'" + str(GHG_Logistics_Monthly.at[i, col]) + "',"
        cols_vals=cols_vals[:-1]
        row_id = str(df_GHG_Logistics_Monthly.at[j, 'id'])
        update_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Logistics_Monthly",cols_vals=cols_vals,row_id = row_id)
    else:
        vals = ''
        cols = ''
        for col in col_list:
            cols = cols + col + ","
            vals = vals + "'" + str(GHG_Logistics_Monthly.at[i, col]) + "',"
        vals=vals[:-1]
        cols=cols[:-1]
        insert_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Logistics_Monthly",cols=cols,vals=vals)
        
# Getting Projects data from Datatbase
GHG_Projects, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Projects"""
        )

# Getting GHG_Projects_Monthly data from Datatbase
df_GHG_Projects_Monthly, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Projects_Monthly"""
        )

# Creating a new dataframe to store GHG output
GHG_Projects_Monthly = pd.DataFrame(columns = ['RecordDate', 'UpdatedDate', 'UpdatedBy', 'Diesel_CO2e', 'Petrol_CO2e', 'Projects_CO2e'])

# Calculating emissions for Diesel_CO2e and storing to dataframe for all months
for i in range(GHG_Projects.shape[0]):
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CO2']),0)
    Diesel = iferror(lambda: float(GHG_Projects['Diesel'][i]),0)
    Diesel_CO2e = iferror(lambda: Diesel/1000 * Diesel_Density/1000 * Diesel_CO2,0)
    GHG_Projects_Monthly.loc[i, 'Diesel_CO2e'] = Diesel_CO2e
    GHG_Projects_Monthly.loc[i, "RecordDate"] = GHG_Projects['RecordDate'][i]
    GHG_Projects_Monthly.loc[i, "UpdatedDate"] = datetime.today().strftime('%Y-%m-%d')
    GHG_Projects_Monthly.loc[i, "UpdatedBy"] = 'Admin'
    
# Calculating emissions for Petrol_CO2e and storing to dataframe for all months
for i in range(GHG_Projects.shape[0]):
    Gasoline_Density = iferror(lambda: float(df_Factors['Factor_Value']['Gasoline_Density']),0)
    Petrol_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Petrol_CO2']),0)
    Petrol = iferror(lambda: float(GHG_Projects['Petrol'][i]),0)
    Petrol_CO2e = iferror(lambda: Petrol/1000 * Gasoline_Density/1000 * Petrol_CO2,0)
    GHG_Projects_Monthly.loc[i, 'Petrol_CO2e'] = Petrol_CO2e
    
# Calculating emissions for Total CO2e and storing to dataframe for all months
for i in range(GHG_Projects.shape[0]):
    Diesel_CO2e = GHG_Projects_Monthly.loc[i, 'Diesel_CO2e']
    Petrol_CO2e = GHG_Projects_Monthly.loc[i, 'Petrol_CO2e']
    GHG_Projects_Monthly.loc[i, 'Projects_CO2e'] = Diesel_CO2e + Petrol_CO2e

# Posting all GHG entries to database
col_list = GHG_Projects_Monthly.columns
for i, row in GHG_Projects_Monthly.iterrows():
    record_date = GHG_Projects_Monthly.at[i, 'RecordDate']
    if df_GHG_Projects_Monthly.shape[0] == 0:
        record_found = "NO"
    else:
        for j, row in df_GHG_Projects_Monthly.iterrows():
            new_date = df_GHG_Projects_Monthly.at[j, 'RecordDate'] 
            if record_date.month==new_date.month and record_date.year==new_date.year:
                record_found = "YES"
                break
            else:
                record_found = "NO"
    if record_found == "YES":
        cols_vals = ''
        cols_vals = ''
        for col in col_list:
            cols_vals = cols_vals + col + "=" + "'" + str(GHG_Projects_Monthly.at[i, col]) + "',"
        cols_vals=cols_vals[:-1]
        row_id = str(df_GHG_Projects_Monthly.at[j, 'id'])
        update_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Projects_Monthly",cols_vals=cols_vals,row_id = row_id)
    else:
        vals = ''
        cols = ''
        for col in col_list:
            cols = cols + col + ","
            vals = vals + "'" + str(GHG_Projects_Monthly.at[i, col]) + "',"
        vals=vals[:-1]
        cols=cols[:-1]
        insert_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Projects_Monthly",cols=cols,vals=vals)


# Getting SPDC data from Datatbase
GHG_SPDC, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_SPDC"""
        )

# Getting TEPNG data from Datatbase
GHG_TEPNG, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_TEPNG"""
        )

# Getting NAOC data from Datatbase
GHG_NAOC, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_NAOC"""
        )

# Getting GHG_Upstream data from Datatbase
df_GHG_Upstream, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Upstream"""
        )

# Creating a new dataframe to store GHG output
GHG_Upstream = pd.DataFrame(columns = ['RecordDate', 'UpdatedDate', 'UpdatedBy', 'SPDC_CO2', 'SPDC_N2O',
       'SPDC_CH4', 'SPDC_GTS_CH4', 'SPDC_CO2e', 'TEPNG_CO2', 'TEPNG_N2O',
       'TEPNG_CH4', 'TEPNG_GTS_CH4', 'TEPNG_CO2e', 'NAOC_CO2', 'NAOC_N2O',
       'NAOC_CH4', 'NAOC_GTS_CH4', 'NAOC_CO2e', 'SPDC_GTS_CO2e',
       'TEPNG_GTS_CO2e', 'NAOC_GTS_CO2e'])

# Calculating emissions for SPDC_CO2 and storing to dataframe for all months
for i in range(GHG_SPDC.shape[0]):
    Combustion_CO2 = iferror(lambda: float(GHG_SPDC['Combustion_CO2'][i]),0)
    Flaring_CO2 = iferror(lambda: float(GHG_SPDC['Flaring_CO2'][i]),0)
    Fugitives_CO2 = iferror(lambda: float(GHG_SPDC['Fugitives_CO2'][i]),0)
    Venting_CO2 = iferror(lambda: float(GHG_SPDC['Venting_CO2'][i]),0)
    Indirect_CO2 = iferror(lambda: float(GHG_SPDC['Indirect_CO2'][i]),0)
    GHG_Upstream.loc[i, 'SPDC_CO2'] = Combustion_CO2+Flaring_CO2+Fugitives_CO2+Indirect_CO2
    GHG_Upstream.loc[i, "RecordDate"] = GHG_SPDC['RecordDate'][i]
    GHG_Upstream.loc[i, "UpdatedDate"] = datetime.today().strftime('%Y-%m-%d')
    GHG_Upstream.loc[i, "UpdatedBy"] = 'Admin'

# Calculating emissions for SPDC_N2O and storing to dataframe for all months
for i in range(GHG_SPDC.shape[0]):
    Combustion_N2O = iferror(lambda: float(GHG_SPDC['Combustion_N2O'][i]),0)
    Flaring_N2O = iferror(lambda: float(GHG_SPDC['Flaring_N2O'][i]),0)
    GHG_Upstream.loc[i, 'SPDC_N2O'] = Combustion_N2O+Flaring_N2O

# Calculating emissions for SPDC_CH4 and storing to dataframe for all months
for i in range(GHG_SPDC.shape[0]):
    Fugitives_CH4 = iferror(lambda: float(GHG_SPDC['Fugitives_CH4'][i]),0)
    Venting_CH4 = iferror(lambda: float(GHG_SPDC['Venting_CH4'][i]),0)
    GHG_Upstream.loc[i, 'SPDC_CH4'] = Fugitives_CH4+Venting_CH4

# Calculating emissions for SPDC_GTS_CH4 and storing to dataframe for all months
for i in range(GHG_SPDC.shape[0]):
    GTS_Fugitives_CH4 = iferror(lambda: float(GHG_SPDC['GTS_Fugitives_CH4'][i]),0)
    GTS_Venting_CH4 = iferror(lambda: float(GHG_SPDC['GTS_Venting_CH4'][i]),0)
    GHG_Upstream.loc[i, 'SPDC_GTS_CH4'] = GTS_Fugitives_CH4+GTS_Venting_CH4

# Calculating emissions for SPDC_CO2e, SPDC_GTS_CO2e and storing to dataframe for all months
for i in range(GHG_Upstream.shape[0]):
    GWP100_N2O = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_N2O']),0)
    GWP100_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)
    SPDC_CO2 = iferror(lambda: float(GHG_Upstream['SPDC_CO2'][i]),0)
    SPDC_N2O = iferror(lambda: float(GHG_Upstream['SPDC_N2O'][i]),0)
    SPDC_CH4 = iferror(lambda: float(GHG_Upstream['SPDC_CH4'][i]),0)
    SPDC_GTS_CH4 = iferror(lambda: float(GHG_Upstream['SPDC_GTS_CH4'][i]),0)
    
    GHG_Upstream.loc[i, 'SPDC_CO2e'] = SPDC_CO2+SPDC_N2O*GWP100_N2O+SPDC_CH4*GWP100_CH4+SPDC_GTS_CH4*GWP100_CH4
    GHG_Upstream.loc[i, 'SPDC_GTS_CO2e'] = SPDC_GTS_CH4*GWP100_CH4

# Calculating emissions for TEPNG_CO2 and storing to dataframe for all months
for i in range(GHG_TEPNG.shape[0]):
    Combustion_CO2 = iferror(lambda: float(GHG_TEPNG['Combustion_CO2'][i]),0)
    Flaring_CO2 = iferror(lambda: float(GHG_TEPNG['Flaring_CO2'][i]),0)
    Fugitives_CO2 = iferror(lambda: float(GHG_TEPNG['Fugitives_CO2'][i]),0)
    Venting_CO2 = iferror(lambda: float(GHG_TEPNG['Venting_CO2'][i]),0)
    Indirect_CO2 = iferror(lambda: float(GHG_TEPNG['Indirect_CO2'][i]),0)
    GHG_Upstream.loc[i, 'TEPNG_CO2'] = Combustion_CO2+Flaring_CO2+Fugitives_CO2+Indirect_CO2

# Calculating emissions for TEPNG_N2O and storing to dataframe for all months
for i in range(GHG_TEPNG.shape[0]):
    Combustion_N2O = iferror(lambda: float(GHG_TEPNG['Combustion_N2O'][i]),0)
    Flaring_N2O = iferror(lambda: float(GHG_TEPNG['Flaring_N2O'][i]),0)
    GHG_Upstream.loc[i, 'TEPNG_N2O'] = Combustion_N2O+Flaring_N2O

# Calculating emissions for TEPNG_CH4 and storing to dataframe for all months
for i in range(GHG_TEPNG.shape[0]):
    Fugitives_CH4 = iferror(lambda: float(GHG_TEPNG['Fugitives_CH4'][i]),0)
    Venting_CH4 = iferror(lambda: float(GHG_TEPNG['Venting_CH4'][i]),0)
    GHG_Upstream.loc[i, 'TEPNG_CH4'] = Fugitives_CH4+Venting_CH4

# Calculating emissions for TEPNG_GTS_CH4 and storing to dataframe for all months
for i in range(GHG_TEPNG.shape[0]):
    GTS_Fugitives_CH4 = iferror(lambda: float(GHG_TEPNG['GTS_Fugitives_CH4'][i]),0)
    GTS_Venting_CH4 = iferror(lambda: float(GHG_TEPNG['GTS_Venting_CH4'][i]),0)
    GHG_Upstream.loc[i, 'TEPNG_GTS_CH4'] = GTS_Fugitives_CH4+GTS_Venting_CH4

# Calculating emissions for TEPNG_CO2e, TEPNG_GTS_CO2e and storing to dataframe for all months
for i in range(GHG_Upstream.shape[0]):
    GWP100_N2O = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_N2O']),0)
    GWP100_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)
    TEPNG_CO2 = iferror(lambda: float(GHG_Upstream['TEPNG_CO2'][i]),0)
    TEPNG_N2O = iferror(lambda: float(GHG_Upstream['TEPNG_N2O'][i]),0)
    TEPNG_CH4 = iferror(lambda: float(GHG_Upstream['TEPNG_CH4'][i]),0)
    TEPNG_GTS_CH4 = iferror(lambda: float(GHG_Upstream['TEPNG_GTS_CH4'][i]),0)
    
    GHG_Upstream.loc[i, 'TEPNG_CO2e'] = TEPNG_CO2+TEPNG_N2O*GWP100_N2O+TEPNG_CH4*GWP100_CH4+TEPNG_GTS_CH4*GWP100_CH4
    GHG_Upstream.loc[i, 'TEPNG_GTS_CO2e'] = TEPNG_GTS_CH4*GWP100_CH4

# Calculating emissions for NAOC_CO2 and storing to dataframe for all months
for i in range(GHG_NAOC.shape[0]):
    Combustion_CO2 = iferror(lambda: float(GHG_NAOC['Combustion_CO2'][i]),0)
    Flaring_CO2 = iferror(lambda: float(GHG_NAOC['Flaring_CO2'][i]),0)
    Fugitives_CO2 = iferror(lambda: float(GHG_NAOC['Fugitives_CO2'][i]),0)
    Venting_CO2 = iferror(lambda: float(GHG_NAOC['Venting_CO2'][i]),0)
    Indirect_CO2 = iferror(lambda: float(GHG_NAOC['Indirect_CO2'][i]),0)
    GHG_Upstream.loc[i, 'NAOC_CO2'] = Combustion_CO2+Flaring_CO2+Fugitives_CO2+Indirect_CO2

# Calculating emissions for NAOC_N2O and storing to dataframe for all months
for i in range(GHG_NAOC.shape[0]):
    Combustion_N2O = iferror(lambda: float(GHG_NAOC['Combustion_N2O'][i]),0)
    Flaring_N2O = iferror(lambda: float(GHG_NAOC['Flaring_N2O'][i]),0)
    GHG_Upstream.loc[i, 'NAOC_N2O'] = Combustion_N2O+Flaring_N2O

# Calculating emissions for NAOC_CH4 and storing to dataframe for all months
for i in range(GHG_NAOC.shape[0]):
    Fugitives_CH4 = iferror(lambda: float(GHG_NAOC['Fugitives_CH4'][i]),0)
    Venting_CH4 = iferror(lambda: float(GHG_NAOC['Venting_CH4'][i]),0)
    GHG_Upstream.loc[i, 'NAOC_CH4'] = Fugitives_CH4+Venting_CH4

# Calculating emissions for NAOC_GTS_CH4 and storing to dataframe for all months
for i in range(GHG_NAOC.shape[0]):
    GTS_Fugitives_CH4 = iferror(lambda: float(GHG_NAOC['GTS_Fugitives_CH4'][i]),0)
    GTS_Venting_CH4 = iferror(lambda: float(GHG_NAOC['GTS_Venting_CH4'][i]),0)
    GHG_Upstream.loc[i, 'NAOC_GTS_CH4'] = GTS_Fugitives_CH4+GTS_Venting_CH4

# Calculating emissions for NAOC_CO2e, TEPNG_GTS_CO2e and storing to dataframe for all months
for i in range(GHG_Upstream.shape[0]):
    GWP100_N2O = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_N2O']),0)
    GWP100_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)
    NAOC_CO2 = iferror(lambda: float(GHG_Upstream['NAOC_CO2'][i]),0)
    NAOC_N2O = iferror(lambda: float(GHG_Upstream['NAOC_N2O'][i]),0)
    NAOC_CH4 = iferror(lambda: float(GHG_Upstream['NAOC_CH4'][i]),0)
    NAOC_GTS_CH4 = iferror(lambda: float(GHG_Upstream['NAOC_GTS_CH4'][i]),0)
    
    GHG_Upstream.loc[i, 'NAOC_CO2e'] = NAOC_CO2+NAOC_N2O*GWP100_N2O+NAOC_CH4*GWP100_CH4+NAOC_GTS_CH4*GWP100_CH4
    GHG_Upstream.loc[i, 'NAOC_GTS_CO2e'] = NAOC_GTS_CH4*GWP100_CH4

# Posting all GHG entries to database
col_list = GHG_Upstream.columns
for i, row in GHG_Upstream.iterrows():
    record_date = GHG_Upstream.at[i, 'RecordDate']
    if df_GHG_Upstream.shape[0] == 0:
        record_found = "NO"
    else:
        for j, row in df_GHG_Upstream.iterrows():
            new_date = df_GHG_Upstream.at[j, 'RecordDate'] 
            if record_date.month==new_date.month and record_date.year==new_date.year:
                record_found = "YES"
                break
            else:
                record_found = "NO"
    if record_found == "YES":
        cols_vals = ''
        cols_vals = ''
        for col in col_list:
            cols_vals = cols_vals + col + "=" + "'" + str(GHG_Upstream.at[i, col]) + "',"
        cols_vals=cols_vals[:-1]
        row_id = str(df_GHG_Upstream.at[j, 'id'])
        update_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Upstream",cols_vals=cols_vals,row_id = row_id)
    else:
        vals = ''
        cols = ''
        for col in col_list:
            cols = cols + col + ","
            vals = vals + "'" + str(GHG_Upstream.at[i, col]) + "',"
        vals=vals[:-1]
        cols=cols[:-1]
        insert_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Upstream",cols=cols,vals=vals)



# Getting Plant data from Datatbase
GHG_Plant, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Plant"""
        )

# Getting GHG_Plant_Monthly data from Datatbase
df_GHG_Plant_Monthly, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Plant_Monthly"""
        )

# Creating a new dataframe to store GHG output
GHG_Plant_Monthly = pd.DataFrame(columns = ['RecordDate', 'UpdatedDate', 'UpdatedBy', 'Waste_CO2',
       'Waste_N2O', 'Waste_CH4', 'Waste_CO2e', 'Mobile_CO2', 'Mobile_N2O',
       'Mobile_CH4', 'Mobile_CO2e', 'Fugitives_CH4', 'Fugitives_CO2e'])

# Calculating emissions for Waste_CO2 and storing to dataframe for all months
for i in range(GHG_Plant.shape[0]):
    Fired_Heaters_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_CO2']),0)
    Fuel_Gas = iferror(lambda: float(GHG_Plant['Fuel_Gas'][i]),0)
    Waste_CO2 = iferror(lambda: Fuel_Gas * Fired_Heaters_CO2,0)
    GHG_Plant_Monthly.loc[i, 'Waste_CO2'] = Waste_CO2
    GHG_Plant_Monthly.loc[i, "RecordDate"] = GHG_Plant['RecordDate'][i]
    GHG_Plant_Monthly.loc[i, "UpdatedDate"] = datetime.today().strftime('%Y-%m-%d')
    GHG_Plant_Monthly.loc[i, "UpdatedBy"] = 'Admin'

# Calculating emissions for Waste_N2O and storing to dataframe for all months
for i in range(GHG_Plant.shape[0]):
    Fired_Heaters_N2O = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_N2O']),0)
    Fuel_Gas = iferror(lambda: float(GHG_Plant['Fuel_Gas'][i]),0)
    Waste_N2O = iferror(lambda: Fuel_Gas * Fired_Heaters_N2O,0)
    GHG_Plant_Monthly.loc[i, 'Waste_N2O'] = Waste_N2O

# Calculating emissions for Waste_CH4 and storing to dataframe for all months
for i in range(GHG_Plant.shape[0]):
    Fired_Heaters_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_CH4']),0)
    Fuel_Gas = iferror(lambda: float(GHG_Plant['Fuel_Gas'][i]),0)
    Waste_CH4 = iferror(lambda: Fuel_Gas * Fired_Heaters_CH4,0)
    GHG_Plant_Monthly.loc[i, 'Waste_CH4'] = Waste_CH4

# Calculating emissions for Waste_CO2e and storing to dataframe for all months
for i in range(GHG_Plant_Monthly.shape[0]):
    GWP100_N2O = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_N2O']),0)
    GWP100_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)    
    Waste_CO2 = iferror(lambda: float(GHG_Plant_Monthly['Waste_CO2'][i]),0)
    Waste_N2O = iferror(lambda: float(GHG_Plant_Monthly['Waste_N2O'][i]),0)
    Waste_CH4 = iferror(lambda: float(GHG_Plant_Monthly['Waste_CH4'][i]),0)
    
    Waste_CO2e = iferror(lambda: Waste_CO2+(Waste_N2O*GWP100_N2O)+(Waste_CH4*GWP100_CH4),0)
    GHG_Plant_Monthly.loc[i, 'Waste_CO2e'] = Waste_CO2e

# Calculating emissions for Mobile_CO2 and storing to dataframe for all months
for i in range(GHG_Plant.shape[0]):
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CO2']),0)
    Diesel = iferror(lambda: float(GHG_Plant['Diesel'][i]),0)
    Mobile_CO2 = iferror(lambda: Diesel/1000 * Diesel_Density/1000 * Diesel_CO2,0)
    GHG_Plant_Monthly.loc[i, 'Mobile_CO2'] = Mobile_CO2

# Calculating emissions for Mobile_N2O and storing to dataframe for all months
for i in range(GHG_Plant.shape[0]):
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_N2O = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_N2O']),0)
    Diesel = iferror(lambda: float(GHG_Plant['Diesel'][i]),0)
    Mobile_N2O = iferror(lambda: Diesel/1000 * Diesel_Density/1000 * Diesel_N2O,0)
    GHG_Plant_Monthly.loc[i, 'Mobile_N2O'] = Mobile_N2O

# Calculating emissions for Mobile_CH4 and storing to dataframe for all months
for i in range(GHG_Plant.shape[0]):
    Diesel_Density = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_Density']),0)
    Diesel_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['Diesel_CH4']),0)
    Diesel = iferror(lambda: float(GHG_Plant['Diesel'][i]),0)
    Mobile_CH4 = iferror(lambda: Diesel/1000 * Diesel_Density/1000 * Diesel_CH4,0)
    GHG_Plant_Monthly.loc[i, 'Mobile_CH4'] = Mobile_CH4

# Calculating emissions for Mobile_CO2e and storing to dataframe for all months
for i in range(GHG_Plant_Monthly.shape[0]):
    GWP100_N2O = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_N2O']),0)
    GWP100_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)    
    Mobile_CO2 = iferror(lambda: float(GHG_Plant_Monthly['Mobile_CO2'][i]),0)
    Mobile_N2O = iferror(lambda: float(GHG_Plant_Monthly['Mobile_N2O'][i]),0)
    Mobile_CH4 = iferror(lambda: float(GHG_Plant_Monthly['Mobile_CH4'][i]),0)
    
    Mobile_CO2e = iferror(lambda: Mobile_CO2+(Mobile_N2O*GWP100_N2O)+(Mobile_CH4*GWP100_CH4),0)
    GHG_Plant_Monthly.loc[i, 'Mobile_CO2e'] = Mobile_CO2e

# Calculating emissions for Fugitives_CH4 and storing to dataframe for all months
for i in range(GHG_Plant.shape[0]):
    Fugitives_CH4 = iferror(lambda: float(GHG_Plant['Fugitives_CH4'][i]),0)
    GHG_Plant_Monthly.loc[i, 'Fugitives_CH4'] = Fugitives_CH4

# Calculating emissions for Fugitives_CO2e and storing to dataframe for all months
for i in range(GHG_Plant_Monthly.shape[0]):
    GWP100_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)
    Fugitives_CH4 = iferror(lambda: float(GHG_Plant_Monthly['Fugitives_CH4'][i]),0)
    Fugitives_CO2e = Fugitives_CH4 * GWP100_CH4
    GHG_Plant_Monthly.loc[i, 'Fugitives_CO2e'] = Fugitives_CO2e

# Posting all GHG entries to database
col_list = GHG_Plant_Monthly.columns
for i, row in GHG_Plant_Monthly.iterrows():
    record_date = GHG_Plant_Monthly.at[i, 'RecordDate']
    if df_GHG_Plant_Monthly.shape[0] == 0:
        record_found = "NO"
    else:
        for j, row in df_GHG_Plant_Monthly.iterrows():
            new_date = df_GHG_Plant_Monthly.at[j, 'RecordDate'] 
            if record_date.month==new_date.month and record_date.year==new_date.year:
                record_found = "YES"
                break
            else:
                record_found = "NO"
    if record_found == "YES":
        cols_vals = ''
        cols_vals = ''
        for col in col_list:
            cols_vals = cols_vals + col + "=" + "'" + str(GHG_Plant_Monthly.at[i, col]) + "',"
        cols_vals=cols_vals[:-1]
        row_id = str(df_GHG_Plant_Monthly.at[j, 'id'])
        update_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Plant_Monthly",cols_vals=cols_vals,row_id = row_id)
    else:
        vals = ''
        cols = ''
        for col in col_list:
            cols = cols + col + ","
            vals = vals + "'" + str(GHG_Plant_Monthly.at[i, col]) + "',"
        vals=vals[:-1]
        cols=cols[:-1]
        insert_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Plant_Monthly",cols=cols,vals=vals)


def update_SQL2(server,db,user,pwd,tbl,cols_vals,Vessel_Name,record_date):
    conn = pyodbc.connect(Driver="{SQL Server}", Server=server, Database=db, Trusted_Connection="NO", User=user, Password=pwd)
    cursor = conn.cursor()
    cursor.execute("""UPDATE """ + tbl + """ SET """ + cols_vals + """ WHERE Vessel_Name = """ + Vessel_Name + """ AND RecordDate = """ + record_date + """;""")
    conn.commit()

# Getting Shipping_Fleet_Mgr data from Datatbase
Shipping_Fleet_Mgr, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.Shipping_Fleet_Mgr"""
        )
Shipping_Fleet_Mgr = Shipping_Fleet_Mgr.set_index('Vessel_Name')

# Getting GHG_Shipping_Monthly data from Datatbase
GHG_Shipping_Monthly, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Shipping_Monthly"""
        )

# Getting GHG_Shipping data from Datatbase
GHG_Shipping, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Shipping"""
        )

this_year = str(datetime.today().year)
GHG_Shipping = GHG_Shipping[GHG_Shipping['RecordDate'].astype(str).str.contains(this_year)]
GHG_Shipping = GHG_Shipping.reset_index()
GHG_Shipping.drop(['index'], axis=1, inplace=True)

for i, row in GHG_Shipping.iterrows():
    GHG_Shipping.at[i, 'Month'] = GHG_Shipping.at[i, 'RecordDate'].month
    GHG_Shipping.at[i, 'Year'] = GHG_Shipping.at[i, 'RecordDate'].year

Vessels = GHG_Shipping['Vessel_Name'].unique()
Years = GHG_Shipping['Year'].unique()
for year in Years:
    for vessel in Vessels:
        curr_df = GHG_Shipping.loc[(GHG_Shipping['Vessel_Name'] == vessel) & (GHG_Shipping['Year'] == year)]
        curr_df = curr_df.sort_values(by=['RecordDate'], ascending=False)
        curr_df = curr_df.reset_index()
        curr_df.drop(['index'], axis=1, inplace=True)
        val_cols = ['Tot_Distance', 'Tot_MGO', 'Tot_HFO', 'Tot_LFO', 'Tot_LNG']
        for k, row in curr_df.iterrows():
            if curr_df.at[k, 'Month'] != 1:
                for col in val_cols:
                    curr_data = iferror(lambda: float(curr_df.at[k, col]) - float(curr_df.at[k+1, col]),float(curr_df.at[k, col]))
                    for j, row in GHG_Shipping.iterrows():
                        if curr_df.at[k, 'id'] == GHG_Shipping.at[j, 'id']:
                            GHG_Shipping.at[j, col] = curr_data


GHG_Shipping.drop(['Month','Year'], axis=1, inplace=True)

# Calculating emissions and storing to dataframe for all months
for i, row in GHG_Shipping.iterrows():
    Vessel_Name = GHG_Shipping.at[i, 'Vessel_Name']
    GHG_Shipping.at[i, 'Fleet_Mgr'] = iferror(lambda: Shipping_Fleet_Mgr['Fleet_Mgr'][Vessel_Name],'Others')
    DWT = Vessel_Name = iferror(lambda: float(GHG_Shipping.at[i, 'DWT']),0)
    Tot_Distance = Vessel_Name = iferror(lambda: float(GHG_Shipping.at[i, 'Tot_Distance']),0)
    Tot_MGO = Vessel_Name = iferror(lambda: float(GHG_Shipping.at[i, 'Tot_MGO']),0)
    Tot_HFO = Vessel_Name = iferror(lambda: float(GHG_Shipping.at[i, 'Tot_HFO']),0)
    Tot_LFO = Vessel_Name = iferror(lambda: float(GHG_Shipping.at[i, 'Tot_LFO']),0)
    Tot_LNG = Vessel_Name = iferror(lambda: float(GHG_Shipping.at[i, 'Tot_LNG']),0)
    
    MGO_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['MGO_CO2']),0)
    HFO_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['HFO_CO2']),0)
    LNG_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['LNG_CO2']),0)
    LFO_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['LFO_CO2']),0)
    MDO_Aux_Eng_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['MDO_Aux_Eng_CH4']),0)
    HFO_Boiler_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['HFO_Boiler_CH4']),0)
    LNG_Boiler_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['LNG_Boiler_CH4']),0)
    MDO_Aux_Eng_N2O = iferror(lambda: float(df_Factors['Factor_Value']['MDO_Aux_Eng_N2O']),0)
    HFO_Boiler_N2O = iferror(lambda: float(df_Factors['Factor_Value']['HFO_Boiler_N2O']),0)
    LNG_Boiler_N2O = iferror(lambda: float(df_Factors['Factor_Value']['LNG_Boiler_N2O']),0)
    GWP100_N2O = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_N2O']),0)
    GWP100_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)
    
    CO2 = iferror(lambda: float((Tot_MGO*MGO_CO2)+(Tot_HFO*HFO_CO2)+(Tot_LFO*LFO_CO2)+(Tot_LNG*LNG_CO2)),0)
    N2O = iferror(lambda: float((Tot_MGO*MDO_Aux_Eng_N2O)+(Tot_HFO*HFO_Boiler_N2O)+(Tot_LFO*HFO_Boiler_N2O)+(Tot_LNG*LNG_Boiler_N2O)),0)
    CH4 = iferror(lambda: float((Tot_MGO*MDO_Aux_Eng_CH4)+(Tot_HFO*HFO_Boiler_CH4)+(Tot_LFO*HFO_Boiler_CH4)+(Tot_LNG*LNG_Boiler_CH4)),0)
    CII = iferror(lambda: float((CO2/DWT)/Tot_Distance*1000000),0)
    CO2e = CO2+N2O*GWP100_N2O+CH4*GWP100_CH4
    
    GHG_Shipping.at[i, 'CO2'] = CO2
    GHG_Shipping.at[i, 'N2O'] = N2O
    GHG_Shipping.at[i, 'CH4'] = CH4
    GHG_Shipping.at[i, 'CII'] = CII
    GHG_Shipping.at[i, 'CO2e'] = CO2e

GHG_Shipping.drop(['id','DWT',
       'Tot_Distance', 'Tot_MGO', 'Tot_HFO', 'Tot_LFO', 'Tot_LNG'], axis=1, inplace=True)

# Posting all GHG entries to database
col_list = GHG_Shipping.columns
for i, row in GHG_Shipping.iterrows():
    record_date = GHG_Shipping.at[i, 'RecordDate']
    Vessel_Name = GHG_Shipping.at[i, 'Vessel_Name']
    if GHG_Shipping_Monthly.shape[0] == 0:
        record_found = "NO"
    else:
        for j, row in GHG_Shipping_Monthly.iterrows():
            new_date = GHG_Shipping_Monthly.at[j, 'RecordDate']
            new_vessel = GHG_Shipping_Monthly.at[j, 'Vessel_Name']
            if record_date.month==new_date.month and record_date.year==new_date.year and Vessel_Name == new_vessel:
                record_found = "YES"
                break
            else:
                record_found = "NO"
    if record_found == "YES":
        cols_vals = ''
        cols_vals = ''
        for col in col_list:
            cols_vals = cols_vals + col + "=" + "'" + str(GHG_Shipping.at[i, col]) + "',"
        cols_vals=cols_vals[:-1]
        Vessel_Name = "'" + Vessel_Name + "'"
        record_date = "'" + str(record_date) + "'"
        update_SQL2(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Shipping_Monthly",cols_vals=cols_vals,Vessel_Name=Vessel_Name,record_date=record_date)
    else:
        vals = ''
        cols = ''
        for col in col_list:
            cols = cols + col + ","
            vals = vals + "'" + str(GHG_Shipping.at[i, col]) + "',"
        vals=vals[:-1]
        cols=cols[:-1]
        insert_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Shipping_Monthly",cols=cols,vals=vals)

# Getting Flaring data from Datatbase
GHG_Flaring, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Flaring"""
        )

# Getting GHG_Flaring_Weekly data from Datatbase
df_GHG_Flaring_Weekly, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Flaring_Weekly"""
        )

# Creating a new dataframe to store GHG output
GHG_Flaring_Weekly = pd.DataFrame(columns = ['RecordDate', 'UpdatedDate', 'UpdatedBy', 'Flare_CO2',
       'Flare_N2O', 'Flare_CH4', 'Flare_CO2e'])

# Calculating emissions for Flare_CO2 and storing to dataframe for all months
for i in range(GHG_Flaring.shape[0]):
    F_CO2 = iferror(lambda: float(df_Factors['Factor_Value']['Flare_CO2']),0)
    Gas_Flared = iferror(lambda: float(GHG_Flaring['Gas_Flared'][i]),0)
    Flare_CO2 = iferror(lambda: Gas_Flared * F_CO2,0)
    GHG_Flaring_Weekly.loc[i, 'Flare_CO2'] = Flare_CO2
    GHG_Flaring_Weekly.loc[i, "RecordDate"] = GHG_Flaring['RecordDate'][i]
    GHG_Flaring_Weekly.loc[i, "UpdatedDate"] = datetime.today().strftime('%Y-%m-%d')
    GHG_Flaring_Weekly.loc[i, "UpdatedBy"] = 'Admin'

# Calculating emissions for Flare_N2O and storing to dataframe for all months
for i in range(GHG_Flaring.shape[0]):
    F_N2O = iferror(lambda: float(df_Factors['Factor_Value']['Flare_N2O']),0)
    Gas_Flared = iferror(lambda: float(GHG_Flaring['Gas_Flared'][i]),0)
    Flare_N2O = iferror(lambda: Gas_Flared * F_N2O,0)
    GHG_Flaring_Weekly.loc[i, 'Flare_N2O'] = Flare_N2O

# Calculating emissions for Flare_CH4 and storing to dataframe for all months
for i in range(GHG_Flaring.shape[0]):
    F_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['Flare_CH4']),0)
    Gas_Flared = iferror(lambda: float(GHG_Flaring['Gas_Flared'][i]),0)
    Flare_CH4 = iferror(lambda: Gas_Flared * F_CH4,0)
    GHG_Flaring_Weekly.loc[i, 'Flare_CH4'] = Flare_CH4

# Calculating emissions for Flare_CO2e and storing to dataframe for all months
for i in range(GHG_Flaring_Weekly.shape[0]):
    GWP100_N2O = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_N2O']),0)
    GWP100_CH4 = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)    
    Flare_CO2 = iferror(lambda: float(GHG_Flaring_Weekly['Flare_CO2'][i]),0)
    Flare_N2O = iferror(lambda: float(GHG_Flaring_Weekly['Flare_N2O'][i]),0)
    Flare_CH4 = iferror(lambda: float(GHG_Flaring_Weekly['Flare_CH4'][i]),0)
    
    Flare_CO2e = iferror(lambda: Flare_CO2+(Flare_N2O*GWP100_N2O)+(Flare_CH4*GWP100_CH4),0)
    GHG_Flaring_Weekly.loc[i, 'Flare_CO2e'] = Flare_CO2e

# Posting all GHG entries to database
col_list = GHG_Flaring_Weekly.columns
for i, row in GHG_Flaring_Weekly.iterrows():
    record_date = GHG_Flaring_Weekly.at[i, 'RecordDate']
    if df_GHG_Flaring_Weekly.shape[0] == 0:
        record_found = "NO"
    else:
        for j, row in df_GHG_Flaring_Weekly.iterrows():
            new_date = df_GHG_Flaring_Weekly.at[j, 'RecordDate'] 
            if record_date.month==new_date.month and record_date.year==new_date.year:
                record_found = "YES"
                break
            else:
                record_found = "NO"
    if record_found == "YES":
        cols_vals = ''
        cols_vals = ''
        for col in col_list:
            cols_vals = cols_vals + col + "=" + "'" + str(GHG_Flaring_Weekly.at[i, col]) + "',"
        cols_vals=cols_vals[:-1]
        row_id = str(df_GHG_Flaring_Weekly.at[j, 'id'])
        update_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Flaring_Weekly",cols_vals=cols_vals,row_id = row_id)
    else:
        vals = ''
        cols = ''
        for col in col_list:
            cols = cols + col + ","
            vals = vals + "'" + str(GHG_Flaring_Weekly.at[i, col]) + "',"
        vals=vals[:-1]
        cols=cols[:-1]
        insert_SQL(server="BNY-S-560",db="dataEntryDB",user="Abimbola.Salami",pwd="NLNG@3070",tbl="GHG_Flaring_Weekly",cols=cols,vals=vals)