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

# Class OOP
class PI(object):
    #initializing PI server
    def __init__(self):
        pi_server = 'bny-s-103'
        try:
            self.pi_srv = Dispatch('PISDK.PISDK').Servers(pi_server)
            self.pi_time = Dispatch('PITimeServer.PITime')
            self.pi_time_intervals = Dispatch('PITimeServer.TimeIntervals')
            self.pi_time_format = Dispatch('PITimeServer.PITimeFormat')
        except Exception as e:
            raise ValueError("unable to contact pi_server : " + str(pi_server) + " " + str(e))
            
               
    def get_data(self, tags: str, days=0, hours=0, mins=1):
        data = pd.DataFrame()
        try:
            tag = self.pi_srv.PIPoints(tags)
            time_now = datetime.today() + timedelta(hours=1, minutes=0)
            time_minus_9m = time_now - timedelta(days=days, hours=days, minutes=mins)
            time_now.strftime("%Y-%m-%d %H:%M:%S")
            time_minus_9m.strftime("%Y-%m-%d %H:%M:%S")
            pi_values = tag.Data.InterpolatedValues2(time_minus_9m, time_now, '60s', asynchStatus=None)
            #pi_values = tag.Data.InterpolatedValues2("24-08-2020 10:27:00", "24-08-2020 10:35:00", '1m', asynchStatus=None)
            data, success, error = self.to_df(pi_values, tags)
        except Exception as e:
            success = False
            error = " unable to fetch pi data" + str(e)
        return data
        
        
    def to_df(self, pi_values: list, col_name: str = 'value'):
        """
        Converts a list of PI-value objects to a pandas data fram pye
        :param pi_values: (list): List of PI-value objects
        :param col_name: (str): desired name of the pandas df column
        :return: data frame: index=datetime, column=list_of_values, success status and error logs
        """
        success = True
        error = ""
        data = pd.DataFrame()
        try:
            values = []
            date_time = []
            for v in pi_values:
                try:
                    # values.append(float(v.Value))
                    values.append(str(v.Value))
                    dt, success, error1 = self.epoch_to_dt(v.TimeStamp)
                    if success:
                        date_time.append(dt)
                    else:
                        error += error1
                except Exception as e:
                    error += str(e)
                    pass
            data = pd.DataFrame({'Timestamp': date_time, col_name: values})
            #data = data.set_index('Timestamp')
        except Exception as e:
            success = False
            error = "Unable to convert pi data to pandas data frame:" + str(e)
        return data, success, error
    
    
    @staticmethod
    def epoch_to_dt(timestamp: float):
        """
        Convert epoch to human readable date
        :param timestamp: (float): Unix epoch timestamp i.e. '1508227058.0'
        :return: (datetime object) , success status, error logs
        """
        dt = None
        try:
            dt = datetime.fromtimestamp(timestamp)
            success = True
            error = ""
        except Exception as e:
            success = False
            error = "Failed to Convert Time Stamp " + str(e)
        finally:
            return dt, success, error

# Function to merge data from multiple tags
def merge_tag_data(tag_list, class_instance, round_time='min', days=1, mins=0):
    " merge tag readings to one df"
    dfs = [(class_instance.get_data(i, days=days, mins=mins)).set_index('Timestamp') for i in tag_list]
    idx =(dfs[0].index).round(round_time) # round down to nearest minute
    dfs = (df.set_index(idx) for df in dfs)
    df_merge = pd.concat(list(dfs), axis=1)
    return df_merge

def tag_list(kpi, trains):
    tags = []
    for kpi_tags in kpi:
        for train_num in trains:
            tags = tags + [
                str(train_num) + kpi_tags.split(':')[0] + ':' + str(train_num) + kpi_tags.split(':')[1]
                ]
    return tags

def iferror(success, failure, *exceptions):
    try:
        return success()
    except Exception as e:
        return failure    

# Defining the various tag elements required
acid_removal_d = ['V1101_1:11FRQ003.MEAS', 'V1101_1:11Q750HCO2.PNT', 'V1101_1:11Q750HMW.PNT']
acid_removal_e = ['V1101_1:11FQ003.MEAS','V1101_3:11Q751.PNT','V1101_3:11Q758.PNT']
area_d = ['V1413_1:14FQ116.MEAS','V1435_1:14FQ117.MEAS','F1211_1:12F008.PNT','F4110_2:41FR028.PNT']
area_e = ['K1410_2:14F116.PNT','K1430_2:14F117.PNT','F1180_1:11FC804.MEAS']
lhu = ['F4111_4:41FR608.PNT']
gtg = ['G4001_2:40FQ006.MEAS','G4002_2:40FQ027.MEAS','G4003_2:40FQ015.MEAS','G4004_2:40FQ025.MEAS','G4006_1:40FQ101.MEAS','G4007_1:40FQ111.MEAS','G4008_1:40FI121.PNT','G4009_1:40FI131.PNT','G4010_1:40FI141.PNT','G4011_1:40FI151.PNT']

# Using the function to derive the corresponding tags for all trains
acid_removal_d_tags = tag_list(acid_removal_d,[1,2,3])
acid_removal_e_tags = tag_list(acid_removal_e,[4,5,6])
area_d_tags = tag_list(area_d,[1,2,3])
area_e_tags = tag_list(area_e,[4,5,6])
lhu_tags = tag_list(lhu,[1,2])
gtg_tags = gtg

# Merging all tags into one list
tag_list = acid_removal_d_tags + acid_removal_e_tags + area_d_tags + area_e_tags + lhu_tags + gtg_tags

# Getting data from PI
ghg_data = merge_tag_data(tag_list, PI(), round_time='min', days=0, mins=0)
ghg_data = ghg_data.reset_index()

# Getting GHG Factors from Datatbase
df_Factors, success = conn_sql_server(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        sql_string = """SELECT * FROM dbo.GHG_Factors"""
        )
df_Factors = df_Factors.set_index('Factor_Name')

# Creating a new dataframe to store GHG output
GHG_Live_Data = pd.DataFrame(columns = ['RecordDate','UpdatedDate','UpdatedBy','Acid_Gas_T1_CO2','Acid_Gas_T2_CO2','Acid_Gas_T3_CO2','Acid_Gas_T4_CO2','Acid_Gas_T5_CO2','Acid_Gas_T6_CO2','Acid_Gas_CO2','T1_CO2','T2_CO2','T3_CO2','T4_CO2','T5_CO2','T6_CO2','Trains_CO2','T1_N2O','T2_N2O','T3_N2O','T4_N2O','T5_N2O','T6_N2O','Trains_N2O','T1_CH4','T2_CH4','T3_CH4','T4_CH4','T5_CH4','T6_CH4','Trains_CH4','T1_CO2e','T2_CO2e','T3_CO2e','T4_CO2e','T5_CO2e','T6_CO2e','Trains_CO2e','LHU_CO2','LHU_N2O','LHU_CH4','LHU_CO2e','GTG_CO2','GTG_N2O','GTG_CH4','GTG_CO2e'])

# Storing data into dataframe
GHG_Live_Data.loc[0, "RecordDate"] = str(ghg_data['Timestamp'][0])
GHG_Live_Data.loc[0, "UpdatedDate"] = datetime.today().strftime('%Y-%m-%d')
GHG_Live_Data.loc[0, "UpdatedBy"] = 'Admin'

col_list = GHG_Live_Data.columns

# Calculating emissions for acid gas removal for T1-3 and storing to dataframe
for i in [1,2,3]:
    a = iferror(lambda: float(ghg_data[acid_removal_d_tags[i-1]][0]),0)
    b = iferror(lambda: float(ghg_data[acid_removal_d_tags[i+2]][0]),0)
    c = iferror(lambda: float(ghg_data[acid_removal_d_tags[i+5]][0]),0)    
    x = iferror(lambda: a/c*b/100*44,'error')
    if x == 'error' or c < 16:
        GHG_Live_Data.loc[0, col_list[i+2]] = 0
    else:
        GHG_Live_Data.loc[0, col_list[i+2]] = iferror(lambda: a/c*b/100*44,0)

# Calculating emissions for acid gas removal for T4-6 and storing to dataframe
for i in [4,5,6]:
    a = iferror(lambda: float(ghg_data[acid_removal_e_tags[i-4]][0]),0)
    b = iferror(lambda: float(ghg_data[acid_removal_e_tags[i-1]][0]),0)
    c = iferror(lambda: float(ghg_data[acid_removal_e_tags[i+2]][0]),0)    
    x = iferror(lambda: a/c*b/100*44,'error')
    if x == 'error' or c < 16:
        GHG_Live_Data.loc[0, col_list[i+2]] = 0
    else:
        GHG_Live_Data.loc[0, col_list[i+2]] = iferror(lambda: a/c*b/100*44,0)

# summing up emissions for T1-6 and storing to dataframe
Acid_Gas_CO2 = 0
for i in [3,4,5,6,7,8]:
    Acid_Gas_CO2 = Acid_Gas_CO2 + GHG_Live_Data.loc[0, col_list[i]]
GHG_Live_Data.loc[0, 'Acid_Gas_CO2'] = Acid_Gas_CO2

# Calculating CO2 emissions for for T1-3 and storing to dataframe
for i in [1,2,3]:
    a = iferror(lambda: float(ghg_data[area_d_tags[i-1]][0]),0)
    b = iferror(lambda: float(ghg_data[area_d_tags[i+2]][0]),0)
    c = iferror(lambda: float(ghg_data[area_d_tags[i+5]][0]),0)
    d = iferror(lambda: float(ghg_data[area_d_tags[i+8]][0]),0)
    k = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_CO2']),0)    
    GHG_Live_Data.loc[0, col_list[i+9]] = iferror(lambda: (a+b+c+d)*k,0)

# Calculating CO2 emissions for for T4-6 and storing to dataframe
for i in [4,5,6]:
    a = iferror(lambda: float(ghg_data[area_e_tags[i-4]][0]),0)
    b = iferror(lambda: float(ghg_data[area_e_tags[i-1]][0]),0)
    c = iferror(lambda: float(ghg_data[area_e_tags[i+2]][0]),0)
    k = iferror(lambda: float(df_Factors['Factor_Value']['GTs_CO2']),0)    
    GHG_Live_Data.loc[0, col_list[i+9]] = iferror(lambda: (a+b+c)*k,0)

# summing up CO2 emissions for T1-6 and storing to dataframe
Trains_CO2 = 0
for i in [10,11,12,13,14,15]:
    Trains_CO2 = Trains_CO2 + GHG_Live_Data.loc[0, col_list[i]]
GHG_Live_Data.loc[0, 'Trains_CO2'] = Trains_CO2

# Calculating N2O emissions for for T1-3 and storing to dataframe
for i in [1,2,3]:
    a = iferror(lambda: float(ghg_data[area_d_tags[i-1]][0]),0)
    b = iferror(lambda: float(ghg_data[area_d_tags[i+2]][0]),0)
    c = iferror(lambda: float(ghg_data[area_d_tags[i+5]][0]),0)
    d = iferror(lambda: float(ghg_data[area_d_tags[i+8]][0]),0)
    k = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_N2O']),0)    
    GHG_Live_Data.loc[0, col_list[i+16]] = iferror(lambda: (a+b+c+d)*k,0)

# Calculating N2O emissions for for T4-6 and storing to dataframe
for i in [4,5,6]:
    a = iferror(lambda: float(ghg_data[area_e_tags[i-4]][0]),0)
    b = iferror(lambda: float(ghg_data[area_e_tags[i-1]][0]),0)
    c = iferror(lambda: float(ghg_data[area_e_tags[i+2]][0]),0)
    k = iferror(lambda: float(df_Factors['Factor_Value']['GTs_N2O']),0)    
    GHG_Live_Data.loc[0, col_list[i+16]] = iferror(lambda: (a+b+c)*k,0)

# summing up N2O emissions for T1-6 and storing to dataframe
Trains_N2O = 0
for i in [17,18,19,20,21,22]:
    Trains_N2O = Trains_N2O + GHG_Live_Data.loc[0, col_list[i]]
GHG_Live_Data.loc[0, 'Trains_N2O'] = Trains_N2O

# Calculating CH4 emissions for for T1-3 and storing to dataframe
for i in [1,2,3]:
    a = iferror(lambda: float(ghg_data[area_d_tags[i-1]][0]),0)
    b = iferror(lambda: float(ghg_data[area_d_tags[i+2]][0]),0)
    c = iferror(lambda: float(ghg_data[area_d_tags[i+5]][0]),0)
    d = iferror(lambda: float(ghg_data[area_d_tags[i+8]][0]),0)
    j = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_CH4']),0)
    k = iferror(lambda: float(df_Factors['Factor_Value']['GTs_CH4']),0)    
    GHG_Live_Data.loc[0, col_list[i+23]] = iferror(lambda: ((a+b)*k)+((c+d)*j),0)

# Calculating CH4 emissions for for T4-6 and storing to dataframe
for i in [4,5,6]:
    a = iferror(lambda: float(ghg_data[area_e_tags[i-4]][0]),0)
    b = iferror(lambda: float(ghg_data[area_e_tags[i-1]][0]),0)
    c = iferror(lambda: float(ghg_data[area_e_tags[i+2]][0]),0)
    k = iferror(lambda: float(df_Factors['Factor_Value']['GTs_CH4']),0)    
    GHG_Live_Data.loc[0, col_list[i+23]] = iferror(lambda: ((a+b)*k)+((c+d)*j),0)

# summing up CH4 emissions for T1-6 and storing to dataframe
Trains_CH4 = 0
for i in [24,25,26,27,28,29]:
    Trains_CH4 = Trains_CH4 + GHG_Live_Data.loc[0, col_list[i]]
GHG_Live_Data.loc[0, 'Trains_CH4'] = Trains_CH4

# Calculating CO2e emissions for for T1-6 and storing to dataframe
for i in [1,2,3,4,5,6]:
    j = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_N2O']),0)
    k = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)
    a = GHG_Live_Data.loc[0, col_list[i+9]]
    b = GHG_Live_Data.loc[0, col_list[i+16]] * j
    c = GHG_Live_Data.loc[0, col_list[i+23]] * k
    GHG_Live_Data.loc[0, col_list[i+30]] = a+b+c

# summing up CO2e emissions for T1-6 and storing to dataframe
Trains_CO2e = 0
for i in [31,32,33,34,35,36]:
    Trains_CO2e = Trains_CO2e + GHG_Live_Data.loc[0, col_list[i]]
GHG_Live_Data.loc[0, 'Trains_CO2e'] = Trains_CO2e

# Calculating CO2 emissions for LHU and storing to dataframe
a = iferror(lambda: float(ghg_data[lhu_tags[0]][0]),0)
b = iferror(lambda: float(ghg_data[lhu_tags[1]][0]),0)
k = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_CO2']),0)
GHG_Live_Data.loc[0, 'LHU_CO2'] = iferror(lambda: (a+b)*k,0)

# Calculating N2O emissions for LHU and storing to dataframe
a = iferror(lambda: float(ghg_data[lhu_tags[0]][0]),0)
b = iferror(lambda: float(ghg_data[lhu_tags[1]][0]),0)
k = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_N2O']),0)
GHG_Live_Data.loc[0, 'LHU_N2O'] = iferror(lambda: (a+b)*k,0)

# Calculating CH4 emissions for LHU and storing to dataframe
a = iferror(lambda: float(ghg_data[lhu_tags[0]][0]),0)
b = iferror(lambda: float(ghg_data[lhu_tags[1]][0]),0)
k = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_CH4']),0)
GHG_Live_Data.loc[0, 'LHU_CH4'] = iferror(lambda: (a+b)*k,0)

# Calculating CO2e emissions for for LHU and storing to dataframe
j = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_N2O']),0)
k = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)
a = GHG_Live_Data.loc[0, 'LHU_CO2']
b = GHG_Live_Data.loc[0, 'LHU_N2O'] * j
c = GHG_Live_Data.loc[0, 'LHU_CH4'] * k
GHG_Live_Data.loc[0, 'LHU_CO2e'] = a+b+c

# Calculating the total GTG data
tot_gtg = 0
for i in [0,1,2,3,4,5,6,7,8,9]:
    tot_gtg = tot_gtg + iferror(lambda: float(ghg_data[gtg_tags[i]][0]),0)

# Calculating CO2 emissions for GTG and storing to dataframe
k = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_CO2']),0)
GHG_Live_Data.loc[0, 'GTG_CO2'] = iferror(lambda: tot_gtg*k,0)

# Calculating N2O emissions for GTG and storing to dataframe
k = iferror(lambda: float(df_Factors['Factor_Value']['Fired_Heaters_N2O']),0)
GHG_Live_Data.loc[0, 'GTG_N2O'] = iferror(lambda: tot_gtg*k,0)

# Calculating CH4 emissions for GTG and storing to dataframe
k = iferror(lambda: float(df_Factors['Factor_Value']['GTs_CH4']),0)
GHG_Live_Data.loc[0, 'GTG_CH4'] = iferror(lambda: tot_gtg*k,0)

# Calculating CO2e emissions for for GTG and storing to dataframe
j = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_N2O']),0)
k = iferror(lambda: float(df_Factors['Factor_Value']['GWP100_CH4']),0)
a = GHG_Live_Data.loc[0, 'GTG_CO2']
b = GHG_Live_Data.loc[0, 'GTG_N2O'] * j
c = GHG_Live_Data.loc[0, 'GTG_CH4'] * k
GHG_Live_Data.loc[0, 'GTG_CO2e'] = a+b+c

# Posting all GHG entries to database
for i, row in GHG_Live_Data.iterrows():
    vals = ''
    cols = ''
    for col in col_list:
        cols = cols + col + ","
        vals = vals + "'" + str(GHG_Live_Data.at[i, col]) + "',"
    vals=vals[:-1]
    cols=cols[:-1]
    insert_SQL(
        server = "BNY-S-560",
        db = "dataEntryDB",
        user = "Abimbola.Salami",
        pwd = "NLNG@3070",
        tbl = "GHG_Live_Data",
        cols = cols,
        vals = vals
    )