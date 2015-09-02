__author__ = 'pramod.kumar'
import pandas as pd
import calendar
import datetime
import MySQLdb
import matplotlib.pyplot as plt
plt.interactive(False)
import numpy as np

sql="select * from rack_analysis.pilot_bp_june where account_type='Channel: 50 - Commercial' and supplier_terminal_name='CANTON OH - BUCKEYE (PLANT 9180)' and product_name='PREMIUM';"
db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
df2=pd.read_sql(sql, con=db)
year=2015
analysisDate='01-06-2015'
dateDetails=datetime.datetime.strptime(analysisDate,"%d-%m-%Y")
month=dateDetails.month
fromDate='2015-'+str(dateDetails.month).zfill(2)+'-01 00:00:00'
toDate='2015-'+str(dateDetails.month+1).zfill(2)+'-01 00:00:00'
daysInMonth=str(calendar.monthrange(dateDetails.year,dateDetails.month)[1])
idx=pd.date_range(dateDetails.strftime('%m-%d-%Y'),str(dateDetails.month)+'-'+daysInMonth+'-'+str(dateDetails.year))
df2['date']= pd.to_datetime(df2[u'date'],format='%Y-%m-%d')
df2.index=pd.DatetimeIndex(df2['date'])
df2 = df2.reindex(idx, fill_value=0)
df2['date']=df2.index.values