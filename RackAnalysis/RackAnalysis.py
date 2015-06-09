__author__ = 'pramod.kumar'
import pandas as pd
import calendar
import datetime
import MySQLdb
db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
filename="exxon_may_daily_complete.csv"

sql="""select *,date(execution_date) as date from
(SELECT
enallocationstatus,percentage_allocation,Account_Type,Base_gallons,Beginning_Gallons,Terminal_Name,Lifted_Gallons,Additional_Gallons_Allowed,Additional_Gallons_Remaining,Next_Refresh_Date,
Next_Refresh_Base_Gallons,product_name,period,batchno,execution_date,rid
FROM enallocationarchive
where supplier='Exxon' and Terminal_Name='LOCKPORT IL (MOC) - 00MD' and product_name='ULSD' and Account_Type='MANSFIELD OIL COMPANY OF 106305 IW'
and Execution_Date>='2015-05-01 00:00:00' and Execution_Date<='2015-06-01 00:00:00' and period='Daily'
order by execution_date desc,batchno desc,period) x
GROUP BY date(execution_date),period
having maX(batchno);"""
df_mysql = pd.read_sql(sql, con=db)
df2=pd.read_sql(sql, con=db)
dateDetails=datetime.datetime.strptime("01-05-2015","%d-%m-%Y")
daysInMonth=str(calendar.monthrange(dateDetails.year,dateDetails.month)[1])
idx=pd.date_range('05-01-2015','05-'+daysInMonth+'-2015')
df2['date']= pd.to_datetime(df2[u'date'],format='%Y-%m-%d')
df2.index=pd.DatetimeIndex(df2['date'])
df2 = df2.reindex(idx, fill_value=0)
df2['date']=df2.index.values
df2['Month']=df2['date'].apply(lambda x:x.month)
df2['Day']=df2['date'].apply(lambda x:x.day)
df2['csn']=df2.groupby(['Month'])['Base_gallons'].cumsum()
df2['avg_csn_perday']=df2.apply(lambda x: x['csn']/x['date'].day ,axis=1)
df2['c_begin']=df2.groupby(['Month'])['Beginning_Gallons'].cumsum()
df2['avg_beginning_perday']=df2.apply(lambda x: x['c_begin']/x['date'].day ,axis=1)
df2.to_csv("Computed_"+filename)
df2['percentage_allocation']=df2['percentage_allocation'].apply(lambda x:int(str(x).strip('%')))
missingDays=df2.loc[df2['period'] == 0]


sqlBatches="""select max(batchno) as batchno, date(Execution_Date) as date from enallocationarchive
where supplier='Exxon' and Terminal_Name='LOCKPORT IL (MOC) - 00MD' and product_name='ULSD'
and Execution_Date>='2015-05-01 00:00:00' and Execution_Date<='2015-06-01 00:00:00'
group by supplier,date(Execution_Date) ;"""