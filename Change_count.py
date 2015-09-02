__author__ = 'pramod.kumar'
import MySQLdb
import pandas as pd
import numpy as np
db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")

# sql="""select Base_Gallons,Beginning_Gallons,Lifted_Gallons,Execution_Date,Supplier,product_name,Account_Type,terminal_name,batchno from enallocationarchive where supplier='citgo' and account_type="MANSFIELD OIL CO OF GAINESVILLE--224531" and Terminal_Name="1102-MADISON-CITGO" and product_name="ULS" and period='Daily' and Execution_Date >='2015-06-01 00:00:00' and Execution_Date<='2015-06-02 00:00:00'"""
def runMe(period):
    sql="""select Base_Gallons,Beginning_Gallons,Lifted_Gallons,Execution_Date,Supplier,
    product_name,Account_Type,terminal_name,period,batchno
    from enallocationarchive
    where
    period='%s' and  Execution_Date >='2015-06-01 00:00:00' and Execution_Date<='2015-06-02 00:00:00'
    group by period,batchno,product_name,Account_Type,Terminal_Name,Supplier
    order by period,product_name,Account_Type,Terminal_Name,Supplier,Execution_Date asc"""%(period)
    df2=pd.read_sql(sql, con=db)
    df2=df2.fillna(0)
    change=[0]
    for i in range(1,len(df2)):
         if df2.iloc[i]['Base_Gallons'] !=df2.iloc[i-1]['Base_Gallons'] or (df2.iloc[i]['Beginning_Gallons'] !=df2.iloc[i-1]['Beginning_Gallons']) or (df2.iloc[i]['Lifted_Gallons'] !=df2.iloc[i-1]['Lifted_Gallons']):
             # print i,1
             change.append(1)
         else:
             change.append(0)
    df2['change']=change
    d=df2.drop(['Base_Gallons','Beginning_Gallons','Lifted_Gallons','Execution_Date','period','batchno','change'],axis=1)
    dupIndex=d.drop_duplicates().index
    for i in dupIndex:
        df2.loc[i,['change']]=0
    df2.to_csv("MOC_"+period+"_Modified.csv")

periodTypes=["Daily","Monthly","Weekly"]
for i in periodTypes:
    runMe(i)
db.close()