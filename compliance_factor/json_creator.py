__author__ = 'pramod.kumar'
import MySQLdb
import pandas as pd
from pymongo import MongoClient
import time

class json_creator:
    def __init__(self):
        self.dbcon = MySQLdb.connect("172.16.0.55","root","admin123*","rules_spark")
    def __del__(self):
        self.dbcon.close()
    def insert_tsap(self,row):
        sql="""select
        date,
        supplier_terminal_name,
        supplier_name,
        account_type,
        product_name,
        Ratability_Monthly,
        Ratability_Daily,
        Ratability_Weekly

        from enallocationarchive_debug where

        supplier_terminal_name='%s'
        and
        supplier_name='%s'

        and account_type='%s'

        and
        product_name='%s'
        ;"""%(row[0],row[1],row[2],row[3])
        df=pd.read_sql(sql, con=self.dbcon,index_col ="date")
        df.fillna(0,inplace=True)
        supplier_terminal_name=df['supplier_terminal_name'][0]
        supplier_name=df['supplier_name'][0]
        account_type=df['account_type'][0]
        product_name=df['product_name'][0]
        tsap_json={
            'supplier_terminal_name': supplier_terminal_name,
            'account_type':account_type,
         'bydate': [],
         'product_name': product_name,
         'supplier_name': supplier_name,

         'bymonth':[]

        }


        # df["Ratability_monthly_average"]=df.Ratability_Monthly.cumsum()/df.index.day
        # df["Ratability_monthly_average"]=df["Ratability_monthly_average"].apply(lambda x:round(x))
        # average_monthly=df["Ratability_monthly_average"][-1]
        tsap_json={'account_type':account_type,
         'bydate': [],
         'product_name': product_name,
         'supplier_name': supplier_name,
         'supplier_terminal_name': supplier_terminal_name}
        bydate=[]
        for index,row in df.iterrows():
            bydate.append(
            {"date":index,
                   "ratability_monthly":row["Ratability_Monthly"],
                   "ratability_daily":row["Ratability_Daily"],
                   "ratability_weekly":row["Ratability_Weekly"]
            }
            )
        tsap_json["bydate"]=bydate


        client = MongoClient('172.16.0.55', 27017)
        db = client.ra
        collection = db.contract_compliance
        collection.insert(tsap_json)

    def fetch_tsap(self):
        sql="""
        select
        distinct
        supplier_terminal_name,
        supplier_name,
        account_type,
        product_name

        from enallocationarchive_debug;
                """
        cursor=self.dbcon.cursor()
        cursor.execute(sql)
        rows=cursor.fetchall()
        for row in rows:
            self.insert_tsap(row)
start=time.time()
obj=json_creator()
obj.fetch_tsap()
print time.time()-start