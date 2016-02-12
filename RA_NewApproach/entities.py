import calendar
import datetime
import MySQLdb
import os
import pandas as pd
import time
import sys
import dateutil
from pymongo import MongoClient
class entity:
    def __init__(self,details):
        self.customer=details[0]
        self.supplier=details[1]
        self.analysisDate=details[2]
        self.savelocation=details[3]
        self.setConfig=details[4]
        self.account=""
        self.terminal=""
        self.product=""
        self.dateDetails=datetime.datetime.strptime(self.analysisDate,"%d-%m-%Y")
        self.month=calendar.month_name[self.dateDetails.month].lower()
        self.frames=[]
        self.pivotFrames=[]
        self.suppliersCombinations=[]
        self.savepath=self.savelocation+"\\"+self.supplier+"\\"+self.month+"\\"
        self.dbcon=[self.setConfig[self.customer]["mysqlIPAddress"],self.setConfig[self.customer]["mysqlUserName"],
                    self.setConfig[self.customer]["mysqlPassword"],self.setConfig[self.customer]["mysqlDatabaseName"]]
        self.db = MySQLdb.connect(self.dbcon[0],self.dbcon[1],self.dbcon[2],self.dbcon[3])
        self.get_databasename()
        self.executionFrom=self.dateDetails.strftime("%Y-%m-%d %H:%M:%S")
        self.executionTo=(self.dateDetails+dateutil.relativedelta.relativedelta(months=1)).strftime("%Y-%m-%d %H:%M:%S")
        self.previousMonthexecutionFrom=(self.dateDetails-dateutil.relativedelta.relativedelta(months=1)).strftime("%Y-%m-%d %H:%M:%S")

        self.previousMonthexecutionTo=self.dateDetails.strftime("%Y-%m-%d %H:%M:%S")
        if not os.path.exists(self.savepath):
            os.makedirs(self.savepath)
        self.oldsys=sys.stdout

        sys.stdout = open(self.savepath+'log.txt','w')
        print "Entities thread id:",self.db.thread_id()

    def __del__(self):
        self.db.close()

    def mongo_connector(self):
        client = MongoClient(self.setConfig[self.customer]["mongoIPAddress"], 27017)
        self.mdb = client[self.customer]
        self.collection=self.mdb[str(self.dateDetails.year)+"_"+self.dateDetails.strftime("%b")]
    def get_databasename(self):
        try:
            sql="SELECT databasename FROM systemdb.ra_databaseredirector where customer='%s'"%(self.customer)
            cursor=self.db.cursor()
            cursor.execute(sql)
            row=cursor.fetchall()
            if row !=():
                self.databasename=row[0][0]
                self.contracts_tablename=self.customer+"_enallocationarchivecontract"
            else:
                print "database not found"
                raise LookupError
        except Exception as e:
            print e

    def get_maxbatch_analysis_pivot(self,executionFrom,executionTo):
        try:
            condition="Execution_Date>='%s' and Execution_Date<='%s' and supplier_name='%s'"%(executionFrom,executionTo,self.supplier)
            sql="""select *,date(execution_date) as date from (select
            `x`.`alerts_ratability`,
            `x`.`en_allocation_status`,
            `x`.`percentage_allocation`,
            `x`.`account_type`,
            `x`.`base_gallons`,
            `x`.`beginning_gallons`,
            `x`.`supplier_terminal_name`,
            `x`.`lifted_gallons`,
            `x`.`remaining_gallons`,
            `x`.`additional_gallons_allowed`,
            `x`.`additional_gallons_remaining`,
            `x`.`next_refresh_date`,
            `x`.`next_refresh_base_gallons`,
            `x`.`product_name`,
            `x`.`period`,
            `x`.`batchno`,
            `x`.`execution_date`,
            `x`.`staging_id`
             from
            (select supplier_name,max(batchno) as batchno,date(Execution_Date) as dt from enallocationarchive
            where %s
            group by supplier_name,date(Execution_Date)) a join

            (SELECT
            *
            FROM enallocationarchive
            where %s and
            (period='Daily' or period='Weekly' or period='Monthly') and (Account_Type<>"Unknown" and supplier_terminal_name<>"Unknown" and product_name<>"Unknown")
                        order by execution_date desc,batchno desc,account_type,supplier_terminal_name,product_name,period ) x on a.batchno=x.batchno and a.dt=date(x.Execution_Date) and a.supplier_name=x.supplier_name)w
                        GROUP BY date(execution_date),account_type,supplier_terminal_name,product_name,period

            having maX(batchno)
            order by account_type,supplier_terminal_name,product_name,date,period;"""%(condition,condition)
            df_mysql = pd.read_sql(sql, con=self.appConst.db)
            time.sleep(15)
            frameName=self.customer+"_"+self.supplier+"_"+self.month+"_maxBatchFrame.csv"
            # df_mysql.to_csv(self.savepath++frameName)
            df_mysql=pd.read_csv(self.savepath+frameName)
            mp=pd.pivot_table(df_mysql,index=["date","account_type","supplier_terminal_name","product_name"],values=["base_gallons","lifted_gallons","beginning_gallons","en_allocation_status",'percentage_allocation','alerts_ratability','next_refresh_date'],columns="period",aggfunc = lambda x: x)
            if len(mp)==0:
                return 0
            mp.columns=['_'.join(col).strip() for col in mp.columns.values]
            dates=[]
            accounts=[]
            terminals=[]
            products=[]
            for i in mp.index.values:
                dates.append(i[0])
                accounts.append(i[1])
                terminals.append(i[2])
                products.append(i[3])
            mp.insert(0,"product_name",products)
            mp.insert(0,"supplier_terminal_name",terminals)
            mp.insert(0,"account_type",accounts)
            mp.insert(0,"date",dates)
            mp=mp.fillna(0)
            return mp
        except Exception as e:
            print e
            return 0