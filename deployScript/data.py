__author__ = 'pramod.kumar'

import sys
import time
import MySQLdb
import csv
import os
class consolidator:
    def __init__(self):

        print "Consolidation Started"
        self.currentDateSaveFile = time.strftime("%d_%B_%Y")
        self.customerName=sys.argv[1]
        if len(sys.argv)>=3:
            self.bno=sys.argv[2]
        self.sqlDate="%"+time.strftime("%m/%d/%y")+"%"
        self.sqlDate1=time.strftime("%m/%d/%y")+" 06:00:00"
        self.sqlDateTerminal=time.strftime("%Y-%m-%d")+" 00:00:00"
        self.folderDate= time.strftime("%Y_%m_%d")
        self.folderPath="C:\\RackinsightOutputFiles\\OutputFilesDump\\"+str(self.customerName)+"\\"+str(self.folderDate)+"\\All_Merged"
        if not os.path.exists(self.folderPath):
            os.makedirs(self.folderPath)
        self.dbIP="172.24.16.21"
        self.rows=[]
        if self.customerName=="Mansfield":
            self.stageTable="enconsolidatedwithterminals"
            self.stageDB='ra_mansfielddb'
            self.applicationDB='enallocationdbdev'

        elif self.customerName=="UPS":
            self.stageTable="enconsolidatedwithterminals"
            self.stageDB='ra_upsdb'
            self.applicationDB='enallocationdbur'

        elif self.customerName=="Ryder":
            self.stageTable="enconsolidatedwithterminals"
            self.stageDB='ra_ryderdb'
            self.applicationDB='enallocationdbryder'

        elif self.customerName=="Costco":
            self.stageTable="enconsolidatedwithterminals"
            self.stageDB='ra_costcodb'
            self.applicationDB='costcostage'
        elif self.customerName=="Pilot":
            self.stageTable="enconsolidatedwithterminals"
            self.stageDB='ra_pilotdb'
            self.applicationDB='ra_pilotdb'
        elif self.customerName=="SAW":
            self.stageTable="enconsolidatedwithterminals"
            self.stageDB='ra_sawdb'
            self.applicationDB='ra_sawdb'
        elif self.customerName=="APP":
            self.stageTable="enconsolidatedwithterminals"
            self.stageDB='ra_appdb'
            self.applicationDB='ra_appdb'
        try:
            pass
            self.dbCon = MySQLdb.connect(self.dbIP,"root","admin123*",self.stageDB)
            self.cursor = self.dbCon.cursor()
        except Exception as e:
            print e
    def consolidateAll(self,dt):
        sql="""select
        a.Customer_Name,
        a.padd as PADD,
        a.subpadd as SubPADD,
        a.Terminal_City as IRS_Terminal_City,
        a.Terminal_State as IRS_Terminal_State,
        a.tcn as IRS_TCN,
        a.enterminalname as EN_Terminal_Name,
        a.termname as IRS_Terminal_Name,
        a.Terminal_Name as Supplier_Terminal_Name,
        a.Supplier as EN_Supplier_Name,
        a.ENAccountType as EN_Account_Type,
        a.ENBranding as EN_Branding,
        a.Account_Type as Supplier_Account_Name,
        a.product_category as EN_Product_Category,
        a.product_type as EN_Product_Type,
        a.Product_Name as Supplier_Product_Name,
        a.Period as Period_Type,
        a.Alerts_Allocation as Allocation_Status,
        a.Alerts_Ratability as Ratability_Status,
        a.Base_Gallons,
        a.Percentage_Allocation,
        a.Beginning_Gallons,
        a.Lifted_Gallons,
        a.Remaining_Gallons,
        a.Additional_Gallons_Allowed,
        a.Additional_Gallons_Remaining,
        a.Next_Refresh_Date,
        a.Next_Refresh_Base_Gallons,
        a.Extracted_Type,
        a.Execution_Date,
        a.UserName as EN_User_Name,
        a.URL as Supplier_Website_URL
        from (select * from `enconsolidatedwithterminals` as a where a.Execution_Date like '%s') as a
        join
        (select max(batchno) as mb,Supplierfullname from `enconsolidatedwithterminals` where execution_date like '%s' group by Supplierfullname) as b
        on a.Supplierfullname=b.Supplierfullname and a.batchno=b.mb"""%(dt,dt)
        self.cursor.execute(sql)
        self.rows.append(self.cursor.fetchall())



if __name__ == '__main__':
    consolidateMe=consolidator()
    m="09"
    for i in range(1,31):
        consolidateMe.consolidateAll(m+"/"+str(i).zfill(2)+"/15%")
    fileSave=open(consolidateMe.folderPath+"\\"+consolidateMe.customerName+"_EN_Allocation_"+consolidateMe.currentDateSaveFile+".csv",'wb')
    # Header
    fileSave.write("Customer_Name, PADD, SubPADD, IRS_Terminal_City, IRS_Terminal_State, IRS_TCN, EN_Terminal_Name, IRS_Terminal_Name, Supplier_Terminal_Name, EN_Supplier_Name, EN_Account_Type, EN_Branding, Supplier_Account_Name, EN_Product_Category, EN_Product_Type, Supplier_Product_Name, Period_Type, Allocation_Status, Ratability_Status, Base_Gallons, Percentage_Allocation, Beginning_Gallons, Lifted_Gallons, Remaining_Gallons, Additional_Gallons_Allowed, Additional_Gallons_Remaining, Next_Refresh_Date, Next_Refresh_Base_Gallons, Extracted_Type, Execution_Date, EN_User_Name, Supplier_Website_URL\n")
    w=csv.writer(fileSave,delimiter=',')
    w.writerows(consolidateMe.rows)
    fileSave.close()
    print "Consolidation-All complete"