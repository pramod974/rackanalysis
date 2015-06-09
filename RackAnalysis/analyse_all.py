__author__ = 'pramod.kumar'
import pandas as pd
import calendar
import datetime
import MySQLdb
import matplotlib.pyplot as plt
plt.interactive(False)
db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
import sys
sys.stdout = open('report.txt', 'w')
class analysis:
    def __init__(self):
        errors={}
        self.filename=""
        self.supplierDetails=[]
        self.getSuppliers()
        self.savepath="C:\\sqlData\\"
        self.month='05'
        self.year=2015
        self.analysisDate='01-05-2015'
        self.dateDetails=datetime.datetime.strptime(self.analysisDate,"%d-%m-%Y")
        self.fromDate='2015-'+str(self.dateDetails.month).zfill(2)+'-01 00:00:00'
        self.toDate='2015-'+str(self.dateDetails.month+1).zfill(2)+'-01 00:00:00'
        print "Execution Started"
        for i in range(len(self.supplierDetails)):
            supplierData=self.supplierDetails[i]
            self.supplier=supplierData['supplier']
            self.account_type=supplierData['account_type']
            self.product_name=supplierData['product_name']
            self.terminal_name=supplierData['terminal_name']
            print "****************************************************************************************************"
            print "---------------------------------------------------------------------------------------------------"
            print "Supplier:",self.supplier,"\t Account type:",self.account_type, "\t\n product name:",self.product_name, "\t Terminal name:",self.terminal_name
            self.df_daily=self.analyse(self.supplierDetails[i],'daily')
            print("===================================================================================================")
            self.df_weekly=self.analyse(self.supplierDetails[i],'weekly')
            print("===================================================================================================")
            self.df_monthly=self.analyse(self.supplierDetails[i],'monthly')
            print("=========================================Contradiction=============================================")
            print "\n\n******** Cumulative Daily Lifted and Lifed Gallons Monthly********\n"
            self.check_LiftedGallons_DM()
            print "\n\n******** Cumulative Daily Lifted per Week and Lifed Gallons Weekly********\n"
            self.check_LiftedGallons_weekly()
            print("\n====================================Consistency of Next_Refresh_Date================================")
            self.weeklyNextRefreshDate()
            print("======================================Trend In Percentage Allocation================================")
            self.trend_percentageAllocation()

            print "---------------------------------------------------------------------------------------------------"
    def getSuppliers(self):
        try:
            db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
            dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
            sql=sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details where supplier='exxon' and Account_Type='MANSFIELD OIL COMPANY OF 106305 IW' and terminal_name='LOCKPORT IL (MOC) - 00MD' and product_name='ULSD';"
            # sql="SELECT distpplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details where supplier='chevron';"
            dbCursor.execute(sql)
            rows=dbCursor.fetchall()
            for i in rows:
                self.supplierDetails.append(i)
            db.close()
        except Exception as e:
            db.close()
            print e
    def analyse(self,supplierData,period):
        supplier=supplierData['supplier']
        account_type=supplierData['account_type']
        product_name=supplierData['product_name']
        terminal_name=supplierData['terminal_name']
        name='_'.join(supplierData.values())
        # name=name.replace(':','')
        name=name.replace(' ','-')
        name=terminal_name.replace(' ','')+"_"+product_name.replace(' ','')
        self.filename=filename=supplier+"_"+period+"_"+name+".csv"
        print "---------------------------------","\t Period:",period,"------------------------------------------------"
        sql="""select *,date(execution_date) as date from
        (SELECT
        enallocationstatus,percentage_allocation,account_type,base_gallons,beginning_gallons,terminal_name,lifted_gallons,remaining_gallons,additional_gallons_allowed,additional_gallons_remaining,next_refresh_date,
        next_refresh_base_gallons,product_name,period,batchno,execution_date,rid
        FROM enallocationarchive
        where supplier='%s' and account_type="%s" and Terminal_Name="%s" and product_name="%s" and period='%s'
        and Execution_Date>='%s' and Execution_Date<='%s'
        order by execution_date desc,batchno desc,period) x
        GROUP BY date(execution_date),period
        having maX(batchno);"""%(supplier,account_type,terminal_name,product_name,period,self.fromDate,self.toDate)
        df_mysql = pd.read_sql(sql, con=db)
        df2=pd.read_sql(sql, con=db)

        daysInMonth=str(calendar.monthrange(self.dateDetails.year,self.dateDetails.month)[1])
        idx=pd.date_range('05-01-2015','05-'+daysInMonth+'-2015')
        df2['date']= pd.to_datetime(df2[u'date'],format='%Y-%m-%d')
        df2.index=pd.DatetimeIndex(df2['date'])
        df2 = df2.reindex(idx, fill_value=0)
        df2['date']=df2.index.values
        df2['Month']=df2['date'].apply(lambda x:x.month)
        df2['Day']=df2['date'].apply(lambda x:x.day)
        df2['cum_base_gallons']=df2.groupby(['Month'])['base_gallons'].cumsum()
        df2['avg_base_perday']=df2.apply(lambda x: x['cum_base_gallons']/x['date'].day ,axis=1)
        df2['cum_beginning_gallons']=df2.groupby(['Month'])['beginning_gallons'].cumsum()
        df2['avg_beginning_perday']=df2.apply(lambda x: x['cum_beginning_gallons']/x['date'].day ,axis=1)
        df2['cum_lifted_gallons']=df2.groupby(['Month'])['lifted_gallons'].cumsum()
        df2['avg_lifted_perday']=df2.apply(lambda x: x['cum_lifted_gallons']/x['date'].day ,axis=1)
        
        df2.to_csv(filename)
        missingDays=df2.loc[df2['period'] == 0]
        print "***************************************Missing Values***************************************************"
        if len(missingDays)==int(daysInMonth):
            print "Missing Values:"
            print "\t \t \t period:",period," Not available for this supplier",len(missingDays)
            print "****************************************************************************************************"
        elif len(missingDays)>0:
            # print "*************************************************************************************************"
            # print "Supplier",supplier,"\t Account type",account_type, "\t product name",product_name, "Terminal name",terminal_name
            print "\t \t \tTotal periods Missing for period",period,len(missingDays)
            print "****************************************************************************************************"
        else:
            # print "*************************************************************************************************"
            # print "Supplier",supplier,"\t Account type",account_type, "\t product name",product_name, "Terminal name",terminal_name
            print "\t \t \t All days of month have the period:",period
            print "****************************************************************************************************"
        self.static_rules(df2)
        return df2
    def static_rules(self,df2):

        print "****************************************Miscellaneous Inferences*****************************************"
        liftedGallonsGTbeginning=df2.loc[(df2['lifted_gallons']>df2['beginning_gallons'])& (df2['period']!=0) ]
        if len(liftedGallonsGTbeginning)>0:
            print "Number of Lifted gallons greater than beginning gallons : ", len(liftedGallonsGTbeginning)

        remainingGallonsLessThanZero=df2.loc[(df2['remaining_gallons']<=0)& (df2['period']!=0) ]
        if len(remainingGallonsLessThanZero)>0:
            print "Number of Remaining gallons less than zero ", len(remainingGallonsLessThanZero)
            additional_gallons_allowed_zero=remainingGallonsLessThanZero.loc[(remainingGallonsLessThanZero['additional_gallons_allowed']==0)& (remainingGallonsLessThanZero['period']!=0) ]
            print "Number of additional_gallons_allowed zero ", len(additional_gallons_allowed_zero)
            additional_gallons_remaining_zero=remainingGallonsLessThanZero.loc[(remainingGallonsLessThanZero['additional_gallons_remaining']<=0)& (remainingGallonsLessThanZero['period']!=0) ]
            print "Number of additional_gallons_remaining less than or equal to zero ", len(additional_gallons_remaining_zero)
    def check_LiftedGallons_DM(self):

        # df_daily=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_daily.csv',index_col='Unnamed: 0')
        # df_monthly=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_monthly.csv',index_col='Unnamed: 0')
        df_daily=self.df_daily.copy()
        df_monthly=self.df_monthly.copy()
        errors_lifted=df_daily['cum_lifted_gallons']==df_monthly['lifted_gallons']
        errors_lifted=errors_lifted.value_counts()
        errors_lifted=dict(errors_lifted)
        if errors_lifted.has_key(True):
            print "Number of rows where lifted_Monthly == cum_lifted_daily   : ",errors_lifted[True]
        if errors_lifted.has_key(False):
            print "Number of Errors where lifted_Monthly != cum_lifted_daily : ",errors_lifted[False]
    def trend_percentageAllocation(self):
        # df_daily=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_daily.csv',index_col='Unnamed: 0')
        df_daily=self.df_daily.copy()
        df_daily['percentage_allocation']=df_daily['percentage_allocation'].apply(lambda x: int(str(x).strip('%')))
        pall=df_daily['percentage_allocation']
        df_daily['percentageAllocation_avg_daily']=pall.mean()
        # df_monthly=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_monthly.csv',index_col='Unnamed: 0')
        df_monthly=self.df_monthly.copy()
        df_monthly['percentage_allocation']=df_monthly['percentage_allocation'].apply(lambda x: int(str(x).strip('%')))
        # df_weekly=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_weekly.csv',index_col='Unnamed: 0')
        df_weekly=self.df_weekly.copy()
        df_weekly['percentage_allocation']=df_weekly['percentage_allocation'].apply(lambda x: int(str(x).strip('%')))
        df_daily['pa_monthly']=df_monthly['percentage_allocation']
        df_daily['pa_weekly']=df_weekly['percentage_allocation']
        pall_monthly=df_monthly['percentage_allocation']
        pall_weekly=df_weekly['percentage_allocation']
        df_daily['percentageAllocation_avg_Monthly']=pall_monthly.mean()
        df_daily['percentageAllocation_avg_Weekly']=pall_weekly.mean()
        print "Percentage allocation Daily Mean",pall.mean()
        print "Percentage allocation Monthly Mean",pall_monthly.mean()
        print "Percentage allocation Weekly Mean",pall_weekly.mean()
        print "Percentage allocation Daily Min: ",pall.min()," max:",pall.max()
        print "Percentage allocation Monthly Min",pall_monthly.min()," max:",pall_monthly.max()
        print "Percentage allocation Weekly Min",pall_weekly.min()," max:",pall_weekly.max()
        print pall.describe()
        df_daily.plot(y=['percentage_allocation','percentageAllocation_avg_daily','pa_monthly','percentageAllocation_avg_Monthly','pa_weekly','percentageAllocation_avg_Weekly'],x='Day',kind='bar',subplots=True)

        plt.plot()
        plt.savefig(self.filename+'.png')
        # raw_input()
    def check_LiftedGallons_weekly(self):
        # df_daily=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_daily.csv',index_col='Unnamed: 0')
        # df_weekly=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_weekly.csv',index_col='Unnamed: 0')
        df_daily=self.df_daily.copy()
        df_weekly=self.df_weekly.copy()
        df_daily['weekly_refresh_date']=df_weekly['next_refresh_date'].apply(lambda x:x)
        # df_daily['cum_lifted_weekly']=df_daily.groupby(['weekly_refresh_date'])['lifted_gallons'].cumsum()
        df_daily['lifted_modified']=df_daily['lifted_gallons'].apply(lambda x:x)
        # df_daily['lifted_modified'][0]=df_weekly['lifted_gallons'][0]
        df_daily.loc[df_daily.index[0],'lifted_modified']=df_weekly['lifted_gallons'][0]
        df_daily['cum_lifted_weekly']=df_daily.groupby(['weekly_refresh_date'])['lifted_modified'].cumsum()
        errors_lifted=df_weekly['lifted_gallons']==df_daily['cum_lifted_weekly']
        errors_lifted=errors_lifted.value_counts()
        errors_lifted=dict(errors_lifted)
        if errors_lifted.has_key(True):
            print "Number of rows where lifted_weekly == cum_lifted_daily_perWeek   : ",errors_lifted[True]
        if errors_lifted.has_key(False):
            print "Number of Errors where lifted_weekly != cum_lifted_daily_perWeek : ",errors_lifted[False]
    def weeklyNextRefreshDate(self):
        df_weekly=self.df_weekly.copy()
        nxtRefreshWeek=df_weekly['next_refresh_date']
        print nxtRefreshWeek.value_counts()

try:
    result=analysis()
except Exception as e:
    print e