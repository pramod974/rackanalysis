__author__ = 'pramod.kumar'
import pandas as pd
import calendar
import datetime
import MySQLdb
db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")

class analysis:
    def __init__(self):
        errors={}
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
            self.analyse(self.supplierDetails[i],'monthly')
            self.analyse(self.supplierDetails[i],'daily')
            self.analyse(self.supplierDetails[i],'weekly')

    def getSuppliers(self):
        try:
            db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
            dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
            sql=sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details where supplier='exxon' and Account_Type='MANSFIELD OIL COMPANY OF 106305 IW' and terminal_name='LOCKPORT IL (MOC) - 00MD' and product_name='ULSD';"
            # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details where supplier='chevron';"
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
        filename=supplier+"_"+period+"_"+name+".csv"
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

        # df2.to_csv(filename)
        missingDays=df2.loc[df2['period'] == 0]
        if len(missingDays)==int(daysInMonth):
            print "************************************************************************************************************"
            print "Supplier",supplier,"\t Account type",account_type, "\t product name",product_name, "Terminal name",terminal_name
            print "\t \t \t period ",period," Not found",len(missingDays)
        elif len(missingDays)>0:
            print "************************************************************************************************************"
            print "Supplier",supplier,"\t Account type",account_type, "\t product name",product_name, "Terminal name",terminal_name
            print "\t \t \tTotal periods Missing for period",period,len(missingDays)
            print "************************************************************************************************************"

    def static_rules(self,df2):
        remainingGallonsLessThanZero=df2.loc[(df2['remaining_gallons']<=0)& (df2['period']!=0) ]
        if len(remainingGallonsLessThanZero)>0:
            print "Number of Remaining gallons less than zero ", len(remainingGallonsLessThanZero)
            additional_gallons_allowed_zero=remainingGallonsLessThanZero.loc[(remainingGallonsLessThanZero['additional_gallons_allowed']==0)& (remainingGallonsLessThanZero['period']!=0) ]
            print "Number of additional_gallons_allowed zero ", len(additional_gallons_allowed_zero)
            additional_gallons_remaining_zero=remainingGallonsLessThanZero.loc[(remainingGallonsLessThanZero['additional_gallons_remaining']<=0)& (remainingGallonsLessThanZero['period']!=0) ]
            print "Number of additional_gallons_remaining less than or equal to zero ", len(additional_gallons_remaining_zero)
    def check_LiftedGallons_DM(self):

        df_daily=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_daily.csv',index_col='Unnamed: 0')
        df_monthly=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_monthly.csv',index_col='Unnamed: 0')
        errors_lifted=df_daily['cum_lifted_gallons']==df_monthly['lifted_gallons']
        errors_lifted=dict(errors_lifted)
        if errors_lifted.has_key(False):
            print "Number of Errors where lifted_Monthly != cum_lifted_daily  ",errors_lifted[False]
    def trend_percentageAllocation(self):
        df_daily=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_daily.csv',index_col='Unnamed: 0')
        df_daily['percentage_allocation']=df_daily['percentage_allocation'].apply(lambda x: int(str(x).strip('%')))
        pall=df_daily['percentage_allocation']
        df_daily['percentageAllocation_avg']=pall.mean()
        df_monthly=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_monthly.csv',index_col='Unnamed: 0')
        df_monthly['percentage_allocation']=df_monthly['percentage_allocation'].apply(lambda x: int(str(x).strip('%')))
        df_weekly=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_weekly.csv',index_col='Unnamed: 0')
        df_weekly['percentage_allocation']=df_weekly['percentage_allocation'].apply(lambda x: int(str(x).strip('%')))
        df_daily['pa_monthly']=df_monthly['percentage_allocation']
        df_daily['pa_weekly']=df_weekly['percentage_allocation']
        pall_monthly=df_monthly['percentage_allocation']
        pall_weekly=df_weekly['percentage_allocation']
        df_daily['percentageAllocation_avg_Monthly']=pall_monthly.mean()
        df_daily['percentageAllocation_avg_Weekly']=pall_weekly.mean()
        df_daily.plot(y=['percentage_allocation','percentageAllocation_avg','pa_monthly','percentageAllocation_avg_Monthly','pa_weekly','percentageAllocation_avg_Weekly'])

    def check_LiftedGallons_weekly(self):
        df_daily=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_daily.csv',index_col='Unnamed: 0')
        df_weekly=pd.read_csv('C:\Users\pramod.kumar\PycharmProjects\PlayArena\RackAnalysis\Chevron_weekly.csv',index_col='Unnamed: 0')
        df_daily['weekly_refresh_date']=df_weekly['next_refresh_date']
        # df_daily['cum_lifted_weekly']=df_daily.groupby(['weekly_refresh_date'])['lifted_gallons'].cumsum()
        df_daily['lifted_modified']=df_daily['lifted_gallons']
        df_daily['lifted_modified'][0]=df_weekly['lifted_gallons'][0]
        df_daily['cum_lifted_weekly']=df_daily.groupby(['weekly_refresh_date'])['lifted_modified'].cumsum()
        errors_lifted=df_weekly['lifted_gallons']==df_daily['cum_lifted_weekly']
        errors_lifted=dict(errors_lifted)
        if errors_lifted.has_key(False):
            print "Number of Errors where lifted_Monthly != cum_lifted_daily  ",errors_lifted[False]
try:
    result=analysis()
except Exception as e:
    print e