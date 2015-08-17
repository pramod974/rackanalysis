__author__ = 'pramod.kumar'
import pandas as pd
import calendar
import datetime
import MySQLdb
import os
class rules:

    def __init__(self,details):
        errors={}
        self.filename=""
        self.savepath=r"C:\Users\pramod.kumar\Documents\rackanalysis_proj\rackanalysis\AnalysisRules"+"\\"
        self.customer=details[0]
        self.supplier=details[1]
        self.account=details[2]
        self.terminal=details[3]
        self.product=details[4]
        self.month=details[5]
        self.analysisDate=details[6]
    def verifyWeekByDailyLiftedValues(self,df2):
        try:
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Weekly')"),"WeeksByLiftedGallons",0)
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Daily')"),"lifted_gallons_modified_WeeksByLiftedGallons",df2["('lifted_gallons', 'Daily')"].values)
            weeksByValues=[]
            x=df2["('lifted_gallons', 'Weekly')"]
            for i in x.keys()[:-1]:
                if x[i]>x[i+1]:
                    weeksByValues.append(i)
            # weeksByValues[0]=weeksByValues[0].replace(day=4)
            tt=[]
            tt.append(pd.date_range(weeksByValues[0].replace(day=1), periods=weeksByValues[0].day).tolist())
            print tt
            while (tt[-1][-1].day != int(self.daysInMonth)):
                startDay=(tt[-1][-1]+datetime.timedelta(1)).day
                # print startDay
                period=7 if int(self.daysInMonth)-startDay >= 7 else (int(self.daysInMonth)-startDay)+1
                # print period
                tt.append(pd.date_range(tt[-1][-1]+datetime.timedelta(1), periods=period).tolist())
                # print tt
            count =1
            for i in tt:
                for k in i:
                    df2.loc[k,"WeeksByLiftedGallons"]='w'+str(count)
                count=count+1
            print df2.loc[:,"WeeksByLiftedGallons"]
            grpNRD=df2.groupby(["WeeksByLiftedGallons"])["('lifted_gallons', 'Weekly')","('lifted_gallons', 'Daily')"]
            df2.insert(df2.keys().get_loc("lifted_gallons_modified_WeeksByLiftedGallons"),"Modified_WeeksByLiftedGallons",0)
            for key,grp in grpNRD:
                if grp["('lifted_gallons', 'Daily')"].cumsum()[-1]==grp["('lifted_gallons', 'Weekly')"][-1]:
                    print "****Valid Week***\n",grp.loc[:,["WeeksByLiftedGallons","('lifted_gallons', 'Daily')","('lifted_gallons', 'Weekly')"]]
                    validLiftedGallons=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"]
                    df2.loc[df2["WeeksByLiftedGallons"]==key,"sanityWeekly_CumulativeDaily_WeeksByLiftedGallons"]=validLiftedGallons
                else:
                    print "****Invalid Week****\n",grp.loc[:,["WeeksByLiftedGallons","('lifted_gallons', 'Daily')","('lifted_gallons', 'Weekly')"]]
                    for i in range(len(grp)):
                        if i!=0:
                            if grp.ix[i-1]["('lifted_gallons', 'Weekly')"] != 0 and grp.ix[i]["('lifted_gallons', 'Weekly')"] != 0:
                                if df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != (grp.ix[i]["('lifted_gallons', 'Weekly')"]-grp.ix[i-1]["('lifted_gallons', 'Weekly')"]):
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]=grp.ix[i]["('lifted_gallons', 'Weekly')"]-grp.ix[i-1]["('lifted_gallons', 'Weekly')"]
                                    df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=1
                                    # print i,grp.ix[i+1]["('lifted_gallons', 'Weekly')"]-grp.ix[i]["('lifted_gallons', 'Weekly')"]
                            else:
                                print "No Enough Data available to Fill Lifted Gallons"
                        else:
                            if df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]:
                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]= grp.ix[i]["('lifted_gallons', 'Weekly')"]
                                df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=1
                    if df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()[-1]==df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"][-1]:
                        print "Modification_Successful"
                    else:
                        print "Modification_UnSuccessful"
                    validLiftedGallons=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"]
                    df2.loc[df2["WeeksByLiftedGallons"]==key,"sanityWeekly_CumulativeDaily_WeeksByLiftedGallons"]=validLiftedGallons
        except Exception as e:
            print e
    def executeRules(self):
        try:
            print "***** Execution in Combination:",self.customer,self.supplier,self.account,self.terminal,self.product
            savepath=self.savepath+"\\"+self.supplier+"\\"+self.month
            if not os.path.exists(savepath):
                os.makedirs(savepath)
            fileName=self.account.replace(' ','').replace(':','')+"_"+self.terminal.replace(' ','').replace(':','')+"_"+self.product.replace(' ','')
            fileName=fileName.replace('/','')
            fileName=fileName.replace('*','')
            fileName=fileName.replace('>','')
            fileName=fileName.replace('<','')
            tableName=self.customer+"_"+self.supplier+"_"+self.month
            sql="select * from %s where account_type='%s' and supplier_terminal_name='%s' and product_name='%s';"%(tableName,self.account,self.terminal,self.product)
            db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
            df2=pd.read_sql(sql, con=db)
            dateDetails=datetime.datetime.strptime(self.analysisDate,"%d-%m-%Y")
            self.daysInMonth=calendar.monthrange(dateDetails.year,dateDetails.month)[1]
            idx=pd.date_range(dateDetails.strftime('%m-%d-%Y'),str(dateDetails.month)+'-'+str(self.daysInMonth)+'-'+str(dateDetails.year))
            df2['date']= pd.to_datetime(df2[u'date'],format='%Y-%m-%d')
            df2.index=pd.DatetimeIndex(df2['date'])
            df2 = df2.reindex(idx, fill_value=0)
            df2['date']=df2.index.values
            df2[u"('next_refresh_date', 'Weekly')"]=df2[u"('next_refresh_date', 'Weekly')"].apply(lambda x:int(x) if x=='0' else x)
            findWeeks=dict(df2[u"('next_refresh_date', 'Weekly')"].value_counts())
            if findWeeks.has_key(0):
                if findWeeks[0]==self.daysInMonth:
                    file=open(savepath+"\\"+fileName+".txt","w")
                    file.write("No Weekly Refresh Dates to Compute for combination "+fileName+"....\n Exiting Execution.....")
                    file.close()
                    print "No Weekly Refresh Dates to Compute....\n Exiting Execution....."
                    return 0
            validateLiftedValuesWeekly=dict(df2["('lifted_gallons', 'Weekly')"].value_counts())
            if validateLiftedValuesWeekly.has_key(0):
                if validateLiftedValuesWeekly[0]==self.daysInMonth:
                    file=open(savepath+"\\"+fileName+".txt","w")
                    file.write("No Weekly Lifted gallons to Compute week switches for combination "+fileName+"....\n Exiting Execution.....")
                    file.close()
                    print "No Weekly Lifted gallons to Compute week switches....\n Exiting Execution....."
                    return 0
            df2[u"('next_refresh_date', 'Weekly')"]=df2[u"('next_refresh_date', 'Weekly')"].apply(lambda x:datetime.datetime.strptime(' '.join(x.split(' ')[:2]),'%m/%d %H:%M:%S') if x!=0 else x)
            df2[u"('next_refresh_date', 'Weekly')"]=df2[u"('next_refresh_date', 'Weekly')"].apply(lambda x:x.replace(year=datetime.datetime.now().year) if x!=0 else x)
            uniqueDates=df2[u"('next_refresh_date', 'Weekly')"].unique()
            uniqueDatesList=uniqueDates.tolist()
            uniqueDatesList.remove(0)
            # print "********************Initial**************************",df2
            validDates=[]
            for i in uniqueDatesList:
                newDate=i-datetime.timedelta(7)
                # print newDate,i
                if newDate.month<dateDetails.month:
                    for a in range(1,i.day):
                        validDates.append([a,i])
                        # print a,i;
                elif i.month>uniqueDatesList[0].month:
                    for a in range(newDate.day,self.daysInMonth+1):
                        validDates.append([a,i])
                        # print a,i;
                else:
                    for a in range(newDate.day,i.day):
                        validDates.append([a,i])
                        # print a,i;
            #fetech rows with valid Next_Refresh_Dates
            validDatesRowValues=df2.loc[df2["('next_refresh_date', 'Weekly')"]!=0,"('next_refresh_date', 'Weekly')"]
            validDatesRowValuesPairs=validDatesRowValues.drop_duplicates().to_dict()
            #Reverese the Key Values
            if(len(validDatesRowValues)<self.daysInMonth):
                xcc={}

                for k,v in validDatesRowValuesPairs.iteritems():
                     xcc[v]=k
                if xcc.has_key(0):
                    xcc.pop(0)
                df2.insert(df2.keys().get_loc("('next_refresh_date', 'Weekly')"),"Modified_NRD",0)
                for validDate in validDates:
                    if df2.loc[str(dateDetails.replace(day=validDate[0])),"('next_refresh_date', 'Weekly')"]==0:
                        df2.loc[str(dateDetails.replace(day=validDate[0])),"('next_refresh_date', 'Weekly')"]=validDate[1]
                        df2.loc[str(dateDetails.replace(day=validDate[0])),'account_type']=df2.loc[str(xcc[validDate[1]]),'account_type']
                        df2.loc[str(dateDetails.replace(day=validDate[0])),'supplier_terminal_name']=df2.loc[str(xcc[validDate[1]]),'supplier_terminal_name']
                        df2.loc[str(dateDetails.replace(day=validDate[0])),'product_name']=df2.loc[str(xcc[validDate[1]]),'product_name']
                        df2.loc[str(dateDetails.replace(day=validDate[0])),"Modified_NRD"]=1
                        # print validDate
                self.verifyWeekByDailyLiftedValues(df2)
                #Find Week_Switch
                df2.insert(df2.keys().get_loc("Modified_NRD"),"Week_switch","Unknown")
                count=1
                #Compute Lifted Gallons
                df2.insert(df2.keys().get_loc("('lifted_gallons', 'Daily')"),"lifted_gallons_modified_NextRefreshDate",df2["('lifted_gallons', 'Daily')"].values)
                df2.insert(df2.keys().get_loc("lifted_gallons_modified_NextRefreshDate"),"Modified_LGD_NextRefreshDate",0)
                grpNRD=df2.groupby(["('next_refresh_date', 'Weekly')"])["('lifted_gallons', 'Weekly')","('lifted_gallons', 'Daily')"]
                df2["sanityWeekly_CumulativeDaily_NextRefreshDate"]=0
                for key,grp in grpNRD:
                    # print key
                    df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"Week_switch"]="w"+str(count)
                    count=count+1
                    for i in range(1,len(grp)):
                        if i!=0:
                            if grp.ix[i-1]["('lifted_gallons', 'Weekly')"] != 0 and grp.ix[i]["('lifted_gallons', 'Weekly')"] != 0:
                                if df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != (grp.ix[i]["('lifted_gallons', 'Weekly')"]-grp.ix[i-1]["('lifted_gallons', 'Weekly')"]):
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_NextRefreshDate"]=grp.ix[i]["('lifted_gallons', 'Weekly')"]-grp.ix[i-1]["('lifted_gallons', 'Weekly')"]
                                    df2.loc[str(grp.ix[i]['date']),"Modified_LGD_NextRefreshDate"]=1
                                    # print i,grp.ix[i+1]["('lifted_gallons', 'Weekly')"]-grp.ix[i]["('lifted_gallons', 'Weekly'
                            else:
                                print "No Data To Fill"
                        else:
                            if df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]:
                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_NextRefreshDate"]= grp.ix[i]["('lifted_gallons', 'Weekly')"]
                                df2.loc[str(grp.ix[i]['date']),"Modified_LGD_NextRefreshDate"]=1
                    if df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"lifted_gallons_modified_NextRefreshDate"].cumsum()[-1]==df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"('lifted_gallons', 'Weekly')"][-1]:
                        print "Modification_Successful"
                    else:
                        print "Modification_UnSuccessful"
                    validLiftedGallons=df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"lifted_gallons_modified_NextRefreshDate"].cumsum()==df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"('lifted_gallons', 'Weekly')"]
                    df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"sanityWeekly_CumulativeDaily_NextRefreshDate"]=validLiftedGallons
                # fileName=self.account.replace(' ','').replace(':','')+"_"+self.terminal.replace(' ','').replace(':','')+"_"+self.product.replace(' ','')
                # if not os.path.exists(savepath):
                #     os.makedirs(savepath)
                if "('lifted_gallons', 'Monthly')" in df2.keys():
                    df2["sanityMonthly_CumulativeDaily_WeeksByLiftedGallons"]=0
                    df2.loc[df2.loc[:,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==df2.loc[:,"('lifted_gallons', 'Monthly')"],"sanityMonthly_CumulativeDaily_WeeksByLiftedGallons"]=1
                    df2["sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate"]=0
                    df2.loc[df2.loc[:,"lifted_gallons_modified_NextRefreshDate"].cumsum()==df2.loc[:,"('lifted_gallons', 'Monthly')"],"sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate"]=1
                df2.to_excel(savepath+"\\"+fileName+".xls")
                db.close()
                return df2
            else:
                print "All next refresh Dates Valid"
        except Exception as e:
            print "Exception in Combination:",self.customer,self.supplier,self.account,self.terminal,self.product,e


class ruleEngine:
    def __init__(self,details):
        self.customer=details[0]
        self.supplier=details[1]
        self.month=details[2]
        self.analysisDate=details[3]
        self.frames=[]

    def fetchSupplier(self):
       try:
           db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
           cursor=db.cursor()
           tableName=self.customer+"_"+self.supplier+"_"+self.month
           sql="""select distinct Account_Type,supplier_terminal_name,product_name from %s where Account_Type<>"Unknown" and supplier_terminal_name<>"Unknown" and product_name<>"Unknown";"""%tableName
           cursor.execute(sql)
           suppliers=cursor.fetchall()
           return suppliers
           db.close()
       except Exception as e:
           db.close()
           print e
    def runRules(self):
        try:
            suppliers=self.fetchSupplier()
            for supplierInfo in suppliers:
                details=[self.customer,self.supplier,supplierInfo[0],supplierInfo[1],supplierInfo[2],self.month,self.analysisDate]
                supplierRule=rules(details)
                frame=supplierRule.executeRules()
                if type(frame) != int:
                    self.frames.append(frame)
            result=pd.concat(self.frames)
            db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
            result.to_sql(name=self.customer+"_"+self.supplier+"_reconsiled",con=db,flavor='mysql', if_exists='append')
            db.close()
            result.to_excel(self.supplier+"_reconsiled.xls")
        except Exception as e:
            print e
# 'BP','Holly','Chevron','Exxon','Valero','P66','Tesoro'
suppliers=['BP','Holly']
for i in suppliers:
    detailedList=["pilot",i,"june",'01-06-2015']
    executeEngine=ruleEngine(detailedList)
    executeEngine.runRules()
