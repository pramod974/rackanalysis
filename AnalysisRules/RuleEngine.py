__author__ = 'pramod.kumar'
#modification 1 -> normal modification
#modification 2-> modification using computeMissingLiftedWeeklyAndDailyOneDay
#modification 3 -> modification using verifyOpeningBalance
import pandas as pd
import calendar
import datetime
import MySQLdb
import os
import sys
import copy
class readDb:
    def __init__(self,tableName,account,terminal,product,dbcon):
        self.tableName = tableName
        self.account=account
        self.terminal=terminal
        self.product=product
        self.dbcon=dbcon
    def fetch_frame(self):
        try:
            sql="select * from %s where account_type='%s' and supplier_terminal_name='%s' and product_name='%s';"%(self.tableName,self.account,self.terminal,self.product)
            db = MySQLdb.connect(self.dbcon[0],self.dbcon[1],self.dbcon[2],self.dbcon[3])
            df2=pd.read_sql(sql, con=db)
            db.close()
            return df2
        except Exception as e:
            if 'db' in locals():
                db.close()
            else:
                print "Exception Oh no Unable to connect to DataBase !"
            print e

class ruleFactory:
    def __init__(self):
        self.ruleAttributes={'pilot':{'chevron':['verifyWeeksByValuesAll','verifyWeekByNRD'],
                                      'exxon':['verifyWeeksByValuesAll','verifyWeekByNRD'],
                                      'holly':['verifyWeeksByValuesAll','verifyWeekByNRD'],
                                      'valero':['verifyWeeksByValuesAll','verifyWeekByNRD'],
                                      'p66':['verifyWeeksByValuesAll','verifyWeekByNRD'],
                                      'tesoro':['verifyWeeksByValuesAll','verifyWeekByNRD']}}

    def fetch_rules(self,customer,supplier):
        if self.ruleAttributes.has_key(customer.lower()):
            if self.ruleAttributes[customer].has_key(supplier.lower()):
                rules=self.ruleAttributes[customer][supplier]
                return rules
            else:
                print 'In valid Supplier ',supplier,' for customer ',customer
                sys.exit(-1)
        else:
            print 'In valid Customer',customer
            sys.exit(-1)
class rules:

    def __init__(self,details):
        errors={}
        self.fileName=""
        self.savepath=r"C:\Users\pramod.kumar\Documents\rackanalysis_proj\rackanalysis\AnalysisRules"+"\\"
        self.customer=details[0]
        self.supplier=details[1]
        self.account=details[2]
        self.terminal=details[3]
        self.product=details[4]
        self.month=details[5]
        self.analysisDate=details[6]
        self.dbcon=details[7]
    def verifyOpeningBalance(self):
        try:
            previousMonth=self.dateDetails.month-1
            previousMonthName=calendar.month_name[previousMonth].lower()
            daysInPreviousMonth=calendar.monthrange(self.dateDetails.year,self.dateDetails.month-1)[1]
            tableName=self.customer+"_"+previousMonthName+"_maxbatch"
            sql="select * from %s where account_type='%s' and supplier_terminal_name='%s' and product_name='%s' and day(execution_date)='%s'"%(tableName,self.account,self.terminal,self.product,daysInPreviousMonth)
            db = MySQLdb.connect(self.dbcon[0],self.dbcon[1],self.dbcon[2],self.dbcon[3])
            dfOpeningBalance=pd.read_sql(sql, con=db)
            db.close()
            lifted_weekly=-1
            lifted_daily=0
            if 'Daily' in dfOpeningBalance['period'].values:
                lifted_daily=dfOpeningBalance.loc[dfOpeningBalance['period']=='Daily','lifted_gallons'].iloc[0]
            if 'Weekly' in dfOpeningBalance['period'].values:
                lifted_weekly=dfOpeningBalance.loc[dfOpeningBalance['period']=='Weekly','lifted_gallons'].iloc[0]
            if lifted_weekly!=-1:
                return {'lifted_daily':lifted_daily,'lifted_weekly':lifted_weekly}
            else:
                print "Unable to fetch Opening Balance"
                return {'lifted_daily':0,'lifted_weekly':0}
        except Exception as e:
            if 'db' in locals():
                db.close()
            else:
                print "Exception Oh no Unable to connect to DataBase !"
            return {'lifted_daily':0,'lifted_weekly':0}
    def computeMissingLiftedWeeklyAndDailyOneDayNRD(self,df2,grp,i):
        if grp.ix[i+1]["('lifted_gallons', 'Weekly')"] != 0 and grp.ix[i+1]["('lifted_gallons', 'Daily')"]!=0:
            if grp.ix[i-1]["('lifted_gallons', 'Weekly')"] != 0 and grp.ix[i-1]["('lifted_gallons', 'Daily')"]!=0:
                newweekly=grp.ix[i+1]["('lifted_gallons', 'Weekly')"]-grp.ix[i+1]["('lifted_gallons', 'Daily')"]
                newlifted=newweekly-grp.ix[i-1]["('lifted_gallons', 'Weekly')"]
                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_NextRefreshDate"]=newlifted
                df2.loc[str(grp.ix[i]['date']),"lifted_gallonsWeekly_modified_aposterioriNrd"]=newweekly
                df2.loc[str(grp.ix[i]['date']),"Modified_LiftedGallonsaposterioriNrd"]=1
                df2.loc[str(grp.ix[i]['date']),"Modified_LGD_NextRefreshDate"]=2
            else:
                print "No Enough Data available to Fill Lifted Gallons for daily and weekly using aposterioriNRD"
        else:
            print "No Enough Data available to Fill Lifted Gallons for daily and weekly using aposterioriNRD"
    def computeMissingLiftedWeeklyAndDailyOneDay(self,df2,grp,i):
        if grp.ix[i+1]["('lifted_gallons', 'Weekly')"] != 0 and grp.ix[i+1]["('lifted_gallons', 'Daily')"]!=0:
            if grp.ix[i-1]["('lifted_gallons', 'Weekly')"] != 0 and grp.ix[i-1]["('lifted_gallons', 'Daily')"]!=0:
                newweekly=grp.ix[i+1]["('lifted_gallons', 'Weekly')"]-grp.ix[i+1]["('lifted_gallons', 'Daily')"]
                newlifted=newweekly-grp.ix[i-1]["('lifted_gallons', 'Weekly')"]
                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]=newlifted
                df2.loc[str(grp.ix[i]['date']),"lifted_gallonsWeekly_modified_aposteriori"]=newweekly
                df2.loc[str(grp.ix[i]['date']),"Modified_LiftedGallonsaposteriori"]=1
                df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=2
            else:
                print "No Enough Data available to Fill Lifted Gallons for daily and weekly using aposterioriWeeklyValues"
        else:
            print "No Enough Data available to Fill Lifted Gallons for daily and weekly using aposterioriWeeklyValues"
    def verifyWeeksByValuesAll(self,df2):
        try:
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Weekly')"),"WeeksByLiftedGallons",0)
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Weekly')"),"lifted_gallonsWeekly_modified_aposteriori",df2["('lifted_gallons', 'Daily')"].values)
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Weekly')"),"Modified_LiftedGallonsaposteriori",0)
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Daily')"),"lifted_gallons_modified_WeeksByLiftedGallons",df2["('lifted_gallons', 'Daily')"].values)
            weeksByValues=[]
            x=df2["('lifted_gallons', 'Weekly')"]
            for i in x.keys()[:-1]:
                if x[i]>x[i+1]:
                    weeksByValues.append(i)
            ct=1
            temp=[]
            wkz=[]
            for i in x.keys()[:-1]:
                if x[i]>x[i+1]:
                    temp.append(i)
                    print i,ct
                    wkz.append(copy.deepcopy(temp))
                    print temp
                    temp=[]
                    ct=ct+1
                else:
                    temp.append(i)
                    print i,ct
            if not(x[i]>x[i+1]):
                print i+1,ct
                temp.append(i+1)
                wkz.append(copy.deepcopy(temp))
                print temp
            else:
                wkz.append([i+1])
            for i in range(len(wkz)):
                self.df2.loc[wkz[i],"WeeksByLiftedGallons"]='w'+str(i+1)
            print df2.loc[:,"WeeksByLiftedGallons"]
            grpNRD=df2.groupby(["WeeksByLiftedGallons"])["('lifted_gallons', 'Weekly')","('lifted_gallons', 'Daily')"]
            df2.insert(df2.keys().get_loc("lifted_gallons_modified_WeeksByLiftedGallons"),"Modified_WeeksByLiftedGallons",0)
            firstNRD=self.df2["WeeksByLiftedGallons"].iloc[0]
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
                                if grp.ix[i]["('lifted_gallons', 'Weekly')"] == 0 and grp.ix[i]["('lifted_gallons', 'Daily')"]==0 and i!=(len(grp)-1):
                                    self.computeMissingLiftedWeeklyAndDailyOneDay(df2,grp,i)
                                else:
                                    print "No Enough Data available to Fill Lifted Gallons"
                        else:
                            # opening balance
                            if key==firstNRD and  firstNRD!=0 :
                                nrdGroups=self.df2["WeeksByLiftedGallons"].value_counts()
                                if nrdGroups[firstNRD]!=7:
                                    openingBalances=self.verifyOpeningBalance()
                                    if openingBalances !={}:
                                        if openingBalances['lifted_weekly'] != 0 and grp.ix[i]["('lifted_gallons', 'Weekly')"] != 0:
                                            if df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]-openingBalances['lifted_weekly']:
                                                modifiedLGD=grp.ix[i]["('lifted_gallons', 'Weekly')"]-openingBalances['lifted_weekly']
                                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]=modifiedLGD
                                                df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=3
                                elif df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]:
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]= grp.ix[i]["('lifted_gallons', 'Weekly')"]
                                    df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=1
                            elif df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]:
                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]= grp.ix[i]["('lifted_gallons', 'Weekly')"]
                                df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=1

                    if key==firstNRD and nrdGroups[firstNRD]!=7:
                        currentMonthCumLGD=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()[-1]
                        presentMonthLastWeeklyLifted=df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"][-1]
                        if (openingBalances['lifted_weekly']+currentMonthCumLGD)==presentMonthLastWeeklyLifted:
                            print "Modification_Successful"
                        else:
                            print "Modification_UnSuccessful"
                        cumSum=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()
                        cumSum=cumSum.apply(lambda x:x+openingBalances['lifted_weekly'])
                        validLiftedGallons=cumSum==df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"]
                        df2.loc[df2["WeeksByLiftedGallons"]==key,"sanityWeekly_CumulativeDaily_WeeksByLiftedGallons"]=validLiftedGallons
                    else:
                        if df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()[-1]==df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"][-1]:
                            print "Modification_Successful"
                        else:
                            print "Modification_UnSuccessful"
                        validLiftedGallons=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"]
                        df2.loc[df2["WeeksByLiftedGallons"]==key,"sanityWeekly_CumulativeDaily_WeeksByLiftedGallons"]=validLiftedGallons
        except Exception as e:
            savepath=self.savepath+"\\"+self.supplier+"\\"+self.month
            file=open(savepath+"\\"+self.fileName+".txt","w")
            file.write("Exception in Combination:"+"\ncustomer : "+self.customer+"\nSupplier : "+self.supplier+"\nAccount : "+self.account+"\nTerminal: "+self.terminal+"\nProduct : "+self.product+"\nException : "+str(e))
            file.close()
            print "Exception in Combination:",self.customer,self.supplier,self.account,self.terminal,self.product,e
            return 0
    def verifyWeekByDailyLiftedValues(self,df2):
        try:
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Weekly')"),"WeeksByLiftedGallons",0)
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Weekly')"),"lifted_gallonsWeekly_modified_aposteriori",df2["('lifted_gallons', 'Daily')"].values)
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Weekly')"),"Modified_LiftedGallonsaposteriori",0)
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
            firstNRD=self.df2["WeeksByLiftedGallons"].iloc[0]
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
                                if grp.ix[i]["('lifted_gallons', 'Weekly')"] == 0 and grp.ix[i]["('lifted_gallons', 'Daily')"]==0 and i!=6:
                                    self.computeMissingLiftedWeeklyAndDailyOneDay(df2,grp,i)
                                else:
                                    print "No Enough Data available to Fill Lifted Gallons"
                        else:
                            # opening balance
                            if key==firstNRD and  firstNRD!=0 :
                                nrdGroups=self.df2["WeeksByLiftedGallons"].value_counts()
                                if nrdGroups[firstNRD]!=7:
                                    openingBalances=self.verifyOpeningBalance()
                                    if openingBalances !={}:
                                        if openingBalances['lifted_weekly'] != 0 and grp.ix[i]["('lifted_gallons', 'Weekly')"] != 0:
                                            if df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]-openingBalances['lifted_weekly']:
                                                modifiedLGD=grp.ix[i]["('lifted_gallons', 'Weekly')"]-openingBalances['lifted_weekly']
                                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]=modifiedLGD
                                                df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=3
                                elif df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]:
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]= grp.ix[i]["('lifted_gallons', 'Weekly')"]
                                    df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=1
                            elif df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]:
                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]= grp.ix[i]["('lifted_gallons', 'Weekly')"]
                                df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=1

                    if key==firstNRD:
                        currentMonthCumLGD=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()[-1]
                        presentMonthLastWeeklyLifted=df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"][-1]
                        if (openingBalances['lifted_weekly']+currentMonthCumLGD)==presentMonthLastWeeklyLifted:
                            print "Modification_Successful"
                        else:
                            print "Modification_UnSuccessful"
                        cumSum=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()
                        cumSum=cumSum.apply(lambda x:x+openingBalances['lifted_weekly'])
                        validLiftedGallons=cumSum==df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"]
                        df2.loc[df2["WeeksByLiftedGallons"]==key,"sanityWeekly_CumulativeDaily_WeeksByLiftedGallons"]=validLiftedGallons
                    else:
                        if df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()[-1]==df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"][-1]:
                            print "Modification_Successful"
                        else:
                            print "Modification_UnSuccessful"
                        validLiftedGallons=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==df2.loc[df2["WeeksByLiftedGallons"]==key,"('lifted_gallons', 'Weekly')"]
                        df2.loc[df2["WeeksByLiftedGallons"]==key,"sanityWeekly_CumulativeDaily_WeeksByLiftedGallons"]=validLiftedGallons
        except Exception as e:
            savepath=self.savepath+"\\"+self.supplier+"\\"+self.month
            file=open(savepath+"\\"+self.fileName+".txt","w")
            file.write("Exception in Combination:"+"\ncustomer : "+self.customer+"\nSupplier : "+self.supplier+"\nAccount : "+self.account+"\nTerminal: "+self.terminal+"\nProduct : "+self.product+"\nException : "+str(e))
            file.close()
            print "Exception in Combination:",self.customer,self.supplier,self.account,self.terminal,self.product,e
            return 0
    def verifyWeekByNRD(self,df2):
        try:
            df2.insert(df2.keys().get_loc("Modified_NRD"),"Week_switch","Unknown")
            count=1
            #Compute Lifted Gallons
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Daily')"),"lifted_gallons_modified_NextRefreshDate",df2["('lifted_gallons', 'Daily')"].values)
            df2.insert(df2.keys().get_loc("lifted_gallons_modified_NextRefreshDate"),"Modified_LGD_NextRefreshDate",0)
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Weekly')"),"lifted_gallonsWeekly_modified_aposterioriNrd",df2["('lifted_gallons', 'Daily')"].values)
            df2.insert(df2.keys().get_loc("('lifted_gallons', 'Weekly')"),"Modified_LiftedGallonsaposterioriNrd",0)
            grpNRD=df2.groupby(["('next_refresh_date', 'Weekly')"])["('lifted_gallons', 'Weekly')","('lifted_gallons', 'Daily')"]
            df2["sanityWeekly_CumulativeDaily_NextRefreshDate"]=0
            #firstNextRefreshDate
            firstNRD=self.df2["('next_refresh_date', 'Weekly')"].iloc[0]
            for key,grp in grpNRD:
                df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"Week_switch"]="w"+str(count)
                count=count+1
                if grp["('lifted_gallons', 'Daily')"].cumsum()[-1]==grp["('lifted_gallons', 'Weekly')"][-1]:
                    print "****Valid Week***\n",grp.loc[:,["('next_refresh_date', 'Weekly')","('lifted_gallons', 'Daily')","('lifted_gallons', 'Weekly')"]]
                    validLiftedGallons=df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"lifted_gallons_modified_NextRefreshDate"].cumsum()==df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"('lifted_gallons', 'Weekly')"]
                    df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"sanityWeekly_CumulativeDaily_NextRefreshDate"]=validLiftedGallons
                else:
                    print "****Invalid Week****\n",grp.loc[:,["('next_refresh_date', 'Weekly')","('lifted_gallons', 'Daily')","('lifted_gallons', 'Weekly')"]]
                    # print key
                    openingBalances={}
                    for i in range(len(grp)):
                        if i!=0:
                            if grp.ix[i-1]["('lifted_gallons', 'Weekly')"] != 0 and grp.ix[i]["('lifted_gallons', 'Weekly')"] != 0:
                                if df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != (grp.ix[i]["('lifted_gallons', 'Weekly')"]-grp.ix[i-1]["('lifted_gallons', 'Weekly')"]):
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_NextRefreshDate"]=grp.ix[i]["('lifted_gallons', 'Weekly')"]-grp.ix[i-1]["('lifted_gallons', 'Weekly')"]
                                    df2.loc[str(grp.ix[i]['date']),"Modified_LGD_NextRefreshDate"]=1
                                    # print i,grp.ix[i+1]["('lifted_gallons', 'Weekly')"]-grp.ix[i]["('lifted_gallons', 'Weekly'
                            else:
                                if grp.ix[i]["('lifted_gallons', 'Weekly')"] == 0 and grp.ix[i]["('lifted_gallons', 'Daily')"]==0 and i!=6:
                                    self.computeMissingLiftedWeeklyAndDailyOneDayNRD(df2,grp,i)
                                else:
                                    print "No Enough Data available to Fill Lifted Gallons NRD"
                        else:
                            # opening balance
                            if key==firstNRD and  firstNRD!=0 :
                                nrdGroups=self.df2["('next_refresh_date', 'Weekly')"].value_counts()
                                if nrdGroups[firstNRD]!=7:
                                    openingBalances=self.verifyOpeningBalance()
                                    if openingBalances !={}:
                                        if openingBalances['lifted_weekly'] != 0 and grp.ix[i]["('lifted_gallons', 'Weekly')"] != 0:
                                            if df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]-openingBalances['lifted_weekly']:
                                                modifiedLGD=grp.ix[i]["('lifted_gallons', 'Weekly')"]-openingBalances['lifted_weekly']
                                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_NextRefreshDate"]=modifiedLGD
                                                df2.loc[str(grp.ix[i]['date']),"Modified_LGD_NextRefreshDate"]=3
                                elif df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]:
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_NextRefreshDate"]= grp.ix[i]["('lifted_gallons', 'Weekly')"]
                                    df2.loc[str(grp.ix[i]['date']),"Modified_LGD_NextRefreshDate"]=1

                            elif df2.loc[str(grp.ix[i]['date']),"('lifted_gallons', 'Daily')"] != grp.ix[i]["('lifted_gallons', 'Weekly')"]:
                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_NextRefreshDate"]= grp.ix[i]["('lifted_gallons', 'Weekly')"]
                                df2.loc[str(grp.ix[i]['date']),"Modified_LGD_NextRefreshDate"]=1

                    if key==firstNRD and nrdGroups[firstNRD]!=7:
                        currentMonthCumLGD=df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"lifted_gallons_modified_NextRefreshDate"].cumsum()[-1]
                        presentMonthLastWeeklyLifted=df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"('lifted_gallons', 'Weekly')"][-1]
                        if (openingBalances['lifted_weekly']+currentMonthCumLGD)==presentMonthLastWeeklyLifted:
                            print "Modification_Successful"
                        else:
                            print "Modification_UnSuccessful"
                        cumSum=df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"lifted_gallons_modified_NextRefreshDate"].cumsum()
                        cumSum=cumSum.apply(lambda x:x+openingBalances['lifted_weekly'])
                        validLiftedGallons=cumSum==df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"('lifted_gallons', 'Weekly')"]
                        df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"sanityWeekly_CumulativeDaily_NextRefreshDate"]=validLiftedGallons
                    else:
                        if df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"lifted_gallons_modified_NextRefreshDate"].cumsum()[-1]==df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"('lifted_gallons', 'Weekly')"][-1]:
                            print "Modification_Successful"
                        else:
                            print "Modification_UnSuccessful"
                        validLiftedGallons=df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"lifted_gallons_modified_NextRefreshDate"].cumsum()==df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"('lifted_gallons', 'Weekly')"]
                        df2.loc[df2["('next_refresh_date', 'Weekly')"]==key,"sanityWeekly_CumulativeDaily_NextRefreshDate"]=validLiftedGallons
        except Exception as e:
            savepath=self.savepath+"\\"+self.supplier+"\\"+self.month
            file=open(savepath+"\\"+self.fileName+".txt","w")
            file.write("Exception in verifyWeekByNRD for Combination:"+"\ncustomer : "+self.customer+"\nSupplier : "+self.supplier+"\nAccount : "+self.account+"\nTerminal: "+self.terminal+"\nProduct : "+self.product+"\nException : "+str(e))
            file.close()
            print "Exception in verifyWeekByNRD in Combination:",self.customer,self.supplier,self.account,self.terminal,self.product,e
            return 0
    def get_filename(self):
            fileName=self.account.replace(' ','').replace(':','')+"_"+self.terminal.replace(' ','').replace(':','')+"_"+self.product.replace(' ','')
            fileName=fileName.replace('/','')
            fileName=fileName.replace('*','')
            fileName=fileName.replace('>','')
            fileName=fileName.replace('<','')
            return fileName

    def executeRules(self):
        try:
            print "***** Execution in Combination:",self.customer,self.supplier,self.account,self.terminal,self.product
            savepath=self.savepath+"\\"+self.supplier+"\\"+self.month
            if not os.path.exists(savepath):
                os.makedirs(savepath)
            self.fileName=self.get_filename()
            tableName=self.customer+"_"+self.supplier+"_"+self.month
            # sql="select * from %s where account_type='%s' and supplier_terminal_name='%s' and product_name='%s';"%(tableName,self.account,self.terminal,self.product)
            # db = MySQLdb.connect(self.dbcon[0],self.dbcon[1],self.dbcon[2],self.dbcon[3])
            # df2=pd.read_sql(sql, con=db)
            readDataBase=readDb(tableName,self.account,self.terminal,self.product,self.dbcon)
            self.df2=readDataBase.fetch_frame()
            dateDetails=datetime.datetime.strptime(self.analysisDate,"%d-%m-%Y")
            self.dateDetails=dateDetails
            self.daysInMonth=calendar.monthrange(dateDetails.year,dateDetails.month)[1]
            idx=pd.date_range(dateDetails.strftime('%m-%d-%Y'),str(dateDetails.month)+'-'+str(self.daysInMonth)+'-'+str(dateDetails.year))
            self.df2['date']= pd.to_datetime(self.df2[u'date'],format='%Y-%m-%d')
            self.df2.index=pd.DatetimeIndex(self.df2['date'])
            self.df2 = self.df2.reindex(idx, fill_value=0)
            self.df2['date']=self.df2.index.values
            self.df2[u"('next_refresh_date', 'Weekly')"]=self.df2[u"('next_refresh_date', 'Weekly')"].apply(lambda x:int(x) if x=='0' else x)
            findWeeks=dict(self.df2[u"('next_refresh_date', 'Weekly')"].value_counts())
            if findWeeks.has_key(0):
                if findWeeks[0]==self.daysInMonth:
                    file=open(savepath+"\\"+self.fileName+".txt","w")
                    file.write("No Weekly Refresh Dates to Compute for combination "+self.fileName+"....\n Exiting Execution.....")
                    file.close()
                    print "No Weekly Refresh Dates to Compute....\n Exiting Execution....."
                    return 0
            validateLiftedValuesWeekly=dict(self.df2["('lifted_gallons', 'Weekly')"].value_counts())
            if validateLiftedValuesWeekly.has_key(0):
                if validateLiftedValuesWeekly[0]==self.daysInMonth:
                    file=open(savepath+"\\"+self.fileName+".txt","w")
                    file.write("No Weekly Lifted gallons to Compute week switches for combination "+self.fileName+"....\n Exiting Execution.....")
                    file.close()
                    print "No Weekly Lifted gallons to Compute week switches....\n Exiting Execution....."
                    return 0
            self.df2[u"('next_refresh_date', 'Weekly')"]=self.df2[u"('next_refresh_date', 'Weekly')"].apply(lambda x:datetime.datetime.strptime(' '.join(x.split(' ')[:2]),'%m/%d %H:%M:%S') if x!=0 else x)
            self.df2[u"('next_refresh_date', 'Weekly')"]=self.df2[u"('next_refresh_date', 'Weekly')"].apply(lambda x:x.replace(year=datetime.datetime.now().year) if x!=0 else x)
            uniqueDates=self.df2[u"('next_refresh_date', 'Weekly')"].unique()
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
            validDatesRowValues=self.df2.loc[self.df2["('next_refresh_date', 'Weekly')"]!=0,"('next_refresh_date', 'Weekly')"]
            validDatesRowValuesPairs=validDatesRowValues.drop_duplicates().to_dict()
            #Reverese the Key Values
            if(len(validDatesRowValues)<self.daysInMonth):
                xcc={}

                for k,v in validDatesRowValuesPairs.iteritems():
                     xcc[v]=k
                if xcc.has_key(0):
                    xcc.pop(0)
                self.df2.insert(self.df2.keys().get_loc("('next_refresh_date', 'Weekly')"),"Modified_NRD",0)
                for validDate in validDates:
                    if self.df2.loc[str(dateDetails.replace(day=validDate[0])),"('next_refresh_date', 'Weekly')"]==0:
                        self.df2.loc[str(dateDetails.replace(day=validDate[0])),"('next_refresh_date', 'Weekly')"]=validDate[1]
                        self.df2.loc[str(dateDetails.replace(day=validDate[0])),'account_type']=self.df2.loc[str(xcc[validDate[1]]),'account_type']
                        self.df2.loc[str(dateDetails.replace(day=validDate[0])),'supplier_terminal_name']=self.df2.loc[str(xcc[validDate[1]]),'supplier_terminal_name']
                        self.df2.loc[str(dateDetails.replace(day=validDate[0])),'product_name']=self.df2.loc[str(xcc[validDate[1]]),'product_name']
                        self.df2.loc[str(dateDetails.replace(day=validDate[0])),"Modified_NRD"]=1
                        # print validDate
                ruleFactoryObj=ruleFactory()
                ruleAttributes=ruleFactoryObj.fetch_rules((self.customer).lower(),(self.supplier).lower())
                for ruleAttribute in ruleAttributes:
                    stat=getattr(self,ruleAttribute)(self.df2)
                    if type(stat) is int:
                        return 0
                # #Find Week_Switch_by weekly values
                # stat=self.verifyWeekByDailyLiftedValues(self.df2)
                # if stat:
                #     return 0
                # #Find Week_Switch_verify LG by NRD
                # stat=self.verifyWeekByNRD(self.df2)
                # if stat:
                #     return 0
                if "('lifted_gallons', 'Monthly')" in self.df2.keys():
                    self.df2["sanityMonthly_CumulativeDaily_WeeksByLiftedGallons"]=0
                    self.df2.loc[self.df2.loc[:,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==self.df2.loc[:,"('lifted_gallons', 'Monthly')"],"sanityMonthly_CumulativeDaily_WeeksByLiftedGallons"]=1
                    self.df2["sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate"]=0
                    self.df2.loc[self.df2.loc[:,"lifted_gallons_modified_NextRefreshDate"].cumsum()==self.df2.loc[:,"('lifted_gallons', 'Monthly')"],"sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate"]=1
                self.df2.to_excel(savepath+"\\"+self.fileName+".xls")
                return self.df2
            else:
                print "All next refresh Dates Valid"
        except Exception as e:
            file=open(savepath+"\\"+self.fileName+".txt","w")
            file.write("Exception in Combination:"+"\ncustomer : "+self.customer+"\nSupplier : "+self.supplier+"\nAccount : "+self.account+"\nTerminal: "+self.terminal+"\nProduct : "+self.product+"\nException : "+str(e))
            file.close()
            print "Exception in Combination:",self.customer,self.supplier,self.account,self.terminal,self.product,e
            return 0


class ruleEngine:
    def __init__(self,details):
        self.customer=details[0]
        self.supplier=details[1]
        self.month=details[2]
        self.analysisDate=details[3]
        self.frames=[]
        self.pivotFrames=[]
        self.savepath=r"C:\Users\pramod.kumar\Documents\rackanalysis_proj\rackanalysis\AnalysisRules"+"\\"+self.supplier+"\\"+self.month+"\\"
        self.dbcon=["172.16.0.55","root","admin123*","rack_analysis"]
        if not os.path.exists(self.savepath):
            os.makedirs(self.savepath)
        sys.stdout = open(self.savepath+'log.txt', 'w')
    def fetchSupplier(self):
       try:
           db = MySQLdb.connect(self.dbcon[0],self.dbcon[1],self.dbcon[2],self.dbcon[3])
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
    def createPivot(self,frame):
        frms=[]
        lgm=frame.loc[frame["Modified_WeeksByLiftedGallons"]==1,["date","account_type","supplier_terminal_name","product_name","lifted_gallons_modified_WeeksByLiftedGallons"]]
        lgm["lifted_cal_type"]="LgWeekValue"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        lgm=frame.loc[frame["Modified_LGD_NextRefreshDate"]==1,["date","account_type","supplier_terminal_name","product_name","lifted_gallons_modified_NextRefreshDate"]]
        lgm["lifted_cal_type"]="LgNRD"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        lgm=frame.loc[(frame["Modified_WeeksByLiftedGallons"]==1)|(frame["Modified_LGD_NextRefreshDate"]==1),["date","account_type","supplier_terminal_name","product_name","('lifted_gallons', 'Daily')"]]
        lgm["lifted_cal_type"]="Actual"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        if len(frms)>0:
            resultNew=pd.concat(frms)
            mp=pd.pivot_table(resultNew,index=["date","account_type","supplier_terminal_name","product_name","lifted_cal_type"])
            self.pivotFrames.append(mp)
    def createPivotAll(self,frame):
        frms=[]
        lgm=frame.loc[frame["Modified_LGD_NextRefreshDate"]!=frame["Modified_WeeksByLiftedGallons"],["date","account_type","supplier_terminal_name","product_name","lifted_gallons_modified_WeeksByLiftedGallons"]]
        lgm["lifted_cal_type"]="LgWeekValue"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        lgm=frame.loc[frame["Modified_LGD_NextRefreshDate"]!=frame["Modified_WeeksByLiftedGallons"],["date","account_type","supplier_terminal_name","product_name","lifted_gallons_modified_NextRefreshDate"]]
        lgm["lifted_cal_type"]="LgNRD"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        lgm=frame.loc[frame["Modified_LGD_NextRefreshDate"]!=frame["Modified_WeeksByLiftedGallons"],["date","account_type","supplier_terminal_name","product_name","('lifted_gallons', 'Daily')"]]
        lgm["lifted_cal_type"]="Actual"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        if len(frms)>0:
            resultNew=pd.concat(frms)
            mp=pd.pivot_table(resultNew,index=["date","account_type","supplier_terminal_name","product_name","lifted_cal_type"])
            self.pivotFrames.append(mp)
    def runRules(self,suppliersCombinations=[]):
        try:
            if suppliersCombinations==[]:
                suppliersCombinations=self.fetchSupplier()
            for supplierInfo in suppliersCombinations:
                details=[self.customer,self.supplier,supplierInfo[0],supplierInfo[1],supplierInfo[2],self.month,self.analysisDate,self.dbcon]
                supplierRule=rules(details)
                frame=supplierRule.executeRules()
                if type(frame) != int:
                    self.frames.append(frame)
                    self.createPivotAll(frame)

            if len(self.pivotFrames)>0:
                resultNew=pd.concat(self.pivotFrames)
                resultNew.to_excel(self.savepath+self.supplier+"_"+self.month+"_reconciledPivotAll.xls")
            result=pd.concat(self.frames)
            db = MySQLdb.connect(self.dbcon[0],self.dbcon[1],self.dbcon[2],self.dbcon[3])
            result.to_sql(name=self.customer+"_"+self.supplier+"_"+self.month+"_reconciled",con=db,flavor='mysql', if_exists='replace')
            # resultNew.to_sql(name=self.customer+"_"+self.supplier+"_reconciledPivot",con=db,flavor='mysql', if_exists='replace')
            db.close()
            result.to_excel(self.savepath+self.supplier+"_"+self.month+"_reconciled.xls")

        except Exception as e:
            print e
if __name__ == "__main__":
    # 'BP','Holly','Chevron','Exxon','Valero','P66','Tesoro'
    # suppliers=['Holly','Valero','P66','Tesoro']
    suppliers=['Tesoro']
    print "Execution Started Please Wait....."
    for i in suppliers:
        detailedList=["pilot",i,"july",'01-07-2015']
        executeEngine=ruleEngine(detailedList)
        # executeEngine.runRules((('PILOT TRAVEL CENTERS LLC : CHV7460761', '1037 PASCAGOULA MS TRM CHEVRON', 'DIESEL #2'),))
        # executeEngine.runRules((('PILOT TRAVEL CENTERS LLC-10024029BR', 'EVANSVILLE WY SI - 0339', 'DISTILLATES'),))
        executeEngine.runRules()
    print "Completed Execution Thanks for you Patience "