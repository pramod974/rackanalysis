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
import entities
import time
import dateutil
class readDb:
    def __init__(self,tableName,account,terminal,product,dbcon):
        self.tableName = tableName
        self.appConst.account=account
        self.appConst.terminal=terminal
        self.appConst.product=product
        self.appConst.dbcon=dbcon
    def fetch_frame(self):
        try:
            sql="select * from %s where account_type='%s' and supplier_terminal_name='%s' and product_name='%s';"%(self.tableName,self.appConst.account,self.appConst.terminal,self.appConst.product)
            # db = MySQLdb.connect(self.appConst.dbcon[0],self.appConst.dbcon[1],self.appConst.dbcon[2],self.appConst.dbcon[3])
            df2=pd.read_sql(sql, con=self.appConst.dbcon)
            # db.close()
            return df2
        except Exception as e:
            if 'db' in locals():
                self.appConst.dbcon.close()
            else:
                print "Exception Oh no Unable to connect to DataBase !"
            print e

class ruleFactory:
    def __init__(self):
        self.ruleAttributes={'pilot':{'chevron':['verifyWeeksByValuesAll','reconcileUsingMonthy','verifyWeekByNRD'],
                                      'exxon':['verifyWeeksByValuesAll','reconcileUsingMonthy','verifyWeekByNRD'],
                                      'holly':['verifyWeeksByValuesAll','reconcileUsingMonthy','verifyWeekByNRD'],
                                      'valero':['verifyWeeksByValuesAll','reconcileUsingMonthy','verifyWeekByNRD'],
                                      'p66':['verifyWeeksByValuesAll','reconcileUsingMonthy','verifyWeekByNRD'],
                                      'tesoro':['verifyWeeksByValuesAll','reconcileUsingMonthy','verifyWeekByNRD']}}

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
    def __init__(self,appConst):
        errors={}
        self.fileName=""
        self.appConst=appConst
        # self.appConst.customer=details[0]
        # self.appConst.supplier=details[1]
        # self.appConst.account=details[2]
        # self.appConst.terminal=details[3]
        # self.appConst.product=details[4]
        # self.appConst.month=details[5]
        # self.appConst.analysisDate=details[6]
        # self.appConst.db=details[7]
        # self.appConst.savelocation=details[8]
        # self.appConst.mp=details[9]
        # self.appConst.savepath=self.appConst.savelocation+"\\"+self.appConst.supplier+"\\"+self.appConst.month

    def verifyOpeningBalance(self):
        try:
            previousMonth=self.appConst.dateDetails.month-1
            previousMonthName=calendar.month_name[previousMonth].lower()
            daysInPreviousMonth=calendar.monthrange(self.appConst.dateDetails.year,self.appConst.dateDetails.month-1)[1]
            # tableName=self.appConst.customer+"_"+previousMonthName+"_maxbatch"
            # sql="select * from %s where account_type='%s' and supplier_terminal_name='%s' and product_name='%s' and day(execution_date)='%s'"%(tableName,self.appConst.account,self.appConst.terminal,self.appConst.product,daysInPreviousMonth)
            # dbcon=["172.16.0.55","root","admin123*","rules_spark"]
            # db = MySQLdb.connect(self.appConst.dbcon[0],self.appConst.dbcon[1],self.appConst.dbcon[2],self.appConst.dbcon[3])
            # dfOpeningBalance=pd.read_sql(sql, con=db)
            previousMonthexecutionFrom=(self.appConst.dateDetails-dateutil.relativedelta.relativedelta(months=1)).strftime("%Y-%m-%d %H:%M:%S")

            previousMonthexecutionTo=self.appConst.dateDetails.strftime("%Y-%m-%d %H:%M:%S")
            dfOpeningBalance=copy.deepcopy(self.appConst.mp.loc[(self.appConst.mp["account_type"]==self.appConst.account)&(self.appConst.mp["supplier_terminal_name"]==self.appConst.terminal)&(self.appConst.mp["product_name"]==self.appConst.product)&(self.appConst.mp["date"]==(self.appConst.dateDetails.replace(month=previousMonth,day=daysInPreviousMonth)).strftime("%Y-%m-%d")),:])
            lifted_weekly=-1
            lifted_daily=0
            if len(dfOpeningBalance)>0:
                lifted_daily=dfOpeningBalance["lifted_gallons_Daily"][0]
                lifted_weekly=dfOpeningBalance["lifted_gallons_Weekly"][0]
                print "Opening Balance Lifted_Daily",lifted_daily
                print "Opening Balance Lifted_Weekly",lifted_weekly
                return {'lifted_daily':lifted_daily,'lifted_weekly':lifted_weekly}
            else:
                print "Unable to fetch Opening Balance"
                return {'lifted_daily':0,'lifted_weekly':0}
        except Exception as e:
            return {'lifted_daily':0,'lifted_weekly':0}
    def computeMissingLiftedWeeklyAndDailyOneDayNRD(self,df2,grp,i):
        if grp.ix[i+1]["lifted_gallons_Weekly"] != 0 and grp.ix[i+1]["lifted_gallons_Daily"]!=0:
            if grp.ix[i-1]["lifted_gallons_Weekly"] != 0 and grp.ix[i-1]["lifted_gallons_Daily"]!=0:
                newweekly=grp.ix[i+1]["lifted_gallons_Weekly"]-grp.ix[i+1]["lifted_gallons_Daily"]
                newlifted=newweekly-grp.ix[i-1]["lifted_gallons_Weekly"]
                # if df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"] !=newweekly:
                #     if df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]==1:
                #         print "Weekly has a different value for Lifted Daily compared to Monthly"
                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"]=newlifted
                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]=4
                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Weekly"]=newweekly
                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_weekly_flag"]=4


            else:
                print "No Enough Data available to Fill Lifted Gallons for daily and weekly using aposterioriNRD"
        else:
            print "No Enough Data available to Fill Lifted Gallons for daily and weekly using aposterioriNRD"
    def computeMissingLiftedWeeklyAndDailyOneDayWeekValue(self,df2,grp,i):
        if grp.ix[i+1]["lifted_gallons_Weekly"] != 0 and grp.ix[i+1]["lifted_gallons_Daily"]!=0:
            if grp.ix[i-1]["lifted_gallons_Weekly"] != 0 and grp.ix[i-1]["lifted_gallons_Daily"]!=0:
                newweekly=grp.ix[i+1]["lifted_gallons_Weekly"]-grp.ix[i+1]["lifted_gallons_Daily"]
                newlifted=newweekly-grp.ix[i-1]["lifted_gallons_Weekly"]
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
            df2.insert(df2.keys().get_loc("lifted_gallons_Weekly"),"WeeksByLiftedGallons",0)
            df2.insert(df2.keys().get_loc("lifted_gallons_Weekly"),"lifted_gallonsWeekly_modified_aposteriori",df2["lifted_gallons_Daily"].values)
            df2.insert(df2.keys().get_loc("lifted_gallons_Weekly"),"Modified_LiftedGallonsaposteriori",0)
            df2.insert(df2.keys().get_loc("lifted_gallons_Daily"),"lifted_gallons_modified_WeeksByLiftedGallons",df2["lifted_gallons_Daily"].values)
            weeksByValues=[]
            x=df2["lifted_gallons_Weekly"]
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
            grpNRD=df2.groupby(["WeeksByLiftedGallons"])["lifted_gallons_Weekly","lifted_gallons_Daily"]
            df2.insert(df2.keys().get_loc("lifted_gallons_modified_WeeksByLiftedGallons"),"Modified_WeeksByLiftedGallons",0)
            firstNRD=self.df2["WeeksByLiftedGallons"].iloc[0]
            for key,grp in grpNRD:
                if grp["lifted_gallons_Daily"].cumsum()[-1]==grp["lifted_gallons_Weekly"][-1]:
                    print "****Valid Week***\n",grp.loc[:,["WeeksByLiftedGallons","lifted_gallons_Daily","lifted_gallons_Weekly"]]
                    validLiftedGallons=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_Weekly"]
                    df2.loc[df2["WeeksByLiftedGallons"]==key,"sanityWeekly_CumulativeDaily_WeeksByLiftedGallons"]=validLiftedGallons
                else:
                    print "****Invalid Week****\n",grp.loc[:,["WeeksByLiftedGallons","lifted_gallons_Daily","lifted_gallons_Weekly"]]
                    for i in range(len(grp)):
                        if i!=0:
                            if grp.ix[i-1]["lifted_gallons_Weekly"] != 0 and grp.ix[i]["lifted_gallons_Weekly"] != 0:
                                if df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"] != (grp.ix[i]["lifted_gallons_Weekly"]-grp.ix[i-1]["lifted_gallons_Weekly"]):
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]=grp.ix[i]["lifted_gallons_Weekly"]-grp.ix[i-1]["lifted_gallons_Weekly"]
                                    df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=1
                                    # print i,grp.ix[i+1]["lifted_gallons_Weekly"]-grp.ix[i]["lifted_gallons_Weekly"]
                            else:
                                if grp.ix[i]["lifted_gallons_Weekly"] == 0 and grp.ix[i]["lifted_gallons_Daily"]==0 and i!=(len(grp)-1):
                                    self.computeMissingLiftedWeeklyAndDailyOneDayWeekValue(df2,grp,i)
                                else:
                                    print "No Enough Data available to Fill Lifted Gallons"
                        else:
                            # opening balance
                            if key==firstNRD and  firstNRD!=0 :
                                openingBalances={}
                                nrdGroups=self.df2["WeeksByLiftedGallons"].value_counts()
                                if nrdGroups[firstNRD]!=7:
                                    openingBalances=self.verifyOpeningBalance()
                                    if openingBalances !={}:
                                        if openingBalances['lifted_weekly'] != 0 and grp.ix[i]["lifted_gallons_Weekly"] != 0:
                                            if df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"] != grp.ix[i]["lifted_gallons_Weekly"]-openingBalances['lifted_weekly']:
                                                modifiedLGD=grp.ix[i]["lifted_gallons_Weekly"]-openingBalances['lifted_weekly']
                                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]=modifiedLGD
                                                df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=3
                                elif df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"] != grp.ix[i]["lifted_gallons_Weekly"]:
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]= grp.ix[i]["lifted_gallons_Weekly"]
                                    df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=1
                            elif df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"] != grp.ix[i]["lifted_gallons_Weekly"]:
                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]= grp.ix[i]["lifted_gallons_Weekly"]
                                df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=1

                    if key==firstNRD and nrdGroups[firstNRD]!=7:
                        currentMonthCumLGD=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()[-1]
                        presentMonthLastWeeklyLifted=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_Weekly"][-1]
                        if (openingBalances['lifted_weekly']+currentMonthCumLGD)==presentMonthLastWeeklyLifted:
                            print "Modification_Successful"
                        else:
                            print "Modification_UnSuccessful"
                        cumSum=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()
                        cumSum=cumSum.apply(lambda x:x+openingBalances['lifted_weekly'])
                        validLiftedGallons=cumSum==df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_Weekly"]
                        df2.loc[df2["WeeksByLiftedGallons"]==key,"sanityWeekly_CumulativeDaily_WeeksByLiftedGallons"]=validLiftedGallons
                    else:
                        if df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()[-1]==df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_Weekly"][-1]:
                            print "Modification_Successful"
                        else:
                            print "Modification_UnSuccessful"
                        validLiftedGallons=df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==df2.loc[df2["WeeksByLiftedGallons"]==key,"lifted_gallons_Weekly"]
                        df2.loc[df2["WeeksByLiftedGallons"]==key,"sanityWeekly_CumulativeDaily_WeeksByLiftedGallons"]=validLiftedGallons
        except Exception as e:
            savepath=self.appConst.savepath+"\\"+"Exception"
            if not os.path.exists(savepath):
                os.makedirs(savepath)
            file=open(savepath+"\\"+self.fileName+".txt","w")
            file.write("Exception in Combination:"+"\ncustomer : "+self.appConst.customer+"\nSupplier : "+self.appConst.supplier+"\nAccount : "+self.appConst.account+"\nTerminal: "+self.appConst.terminal+"\nProduct : "+self.appConst.product+"\nException : "+str(e))
            file.close()
            print "Exception in Combination:",self.appConst.customer,self.appConst.supplier,self.appConst.account,self.appConst.terminal,self.appConst.product,e
            return 0
       
    def verifyWeekByNRD(self,df2):
        try:
            df2.insert(df2.keys().get_loc("Modified_NRD"),"Week_switch","Unknown")
            count=1
            #Compute Lifted Gallons
            # df2.insert(df2.keys().get_loc("lifted_gallons_Daily"),"lifted_gallons_daily_modified",df2["lifted_gallons_Daily"].values)
            # df2.insert(df2.keys().get_loc("lifted_gallons_daily_modified"),"lifted_gallons_daily_flag",0)
            # df2.insert(df2.keys().get_loc("lifted_gallons_Weekly"),"lifted_gallons_weekly_modified",df2["lifted_gallons_Daily"].values)
            # df2.insert(df2.keys().get_loc("lifted_gallons_Weekly"),"lifted_gallons_weekly_flag",0)
            grpNRD=df2.groupby(["next_refresh_date_Weekly"])["lifted_gallons_Weekly","lifted_gallons_Daily"]
            df2["sanityWeekly_CumulativeDaily_NextRefreshDate"]=0
            #firstNextRefreshDate
            firstNRD=self.df2["next_refresh_date_Weekly"].iloc[0]
            for key,grp in grpNRD:
                df2.loc[df2["next_refresh_date_Weekly"]==key,"Week_switch"]="w"+str(count)
                count=count+1
                if grp["lifted_gallons_Daily"].cumsum()[-1]==grp["lifted_gallons_Weekly"][-1]:
                    print "****Valid Week***\n",grp.loc[:,["next_refresh_date_Weekly","lifted_gallons_Daily","lifted_gallons_Weekly"]]
                    validLiftedGallons=df2.loc[df2["next_refresh_date_Weekly"]==key,"lifted_gallons_daily_modified"].cumsum()==df2.loc[df2["next_refresh_date_Weekly"]==key,"lifted_gallons_Weekly"]
                    df2.loc[df2["next_refresh_date_Weekly"]==key,"sanityWeekly_CumulativeDaily_NextRefreshDate"]=validLiftedGallons
                else:
                    print "****Invalid Week****\n",grp.loc[:,["next_refresh_date_Weekly","lifted_gallons_Daily","lifted_gallons_Weekly"]]
                    # print key
                    # opening balance
                    # openingBalances={'lifted_daily':0,'lifted_weekly':0}
                    nrdGroups=self.df2["next_refresh_date_Weekly"].value_counts()
                    openingBalances={}
                    for i in range(len(grp)):
                        if df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]==0:
                            if i!=0:
                                if grp.ix[i-1]["lifted_gallons_Weekly"] != 0 and grp.ix[i]["lifted_gallons_Weekly"] != 0:

                                    if df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"] != (grp.ix[i]["lifted_gallons_Weekly"]-grp.ix[i-1]["lifted_gallons_Weekly"]):
                                        df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"]=grp.ix[i]["lifted_gallons_Weekly"]-grp.ix[i-1]["lifted_gallons_Weekly"]
                                        df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]=3

                                else:
                                    if grp.ix[i]["lifted_gallons_Weekly"] == 0 and grp.ix[i]["lifted_gallons_Daily"]==0 and i!=(len(grp)-1):
                                        self.computeMissingLiftedWeeklyAndDailyOneDayNRD(df2,grp,i)
                                    else:
                                        print "No Enough Data available to Fill Lifted Gallons NRD"
                            else:

                                if key==firstNRD and  firstNRD!=0 :

                                    if nrdGroups[firstNRD]!=7:
                                        openingBalances=self.verifyOpeningBalance()
                                        if openingBalances !={}:
                                            if openingBalances['lifted_weekly'] != 0 and grp.ix[i]["lifted_gallons_Weekly"] != 0:
                                                if df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"] != grp.ix[i]["lifted_gallons_Weekly"]-openingBalances['lifted_weekly']:
                                                    modifiedLGD=grp.ix[i]["lifted_gallons_Weekly"]-openingBalances['lifted_weekly']
                                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"]=modifiedLGD
                                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]=5
                                    elif df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"] != grp.ix[i]["lifted_gallons_Weekly"]:
                                        df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"]= grp.ix[i]["lifted_gallons_Weekly"]
                                        df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]=3

                                elif df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"] != grp.ix[i]["lifted_gallons_Weekly"]:
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"]= grp.ix[i]["lifted_gallons_Weekly"]
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]=3
                        else:
                                    print "The Lifted Gallons cant be modified using weekly Lifted, as it is already Modified using Monthly values "
                    if key==firstNRD and nrdGroups[firstNRD]!=7 and openingBalances!={}:
                        currentMonthCumLGD=df2.loc[df2["next_refresh_date_Weekly"]==key,"lifted_gallons_daily_modified"].cumsum()[-1]
                        presentMonthLastWeeklyLifted=df2.loc[df2["next_refresh_date_Weekly"]==key,"lifted_gallons_Weekly"][-1]
                        if (openingBalances['lifted_weekly']+currentMonthCumLGD)==presentMonthLastWeeklyLifted:
                            print "Modification_Successful"
                        else:
                            print "Modification_UnSuccessful"
                        cumSum=df2.loc[df2["next_refresh_date_Weekly"]==key,"lifted_gallons_daily_modified"].cumsum()
                        cumSum=cumSum.apply(lambda x:x+openingBalances['lifted_weekly'])
                        validLiftedGallons=cumSum==df2.loc[df2["next_refresh_date_Weekly"]==key,"lifted_gallons_Weekly"]
                        df2.loc[df2["next_refresh_date_Weekly"]==key,"sanityWeekly_CumulativeDaily_NextRefreshDate"]=validLiftedGallons
                    else:
                        if df2.loc[df2["next_refresh_date_Weekly"]==key,"lifted_gallons_daily_modified"].cumsum()[-1]==df2.loc[df2["next_refresh_date_Weekly"]==key,"lifted_gallons_Weekly"][-1]:
                            print "Modification_Successful"
                        else:
                            print "Modification_UnSuccessful"
                        validLiftedGallons=df2.loc[df2["next_refresh_date_Weekly"]==key,"lifted_gallons_daily_modified"].cumsum()==df2.loc[df2["next_refresh_date_Weekly"]==key,"lifted_gallons_Weekly"]
                        df2.loc[df2["next_refresh_date_Weekly"]==key,"sanityWeekly_CumulativeDaily_NextRefreshDate"]=validLiftedGallons
        except Exception as e:
            savepath=self.appConst.savepath+"\\"+"Exception"
            if not os.path.exists(savepath):
                os.makedirs(savepath)
            file=open(savepath+"\\"+self.fileName+".txt","w")
            file.write("Exception in verifyWeekByNRD for Combination:"+"\ncustomer : "+self.appConst.customer+"\nSupplier : "+self.appConst.supplier+"\nAccount : "+self.appConst.account+"\nTerminal: "+self.appConst.terminal+"\nProduct : "+self.appConst.product+"\nException : "+str(e))
            file.close()
            print "Exception in verifyWeekByNRD in Combination:",self.appConst.customer,self.appConst.supplier,self.appConst.account,self.appConst.terminal,self.appConst.product,e
            return 0
    def get_filename(self):
            fileName=self.appConst.account.replace(' ','').replace(':','')+"_"+self.appConst.terminal.replace(' ','').replace(':','')+"_"+self.appConst.product.replace(' ','')
            fileName=fileName.replace('/','')
            fileName=fileName.replace('*','')
            fileName=fileName.replace('>','')
            fileName=fileName.replace('<','')
            return fileName
    def fillNRDbackward(self,uniqueDatesList):
        nd=uniqueDatesList[0]-datetime.timedelta(7)
        #until date goes past the current month then exit
        while (nd.month == uniqueDatesList[0].month):
            if nd.month<self.appConst.dateDetails.month:
                if nd not in uniqueDatesList:
                    uniqueDatesList.insert(0,nd)
            else:
                if nd not in uniqueDatesList:
                    uniqueDatesList.insert(0,nd)
            nd=uniqueDatesList[0]-datetime.timedelta(7)
        print uniqueDatesList
    def fillNRDforward(self,uniqueDatesList):
        while (1):
            nd=uniqueDatesList[-1]+datetime.timedelta(7)
            if nd.month>self.appConst.dateDetails.month:
                if nd not in uniqueDatesList and len(uniqueDatesList) <6:
                    uniqueDatesList.append(nd)
                break
            else:
                if nd not in uniqueDatesList and len(uniqueDatesList) <6:
                    uniqueDatesList.append(nd)
        print uniqueDatesList

    def getValidDates(self,uniqueDatesList):
        validDates=[]
        for i in uniqueDatesList:
            newDate=i-datetime.timedelta(7)
            # print newDate,i
            if newDate.month<self.appConst.dateDetails.month:
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
        return validDates
    def computeMissingLiftedMonthlyAndDailyOneDayNRD(self,df2,ind):
            previousDayIndex=ind-datetime.timedelta(1)
            nextDayIndex=ind+datetime.timedelta(1)
            if df2.ix[nextDayIndex]["lifted_gallons_Monthly"] != 0 :
                if df2.ix[previousDayIndex]["lifted_gallons_Monthly"] != 0 :
                    newMonthly=df2.ix[nextDayIndex]["lifted_gallons_Monthly"]-df2.ix[nextDayIndex]["lifted_gallons_Daily"]
                    newlifted=newMonthly-df2.ix[previousDayIndex]["lifted_gallons_Monthly"]
                    df2.loc[str(df2.ix[ind]['date']),"lifted_gallons_daily_modified"]=newlifted
                    df2.loc[str(df2.ix[ind]['date']),"lifted_gallons_Monthly"]=newMonthly
                    df2.loc[str(df2.ix[ind]['date']),"lifted_gallons_monthly_flag"]=2
                    df2.loc[str(df2.ix[ind]['date']),"lifted_gallons_daily_flag"]=2
                else:
                    print "No Enough Data available to Fill Lifted Gallons for daily and monthly using aposterioriNRD"
            else:
                print "No Enough Data available to Fill Lifted Gallons for daily and monthly using aposterioriNRD"
    def reconcileUsingMonthy(self,d2):
        # monthlyValuesIndex=self.df2.loc[(self.df2["lifted_gallons_Monthly"] != 0)].index
        monthlyValuesIndex=self.df2.index
        for ind in monthlyValuesIndex:
            if ind.day !=1:
                liftedMonthlyPrevious=self.df2.loc[ind-datetime.timedelta(1),"lifted_gallons_Monthly"]
                actualLiftedDaily=self.df2.loc[ind,"lifted_gallons_Daily"]
                liftedMonthlyPresent=self.df2.loc[ind,"lifted_gallons_Monthly"]
                print liftedMonthlyPresent,liftedMonthlyPrevious,actualLiftedDaily
                if liftedMonthlyPrevious !=0 and liftedMonthlyPresent!=0:
                    newLifted=liftedMonthlyPresent-liftedMonthlyPrevious
                    if newLifted >0 and newLifted !=actualLiftedDaily:
                        self.df2.loc[ind,"lifted_gallons_daily_modified"]=newLifted
                        self.df2.loc[ind,"lifted_gallons_daily_flag"]=1
                else:
                    if liftedMonthlyPresent == 0 and actualLiftedDaily==0 and ind.day!=self.daysInMonth:
                        self.computeMissingLiftedMonthlyAndDailyOneDayNRD(self.df2,ind)
                    else:
                        print "Insufficient Data to Compute Daily Lifted using Monthly"
            else:
                actualLiftedDaily=self.df2.loc[ind,"lifted_gallons_Daily"]
                liftedMonthlyPresent=self.df2.loc[ind,"lifted_gallons_Monthly"]
                if actualLiftedDaily != liftedMonthlyPresent:
                    self.df2.loc[ind,"lifted_gallons_daily_modified"]=liftedMonthlyPresent
                    self.df2.loc[ind,"lifted_gallons_daily_flag"]=1
                elif actualLiftedDaily == liftedMonthlyPresent:
                    self.df2.loc[ind,"lifted_gallons_daily_flag"]=1
    def executeRules(self):
        try:
            print "***** Execution in Combination:",self.appConst.customer,self.appConst.supplier,self.appConst.account,self.appConst.terminal,self.appConst.product
            savepath=self.appConst.savepath
            if not os.path.exists(savepath):
                os.makedirs(savepath)
            self.fileName=self.get_filename()
            self.df2=copy.deepcopy(self.appConst.mp.loc[(self.appConst.mp["account_type"]==self.appConst.account)&(self.appConst.mp["supplier_terminal_name"]==self.appConst.terminal)&(self.appConst.mp["product_name"]==self.appConst.product)&(self.appConst.mp["date"]>=self.appConst.dateDetails.strftime("%Y-%m-%d")),:])
            dateDetails=datetime.datetime.strptime(self.appConst.analysisDate,"%d-%m-%Y")
            self.appConst.dateDetails=dateDetails
            self.daysInMonth=calendar.monthrange(dateDetails.year,dateDetails.month)[1]
            idx=pd.date_range(dateDetails.strftime('%m-%d-%Y'),str(dateDetails.month)+'-'+str(self.daysInMonth)+'-'+str(dateDetails.year))
            self.df2['date']= pd.to_datetime(self.df2[u'date'],format='%Y-%m-%d')
            self.df2.index=pd.DatetimeIndex(self.df2['date'])
            self.df2 = self.df2.reindex(idx, fill_value=0)
            self.df2['date']=self.df2.index.values
            self.df2[u"next_refresh_date_Weekly"]=self.df2[u"next_refresh_date_Weekly"].apply(lambda x:int(x) if x=='0' else x)
            findWeeks=dict(self.df2[u"next_refresh_date_Weekly"].value_counts())
            if findWeeks.has_key(0):
                if findWeeks[0]==self.daysInMonth:
                    savepath=savepath+"\\No_Weekly_Refresh_Dates"
                    if not os.path.exists(savepath):
                        os.makedirs(savepath)
                    file=open(savepath+"\\"+self.fileName+".txt","w")
                    file.write("No Weekly Refresh Dates to Compute for combination "+self.fileName+"....\n Exiting Execution.....")
                    file.close()
                    print "No Weekly Refresh Dates to Compute....\n Exiting Execution....."
                    return 0
            validateLiftedValuesWeekly=dict(self.df2["lifted_gallons_Weekly"].value_counts())
            if validateLiftedValuesWeekly.has_key(0):
                if validateLiftedValuesWeekly[0]==self.daysInMonth:
                    savepath=savepath+"\\No_Weekly_Lifted_Gallons"
                    if not os.path.exists(savepath):
                        os.makedirs(savepath)
                    file=open(savepath+"\\"+self.fileName+".txt","w")
                    file.write("No Weekly Lifted gallons to Compute week switches for combination "+self.fileName+"....\n Exiting Execution.....")
                    file.close()
                    print "No Weekly Lifted gallons to Compute week switches....\n Exiting Execution....."
                    return 0
            self.df2[u"next_refresh_date_Weekly"]=self.df2[u"next_refresh_date_Weekly"].apply(lambda x:datetime.datetime.strptime(' '.join(x.split(' ')[:2]),'%m/%d %H:%M:%S') if x!=0 else x)
            self.df2[u"next_refresh_date_Weekly"]=self.df2[u"next_refresh_date_Weekly"].apply(lambda x:x.replace(year=datetime.datetime.now().year) if x!=0 else x)
            uniqueDates=self.df2[u"next_refresh_date_Weekly"].unique()
            uniqueDatesList=uniqueDates.tolist()
            uniqueDatesList.remove(0)
            #Generate Missing Next RefreshDates
            generatedList=[]
            for i in uniqueDatesList:
                if i not in generatedList:
                    generatedList.append(i)
                    if len(generatedList) <6:
                        generatedList.sort()
                        self.fillNRDbackward(generatedList)
                        generatedList.sort()
                        self.fillNRDforward(generatedList)
            validDates=self.getValidDates(generatedList)
            #fetech rows with valid Next_Refresh_Dates
            validDatesRowValues=self.df2.loc[self.df2["next_refresh_date_Weekly"]!=0,"next_refresh_date_Weekly"]
            validDatesRowValuesPairs=validDatesRowValues.drop_duplicates().to_dict()
            #Reverese the Key Values
            if(len(validDatesRowValues)<self.daysInMonth):
                xcc={}

                for k,v in validDatesRowValuesPairs.iteritems():
                     xcc[v]=k
                if xcc.has_key(0):
                    xcc.pop(0)
                self.df2.insert(self.df2.keys().get_loc("next_refresh_date_Weekly"),"Modified_NRD",0)
                for validDate in validDates:
                    if self.df2.loc[str(dateDetails.replace(day=validDate[0])),"next_refresh_date_Weekly"]==0:
                        self.df2.loc[str(dateDetails.replace(day=validDate[0])),"next_refresh_date_Weekly"]=validDate[1]
                        # self.df2.loc[str(dateDetails.replace(day=validDate[0])),'account_type']=self.df2.loc[str(xcc[validDate[1]]),'account_type']
                        # self.df2.loc[str(dateDetails.replace(day=validDate[0])),'supplier_terminal_name']=self.df2.loc[str(xcc[validDate[1]]),'supplier_terminal_name']
                        # self.df2.loc[str(dateDetails.replace(day=validDate[0])),'product_name']=self.df2.loc[str(xcc[validDate[1]]),'product_name']
                        self.df2.loc[str(dateDetails.replace(day=validDate[0])),'account_type']=self.appConst.account
                        self.df2.loc[str(dateDetails.replace(day=validDate[0])),'supplier_terminal_name']=self.appConst.terminal
                        self.df2.loc[str(dateDetails.replace(day=validDate[0])),'product_name']=self.appConst.product
                        self.df2.loc[str(dateDetails.replace(day=validDate[0])),"Modified_NRD"]=1
                        # print validDate
            else:
                print "All next refresh Dates Valid"
            #set up columns for Daily weekly and Monthly
            self.df2.insert(self.df2.keys().get_loc("lifted_gallons_Daily"),"lifted_gallons_daily_modified",self.df2["lifted_gallons_Daily"].values)
            self.df2.insert(self.df2.keys().get_loc("lifted_gallons_daily_modified"),"lifted_gallons_daily_flag",0)
            self.df2.insert(self.df2.keys().get_loc("lifted_gallons_Weekly"),"Lifted_actual_weekly",self.df2["lifted_gallons_Weekly"].values)
            self.df2.insert(self.df2.keys().get_loc("lifted_gallons_Weekly"),"lifted_gallons_weekly_flag",0)
            self.df2.insert(self.df2.keys().get_loc("lifted_gallons_Monthly"),"Lifted_actual_monthly",self.df2["lifted_gallons_Monthly"].values)
            self.df2.insert(self.df2.keys().get_loc("lifted_gallons_Monthly"),"lifted_gallons_monthly_flag",0)
            ruleFactoryObj=ruleFactory()
            ruleAttributes=ruleFactoryObj.fetch_rules((self.appConst.customer).lower(),(self.appConst.supplier).lower())
            for ruleAttribute in ruleAttributes:
                stat=getattr(self,ruleAttribute)(self.df2)
                if type(stat) is int:
                    return 0

            if "lifted_gallons_Monthly" in self.df2.keys():
                self.df2["sanityMonthly_CumulativeDaily_WeeksByLiftedGallons"]=0
                self.df2.loc[self.df2.loc[:,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==self.df2.loc[:,"lifted_gallons_Monthly"],"sanityMonthly_CumulativeDaily_WeeksByLiftedGallons"]=1
                self.df2["sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate"]=0
                self.df2.loc[self.df2.loc[:,"lifted_gallons_daily_modified"].cumsum()==self.df2.loc[:,"lifted_gallons_Monthly"],"sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate"]=1
            self.df2.to_excel(savepath+"\\"+self.fileName+".xls")
            return self.df2
        except Exception as e:
            savepath=self.appConst.savepath+"\\"+"Exception"
            if not os.path.exists(savepath):
                os.makedirs(savepath)
            file=open(savepath+"\\"+self.fileName+".txt","w")
            file.write("Exception in Combination:"+"\ncustomer : "+self.appConst.customer+"\nSupplier : "+self.appConst.supplier+"\nAccount : "+self.appConst.account+"\nTerminal: "+self.appConst.terminal+"\nProduct : "+self.appConst.product+"\nException : "+str(e))
            file.close()
            print "Exception in Combination:",self.appConst.customer,self.appConst.supplier,self.appConst.account,self.appConst.terminal,self.appConst.product,e
            return 0




class ruleEngine:
    def __init__(self,appConst):
        self.appConst=appConst
        # self.appConst.customer=details[0]
        # self.appConst.supplier=details[1]
        # self.appConst.analysisDate=details[2]
        # self.appConst.savelocation=details[3]
        # self.appConst.dateDetails=datetime.datetime.strptime(self.appConst.analysisDate,"%d-%m-%Y")
        # self.appConst.month=calendar.month_name[self.appConst.dateDetails.month].lower()
        # self.appConst.frames=[]
        # self.appConst.pivotFrames=[]
        # self.appConst.suppliersCombinations=[]
        # self.appConst.savepath=self.appConst.savelocation+"\\"+self.appConst.supplier+"\\"+self.appConst.month+"\\"
        # self.appConst.dbcon=["172.16.0.55","root","admin123*","rules_spark"]
        # self.appConst.db = MySQLdb.connect(self.appConst.dbcon[0],self.appConst.dbcon[1],self.appConst.dbcon[2],self.appConst.dbcon[3])
        # # self.cursor=self.appConst.db.cursor()
        # self.appConst.executionFrom=self.appConst.dateDetails.strftime("%Y-%m-%d %H:%M:%S")
        # self.appConst.executionTo=self.appConst.dateDetails.replace(month=self.appConst.dateDetails.month+1).strftime("%Y-%m-%d %H:%M:%S")
        # if not os.path.exists(self.appConst.savepath):
        #     os.makedirs(self.appConst.savepath)
        # sys.stdout = open(self.appConst.savepath+'log.txt_w')
    def fetchSupplierCombi(self):
       try:
           # combi=self.appConst.mp[["account_type","supplier_terminal_name","product_name"]].drop_duplicates()
           combi=self.appConst.mp.loc[self.appConst.mp["date"]>=self.appConst.dateDetails.strftime("%Y-%m-%d"),["account_type","supplier_terminal_name","product_name"]].drop_duplicates()
           return combi
       except Exception as e:
           print "Exception:",e
    def createPivot(self,frame):
        frms=[]
        lgm=frame.loc[frame["Modified_WeeksByLiftedGallons"]==1,["date","account_type","supplier_terminal_name","product_name","lifted_gallons_modified_WeeksByLiftedGallons"]]
        lgm["lifted_cal_type"]="LgWeekValue"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        lgm=frame.loc[frame["lifted_gallons_daily_flag"]==1,["date","account_type","supplier_terminal_name","product_name","lifted_gallons_daily_modified"]]
        lgm["lifted_cal_type"]="LgNRD"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        lgm=frame.loc[(frame["Modified_WeeksByLiftedGallons"]==1)|(frame["lifted_gallons_daily_flag"]==1),["date","account_type","supplier_terminal_name","product_name","lifted_gallons_Daily"]]
        lgm["lifted_cal_type"]="Actual"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        if len(frms)>0:
            resultNew=pd.concat(frms)
            mp=pd.pivot_table(resultNew,index=["date","account_type","supplier_terminal_name","product_name","lifted_cal_type"])
            self.appConst.pivotFrames.append(mp)
    def createPivotAll(self,frame):
        frms=[]
        frame["excel_color_flag"]=0
        frame["original"]=0
        frame.loc[frame["lifted_gallons_modified_WeeksByLiftedGallons"]!=frame["lifted_gallons_daily_modified"],"excel_color_flag"]=1
        lgm=frame.loc[:,["date","account_type","supplier_terminal_name","product_name","lifted_gallons_modified_WeeksByLiftedGallons","excel_color_flag","Modified_WeeksByLiftedGallons"]]
        lgm["lifted_cal_type"]="LgWeekValue"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon',u'excel_color_flag',u'modified_flag', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        lgm=frame.loc[:,["date","account_type","supplier_terminal_name","product_name","lifted_gallons_daily_modified","excel_color_flag","lifted_gallons_daily_flag"]]
        lgm["lifted_cal_type"]="LgNRD"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon',u'excel_color_flag',u'modified_flag', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        lgm=frame.loc[:,["date","account_type","supplier_terminal_name","product_name","lifted_gallons_Daily","excel_color_flag","original"]]
        lgm["lifted_cal_type"]="Actual"
        lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon',u'excel_color_flag',u'modified_flag', u'lifted_cal_type']
        if len(lgm)>0:
            frms.append(lgm)
        if len(frms)>0:
            resultNew=pd.concat(frms)
            mp=pd.pivot_table(resultNew,index=["date","account_type","supplier_terminal_name","product_name","lifted_cal_type"])
            self.appConst.pivotFrames.append(mp)
    def runRules(self,suppliersCombinations=[]):
        try:
            self.appConst.mp=self.get_maxbatch_analysis_pivot(self.appConst.executionFrom,self.appConst.executionTo)
            if suppliersCombinations==[]:
                suppliersCombinations=self.fetchSupplierCombi()
                if len(suppliersCombinations)==0:
                    raise ValueError("No Combinations found to execute!")
                for supplierInfo in suppliersCombinations.values:
                    self.appConst.account=supplierInfo[0]
                    self.appConst.terminal=supplierInfo[1]
                    self.appConst.product=supplierInfo[2]
                    # details=[self.appConst.customer,self.appConst.supplier,supplierInfo[0],supplierInfo[1],supplierInfo[2],self.appConst.month,self.appConst.analysisDate,self.appConst.db,self.appConst.savelocation,copy.deepcopy(self.appConst.mp)]
                    supplierRule=rules(self.appConst)
                    frame=supplierRule.executeRules()
                    if type(frame) != int:
                        self.appConst.frames.append(frame)
                        self.createPivotAll(frame)
            else:
                for supplierInfo in suppliersCombinations:
                    self.appConst.account=supplierInfo[0]
                    self.appConst.terminal=supplierInfo[1]
                    self.appConst.product=supplierInfo[2]
                    supplierRule=rules(self.appConst)
                    frame=supplierRule.executeRules()
                    if type(frame) != int:
                            self.appConst.frames.append(frame)
                            self.createPivotAll(frame)

            if len(self.appConst.pivotFrames)>0:
                resultNew=pd.concat(self.appConst.pivotFrames)
                resultNew.to_excel(self.appConst.savepath+self.appConst.supplier+"_"+self.appConst.month+"_reconciledPivotAll.xls")
            result=pd.concat(self.appConst.frames)
            db = MySQLdb.connect(self.appConst.dbcon[0],self.appConst.dbcon[1],self.appConst.dbcon[2],self.appConst.dbcon[3])
            result.to_sql(name=self.appConst.customer+"_"+self.appConst.supplier+"_"+self.appConst.month+"_reconciled",con=db,flavor='mysql', if_exists='replace')
            # resultNew.to_sql(name=self.appConst.customer+"_"+self.appConst.supplier+"_reconciledPivot",con=db,flavor='mysql', if_exists='replace')
            db.close()
            indexCol=[u'date', u'account_type', u'supplier_terminal_name', u'product_name','lifted_gallons_modified_WeeksByLiftedGallons','lifted_gallons_daily_flag','lifted_gallons_daily_modified', "lifted_gallons_Daily",'lifted_gallons_weekly_flag',"lifted_gallons_Weekly", "Lifted_actual_weekly",'lifted_gallons_monthly_flag',"lifted_gallons_Monthly", "Lifted_actual_monthly", 'WeeksByLiftedGallons','Week_switch', "base_gallons_Daily", "base_gallons_Monthly", "base_gallons_Weekly", 'Modified_WeeksByLiftedGallons', "beginning_gallons_Daily", "beginning_gallons_Monthly", "beginning_gallons_Weekly", "en_allocation_status_Daily", "en_allocation_status_Monthly", "en_allocation_status_Weekly", "percentage_allocation_Daily", "percentage_allocation_Monthly", "percentage_allocation_Weekly", "alerts_ratability_Daily", "alerts_ratability_Monthly", "alerts_ratability_Weekly", "next_refresh_date_Daily", "next_refresh_date_Monthly", 'Modified_NRD', "next_refresh_date_Weekly", 'sanityWeekly_CumulativeDaily_WeeksByLiftedGallons','sanityWeekly_CumulativeDaily_NextRefreshDate','sanityMonthly_CumulativeDaily_WeeksByLiftedGallons','sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate']
            columnHeader=[u'date', u'account_type', u'supplier_terminal_name', u'product_name','Lifted_mod_daily_weekly_value','lifted_gallons_daily_flag','Lifted_mod_daily_nextrefresh','Lifted_actual_daily','lifted_gallons_weekly_flag','lifted_gallons_weekly_modified','Lifted_actual_weekly','lifted_gallons_weekly_flag','lifted_gallons_monthly_modified','Lifted_actual_monthly','Week_structure_weekly_value','Week_structure_next_refresh', "base_gallons_Daily", "base_gallons_Monthly", "base_gallons_Weekly", 'daily_weekValue_flag', "beginning_gallons_Daily", "beginning_gallons_Monthly", "beginning_gallons_Weekly", "en_allocation_status_Daily", "en_allocation_status_Monthly", "en_allocation_status_Weekly", "percentage_allocation_Daily", "percentage_allocation_Monthly", "percentage_allocation_Weekly", "alerts_ratability_Daily", "alerts_ratability_Monthly", "alerts_ratability_Weekly", "next_refresh_date_Daily", "next_refresh_date_Monthly", 'Modified_NRD', "next_refresh_date_Weekly", 'sanityWeekly_CumulativeDaily_WeeksByLiftedGallons','sanityWeekly_CumulativeDaily_NextRefreshDate','sanityMonthly_CumulativeDaily_WeeksByLiftedGallons','sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate']
            result.to_excel(self.appConst.savepath+self.appConst.supplier+"_"+self.appConst.month+"_reconciled.xls",columns=indexCol,header=columnHeader)

        except Exception as e:
            print "Exception:",e

    def get_maxbatch_analysis_pivot(self,executionFrom,executionTo):
        try:
            condition="Execution_Date>='%s' and Execution_Date<='%s' and supplier_name='%s'"%(self.appConst.previousMonthexecutionFrom,executionTo,self.appConst.supplier)
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
            # time.sleep(1)
            frameName=self.appConst.customer+"_"+self.appConst.supplier+"_"+self.appConst.month+"_maxBatchFrame.csv"
            df_mysql.to_csv(self.appConst.savepath+frameName)
            # df_mysql=pd.read_csv(self.appConst.savepath+frameName)
            mp=pd.pivot_table(df_mysql,index=["date","account_type","supplier_terminal_name","product_name"],values=["base_gallons","lifted_gallons","beginning_gallons","en_allocation_status",'percentage_allocation','alerts_ratability','next_refresh_date'],columns="period",aggfunc = lambda x: x)
            mp.columns=['_'.join(col).strip() for col in mp.columns.values]
            dates=[]
            accounts=[]
            terminals=[]
            products=[]
            for i in mp.index.values:
                dates.append(str(i[0]))
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
if __name__ == "__main__":
    # 'BP','Holly','Chevron','Exxon','Valero','P66','Tesoro'
    # suppliers=['Holly','Valero','P66','Tesoro']
    # suppliers=['Chevron','Exxon','Holly','Valero','P66','Tesoro','BP']

    suppliers=['Holly','P66','Tesoro','BP']
    for i in suppliers:
        start=datetime.datetime.now()
        print "Execution Started Please Wait....."
        detailedList=["pilot",i,'01-07-2015',r"C:\spark_output\AnalysisRulesSept8"]
        appConst=entities.entity(detailedList)
        executeEngine=ruleEngine(appConst)
        # executeEngine.runRules((('PILOT TRAVEL CENTERS LLC : CHV7460761_1037 PASCAGOULA MS TRM CHEVRON_DIESEL #2'),))
        # executeEngine.runRules((('PILOT TRAVEL CENTERS LLC 103637 IW','BATON ROUGE LA (MOC) - 00CE','ULSD'),))
        executeEngine.runRules()
        print "Completed Execution Thanks for you Patience "
        print datetime.datetime.now()-start