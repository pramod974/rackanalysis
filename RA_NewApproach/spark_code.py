from IPython.core.magic_arguments import magic_arguments

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
        print "ReadDB thread id :",self.db.thread_id()
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
        self.ruleAttributes={'pilot':{'chevron':['reconcilePA','reconcileBase','reconcileBe','reconcileNRBD','reconcileNRBM','verifyWeeksByValuesAll','reconcileUsingMonthy','computeWeeklyfromReconciledDaily','verifyWeekByNRD','computeMonthlyandWeeklyFromReconciledDaily','reconcileRG','reconcileAGR','ratability_Monthly','ratability_Daily','ratability_Weekly','reconcile_unknown_base'],
                                      'exxon':['reconcilePA','reconcileBase','reconcileBe','reconcileNRBD','reconcileNRBM','verifyWeeksByValuesAll','reconcileUsingMonthy','computeWeeklyfromReconciledDaily','verifyWeekByNRD','computeMonthlyandWeeklyFromReconciledDaily','reconcileRG','reconcileAGR','ratability_Monthly','ratability_Daily','ratability_Weekly','reconcile_unknown_base'],
                                      'holly':['reconcilePA','reconcileBase','reconcileBe','reconcileNRBD','reconcileNRBM','verifyWeeksByValuesAll','reconcileUsingMonthy','computeWeeklyfromReconciledDaily','verifyWeekByNRD','computeMonthlyandWeeklyFromReconciledDaily','reconcileRG','reconcileAGR','ratability_Monthly','ratability_Daily','ratability_Weekly','reconcile_unknown_base'],
                                      'valero':['reconcilePA','reconcileBase','reconcileBe','reconcileNRBD','reconcileNRBM','verifyWeeksByValuesAll','reconcileUsingMonthy','computeWeeklyfromReconciledDaily','verifyWeekByNRD','computeMonthlyandWeeklyFromReconciledDaily','reconcileRG','reconcileAGR','ratability_Monthly','ratability_Daily','ratability_Weekly','reconcile_unknown_base'],
                                      'p66':['reconcilePA','reconcileBase','reconcileBe','reconcileNRBD','reconcileNRBM','verifyWeeksByValuesAll','reconcileUsingMonthy','computeWeeklyfromReconciledDaily','verifyWeekByNRD','computeMonthlyandWeeklyFromReconciledDaily','reconcileRG','reconcileAGR','ratability_Monthly','ratability_Daily','ratability_Weekly','reconcile_unknown_base'],
                                      'tesoro':['reconcilePA','reconcileBase','reconcileBe','reconcileNRBD','reconcileNRBM','verifyWeeksByValuesAll','reconcileUsingMonthy','computeWeeklyfromReconciledDaily','verifyWeekByNRD','computeMonthlyandWeeklyFromReconciledDaily','reconcileRG','reconcileAGR','ratability_Monthly','ratability_Daily','ratability_Weekly','reconcile_unknown_base'],
                                      'bp':['reconcilePA','reconcileBase','reconcileBe','reconcileNRBD','reconcileNRBM','verifyWeeksByValuesAll','verifyWeekByNRD','computeMonthlyandWeeklyFromReconciledDaily','reconcileRG','reconcileAGR','reconcile_unknown_base'],
                                      'shell':['reconcilePA','reconcileBase','reconcileBe','reconcileNRBD','reconcileNRBM','verifyWeeksByValuesAll','reconcileUsingMonthy','reconcileRG','reconcileAGR']}}

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

    def ratability_Monthly(self,df2):
        try:
            # lg_cols=['lifted_gallons_daily_modified','lifted_gallons_Monthly',u'lifted_gallons_Weekly']
            # ba_cols=[u'base_gallons_Daily_Reconciled', u'base_gallons_Monthly_Reconciled', u'base_gallons_Weekly_Reconciled']
            # for lg,ba in zip(lg_cols,ba_cols):
            #     self.df2["Ratability_Monthly"]=(df2[lg]/df2[ba])/(df2.index.day/float(self.daysInMonth))
            self.df2["Ratability_Monthly"]=-1
            self.df2.loc[(df2["lifted_gallons_Monthly"]!=-1)&(df2["base_gallons_Monthly_Reconciled"]!=-1),"Ratability_Monthly"]=(df2.loc[(df2["lifted_gallons_Monthly"]!=-1)&(df2["base_gallons_Monthly_Reconciled"]!=-1),"lifted_gallons_Monthly"]/df2.loc[(df2["lifted_gallons_Monthly"]!=-1)&(df2["base_gallons_Monthly_Reconciled"]!=-1),"base_gallons_Monthly_Reconciled"])/((df2.loc[(df2["lifted_gallons_Monthly"]!=-1)&(df2["base_gallons_Monthly_Reconciled"]!=-1),"date"].apply(lambda x :x.day)/float(self.daysInMonth)))*100
            self.df2["Ratability_Monthly"]=self.df2["Ratability_Monthly"].round(0)
        except Exception as e:
            print "Exception",e
    def ratability_Daily(self,df2):
        pass
        try:
            self.df2["Ratability_Daily"]=-1
            for i in self.df2.index:
                rFactor=0
                executionDate=pd.Timestamp(self.df2.loc[i,'execution_date_Daily']).hour
                if executionDate>=6 and executionDate<=12:
                    rFactor=0.25
                elif executionDate>12 and executionDate<=18:
                    rFactor=0.5
                elif executionDate>18 and executionDate<=24:
                    rFactor=0.75
                lifted_gallon=self.df2.loc[i,"lifted_gallons_daily_modified"]
                base_gallon=self.df2.loc[i,"base_gallons_Daily_Reconciled"]
                if lifted_gallon!=-1 and base_gallon!=-1:
                    self.df2.loc[i,"Ratability_Daily"]=((lifted_gallon/base_gallon)/rFactor)*100
            # self.df2.loc[(df2["lifted_gallons_daily_modified"]!=-1)&(df2["base_gallons_Daily_Reconciled"]!=-1),"Ratability_Daily"]=(df2.loc[(df2["lifted_gallons_daily_modified"]!=-1)&(df2["base_gallons_Daily_Reconciled"]!=-1),"lifted_gallons_daily_modified"]/df2.loc[(df2["lifted_gallons_daily_modified"]!=-1)&(df2["base_gallons_Daily_Reconciled"]!=-1),"base_gallons_Daily_Reconciled"])/rFactor*100
            self.df2["Ratability_Daily"]=self.df2["Ratability_Daily"].round(0)
        except Exception as e:
            print "Exception",e
    def ratability_Weekly(self,df2):
        pass
        try:
            self.df2["Ratability_Weekly"]=-1
            for i in self.df2.index:
                lifted_gallon=self.df2.loc[i,"lifted_gallons_Weekly"]
                base_gallon=self.df2.loc[i,"base_gallons_Weekly_Reconciled"]
                nextRefreshDate=self.df2.loc[i,"next_refresh_date_Weekly"]
                if lifted_gallon!=-1 and base_gallon!=-1 and nextRefreshDate !=-1:
                    timeperiod=7-int((nextRefreshDate-i).days)+1
                    if timeperiod ==0:
                        timeperiod=7
                    self.df2.loc[i,"Ratability_Weekly"]=(lifted_gallon/base_gallon)/(timeperiod/7)*100
            self.df2["Ratability_Weekly"]=self.df2["Ratability_Weekly"].round(0)
        except Exception as e:
            print "Exception",e
    def reconcileAGR(self,df2):
        try:
            pass
            aga_col=[ u'additional_gallons_allowed_Daily', u'additional_gallons_allowed_Monthly', u'additional_gallons_allowed_Weekly']
            agr_col=[ u'additional_gallons_remaining_Daily', u'additional_gallons_remaining_Monthly', u'additional_gallons_remaining_Weekly']
            rg_col=[u'remaining_gallons_Daily', u'remaining_gallons_Monthly', u'remaining_gallons_Weekly']
            for agr,aga,rg in zip(agr_col,aga_col,rg_col):
                    self.df2[agr+"_Reconciled"] = -1
                    self.df2.loc[(self.df2[rg+"_Reconciled"] < 0)&(self.df2[aga] != 0), [agr+"_Reconciled"] ]=\
                        self.df2[(self.df2[rg+"_Reconciled"] < 0)&(self.df2[aga] != 0)] [aga] + self.df2[(self.df2[rg+"_Reconciled"] < 0)&(self.df2[aga] != 0)] [rg+"_Reconciled"]
                    self.df2.loc[(self.df2[rg+"_Reconciled"] < 0)&(self.df2[aga] != 0), [agr+"_Reconciled"] ]=\
                        self.df2[(self.df2[rg+"_Reconciled"] < 0)&(self.df2[aga] != 0)] [aga]
            # self.df2.loc[self.df2["remaining_gallons_Daily_Reconciled"] < 0, ["additional_gallons_remaining_Daily_Reconciled"] ]=\
            #     self.df2[self.df2["remaining_gallons_Daily_Reconciled"] < 0]["additional_gallons_allowed_Daily"]+\
            #     self.df2[self.df2["remaining_gallons_Daily_Reconciled"] < 0]["remaining_gallons_Daily_Reconciled"]
            # self.df2.loc[self.df2["remaining_gallons_Daily_Reconciled"] > 0, ["additional_gallons_remaining_Daily_Reconciled"] ]=\
            #     self.df2[self.df2["remaining_gallons_Daily_Reconciled"] > 0]["additional_gallons_allowed_Daily"]
        except Exception as e:
            print "Exception",e
    def reconcileRG(self,df2):
        try:
            rg_col=[u'remaining_gallons_Daily', u'remaining_gallons_Monthly', u'remaining_gallons_Weekly']
            lg_cols=['lifted_gallons_daily_modified','lifted_gallons_Monthly',u'lifted_gallons_Weekly']
            bg_cols=[u'beginning_gallons_Daily', u'beginning_gallons_Monthly', u'beginning_gallons_Weekly']
            for rg,lg,bg in zip(rg_col,lg_cols,bg_cols):
                self.df2[rg+"_Reconciled"] = -1
                self.df2.loc[self.df2[bg] != -1, [rg+"_Reconciled"] ]= (self.df2[self.df2[bg]!=-1][bg] - self.df2[self.df2[bg]!=-1][lg] )
            pass
        except Exception as e:
            print "Exception",e
    def reconcileNRBM(self,df2):
        try:
            self.df2.insert(df2.keys().get_loc("next_refresh_base_gallons_Monthly"),"NRB_Monthly",self.df2.next_refresh_base_gallons_Monthly)
            self.df2.loc[self.df2["NRB_Monthly"]==-1,"NRB_Monthly"]=self.df2.next_refresh_base_gallons_Monthly.max()
            for i in self.df2.index[1:]:
                if self.df2.loc[i,"base_gallons_Monthly"]!=-1:
                    self.df2.loc[i-datetime.timedelta(1),"NRB_Monthly"]=self.df2.loc[i,"base_gallons_Monthly"]
            pass
        except Exception as e:
            print "Exception",e
    def reconcileNRBD(self,df2):
        try:
            self.df2.insert(df2.keys().get_loc("next_refresh_base_gallons_Daily"),"NRB_Daily",self.df2.next_refresh_base_gallons_Daily)
            self.df2.loc[self.df2["NRB_Daily"]==-1,"NRB_Daily"]=self.df2.next_refresh_base_gallons_Daily.max()
            for i in self.df2.index[1:]:
                if self.df2.loc[i,"base_gallons_Daily"]!=-1:
                    self.df2.loc[i-datetime.timedelta(1),"NRB_Daily"]=self.df2.loc[i,"base_gallons_Daily"]
            pass
        except Exception as e:
            print "Exception",e
    def reconcilePA(self,df2):
            try:
                reconcile_pa=["percentage_allocation_Daily","percentage_allocation_Monthly","percentage_allocation_Weekly"]
                for i in reconcile_pa:
                    # self.df2[i+"_actual"]=self.df2[i]
                    self.df2.insert(df2.keys().get_loc(i),i+"_actual",self.df2[i])
                    findPA=dict(self.df2[i].value_counts())
                    countPA=0
                    if findPA.has_key(-1):
                        countPA+=findPA[-1]
                    if countPA==self.daysInMonth:
                        self.df2[i]=100
                    else:
                        for ind in self.df2.index:
                            if ind.day!=1:
                                if self.df2.loc[ind,i]==-1:
                                    self.df2.loc[ind,i]=self.df2.loc[ind-datetime.timedelta(1),i]
                                else:
                                    self.df2.loc[ind,i]=int(str(self.df2.loc[ind,i]).strip().replace('%',''))
                            elif ind.day==1:
                                if self.df2.loc[ind,i]==-1:
                                    pass
                                else:
                                    self.df2.loc[ind,i]=int(str(self.df2.loc[ind,i]).strip().replace('%',''))

            except Exception as e:
                print "Exception",e

    def reconcile_unknown_base(self,df2):
        try:
            grpNRD=df2.groupby(["next_refresh_date_Weekly"])
            self.df2['Synthetic_Base_Weekly']=-1
            self.df2['Synthetic_Base_Daily']=-1
            self.df2['Synthetic_Base_Monthly']=-1
            for i in self.df2.index:
                maxWeekly=self.df2.loc[i,'base_gallons_Weekly_Reconciled']
                maxDaily=self.df2.loc[i,'base_gallons_Daily_Reconciled']
                maxMonthly=self.df2.loc[i,'base_gallons_Monthly_Reconciled']
                if maxMonthly !=-1:
                    synDaily=round((maxMonthly+0.0)/30.0)
                    synWeekly=round(((maxMonthly+0.0)/30.0)*7.0)
                    self.df2.loc[i,'Synthetic_Base_Monthly']=maxMonthly
                    self.df2.loc[i,'Synthetic_Base_Daily']=synDaily
                    self.df2.loc[i,'Synthetic_Base_Weekly']=synWeekly
                if maxWeekly !=-1:
                    synDaily=round((maxWeekly+0.0)/7.0)
                    synMonthly=self.daysInMonth*synDaily
                    self.df2.loc[i,'Synthetic_Base_Monthly']=synMonthly
                    self.df2.loc[i,'Synthetic_Base_Daily']=synDaily
                    self.df2.loc[i,'Synthetic_Base_Weekly']=maxWeekly
                elif maxDaily !=-1:
                    synWeekly=maxDaily*7
                    synMonthly=self.daysInMonth*maxDaily
                    self.df2.loc[i,'Synthetic_Base_Weekly']=synWeekly
                    self.df2.loc[i,'Synthetic_Base_Daily']=maxDaily
                    self.df2.loc[i,'Synthetic_Base_Monthly']=synMonthly
        except Exception as e:
            print "Exception",e
    def reconcileBase(self,df2):
        try:
            reconcile_ba=[u'base_gallons_Daily', u'base_gallons_Monthly', u'base_gallons_Weekly']
            reconcile_pa=["percentage_allocation_Daily","percentage_allocation_Monthly","percentage_allocation_Weekly"]
            reconcile_be=[u'beginning_gallons_Daily', u'beginning_gallons_Monthly', u'beginning_gallons_Weekly']
            # self.df2["base_gallons_Daily_Reconciled"]=-1
            # self.df2.loc[self.df2.base_gallons_Daily!=-1,["base_gallons_Daily_Reconciled"]]=self.df2[self.df2.base_gallons_Daily!=-1]["percentage_allocation_Daily"]*self.df2[self.df2.base_gallons_Daily!=-1]["beginning_gallons_Daily"]
            for i,j,k in zip(reconcile_ba,reconcile_pa,reconcile_be):
                self.df2[i+"_Reconciled"] = -1
                # self.df2.loc[self.df2[i] != -1, [i+"_Reconciled"] ]= (self.df2[self.df2[i]!=-1][k] / self.df2[self.df2[i]!=-1][j] * 100).apply(lambda x:int(round(x)))
                for ind in self.df2.index:
                    if ind.day!=1:
                        if self.df2.loc[ind,i]==-1:
                            if self.df2.loc[ind-datetime.timedelta(1), k] != -1:
                                self.df2.loc[ind,i+"_Reconciled"]=int(round((self.df2.loc[ind-datetime.timedelta(1),k]+0.0) / (self.df2.loc[ind-datetime.timedelta(1),j]+0.0) * 100.0))
                            else:
                                self.df2.loc[ind,i+"_Reconciled"]=self.df2.loc[ind-datetime.timedelta(1),i]
                        else:
                            self.df2.loc[ind,i+"_Reconciled"]=int(round((self.df2.loc[ind,k]+0.0) / (self.df2.loc[ind,j]+0.0) * 100.0))
                    elif ind.day==1:
                        if self.df2.loc[ind,i] == -1:
                            pass
                        else:
                            self.df2.loc[ind,i+"_Reconciled"]=int(round((self.df2.loc[ind,k]+0.0) / (self.df2.loc[ind,j]+0.0) * 100.0))
        except Exception as e:
            print "Exception",e

    def reconcileBe(self,df2):
            try:
                reconcile_ba=[u'base_gallons_Daily', u'base_gallons_Monthly', u'base_gallons_Weekly']
                reconcile_pa=["percentage_allocation_Daily","percentage_allocation_Monthly","percentage_allocation_Weekly"]
                reconcile_be=[u'beginning_gallons_Daily', u'beginning_gallons_Monthly', u'beginning_gallons_Weekly']
                for i,j,k in zip(reconcile_ba,reconcile_pa,reconcile_be):
                    self.df2[k+"_Reconciled"] = -1
                    # self.df2.loc[self.df2[k] != -1, [k+"_Reconciled"]] = (self.df2[self.df2[k] != -1][i] * self.df2[self.df2[k] != -1][j] / 100).apply(lambda x:round(x))
                    for ind in self.df2.index:
                        if ind.day!=1:
                            if self.df2.loc[ind, k] == -1:
                                if self.df2.loc[ind-datetime.timedelta(1), i] != -1:
                                    self.df2.loc[ind,k+"_Reconciled"]=int(round((self.df2.loc[ind-datetime.timedelta(1),i]+0.0) * (self.df2.loc[ind-datetime.timedelta(1),j]+0.0) / 100.0))
                                else:
                                    self.df2.loc[ind,k+"_Reconciled"]=self.df2.loc[ind-datetime.timedelta(1),k]
                            else:
                                self.df2.loc[ind,k+"_Reconciled"]=int(round((self.df2.loc[ind,i]+0.0) * (self.df2.loc[ind,j]+0.0) / 100.0))
                        elif ind.day==1:
                            if self.df2.loc[ind,k]==-1:
                                pass
                            else:
                                self.df2.loc[ind,k+"_Reconciled"]=int((round(self.df2.loc[ind,i]+0.0) * (self.df2.loc[ind,j]+0.0)/ 100.0))
            except Exception as e:
                print "Exception",e
    def verifyOpeningBalanceReconciled(self):
        try:
            previousMonth=self.appConst.dateDetails.month-1
            daysInPreviousMonth=calendar.monthrange(self.appConst.dateDetails.year,self.appConst.dateDetails.month-1)[1]
            lastdayPrevious=self.appConst.dateDetails.replace(month=previousMonth,day=daysInPreviousMonth)
            sql="SELECT supplier_name,lifted_gallons_daily_modified,lifted_gallons_Weekly,computedWeekly FROM rules_spark.enallocationarchive_debug where supplier_name='%s' and account_type='%s' and supplier_terminal_name='%s' and product_name='%s' and date='%s'"%(self.appConst.supplier,self.appConst.account,self.appConst.terminal,self.appConst.product,str(lastdayPrevious))
            dfOpeningBalance=pd.read_sql(sql, con=self.appConst.db)
            # dfOpeningBalance=copy.deepcopy(self.appConst.mp.loc[(self.appConst.mp["account_type"]==self.appConst.account)&(self.appConst.mp["supplier_terminal_name"]==self.appConst.terminal)&(self.appConst.mp["product_name"]==self.appConst.product)&(self.appConst.mp["date"]==(self.appConst.dateDetails.replace(month=previousMonth,day=daysInPreviousMonth)).strftime("%Y-%m-%d")),:])
            if len(dfOpeningBalance)>0:
                lifted_daily=dfOpeningBalance["lifted_gallons_daily_modified"][0]
                lifted_weekly=dfOpeningBalance["lifted_gallons_Weekly"][0]
                print "Opening Balance Lifted_Daily",lifted_daily
                print "Opening Balance Lifted_Weekly",lifted_weekly
                return {'lifted_daily':lifted_daily,'lifted_weekly':lifted_weekly}
            else:
                print "Unable to fetch Opening Balance"
                return {'lifted_daily':0,'lifted_weekly':0}
        except Exception as e:
            return {'lifted_daily':0,'lifted_weekly':0}

    def verifyOpeningBalance(self):
        try:
            previousMonth=self.appConst.dateDetails.month-1
            daysInPreviousMonth=calendar.monthrange(self.appConst.dateDetails.year,self.appConst.dateDetails.month-1)[1]
            dfOpeningBalance=copy.deepcopy(self.appConst.mp.loc[(self.appConst.mp["account_type"]==self.appConst.account)&(self.appConst.mp["supplier_terminal_name"]==self.appConst.terminal)&(self.appConst.mp["product_name"]==self.appConst.product)&(self.appConst.mp["date"]==(self.appConst.dateDetails.replace(month=previousMonth,day=daysInPreviousMonth)).strftime("%Y-%m-%d")),:])
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
        if grp.ix[i+1]["lifted_gallons_Weekly"] != -1 and grp.ix[i+1]["lifted_gallons_Daily"]!=-1:
            if grp.ix[i-1]["lifted_gallons_Weekly"] != -1 and grp.ix[i-1]["lifted_gallons_Daily"]!=-1:
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
        if grp.ix[i+1]["lifted_gallons_Weekly"] != -1 and grp.ix[i+1]["lifted_gallons_Daily"]!=-1:
            if grp.ix[i-1]["lifted_gallons_Weekly"] != -1 and grp.ix[i-1]["lifted_gallons_Daily"]!=-1:
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
    def computeMissingLiftedWeeklyAndDailyFirstDayWeek(self,df2,grp,i):
        if grp.ix[i+1]["lifted_gallons_Weekly"] != -1 and grp.ix[i+1]["lifted_gallons_daily_modified"]!=-1:
                newweekly=grp.ix[i+1]["lifted_gallons_Weekly"]-grp.ix[i+1]["lifted_gallons_daily_modified"]
                newlifted=newweekly
                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"]=newlifted
                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]=6
                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Weekly"]=newweekly
                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_weekly_flag"]=6
    def computeMissingLiftedWeeklyAndDailyFirstDayMonth(self,ind):

        liftedMonthlyNext=self.df2.loc[ind+datetime.timedelta(1),"lifted_gallons_Monthly"]
        liftedDailyNext=self.df2.loc[ind+datetime.timedelta(1),"lifted_gallons_daily_modified"]
        if liftedMonthlyNext!=-1 and liftedDailyNext!=-1:
            newLifted=liftedMonthlyNext-liftedDailyNext
            self.df2.loc[ind,"lifted_gallons_daily_modified"]=newLifted
            self.df2.loc[ind,"lifted_gallons_daily_flag"]=7
            self.df2.loc[ind,"lifted_gallons_Monthly"]=newLifted
            self.df2.loc[ind,"lifted_gallons_monthly_flag"]=7
        else:
            print "Not enough Data avaiable to fill lifted Daily and Monthly since next day Monthly"

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
                            liftedWeeklyPrevious=df2.loc[str(grp.ix[i-1]['date']),"lifted_gallons_Weekly"]
                            lifteddailyPrevious=df2.loc[str(grp.ix[i-1]['date']),"lifted_gallons_Daily"]
                            actualLiftedDaily=df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"]
                            liftedWeeklyPresent=df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Weekly"]
                            if liftedWeeklyPrevious != -1 and liftedWeeklyPresent != -1 and lifteddailyPrevious!=-1 and actualLiftedDaily!=-1:
                                    beginPreviousWeekly=liftedWeeklyPrevious-lifteddailyPrevious
                                    beginPresentWeekly=liftedWeeklyPresent-actualLiftedDaily
                                    previousTrueDaily=beginPresentWeekly-beginPreviousWeekly
                                    df2.loc[str(grp.ix[i-1]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]=previousTrueDaily
                                    df2.loc[str(grp.ix[i-1]['date']),"Modified_WeeksByLiftedGallons"]=3

                            else:
                                if liftedWeeklyPresent == -1 and actualLiftedDaily==-1 and i!=(len(grp)-1):
                                    self.computeMissingLiftedWeeklyAndDailyOneDayNRD(df2,grp,i)
                                else:
                                    print "No Enough Data available to Fill Lifted Gallons NRD"
                            # if grp.ix[i-1]["lifted_gallons_Weekly"] != -1 and grp.ix[i]["lifted_gallons_Weekly"] != -1:
                            #     if df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"] != (grp.ix[i]["lifted_gallons_Weekly"]-grp.ix[i-1]["lifted_gallons_Weekly"]):
                            #         df2.loc[str(grp.ix[i]['date']),"lifted_gallons_modified_WeeksByLiftedGallons"]=grp.ix[i]["lifted_gallons_Weekly"]-grp.ix[i-1]["lifted_gallons_Weekly"]
                            #         df2.loc[str(grp.ix[i]['date']),"Modified_WeeksByLiftedGallons"]=1
                            #         # print i,grp.ix[i+1]["lifted_gallons_Weekly"]-grp.ix[i]["lifted_gallons_Weekly"]
                            # else:
                            #     if grp.ix[i]["lifted_gallons_Weekly"] == -1 and grp.ix[i]["lifted_gallons_Daily"]==-1 and i!=(len(grp)-1):
                            #         self.computeMissingLiftedWeeklyAndDailyOneDayWeekValue(df2,grp,i)
                            #     else:
                            #         print "No Enough Data available to Fill Lifted Gallons"
                        else:
                            # opening balance
                            if key==firstNRD and  firstNRD!=0 :
                                openingBalances={}
                                nrdGroups=self.df2["WeeksByLiftedGallons"].value_counts()
                                if nrdGroups[firstNRD]!=7:
                                    openingBalances=self.verifyOpeningBalanceReconciled()
                                    if openingBalances !={}:
                                        if openingBalances['lifted_weekly'] != -1 and grp.ix[i]["lifted_gallons_Weekly"] != -1:
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
    def computeMonthlyandWeeklyFromReconciledDailyMissing(self,df2):
        try:
            if -1 in self.df2["lifted_gallons_daily_modified"].values:
                print "Reconcile using Round 2"
                self.df2.insert(self.df2.keys().get_loc("lifted_gallons_daily_flag"),"lifted_gallons_daily_flag_round1",self.df2["lifted_gallons_daily_flag"].values)
                for i in self.df2.index:
                    if self.df2.loc[i,"lifted_gallons_daily_modified"]!=-1:
                        if self.df2.loc[i,"lifted_gallons_Monthly"]==-1:
                            if i.day==1:
                                self.df2.loc[i,"lifted_gallons_Monthly"]=self.df2.loc[i,"lifted_gallons_daily_modified"]
                            else:
                                self.df2.loc[i,"lifted_gallons_Monthly"]=self.df2.loc[i-datetime.timedelta(1),"lifted_gallons_Monthly"]+self.df2.loc[i,"lifted_gallons_daily_modified"]
                    else:
                        break
                self.reconcileUsingMonthy(self.df2)
            else:
                print "No missing Lifted Daily to Reconcile using Round 2"
                return 1
        except Exception as e:
            print e
    def computeMonthlyandWeeklyFromReconciledDaily(self,df2):
        try:
            self.df2["computedWeekly"]=0
            self.df2["computedMonthly"]=0
            self.df2["sanityComputedMonthly"]=0
            if -1 not in self.df2["lifted_gallons_daily_modified"].values:
                    self.df2["computedMonthly"]=self.df2["lifted_gallons_daily_modified"].cumsum()
                    #Sanity using Computed Monthly
                    self.df2["sanityComputedMonthly"]=self.df2.loc[:,"lifted_gallons_daily_modified"].cumsum()[-1]==self.df2.loc[:,"Lifted_actual_monthly"][-1]
            grpNRD=self.df2.groupby(["next_refresh_date_Weekly"])
            # self.df2["sanityComputedWeekly"]=0
            df2.insert(df2.keys().get_loc("computedMonthly"),"sanityComputedWeekly",0)
            for key,grp in grpNRD:
                if -1 not in grp["lifted_gallons_daily_modified"].values:
                    self.df2.loc[grp.index,"computedWeekly"]=grp["lifted_gallons_daily_modified"].cumsum()
                    validLiftedGallons=(df2.loc[grp.index,"lifted_gallons_daily_modified"].cumsum())[-1]==(df2.loc[grp.index,"lifted_gallons_Weekly"])[-1]
                    df2.loc[grp.index,"sanityComputedWeekly"]=validLiftedGallons
            #Sanity using Computed Monthly
            # self.df2.loc[self.df2.loc[:,"lifted_gallons_daily_modified"].cumsum()==self.df2.loc[:,"computedMonthly"],"sanityComputedMonthly"]=1
        except Exception as e:
            print "Exception",e
    def computeWeeklyfromReconciledDaily(self,df2):
        grpNRD=self.df2.groupby(["next_refresh_date_Weekly"])
        for key,grp in grpNRD:
            for i in range(len(grp)):
                        #check if not first day of week
                        if i!=0:
                            if grp.ix[i]["lifted_gallons_daily_flag"]>0:
                                previousDayWeekly=self.df2.loc[grp.ix[i-1]["date"],"lifted_gallons_Weekly"]
                                if previousDayWeekly !=-1:
                                    self.df2.loc[grp.ix[i]["date"],"lifted_gallons_Weekly"]=previousDayWeekly+grp.ix[i]["lifted_gallons_daily_modified"]
                                    self.df2.loc[grp.ix[i]["date"],"lifted_gallons_weekly_flag"]=grp.ix[i]["lifted_gallons_daily_flag"]
                        else:
                            if grp.ix[i]["lifted_gallons_daily_flag"]>0:
                                self.df2.loc[grp.ix[i]["date"],"lifted_gallons_Weekly"]=grp.ix[i]["lifted_gallons_daily_modified"]
                                self.df2.loc[grp.ix[i]["date"],"lifted_gallons_weekly_flag"]=grp.ix[i]["lifted_gallons_daily_flag"]
    def verifyWeekByNRD(self,df2):
        try:
            df2.insert(df2.keys().get_loc("Modified_NRD"),"Week_switch","Unknown")
            count=1
            grpNRD=df2.groupby(["next_refresh_date_Weekly"])["lifted_gallons_Weekly","lifted_gallons_Daily"]
            df2["sanityWeekly_CumulativeDaily_NextRefreshDate"]=0
            #firstNextRefreshDate
            firstNRD=self.df2["next_refresh_date_Weekly"].iloc[0]
            for key,grp in grpNRD:
                df2.loc[df2["next_refresh_date_Weekly"]==key,"Week_switch"]="w"+str(count)
                count=count+1
                # print key
                # opening balance
                nrdGroups=self.df2["next_refresh_date_Weekly"].value_counts()
                openingBalances={}
                for i in range(len(grp)):
                    #check if it not reconciled by monthly
                    if df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]==0:
                        #check if it is not the first day of the week
                        if i!=0:
                            liftedWeeklyPrevious=df2.loc[str(grp.ix[i-1]['date']),"lifted_gallons_Weekly"]
                            lifteddailyPrevious=df2.loc[str(grp.ix[i-1]['date']),"lifted_gallons_Daily"]
                            actualLiftedDaily=df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Daily"]
                            liftedWeeklyPresent=df2.loc[str(grp.ix[i]['date']),"lifted_gallons_Weekly"]
                            if liftedWeeklyPrevious != -1 and liftedWeeklyPresent != -1 and lifteddailyPrevious!=-1 and actualLiftedDaily!=-1:
                                    beginPreviousWeekly=liftedWeeklyPrevious-lifteddailyPrevious
                                    beginPresentWeekly=liftedWeeklyPresent-actualLiftedDaily
                                    previousTrueDaily=beginPresentWeekly-beginPreviousWeekly
                                    df2.loc[str(grp.ix[i-1]['date']),"lifted_gallons_daily_modified"]=previousTrueDaily
                                    df2.loc[str(grp.ix[i-1]['date']),"lifted_gallons_daily_flag"]=3

                            else:
                                if liftedWeeklyPresent == -1 and actualLiftedDaily==-1 and i!=(len(grp)-1):
                                    self.computeMissingLiftedWeeklyAndDailyOneDayNRD(df2,grp,i)
                                else:
                                    print "No Enough Data available to Fill Lifted Gallons NRD"
                        else:
                            #check if it is first week of the month
                            if key==firstNRD and  firstNRD!=0 :

                                if nrdGroups[firstNRD]!=7:
                                    openingBalances=self.verifyOpeningBalanceReconciled()
                                    if openingBalances !={}:
                                        if openingBalances['lifted_weekly'] != -1 and grp.ix[i]["lifted_gallons_Weekly"] != -1:
                                            if df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"] != grp.ix[i]["lifted_gallons_Weekly"]-openingBalances['lifted_weekly']:
                                                modifiedLGD=grp.ix[i]["lifted_gallons_Weekly"]-openingBalances['lifted_weekly']
                                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"]=modifiedLGD
                                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]=5
                                elif grp.ix[i]["lifted_gallons_Weekly"] == -1 and grp.ix[i]["lifted_gallons_daily_modified"]==-1:
                                    self.computeMissingLiftedWeeklyAndDailyFirstDayWeek(df2,grp,i)
                                elif (df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"] != grp.ix[i]["lifted_gallons_Weekly"]) and grp.ix[i]["lifted_gallons_Weekly"] != -1:
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"]= grp.ix[i]["lifted_gallons_Weekly"]
                                    df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]=3
                            #reconcile when first day of the week is missing
                            elif grp.ix[i]["lifted_gallons_Weekly"] == -1 and grp.ix[i]["lifted_gallons_daily_modified"]==-1:
                                    self.computeMissingLiftedWeeklyAndDailyFirstDayWeek(df2,grp,i)
                            elif (df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"] != grp.ix[i]["lifted_gallons_Weekly"]) and grp.ix[i]["lifted_gallons_Weekly"] != -1:
                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_modified"]= grp.ix[i]["lifted_gallons_Weekly"]
                                df2.loc[str(grp.ix[i]['date']),"lifted_gallons_daily_flag"]=3
                    else:
                                print "The Lifted Gallons cant be modified using weekly Lifted, as it is already Modified using Monthly values "
                if key==firstNRD and nrdGroups[firstNRD]!=7 :
                    if openingBalances =={}:
                        openingBalances=self.verifyOpeningBalanceReconciled()
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
        while (nd.month == self.appConst.dateDetails.month):
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
            liftedMonthlyPrevious=df2.ix[previousDayIndex]["lifted_gallons_Monthly"]
            lifteddailyPrevious=df2.ix[previousDayIndex]["lifted_gallons_Daily"]
            liftedMonthlyNext=df2.ix[nextDayIndex]["lifted_gallons_Monthly"]
            lifteddailyNext=df2.ix[nextDayIndex]["lifted_gallons_Daily"]
            if liftedMonthlyNext != -1 and lifteddailyNext != -1:
                if liftedMonthlyPrevious != -1 and lifteddailyPrevious != -1 :
                    beginPreviousMonthly=liftedMonthlyPrevious-lifteddailyPrevious
                    beginPresentMonthly=liftedMonthlyPrevious
                    #newMonthly
                    beginNextMonthly=liftedMonthlyNext-lifteddailyNext
                    #newLifted daily
                    presentTrueDaily=beginNextMonthly-beginPresentMonthly
                    # newMonthly=df2.ix[nextDayIndex]["lifted_gallons_Monthly"]-df2.ix[nextDayIndex]["lifted_gallons_Daily"]
                    # newlifted=newMonthly-df2.ix[previousDayIndex]["lifted_gallons_Monthly"]
                    df2.loc[str(df2.ix[ind]['date']),"lifted_gallons_daily_modified"]=presentTrueDaily
                    df2.loc[str(df2.ix[ind]['date']),"lifted_gallons_Monthly"]=beginNextMonthly
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
                lifteddailyPrevious=self.df2.loc[ind-datetime.timedelta(1),"lifted_gallons_Daily"]
                actualLiftedDaily=self.df2.loc[ind,"lifted_gallons_Daily"]
                liftedMonthlyPresent=self.df2.loc[ind,"lifted_gallons_Monthly"]
                print liftedMonthlyPresent,liftedMonthlyPrevious,actualLiftedDaily
                if liftedMonthlyPrevious != -1 and liftedMonthlyPresent!= -1 and actualLiftedDaily!=-1 and lifteddailyPrevious !=-1:
                    beginPreviousMonthly=liftedMonthlyPrevious-lifteddailyPrevious
                    beginPresentMonthly=liftedMonthlyPresent-actualLiftedDaily
                    previousTrueDaily=beginPresentMonthly-beginPreviousMonthly
                    # newLifted=liftedMonthlyPresent-liftedMonthlyPrevious
                    if previousTrueDaily >=0:
                        self.df2.loc[ind-datetime.timedelta(1),"lifted_gallons_daily_modified"]=previousTrueDaily
                        self.df2.loc[ind-datetime.timedelta(1),"lifted_gallons_daily_flag"]=1
                    if ind.day==self.daysInMonth:
                        print "Last Day is Assumed to be true!"
                        self.df2.loc[ind,"lifted_gallons_daily_flag"]=1
                else:
                    if liftedMonthlyPresent == -1 and actualLiftedDaily==-1 and ind.day!=self.daysInMonth:
                        self.computeMissingLiftedMonthlyAndDailyOneDayNRD(self.df2,ind)
                    elif liftedMonthlyPrevious == -1 or liftedMonthlyPresent== -1:
                        print "Insufficient Data to Compute Daily Lifted using Monthly as it has a Missing Previous Day"
                    else:
                        print "Insufficient Data to Compute Daily Lifted using Monthly"
            else:
                actualLiftedDaily=self.df2.loc[ind,"lifted_gallons_daily_modified"]
                liftedMonthlyPresent=self.df2.loc[ind,"lifted_gallons_Monthly"]
                if liftedMonthlyPresent == -1 and actualLiftedDaily ==-1:
                    self.computeMissingLiftedWeeklyAndDailyFirstDayMonth(ind)
                elif actualLiftedDaily != liftedMonthlyPresent and liftedMonthlyPresent!=-1:
                        self.df2.loc[ind,"lifted_gallons_daily_modified"]=liftedMonthlyPresent
                        self.df2.loc[ind,"lifted_gallons_daily_flag"]=1
                elif actualLiftedDaily == liftedMonthlyPresent:
                        self.df2.loc[ind,"lifted_gallons_daily_flag"]=1

    def executeRules(self):
        try:
            print "***** Execution in Combination:",self.appConst.customer,self.appConst.supplier,self.appConst.account,self.appConst.terminal,self.appConst.product

            if not os.path.exists(self.appConst.savepath):
                os.makedirs(self.appConst.savepath)
            self.fileName=self.get_filename()
            self.df2=copy.deepcopy(self.appConst.mp.loc[(self.appConst.mp["account_type"]==self.appConst.account)&(self.appConst.mp["supplier_terminal_name"]==self.appConst.terminal)&(self.appConst.mp["product_name"]==self.appConst.product)&(self.appConst.mp["date"]>=self.appConst.dateDetails.strftime("%Y-%m-%d")),:])
            # time.sleep(10)
            dateDetails=datetime.datetime.strptime(self.appConst.analysisDate,"%d-%m-%Y")
            self.appConst.dateDetails=dateDetails
            self.daysInMonth=calendar.monthrange(dateDetails.year,dateDetails.month)[1]
            idx=pd.date_range(dateDetails.strftime('%m-%d-%Y'),str(dateDetails.month)+'-'+str(self.daysInMonth)+'-'+str(dateDetails.year))
            self.df2['date']= pd.to_datetime(self.df2[u'date'],format='%Y-%m-%d')
            self.df2.index=pd.DatetimeIndex(self.df2['date'])
            self.df2 = self.df2.reindex(idx, fill_value=-1)
            self.df2['date']=self.df2.index.values
            self.df2[u"next_refresh_date_Weekly"]=self.df2[u"next_refresh_date_Weekly"].apply(lambda x:-1 if x=='0' or x==0  else x)
            self.df2[u"lifted_gallons_Weekly"]=self.df2[u"lifted_gallons_Weekly"].apply(lambda x:int(x) if type(x)!=int  else x)
            findWeeks=dict(self.df2[u"next_refresh_date_Weekly"].value_counts())
            countFindWeeks=0
            if findWeeks.has_key(-1):
                countFindWeeks+=findWeeks[-1]
            if findWeeks.has_key(0):
                countFindWeeks+=findWeeks[0]
            if countFindWeeks==self.daysInMonth:
                savepath=self.appConst.savepath+"\\No_Weekly_Refresh_Dates"
                if not os.path.exists(savepath):
                    os.makedirs(savepath)
                file=open(savepath+"\\"+self.fileName+".txt","w")
                file.write("No Weekly Refresh Dates to Compute for combination "+self.fileName+"....\n Exiting Execution.....")
                file.close()
                print "No Weekly Refresh Dates to Compute....\n Exiting Execution....."
                if self.appConst.supplier!='Shell':
                    return 0
            validateLiftedValuesWeekly=dict(self.df2["lifted_gallons_Weekly"].value_counts())
            countValidLiftedValues=0
            if validateLiftedValuesWeekly.has_key(-1):
                countValidLiftedValues+=validateLiftedValuesWeekly[-1]
            if validateLiftedValuesWeekly.has_key(0):
                countValidLiftedValues+=validateLiftedValuesWeekly[0]
            if countValidLiftedValues==self.daysInMonth:
                savepath=self.appConst.savepath+"\\No_Weekly_Lifted_Gallons"
                if not os.path.exists(savepath):
                    os.makedirs(savepath)
                file=open(savepath+"\\"+self.fileName+".txt","w")
                file.write("No Weekly Lifted gallons to Compute week switches for combination "+self.fileName+"....\n Exiting Execution.....")
                file.close()
                print "No Weekly Lifted gallons to Compute week switches....\n Exiting Execution....."
                if self.appConst.supplier!='Shell':
                    return 0
            self.df2[u"next_refresh_date_Weekly"]=self.df2[u"next_refresh_date_Weekly"].apply(lambda x:datetime.datetime.strptime(' '.join(str(x).split(' ')[:2]),'%m/%d %H:%M:%S') if x!= -1 else x)
            self.df2[u"next_refresh_date_Weekly"]=self.df2[u"next_refresh_date_Weekly"].apply(lambda x:x.replace(year=datetime.datetime.now().year) if x!= -1 else x)
            self.df2[u"next_refresh_date_Weekly"]=self.df2[u"next_refresh_date_Weekly"].astype(datetime.datetime)
            uniqueDates=self.df2[u"next_refresh_date_Weekly"].astype(datetime.datetime).unique()
            uniqueDatesList=uniqueDates.tolist()
            if -1 in uniqueDatesList:
                uniqueDatesList.remove(-1)
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
            validDatesRowValues=self.df2.loc[self.df2["next_refresh_date_Weekly"]!=-1,"next_refresh_date_Weekly"]
            validDatesRowValuesPairs=validDatesRowValues.drop_duplicates().to_dict()
            #Reverese the Key Values
            if(len(validDatesRowValues)<self.daysInMonth):
                xcc={}

                for k,v in validDatesRowValuesPairs.iteritems():
                     xcc[v]=k
                if xcc.has_key(-1):
                    xcc.pop(-1)
                self.df2.insert(self.df2.keys().get_loc("next_refresh_date_Weekly"),"Modified_NRD",0)

                # values=self.df2.loc[self.df2['base_gallons_Daily']!=-1]['base_gallons_Daily'].unique()
                # baseDaily=min(values)if len(values)>0 else -1
                # values=self.df2.loc[self.df2['base_gallons_Monthly']!=-1]['base_gallons_Monthly'].unique()
                # baseMonthly=min(values)if len(values)>0 else -1
                # values=self.df2.loc[self.df2['base_gallons_Weekly']!=-1]['base_gallons_Weekly'].unique()
                # baseWeekly=min(values)if len(values)>0 else -1
                # values=self.df2.loc[self.df2['beginning_gallons_Daily']!=-1]['beginning_gallons_Daily'].unique()
                # beginningDaily=min(values)if len(values)>0 else -1
                # values=self.df2.loc[self.df2['beginning_gallons_Monthly']!=-1]['beginning_gallons_Monthly'].unique()
                # beginningMonthly=min(values)if len(values)>0 else -1
                # values=self.df2.loc[self.df2['beginning_gallons_Weekly']!=-1]['beginning_gallons_Weekly'].unique()
                # beginningWeekly=min(values)if len(values)>0 else -1
                # values=self.df2.loc[self.df2['execution_date_Daily']!=-1]['execution_date_Daily'].unique()
                # execution_date_Daily=min(values)
                for i in self.df2.index:
                    if self.df2.loc[i,'execution_date_Daily']==-1:
                        self.df2.loc[i,'execution_date_Daily']=str(i)
                    if self.df2.loc[i,'execution_date_Monthly']==-1:
                        self.df2.loc[i,'execution_date_Monthly']=str(i)
                    if self.df2.loc[i,'execution_date_Weekly']==-1:
                        self.df2.loc[i,'execution_date_Weekly']=str(i)
                for validDate in validDates:
                    validDateday=validDate[0]
                    if self.df2.loc[str(dateDetails.replace(day=validDateday)),"next_refresh_date_Weekly"]==-1:
                        self.df2.loc[str(dateDetails.replace(day=validDateday)),"next_refresh_date_Weekly"]=validDate[1]
                        # self.df2.loc[str(dateDetails.replace(day=validDate[0])),'account_type']=self.df2.loc[str(xcc[validDate[1]]),'account_type']
                        # self.df2.loc[str(dateDetails.replace(day=validDate[0])),'supplier_terminal_name']=self.df2.loc[str(xcc[validDate[1]]),'supplier_terminal_name']
                        # self.df2.loc[str(dateDetails.replace(day=validDate[0])),'product_name']=self.df2.loc[str(xcc[validDate[1]]),'product_name']
                        self.df2.loc[str(dateDetails.replace(day=validDateday)),'account_type']=self.appConst.account
                        self.df2.loc[str(dateDetails.replace(day=validDateday)),'supplier_name']=self.appConst.supplier
                        self.df2.loc[str(dateDetails.replace(day=validDateday)),'supplier_terminal_name']=self.appConst.terminal
                        self.df2.loc[str(dateDetails.replace(day=validDateday)),'product_name']=self.appConst.product
                        self.df2.loc[str(dateDetails.replace(day=validDateday)),"Modified_NRD"]=1
                        self.df2.loc[str(dateDetails.replace(day=validDateday)),'execution_date_Daily']=str(dateDetails.replace(day=validDateday))
                        self.df2.loc[str(dateDetails.replace(day=validDateday)),'execution_date_Monthly']=str(dateDetails.replace(day=validDateday))
                        self.df2.loc[str(dateDetails.replace(day=validDateday)),'execution_date_Weekly']=str(dateDetails.replace(day=validDateday))
                        # self.df2.loc[str(dateDetails.replace(day=validDateday)),'base_gallons_Daily']=baseDaily
                        # self.df2.loc[str(dateDetails.replace(day=validDateday)),'base_gallons_Monthly']=baseMonthly
                        # self.df2.loc[str(dateDetails.replace(day=validDateday)),'base_gallons_Weekly']=baseWeekly
                        # self.df2.loc[str(dateDetails.replace(day=validDateday)),'beginning_gallons_Daily']=beginningDaily
                        # self.df2.loc[str(dateDetails.replace(day=validDateday)),'beginning_gallons_Monthly']=beginningMonthly
                        # self.df2.loc[str(dateDetails.replace(day=validDateday)),'beginning_gallons_Weekly']=beginningWeekly

                        # print validDate
            else:
                print "All next refresh Dates Valid"
                self.df2.insert(self.df2.keys().get_loc("next_refresh_date_Weekly"),"Modified_NRD",0)
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
            # self.df2["computed_weekly"]=self.df2.groupby(["Week_switch"])["lifted_gallons_daily_modified"].cumsum()
            if "lifted_gallons_Monthly" in self.df2.keys():
                self.df2["sanityMonthly_CumulativeDaily_WeeksByLiftedGallons"]=0
                self.df2.loc[self.df2.loc[:,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==self.df2.loc[:,"lifted_gallons_Monthly"],"sanityMonthly_CumulativeDaily_WeeksByLiftedGallons"]=1
                self.df2["sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate"]=0
                self.df2.loc[self.df2.loc[:,"lifted_gallons_daily_modified"].cumsum()==self.df2.loc[:,"lifted_gallons_Monthly"],"sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate"]=1
            self.df2.to_csv(self.appConst.savepath+"\\"+self.fileName+".csv")
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
            if len(self.appConst.frames)>0:
                result=pd.concat(self.appConst.frames)
                # db = MySQLdb.connect(self.appConst.dbcon[0],self.appConst.dbcon[1],self.appConst.dbcon[2],self.appConst.dbcon[3])
                if self.deleteOldMonth():

                    print "Deletion Success"
                # resultNew.to_sql(name=self.appConst.customer+"_"+self.appConst.supplier+"_reconciledPivot",con=db,flavor='mysql', if_exists='replace')
                # db.close()
                indexCol=[u'date', u'account_type', u'supplier_terminal_name', u'product_name','lifted_gallons_modified_WeeksByLiftedGallons','lifted_gallons_daily_flag','lifted_gallons_daily_modified', "lifted_gallons_Daily",'lifted_gallons_weekly_flag',"lifted_gallons_Weekly", "Lifted_actual_weekly",'lifted_gallons_monthly_flag',"lifted_gallons_Monthly", "Lifted_actual_monthly", 'WeeksByLiftedGallons','Week_switch', "base_gallons_Daily", "base_gallons_Monthly", "base_gallons_Weekly", 'Modified_WeeksByLiftedGallons', "beginning_gallons_Daily", "beginning_gallons_Monthly", "beginning_gallons_Weekly", "en_allocation_status_Daily", "en_allocation_status_Monthly", "en_allocation_status_Weekly", "percentage_allocation_Daily", "percentage_allocation_Monthly", "percentage_allocation_Weekly","percentage_allocation_Daily_actual","percentage_allocation_Monthly_actual","percentage_allocation_Weekly_actual", "alerts_ratability_Daily", "alerts_ratability_Monthly", "alerts_ratability_Weekly", "next_refresh_date_Daily", "next_refresh_date_Monthly", 'Modified_NRD', "next_refresh_date_Weekly", 'sanityWeekly_CumulativeDaily_WeeksByLiftedGallons','sanityWeekly_CumulativeDaily_NextRefreshDate','sanityMonthly_CumulativeDaily_WeeksByLiftedGallons','sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate','computedWeekly','computedMonthly','sanityComputedMonthly','sanityComputedWeekly', "base_gallons_Daily_Reconciled", "base_gallons_Monthly_Reconciled", "base_gallons_Weekly_Reconciled" ,"beginning_gallons_Daily_Reconciled", "beginning_gallons_Monthly_Reconciled", "beginning_gallons_Weekly_Reconciled","next_refresh_base_gallons_Daily","next_refresh_base_gallons_Monthly","NRB_Daily","NRB_Monthly",u'remaining_gallons_Daily', u'remaining_gallons_Monthly', u'remaining_gallons_Weekly', u'additional_gallons_allowed_Daily', u'additional_gallons_allowed_Monthly', u'additional_gallons_allowed_Weekly', u'additional_gallons_remaining_Daily', u'additional_gallons_remaining_Monthly', u'additional_gallons_remaining_Weekly',"remaining_gallons_Daily_Reconciled","remaining_gallons_Monthly_Reconciled","remaining_gallons_Weekly_Reconciled", "additional_gallons_remaining_Daily_Reconciled","additional_gallons_remaining_Monthly_Reconciled","additional_gallons_remaining_Weekly_Reconciled","Ratability_Daily","Ratability_Weekly","Ratability_Monthly",'Synthetic_Base_Daily','Synthetic_Base_Weekly','Synthetic_Base_Monthly']
                columnHeader=[u'date', u'account_type', u'supplier_terminal_name', u'product_name','Lifted_mod_daily_weekly_value','lifted_gallons_daily_flag','Lifted_mod_daily_nextrefresh','Lifted_actual_daily','lifted_gallons_weekly_flag','lifted_gallons_weekly_modified','Lifted_actual_weekly','lifted_gallons_monthly_flag','lifted_gallons_monthly_modified','Lifted_actual_monthly','Week_structure_weekly_value','Week_structure_next_refresh', "base_gallons_Daily", "base_gallons_Monthly", "base_gallons_Weekly", 'daily_weekValue_flag', "beginning_gallons_Daily", "beginning_gallons_Monthly", "beginning_gallons_Weekly", "en_allocation_status_Daily", "en_allocation_status_Monthly", "en_allocation_status_Weekly", "percentage_allocation_Daily", "percentage_allocation_Monthly", "percentage_allocation_Weekly", "percentage_allocation_Daily_actual","percentage_allocation_Monthly_actual","percentage_allocation_Weekly_actual" ,"alerts_ratability_Daily", "alerts_ratability_Monthly", "alerts_ratability_Weekly", "next_refresh_date_Daily", "next_refresh_date_Monthly", 'Modified_NRD', "next_refresh_date_Weekly", 'sanityWeekly_CumulativeDaily_WeeksByLiftedGallons','sanityWeekly_CumulativeDaily_NextRefreshDate','sanityMonthly_CumulativeDaily_WeeksByLiftedGallons','sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate','computedWeekly','computedMonthly','sanityComputedMonthly','sanityComputedWeekly',"base_gallons_Daily_Reconciled", "base_gallons_Monthly_Reconciled", "base_gallons_Weekly_Reconciled", "beginning_gallons_Daily_Reconciled", "beginning_gallons_Monthly_Reconciled", "beginning_gallons_Weekly_Reconciled","next_refresh_base_gallons_Daily","next_refresh_base_gallons_Monthly","NRB_Daily","NRB_Monthly",u'remaining_gallons_Daily', u'remaining_gallons_Monthly', u'remaining_gallons_Weekly', u'additional_gallons_allowed_Daily', u'additional_gallons_allowed_Monthly', u'additional_gallons_allowed_Weekly', u'additional_gallons_remaining_Daily', u'additional_gallons_remaining_Monthly', u'additional_gallons_remaining_Weekly',"remaining_gallons_Daily_Reconciled","remaining_gallons_Monthly_Reconciled","remaining_gallons_Weekly_Reconciled", "additional_gallons_remaining_Daily_Reconciled","additional_gallons_remaining_Monthly_Reconciled","additional_gallons_remaining_Weekly_Reconciled","Ratability_Daily","Ratability_Weekly","Ratability_Monthly",'Synthetic_Base_Daily','Synthetic_Base_Weekly','Synthetic_Base_Monthly']
                result.to_excel(self.appConst.savepath+self.appConst.supplier+"_"+self.appConst.month+"_reconciled.xls",columns=indexCol,header=columnHeader)
                result.to_sql(name="enallocationarchive_debug",con=self.appConst.db,flavor='mysql', if_exists='append')
        except Exception as e:
            print "Exception:",e
    def deleteOldMonth(self):
        try:
            cursor=self.appConst.db.cursor()
            sql="delete from enallocationarchive_debug where supplier_name='%s' and month(date)=%s"%(self.appConst.supplier,self.appConst.dateDetails.month)
            rows=cursor.execute(sql)
            print "Rows ",rows,"Deleted Successfully"
            return True
            self.appConst.db.commit()
        except Exception as e:
            print "Exception",e
            return False
    def get_maxbatch_analysis_pivot(self,executionFrom,executionTo):
        try:
            condition="Execution_Date>='%s' and Execution_Date<='%s' and supplier_name='%s'"%(self.appConst.previousMonthexecutionFrom,executionTo,self.appConst.supplier)
            sql="""select *,date(execution_date) as date from (select
            `x`.`supplier_name`,
            `x`.`en_account_type`,
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
            (select supplier_name,max(batchno) as batchno,date(Execution_Date) as dt from `pilot_production_dump`.`enallocationarchivecontract`
            where %s
            group by supplier_name,date(Execution_Date)) a join

            (SELECT
            *
            FROM `pilot_production_dump`.`enallocationarchivecontract`
            where %s and (Account_Type<>"Unknown" and supplier_terminal_name<>"Unknown" and product_name<>"Unknown")
                        order by execution_date desc,batchno desc,account_type,supplier_terminal_name,product_name,period ) x on a.batchno=x.batchno and a.dt=date(x.Execution_Date) and a.supplier_name=x.supplier_name)w
                        GROUP BY date(execution_date),account_type,supplier_terminal_name,product_name,period

            having maX(batchno)
            order by account_type,supplier_terminal_name,product_name,date,period;"""%(condition,condition)
            frameName=self.appConst.customer+"_"+self.appConst.supplier+"_"+self.appConst.month+"_maxBatchFrame.csv"
            if os.path.isfile(self.appConst.savepath+frameName):
                df_mysql=pd.read_csv(self.appConst.savepath+frameName)
            else:
                df_mysql = pd.read_sql(sql, con=self.appConst.db)
            # time.sleep(1)
                df_mysql.to_csv(self.appConst.savepath+frameName)
            mp=pd.pivot_table(df_mysql,index=["date","supplier_name","account_type","supplier_terminal_name","product_name"],values=["base_gallons","lifted_gallons","beginning_gallons","en_allocation_status",'percentage_allocation','alerts_ratability','next_refresh_date','en_account_type','next_refresh_base_gallons','remaining_gallons','additional_gallons_allowed','additional_gallons_remaining','staging_id','execution_date'],columns="period",aggfunc = lambda x: x)
            mp.columns=['_'.join(col).strip() for col in mp.columns.values]
            stdColumns=['base_gallons_Daily', 'base_gallons_Monthly', 'base_gallons_Weekly',
       'lifted_gallons_Daily', 'lifted_gallons_Monthly',
       'lifted_gallons_Weekly', 'beginning_gallons_Daily',
       'beginning_gallons_Monthly', 'beginning_gallons_Weekly',
       'en_allocation_status_Daily', 'en_allocation_status_Monthly',
       'en_allocation_status_Weekly', 'percentage_allocation_Daily',
       'percentage_allocation_Monthly', 'percentage_allocation_Weekly',
       'alerts_ratability_Daily', 'alerts_ratability_Monthly',
       'alerts_ratability_Weekly', 'next_refresh_date_Daily',
       'next_refresh_date_Monthly', 'next_refresh_date_Weekly',
       'next_refresh_base_gallons_Daily','next_refresh_base_gallons_Weekly',
       'next_refresh_base_gallons_Monthly',u'remaining_gallons_Daily',
       u'remaining_gallons_Monthly', u'remaining_gallons_Weekly',
       u'additional_gallons_allowed_Daily', u'additional_gallons_allowed_Monthly',
       u'additional_gallons_allowed_Weekly', u'additional_gallons_remaining_Daily',
       u'additional_gallons_remaining_Monthly', u'additional_gallons_remaining_Weekly',
       'execution_date_Daily','execution_date_Monthly','execution_date_Weekly']
            keys=mp.keys().values
            for column in stdColumns:
                if column not in keys:
                    print column," Not found! Inserted it..."
                    mp[column]=0
            supplier=[]
            dates=[]
            accounts=[]
            terminals=[]
            products=[]
            for i in mp.index.values:
                dates.append(str(i[0]))
                supplier.append(str(i[1]))
                accounts.append(i[2])
                terminals.append(i[3])
                products.append(i[4])
            mp.insert(0,"product_name",products)
            mp.insert(0,"supplier_terminal_name",terminals)
            mp.insert(0,"account_type",accounts)
            mp.insert(0,"supplier_name",supplier)
            mp.insert(0,"date",dates)
            mp=mp.fillna(-1)
            return mp
        except Exception as e:
            print e
if __name__ == "__main__":
    # 'BP','Holly','Chevron','Exxon','Valero','P66','Tesoro'
    # suppliers=['Holly','Valero','P66','Tesoro']
    suppliers=['Chevron','Exxon','Holly','Valero','P66','Tesoro','BP','Shell']
    customer="pilot"
    suppliers=['P66']
    # exeDates=['01-06-2015','01-07-2015','01-08-2015']
    exeDates=['01-09-2015','01-10-2015']
    savePath=r"C:\spark_output\Ra_Newapproach\AnalysisRules_Contract_NewApproach"+str(datetime.datetime.now().date())
    # customer=sys.argv[1]
    # suppliers=sys.argv[2]
    # exeDate=sys.argv[3]
    # savePath=sys.argv[4]
    for exeDate in exeDates:
        for i in suppliers:
            start=datetime.datetime.now()
            print "Execution Started Please Wait....."
            detailedList=[customer,i,exeDate,savePath]
            appConst=entities.entity(detailedList)
            executeEngine=ruleEngine(appConst)
            # executeEngine.runRules((('PILOT TRAVEL CENTERS LLC-10024029BR', 'ALBUQUERQUE NM PSX - 030A', 'GASOLINE'),))
            # executeEngine.runRules((('UNBRANDED', 'Albuquerque NM - VEC - T109', 'V'),))
            executeEngine.runRules()
            print "Completed Execution Thanks for you Patience "
            print datetime.datetime.now()-start