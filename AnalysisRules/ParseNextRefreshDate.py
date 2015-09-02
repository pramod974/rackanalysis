__author__ = 'pramod.kumar'
__author__ = 'pramod.kumar'
import pandas as pd
import calendar
import datetime
import MySQLdb
import matplotlib.pyplot as plt
plt.interactive(False)
import numpy as np
def verifyWeekByDailyLiftedValues(df2):
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
    while (tt[-1][-1].day != int(daysInMonth)):
        startDay=(tt[-1][-1]+datetime.timedelta(1)).day
        # print startDay
        period=7 if int(daysInMonth)-startDay >= 7 else (int(daysInMonth)-startDay)+1
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
    df2["sanityWeekly_CumulativeDaily_WeeksByLiftedGallons"]=0
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

# sql="select * from rack_analysis.pilot_chevron_june where account_type='PILOT TRAVEL CENTERS LLC : CHV7460761' and supplier_terminal_name='1002 TRACY CA TRM CHEVRON' and product_name='DIESEL #2';"
sql="select * from rack_analysis.pilot_exxon_june where account_type='PILOT TRAVEL CENTERS LLC 103637 IW' and supplier_terminal_name='HAMMOND IN (MOC) - 00DH' and product_name='Prem RFG';"
db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
df2=pd.read_sql(sql, con=db)
year=2015
analysisDate='01-06-2015'
dateDetails=datetime.datetime.strptime(analysisDate,"%d-%m-%Y")
month=dateDetails.month
fromDate='2015-'+str(dateDetails.month).zfill(2)+'-01 00:00:00'
toDate='2015-'+str(dateDetails.month+1).zfill(2)+'-01 00:00:00'
daysInMonth=str(calendar.monthrange(dateDetails.year,dateDetails.month)[1])
idx=pd.date_range(dateDetails.strftime('%m-%d-%Y'),str(dateDetails.month)+'-'+daysInMonth+'-'+str(dateDetails.year))
df2['date']= pd.to_datetime(df2[u'date'],format='%Y-%m-%d')
df2.index=pd.DatetimeIndex(df2['date'])
df2 = df2.reindex(idx, fill_value=0)
df2['date']=df2.index.values

df2[u"('next_refresh_date', 'Weekly')"]=df2[u"('next_refresh_date', 'Weekly')"].apply(lambda x:int(x) if x=='0' else x)
findWeeks=dict(df2[u"('next_refresh_date', 'Weekly')"].value_counts())
if findWeeks.has_key(0):
    if findWeeks[0]==int(daysInMonth):
        # print
        exit ("No Weekly Refresh Dates to Compute....\n Exiting Execution.....")
df2[u"('next_refresh_date', 'Weekly')"]=df2[u"('next_refresh_date', 'Weekly')"].apply(lambda x:datetime.datetime.strptime(' '.join(x.split(' ')[:2]),'%m/%d %H:%M:%S') if x!=0 else x)
df2[u"('next_refresh_date', 'Weekly')"]=df2[u"('next_refresh_date', 'Weekly')"].apply(lambda x:x.replace(year=datetime.datetime.now().year) if x!=0 else x)
uniqueDates=df2[u"('next_refresh_date', 'Weekly')"].unique()
uniqueDatesList=uniqueDates.tolist()
uniqueDatesList.remove(0)
# print "********************Initial**************************",df2
validDates=[]
for i in uniqueDatesList:
    newDate=i-datetime.timedelta(7);print newDate,i
    if newDate.month<dateDetails.month:
        for a in range(1,i.day):
            print a,i;validDates.append([a,i])
    elif i.month>uniqueDatesList[0].month:
        for a in range(newDate.day,int(daysInMonth)+1):
            print a,i;validDates.append([a,i])
    else:
        for a in range(newDate.day,i.day):
            print a,i;validDates.append([a,i])
# validDates.sort()
# print df2
# df2["('next_refresh_date', 'Weekly')"].value_counts()
# df2.loc[str(validDates[0][1].replace(day=validDates[0][0])),"('next_refresh_date', 'Weekly')"]

#fetech rows with valid Next_Refresh_Dates
validDatesRowValues=df2.loc[df2["('next_refresh_date', 'Weekly')"]!=0,"('next_refresh_date', 'Weekly')"]
validDatesRowValuesPairs=validDatesRowValues.drop_duplicates().to_dict()
#Reverese the Key Values
if(len(validDatesRowValues)<int(daysInMonth)):
    xcc={}

    for k,v in validDatesRowValuesPairs.iteritems():
         xcc[v]=k
    if xcc.has_key(0):
        xcc.pop(0)
    df2.insert(df2.keys().get_loc("('next_refresh_date', 'Weekly')"),"Modified_NRD",0)
    # for k,v in validDatesRowValuesPairs.iteritems():
    #     validDatesRowValuesPairs[str(k)]=str(vars().pop(k))
    # df2['Modified_NRD']=0

    for validDate in validDates:
        if df2.loc[str(dateDetails.replace(day=validDate[0])),"('next_refresh_date', 'Weekly')"]==0:
            df2.loc[str(dateDetails.replace(day=validDate[0])),"('next_refresh_date', 'Weekly')"]=validDate[1]
            df2.loc[str(dateDetails.replace(day=validDate[0])),'account_type']=df2.loc[str(xcc[validDate[1]]),'account_type']
            df2.loc[str(dateDetails.replace(day=validDate[0])),'supplier_terminal_name']=df2.loc[str(xcc[validDate[1]]),'supplier_terminal_name']
            df2.loc[str(dateDetails.replace(day=validDate[0])),'product_name']=df2.loc[str(xcc[validDate[1]]),'product_name']
            df2.loc[str(dateDetails.replace(day=validDate[0])),"Modified_NRD"]=1
            print validDate
    verifyWeekByDailyLiftedValues(df2)
    #Find Week_Switch
    df2.insert(df2.keys().get_loc("Modified_NRD"),"Week_switch","Unknown")
    count=1
    #Compute Lifted Gallons

    df2.insert(df2.keys().get_loc("('lifted_gallons', 'Daily')"),"lifted_gallons_modified_NextRefreshDate",df2["('lifted_gallons', 'Daily')"].values)
    df2.insert(df2.keys().get_loc("lifted_gallons_modified_NextRefreshDate"),"Modified_LGD_NextRefreshDate",0)
    grpNRD=df2.groupby(["('next_refresh_date', 'Weekly')"])["('lifted_gallons', 'Weekly')","('lifted_gallons', 'Daily')"]
    df2["sanityWeekly_CumulativeDaily_NextRefreshDate"]=0
    for key,grp in grpNRD:
        print key
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
    # print "********************Initial**************************",df2
    df2["sanityMonthly_CumulativeDaily_WeeksByLiftedGallons"]=0
    df2.loc[df2.loc[:,"lifted_gallons_modified_WeeksByLiftedGallons"].cumsum()==df2.loc[:,"('lifted_gallons', 'Monthly')"],"sanityMonthly_CumulativeDaily_WeeksByLiftedGallons"]=1
    df2["sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate"]=0
    df2.loc[df2.loc[:,"lifted_gallons_modified_NextRefreshDate"].cumsum()==df2.loc[:,"('lifted_gallons', 'Monthly')"],"sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate"]=1
    df2.to_excel("testNRDwithvaluesModifiedFinal.xls")
    db.close()
else:
    print "All next refresh Dates Valid"
# for k,i in validDatesRowValuesPairs.iteritems():
#     newDate=i-datetime.timedelta(7);print newDate,i
#     if newDate.month<6:
#         for a in range(1,i.day):
#             print a,i;validDates.append([a,k,i])
#     elif i.month>uniqueDatesList[0].month:
#         for a in range(newDate.day,int(daysInMonth)+1):
#             print a,i;validDates.append([a,k,i])
#     else:
#         for a in range(newDate.day,i.day):
#             print a,i;validDates.append([a,k,i])



# grp["('lifted_gallons', 'Daily')"].cumsum()[-1]==grp["('lifted_gallons', 'Weekly')"][-1]
print "code further"



distinctNRDcounts=df2["('next_refresh_date', 'Weekly')"].value_counts()
zeros=df2.loc[df2["('next_refresh_date', 'Weekly')"]==0]

while(len(zeros)!=0):
        # nonzerosAhead=df2.loc[zeros.index[-1]+1:]["('next_refresh_date', 'Weekly')"]!=0
        nonzeroNearestNRD=df2.loc[(df2['date']>zeros.index[-1]) & (df2["('next_refresh_date', 'Weekly')"]!=0)][0]
        countOfNRD=distinctNRDcounts[nonzeroNearestNRD["('next_refresh_date', 'Weekly')"][0]]
        zeros=df2.loc[df2["('next_refresh_date', 'Weekly')"]==0]
        df2.loc[nonzeroNearestNRD.index[0]-min(7,nonzeroNearestNRD.index[0].day-1):nonzeroNearestNRD.index[0]-1]
        if countOfNRD==7:
            # df2.loc[nonzeroNearestNRD.index[0]-min(7,nonzeroNearestNRD.index[0].day-1):nonzeroNearestNRD.index[0]-1,"('next_refresh_date', 'Weekly')"]=nonzeroNearestNRD["('next_refresh_date', 'Weekly')"][0]-datetime.timedelta(1)
            df2.loc[nonzeroNearestNRD.index[0]-min(7,nonzeroNearestNRD.index[0].day-1):nonzeroNearestNRD.index[0]-1,"('next_refresh_date', 'Weekly')"]=nonzeroNearestNRD.index[0]
            df2.loc[nonzeroNearestNRD.index[0]-min(7,nonzeroNearestNRD.index[0].day-1):nonzeroNearestNRD.index[0]-1,"Modified_NRD"]=1
--------------------------------------------


#firstNextRefreshDate
firstNRD=self.df2["('next_refresh_date', 'Weekly')"].iloc[0]
# opening balance
if key==firstNRD :
    if firstNRD!=0:
        nrdGroups=self.df2["('next_refresh_date', 'Weekly')"].value_counts()
        if nrdGroups[firstNRD]!=7:
            openingBalance=self.verifyOpeningBalance()
----------------------------------

nf = pd.DataFrame(index=frame.index, columns=["date","account_type","supplier_terminal_name","product_name","lifted_gallon","lifted_type"] )

xyz = pd.DataFrame(index=np.arange(0, len(frame)*3), columns=["date","account_type","supplier_terminal_name","product_name","lifted_gallon","lifted_type"] )

mp=pd.pivot_table(resultNew,index=["date","account_type","supplier_terminal_name","product_name",'lifted_cal_type'],values=['lifted_gallon'],columns="lifted_cal_type",aggfunc = lambda x: x)

frms=[]
lgm=frame.loc[:,["date","account_type","supplier_terminal_name","product_name","lifted_gallons_modified_WeeksByLiftedGallons","Modified_WeeksByLiftedGallons"]]
lgm["lifted_cal_type"]="LgWeekValue"
lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
frms.append(lgm)
lgm=frame.loc[:,["date","account_type","supplier_terminal_name","product_name","lifted_gallons_modified_NextRefreshDate","Modified_LGD_NextRefreshDate"]]
lgm["lifted_cal_type"]="LgNRD"
lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
frms.append(lgm)
lgm=frame.loc[:,["date","account_type","supplier_terminal_name","product_name","('lifted_gallons', 'Daily')"]]
lgm["lifted_cal_type"]="Actual"
lgm.columns=[u'date', u'account_type', u'supplier_terminal_name', u'product_name', u'lifted_gallon', u'lifted_cal_type']
frms.append(lgm)
resultNew=pd.concat(frms)
mp=pd.pivot_table(resultNew,index=["date","account_type","supplier_terminal_name","product_name","lifted_cal_type"])















---------------------
LG
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
    self.df2.loc[wkz[i],"testit"]='w'+str(i+1)


--------------------

sql="""SELECT `index`,
   `lifted_gallons_modified_WeeksByLiftedGallons` as Lifted_mod_daily_weekly_value,
   `lifted_gallons_modified_NextRefreshDate` as Lifted_mod_daily_nextrefresh,
    `('lifted_gallons', 'Daily')` as Lifted_actual_daily,
   `('lifted_gallons', 'Weekly')` as Lifted_actual_weekly,
   `('lifted_gallons', 'Monthly')` as Lifted_actual_monthly,
   `WeeksByLiftedGallons` as Week_structure_weekly_value,
   `Week_switch` as Week_structure_next_refresh,
   `date`,
   `account_type`,
   `supplier_terminal_name`,
   `product_name`,
   `('base_gallons', 'Daily')`,
   `('base_gallons', 'Monthly')`,
   `('base_gallons', 'Weekly')`,
   `Modified_WeeksByLiftedGallons` as daily_nextrefresh_flag,

   `Modified_LGD_NextRefreshDate` as daily_weekValue_flag,


   `('beginning_gallons', 'Daily')`,
   `('beginning_gallons', 'Monthly')`,
   `('beginning_gallons', 'Weekly')`,
   `('en_allocation_status', 'Daily')`,
   `('en_allocation_status', 'Monthly')`,
   `('en_allocation_status', 'Weekly')`,
   `('percentage_allocation', 'Daily')`,
   `('percentage_allocation', 'Monthly')`,
   `('percentage_allocation', 'Weekly')`,
   `('alerts_ratability', 'Daily')`,
   `('alerts_ratability', 'Monthly')`,
   `('alerts_ratability', 'Weekly')`,
   `('next_refresh_date', 'Daily')`,
   `('next_refresh_date', 'Monthly')`,
   `Modified_NRD`,
   `('next_refresh_date', 'Weekly')`,
   `id`,
   `sanityWeekly_CumulativeDaily_WeeksByLiftedGallons`,
   `sanityWeekly_CumulativeDaily_NextRefreshDate`,
   `sanityMonthly_CumulativeDaily_WeeksByLiftedGallons`,
   `sanityMonthly_CumulativeDaily_WeeksByNextRefreshDate`
FROM %s ;"""%table_name


def getValidDates(uniqueDateList):
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
    return validDates
def fillNRDbackward(uniqueDatesList):
    nd=uniqueDatesList[0]-datetime.timedelta(7)
    #until date goes past the current month then exit
    while (nd.month == uniqueDatesList[0].month):
        if nd.month<dateDetails.month:
            if nd not in uniqueDatesList:
                uniqueDatesList.insert(0,nd)
        else:
            uniqueDatesList.insert(0,nd)
        nd=uniqueDatesList[0]-datetime.timedelta(7)
    print uniqueDatesList
# nd=uniqueDatesList[-1]+datetime.timedelta(7)
# while (nd.month <= uniqueDatesList[-1].month):
#     if nd.month>dateDetails.month:
#         if nd not in uniqueDatesList:
#             uniqueDatesList.append(nd)
#     else:
#         uniqueDatesList.append(nd)
#     nd=uniqueDatesList[-1]+datetime.timedelta(7)
# print uniqueDatesList

print uniqueDatesList
def fillNRDforward(uniqueDatesList):
    while (1):
        nd=uniqueDatesList[-1]+datetime.timedelta(7)
        if nd.month>dateDetails.month:
            if nd not in uniqueDatesList and len(uniqueDatesList) <6:
                uniqueDatesList.append(nd)
            break
        else:
            uniqueDatesList.append(nd)
    print uniqueDatesList

    ----------------
uniqueDatesList.sort()
generatedList=[]
for i in uniqueDatesList:
    if i not in generatedList:
        generatedList.append(i)
        if len(generatedList) <6:
            generatedList.sort()
            fillNRDbackward(generatedList)
            generatedList.sort()
            fillNRDforward(generatedList)