from pymongo import MongoClient
import sys

class DatasetGenerator:
    def __init__(self):
        self.client=MongoClient('172.16.0.55',27017)

    def __del__(self):
        self.client.close()

    def GetAnalyticsData(self,database,collectionname):
        try:
            print "Beginning of dataset generation"
            db=self.client[database]
            monthyear=collectionname.split("_")
            month=monthyear[1]
            year=int(monthyear[0])

            # to remove old data
            self.RemoveOldData(db,month,year)

            currentmonth=db[collectionname]
            pipeline=[{'$sort':{'date':1}},
                 {"$group": {'_id':
                     {'supplier_terminal_name':'$supplier_terminal_name','supplier_name':'$supplier_name','account_type':'$account_type','product_name':'$product_name'}
                       ,'bydate': {'$push': {'day' :'$day','month' :'$month','year' :'$year','date' :'$date','Ratability_Daily':'$Ratability_Daily','Ratability_Weekly':'$Ratability_Weekly',
                      'Ratability_Monthly' :'$Ratability_Monthly_computed'}},'Ratability_Monthly_Avg':{'$last':'$Ratability_Monthly_computed'},
                      'WeightedAvgMonthlyBaseGallons':{ '$avg':'$base_gallons_Monthly_Reconciled'},'WeightedAvgMonthlyBegGallons':{ '$avg':'$beginning_gallons_Monthly_Reconciled'},
                      'EOMReconciledLG':{'$last':'$computedMonthly'}}} ,
                     {'$project' : {'Ratability_Monthly_Avg':'$Ratability_Monthly_Avg','supplier_terminal_name' :
                         '$_id.supplier_terminal_name', 'supplier_name' : '$_id.supplier_name', 'account_type' : '$_id.account_type',
                             'product_name':'$_id.product_name','bydate':'$bydate',
                        'WeightedAvgMonthlyBaseGallons':1,'WeightedAvgMonthlyBegGallons':1,'EOMReconciledLG':1,'_id' : 0}}  ]
            rows=list(currentmonth.aggregate(pipeline))
            print "Inserting data to dataset"
            for row in rows:
                    # print row
                    self.InsertToContractCompliance(row,month,year,db)
            print "Completed data insertion to dataset"
        except Exception as e:
            print e.message

    def RemoveOldData(self,db,monthname,year):
        try:
            print "Removing passed old data for year and month"
            collection=db['contract_compliance']
            # remove month
            res=list(collection.aggregate([{'$unwind': "$bymonth"},
                    {'$match':{'bymonth.year':year,'bymonth.month':monthname}},
                        {'$project':{'bymonth':1}}
                ]))
            for row in res:
                 collection.update({ "_id" : row['_id']},{ '$pull': {'bymonth' : row['bymonth']}},False)

            #     remove bydate data
            month=self.month_converter(monthname)
            resultdate=list(collection.aggregate( [
                {'$unwind': "$bydate"},
                {'$match':{'bydate.year':year,'bydate.month':month}},
                {'$project':{'bydate':1}}]))

            for row in resultdate:
                 collection.update({ "_id" : row['_id']},{ '$pull': {'bydate' : row['bydate']}},False)


            print "Old data for passed year and month removed"
        except Exception as e:
            print e.message
            return  e.message

    def month_converter(self,month):
         months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
         return months.index(month) + 1

    def InsertToContractCompliance(self,tsapData,monthname,year,db):
        try:
            month=self.month_converter(monthname)
            collection=db['contract_compliance']
            query = {'supplier_terminal_name':tsapData['supplier_terminal_name'],
                   'supplier_name':tsapData['supplier_name'],
                   'account_type':tsapData['account_type'],
                   'product_name':tsapData['product_name'] }
            monthlyratability=tsapData['Ratability_Monthly_Avg']
            WeightedAvgMonthlyBase=tsapData['WeightedAvgMonthlyBaseGallons']
            WeightedAvgMonthlyBeg=tsapData['WeightedAvgMonthlyBegGallons']
            EOMReconciledLG=tsapData['EOMReconciledLG']
            del tsapData['Ratability_Monthly_Avg']
            del tsapData['WeightedAvgMonthlyBaseGallons']
            del tsapData['WeightedAvgMonthlyBegGallons']
            del tsapData['EOMReconciledLG']
            tsapData['bymonth']=[]
            WeightedAvgMonthlyBaseLessEOMReconciledLG=WeightedAvgMonthlyBase-EOMReconciledLG
            WeightedAvgMonthlyBegLessWeightedAvgMonthlyBase=WeightedAvgMonthlyBeg-WeightedAvgMonthlyBase
            bymonth={"monthname":monthname,"month":month,"year" : year,"ratability" : monthlyratability,'WeightedAvgMonthlyBase':WeightedAvgMonthlyBase,'WeightedAvgMonthlyBeg':WeightedAvgMonthlyBeg,'EOMReconciledLG':EOMReconciledLG,'WeightedAvgMonthlyBaseLessEOMReconciledLG':WeightedAvgMonthlyBaseLessEOMReconciledLG,'WeightedAvgMonthlyBegLessWeightedAvgMonthlyBase':WeightedAvgMonthlyBegLessWeightedAvgMonthlyBase}
            isTSAPExist=collection.find_one(query)
            if isTSAPExist is None:
                # print "insert new tsap"
                tsapData['bymonth'].append(bymonth)
                collection.insert(tsapData)
            else:
                collection.update({ "_id" : isTSAPExist['_id']},{ '$push': {'bymonth' : bymonth}},False)
                for row in tsapData['bydate']:
                     collection.update({ "_id" : isTSAPExist['_id']},{ '$push': {'bydate' : row}},False)



        except Exception as e:
            print e.message
            return e.message



# dbname=sys.argv[1]
# collectionname=sys.argv[2]
# print "dbname",dbname
# print "collectionname",collectionname
# dataset=DatasetGenerator()
# # database and collectioname
# dataset.GetAnalyticsData(dbname,collectionname)

if len(sys.argv)<=2:
    print "Please Enter The Database name!"
    exit(-1)
dbname=sys.argv[1]
print "dbname",dbname
if len(sys.argv)>=3:
    collectionname=sys.argv[2]
    print "collectionname",collectionname
    dataset=DatasetGenerator()
    # database and collectioname
    dataset.GetAnalyticsData(dbname,collectionname)
else:
    client=MongoClient('172.16.0.55',27017)
    db=client[dbname]
    collections=db.collection_names(False)
    collections.remove('contract_compliance')
    for collectionname in collections:
        dataset=DatasetGenerator()
        # database and collectioname
        dataset.GetAnalyticsData(dbname,collectionname)