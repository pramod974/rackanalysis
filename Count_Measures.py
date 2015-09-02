__author__ = 'pramod.kumar'
import MySQLdb
import pandas as pd
db = MySQLdb.connect("172.24.16.21","root","admin123*","enallocationdbdev")


sqlBase="SELECT supplier,count(*) base_gallons FROM enallocationdbdev.enallocationactive where enaccount_type='Rack' and (Base_Gallons<>0 or  Base_Gallons is not null) group by supplier"
sqlBegin="SELECT supplier,count(*) beginning_gallons FROM enallocationdbdev.enallocationactive where enaccount_type='Rack' and (Beginning_Gallons<>0 or  Beginning_Gallons is not null) group by supplier)"


sqlLift="SELECT supplier,count(*) lifted_gallons FROM enallocationdbdev.enallocationactive where enaccount_type='Rack' and ( Lifted_Gallons<>0 or  Lifted_Gallons is not null) group by supplier"

sqlRemain="SELECT supplier,count(*) Remaining_Gallons FROM enallocationdbdev.enallocationactive where enaccount_type='Rack' and ( Remaining_Gallons <>0 or  Remaining_Gallons is not null) group by supplier"

sqlAddAllow="SELECT supplier,count(*) Additional_Gallons_Allowed FROM enallocationdbdev.enallocationactive where enaccount_type='Rack' and ( Additional_Gallons_Allowed <>0 or  Additional_Gallons_Allowed is not null) group by supplier"

sqlAddRemain="SELECT supplier,count(*) Additional_Gallons_Remaining FROM enallocationdbdev.enallocationactive where enaccount_type='Rack' and ( Additional_Gallons_Remaining <>0 or  Additional_Gallons_Remaining is not null) group by supplier"

dfBase=pd.read_sql(sqlBase, con=db)
dfBegin=pd.read_sql(sqlBegin, con=db)
dfLift=pd.read_sql(sqlLift, con=db)
dfRemain=pd.read_sql(sqlRemain, con=db)
dfAddAllow=pd.read_sql(sqlAddAllow, con=db)
dfAddRemain=pd.read_sql(sqlAddRemain, con=db)


print dfBase.merge(dfBegin,on='supplier').merge(dfLift,on='supplier').merge(dfRemain,on='supplier').merge(dfAddAllow,on='supplier').merge(dfAddRemain,on='supplier')


