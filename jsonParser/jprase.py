__author__ = 'pramod.kumar'
import pandas as pd
import json
import time
start=time.time()
df=pd.read_csv(r"C:\Users\pramod.kumar\Documents\rackanalysis_proj\rackanalysis\jsonParser\90daysdata.csv")
df["PriceEffectiveDateTime"]=df["PriceEffectiveDateTime"].apply(lambda x : x.split(' ')[0])
df=df.fillna('')
grps=df.groupby(['PriceEffectiveDateTime', 'shipto_name'])
allj=[]
for k,grp in grps:
        tgs={    'date':'',
        'trukstop':'',
        'values':''}
        tgs['date']=k[0]
        tgs['trukstop']=k[1]
        tgs['values']=grp.to_dict(orient = 'records')
        allj.append(tgs)
with open('90dd.json','w') as fj:
        fj.write(json.dumps(allj))
print "Total",time.time()-start