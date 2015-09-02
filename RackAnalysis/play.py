__author__ = 'pramod.kumar'
import numpy as np
sanityString=['enallocationstatus','percentage_allocation','next_refresh_date']
sanityNum=['base_gallons', 'beginning_gallons', 'lifted_gallons', 'remaining_gallons', 'additional_gallons_allowed', 'additional_gallons_remaining']
for i in sanityNum:
    print "    "
    sanityResult=dict(np.isnan(df2[i]).value_counts())
    if sanityResult.has_key(False):
        print "For ",i," Rows with Valid Numerics: ",sanityResult[False]
    if sanityResult.has_key(True):
        print "For ",i," Rows with No Values: ",sanityResult[True]
    print "    "
for i in sanityString:
    print "    "
    sanityResult=len(df2.loc[(df2[i]=='Unknown') | (df2[i]=='') | (df2[i]==None)])
    print "For ",i," Rows with Unknown Values: ",sanityResult
    print "    "