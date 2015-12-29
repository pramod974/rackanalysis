__author__ = 'pramod.kumar'
import MySQLdb
try:
    radbs=["ra_appdb","ra_costcodb", "ra_equusnoxdb", "ra_greenoil", "ra_mansfielddb", "ra_pilotdb", "ra_ryderdb", "ra_sawdb", "ra_upsdb"]
    for radb in radbs:
        try:
            sql=""" UPDATE `%s`.dataalerts
SET supplier_fullname=supplier_name;
"""%(radb)
            # db = MySQLdb.connect("172.24.16.21","root","admin123*",radb)
            # db.autocommit(False)
            # cur=db.cursor()
            # cur.execute(sql)
            # db.commit()
            # db.close()
            # print "updated"
            print sql
        except Exception as e:
            print e
            # db.rollback()
            # db.close()
except Exception as e:
    print e

