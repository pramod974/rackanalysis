import MySQLdb
import csv
def insert_TCNMapping():
    try:
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        cursor=db.cursor()
        sql="""Delete FROM `nmfta`;"""
        s=cursor.execute(sql)
        print "Rows deleted: ",s
        with open('C:\\Users\\pramod.kumar\\Desktop\\ntest.csv', 'rb') as csvfile:
            terminalreader = csv.reader(csvfile, delimiter=';')
            header=terminalreader.next()
            for row in terminalreader:
                # print ','.join(row)
                Scac=  row[0].strip()
                MCN=  row[1].strip()
                Carrier_Name=  row[2].strip()
                Address=  row[3].strip()
                City=  row[4].strip()
                State= row[5].strip()
                ZipCode=  row[6].strip()
                Country=  row[7].strip()
                Phone=  row[8].strip()
                DOT=  row[9].strip()
                if DOT=='NULL' or DOT=='':
                    DOT=None
                id=  row[10].strip()

                sql="""INSERT ignore INTO `rack_analysis`.`nmfta`
                (`Scac`,
                `MCN #`,
                `Carrier Name`,
                `Address`,
                `City`,
                `State`,
                `ZipCode`,
                `Country`,
                `Phone`,
                `DOT #`,
                `id`)
                VALUES
                ('%s','%s','%s','%s', '%s', '%s', '%s', '%s', '%s', %s, '%s')"""%(Scac, MCN, Carrier_Name, Address, City, State, ZipCode, Country, Phone,DOT, id)
                cursor.execute(sql)
        db.commit()
        sql="""select count(*) FROM `nmfta`;"""
        s=cursor.execute(sql)
        s=cursor.fetchall()
        print "Rows Inserted: ",s
        db.close()
    except Exception as e:
        print e
        db.rollback()
        db.close()
insert_TCNMapping()