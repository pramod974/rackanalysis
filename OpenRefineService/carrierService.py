__author__ = 'pramod.kumar'
__author__ = 'Pramod.Kumar'
from flask import Flask,jsonify,abort,request
import json
import MySQLdb

app = Flask(__name__)

tasks = [{}]

@app.route('/')
def hello_world():
    return 'Hello World !'
@app.route('/tasks',  methods=['POST', 'GET'])
def get_tasks():
    query = request.form.get('query')
    return jsonify({'name':'Percentage Allocation Reconsiliation service','percentageAlloction': query})
    # return jsonify({'name':'Percentage Allocation Reconsiliation service','percentageAlloction': query})

@app.route('/tasks/<carrier>', methods=['POST','GET'])
def get_task(carrier):
    try:
        carrierKey =  str(carrier).strip()
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="SELECT distinct name FROM rack_analysis.mcmis1 where name ='"+carrierKey+"'"
        dbCursor.execute(sql)
        resultsMcmis=dbCursor.fetchall()
        sql="SELECT distinct `carrier_name` FROM rack_analysis.nmfta where `carrier_name` ='"+carrierKey+"'"
        dbCursor.execute(sql)
        resultsNmfta=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        return jsonify({'MCMIS':resultsMcmis,'NMFTA': resultsNmfta})

    except Exception as e:
        return jsonify({'exception':e})

if __name__ == '__main__':
    # print "hekkoi"
    # get_task("ADVANTAGE TANK LINE")
    app.run(debug=True)