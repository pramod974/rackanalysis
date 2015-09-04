__author__ = 'pramod.kumar'
import MySQLdb
import pandas as pd
import spark_code as sc
class data_setup(ruleEngine):
    def __init__(self):
        try:
            self.executionFrom=self.dateDetails.strftime("%Y-%m-%d %H:%M:%S")
            self.executionTo=self.executionFrom.replace(month=self.executionFrom.month+1)
        except Exception as e:
            print e
    def fetchSupplier(self):
       try:
           tableName=self.customer+"_"+self.supplier+"_"+self.month
           sql="""select distinct Account_Type,supplier_terminal_name,product_name from %s where Account_Type<>"Unknown" and supplier_terminal_name<>"Unknown" and product_name<>"Unknown";"""%tableName
           self.cursor.execute(sql)
           suppliers=self.cursor.fetchall()
           self.suppliersCombinations=suppliers
       except Exception as e:
           self.db.close()
           print "Exception:",e
    def get_maxbatch(self):
        tableName=self.customer+"_"+self.month+"_maxbatch"
        sql="""create temporary table %s as
select
*
 from
(select supplier,max(batchno) as batchno,date(Execution_Date) as dt from enallocationarchive
where Execution_Date>='%s' and Execution_Date<='%s'
group by supplier,date(Execution_Date)) a join

(SELECT
*
FROM enallocationarchive
where Execution_Date>='%s' and Execution_Date<='%s') x on a.batchno=x.batchno and a.dt=date(x.Execution_Date) and a.supplier=x.supplier;
        """%(tableName,self.executionFrom,self.executionTo,self.executionFrom,self.executionTo)
        self.cursor.execute(sql)
    def get_maxbatch_analysis_pivot(self):
        try:
            # db = MySQLdb.connect("172.16.0.55","root","admin123*","`rules_spark`")
            self.get_maxbatch()
            supplier="exxon"
            customer="pilot"
            month="june"
            sql="""select *,date(execution_date) as date from
                    (SELECT
                    alerts_ratability,en_allocation_status,percentage_allocation,account_type,base_gallons,beginning_gallons,supplier_terminal_name,lifted_gallons,remaining_gallons,additional_gallons_allowed,additional_gallons_remaining,next_refresh_date,
                    next_refresh_base_gallons,product_name,period,batchno,execution_date,staging_id
                    FROM `%s_%s_maxbatch`
                    where supplier_name='%s' and (period='Daily' or period='Weekly' or period='Monthly')
                    order by execution_date desc,batchno desc,account_type,supplier_terminal_name,product_name,period) x

                    GROUP BY date(execution_date),account_type,supplier_terminal_name,product_name,period

                    having maX(batchno)
                    order by account_type,supplier_terminal_name,product_name,date,period;"""%(customer,month,supplier)
            df_mysql = pd.read_sql(sql, con=self.db)
            df_mysql.to_excel(customer+"_"+supplier+"_actualData.xls")
            mp=pd.pivot_table(df_mysql,index=["date","account_type","supplier_terminal_name","product_name"],values=["base_gallons","lifted_gallons","beginning_gallons","en_allocation_status",'percentage_allocation','alerts_ratability','next_refresh_date'],columns="period",aggfunc = lambda x: x)
            # mp.fillna(0)
            self.mp=mp
            # try:
            #     mp.to_sql(name=customer+"_"+supplier+"_"+month,con=db,flavor='mysql', if_exists='append')
            #     # pass
            # except :
            #     mp.to_sql(name=customer+"_"+supplier+"_"+month,con=db,flavor='mysql', if_exists='append')
            #     # pass
            mp.to_excel(customer+"_"+supplier+"_"+month+"completeDataPivot.xls")
            sql="""ALTER TABLE `rack_analysis`.`%s_%s_%s`
        ADD COLUMN `id` INT NOT NULL AUTO_INCREMENT,
        ADD PRIMARY KEY (`id`);"""%(customer,supplier,month)

            # cursor.execute(sql)
            tableName=customer+"_"+supplier+"_"+month
            sql="""CALL `rack_analysis`.`column_procedure`('%s');"""%(tableName)
            self.cursor.execute(sql)
        except Exception as e:
            print e
            # db.close()
