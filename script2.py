# Query for pulling the parameters under kpiId

import pyodbc as pyodbc
import pandas as pd
import numpy as np


#The main driver function that calls the other function to get data
#this function calls the other function with the asOfDate and eff_Date parameters to calculate data by kpiId returnd by first query
def func(asOfDate, eff_date, kpiId):
    # this is first query
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=bed-994-307,61202;'
                          'Database=Summit;'
                          'Trusted_Connection=yes;')
 
    kpimetrics = pd.read_sql_query(''' 
    DECLARE @measureName VARCHAR(200)
    SELECT @measureName = 'Gender/Diversity'

    SELECT meas.measureId
    , meas.measureName
    , meas.measureType
    , calc.logicOperation
    , calc.includeCash
    , calc.calcOwnership
    , esgkpi.?
    , esgkpi.operator
    , esgkpi.threshold
    , esgkpi.sequence
    from summit.analytics.measureMaster meas
    JOIN CustomAnalyticsEngineDB.dbo.measureCalc calc
    ON meas.measureId = calc.measureId
    JOIN CustomAnalyticsEngineDB.dbo.measureESGDetail esgkpi
    ON meas.measureId =  esgkpi.measureId
    WHERE measureName = @measureName
    ''', conn,params=[kpiId])

    df1 = pd.DataFrame(kpimetrics,)
    kpimetrics.head(n=3)
    # If the above is working fine then please uncommit below three lines and run again and send me the output
    # KpiIds = kpimetrics['kpiId'] 
    # for id in KpiIds:
    #     print(id)




if __name__ == "__main__":
    asOfDate= '2021-04-01'
    eff_date = '2021-03-31'
    kpiId = 1227260
    func(asOfDate, eff_date, kpiId)

    
