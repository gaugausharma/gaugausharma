# Query for pulling the parameters under kpiId

import pyodbc as pyodbc
import pandas as pd
import numpy as np

###############
# This is not a main function, main functon is defined below 
def pullingDataSet(kpiId, asOfDate ,eff_date ):
##############
    
    conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=bed-400-307;'
                        'Database=Summit;'
                        'Trusted_Connection=yes;')

    posdet = pd.read_sql_query(
    '''

    SELECT 

    Pos.effectiveDate, Pos.entityId, Pos.sourceCode, Pos.securityId, Pos.marketValueIncomeBase, 
    Pos.marketValueBase,pos.localCurrency, 
    secm.securityId, secm.issuerId, secm.issueName, secm.issueDescription, 
    kpi.kpiid, kpi.kpivalue, kpi.asOfDate, kpi.kpiDimensionIdentifier,
    pr.priceCurrency, pr.sourceCode, 
    fx.fromCurrency, fx.toCurrency, fx.spotRate, fx.sourceCode

    , Case 
            when  pos.localCurrency <> 'USD' THEN
            Pos.marketValueBase * fx.spotRate
            ELSE
            Pos.marketValueBase 
        End As marketValueBaseUSD
    

    FROM Summit.ent.vwPositionDetail Pos
    Join Summit.ent.vwSecurityMaster secm 
    ON Pos.securityid = secm.securityid
    Left Join ESG.esg.kpi kpi
    ON kpi.kpiDimensionIdentifier = secm.issuerId
        and kpi.kpiid in ('''+kpiId+''')
        and kpi.asOfDate = '''+asOfDate+'''

    LEFT OUTER JOIN summit.ent.vwSecurityPrice pr
        ON pos.securityId = pr.securityId
                AND pr.effectiveDate = pos.effectiveDate
                AND pr.sourceCode = 'BB'
    LEFT OUTER JOIN summit.ent.vwFxRate fx
        ON pr.priceCurrency = fx.fromCurrency
                AND pr.effectiveDate = fx.effectiveDate
                AND fx.toCurrency = 'USD'
                AND fx.sourceCode = 'REUTERSWM'

    where Pos.sourcecode = 'SSBADJ' 
    and pos.entityId = 116509 
    and pos.effectiveDate = '''+eff_date+'''

    ''', conn)

    df = pd.DataFrame(posdet, )

    posdet.head(n=3)

    # Checking for null and missing values
    nrow = posdet.shape[0]
    nrow

    posdet.count()

    # Extracting the columns needed
    posdet.loc[0:5,["issueName","marketValueBase","kpivalue"]]

    # Getting total for MarketValueBase
    Totalmarketval = sum(posdet['marketValueBase'])
    Totalmarketval

    # New column to calculate weights
    posdet['weights'] = posdet['marketValueBase']/posdet['marketValueBase'].sum()
    posdet.head(n=5)

    # Getting total for MarketValueBase
    Totalweights = sum(posdet['weights'])
    Totalweights

    # Create new columns to aplly condition and calculate weights

    posdet['kpivalue'] = posdet['kpivalue'].apply(pd.to_numeric)
    posdet["kpiweight"] = np.where(posdet.kpivalue >= 30,1,0)
    posdet['genderscore'] = posdet["weights"]*posdet["kpiweight"]
    posdet.head()

    # Total score for Fund for Gender
    calvertgenderscore = sum(posdet['genderscore'])*100
    calvertgenderscore 

    # Calculating number fo rows for coverage calulation
    nrowbench = benchm.kpicoverage.count() 
    nrowpos = posdet.kpicoverage.count()

    # Calculating coverage score
    calvertgendercoverage = (sum(posdet['kpicoverage']) / nrowpos)*100
    rimesgendercoverage = (sum(benchm['kpicoverage']) / nrowbench)*100

    rimesgendercoverage

    # Output for gender in dataframe

    summary = pd.DataFrame(columns = ('Calvert_Score', 'Rimes_Score', 'Calvert_Coverage', 'Rimes_Coverage'),
                        index=['Gender'])
    summary = summary.assign(Calvert_Score = calvertgenderscore)
    summary = summary.assign(Rimes_Score = rimesgenderscore)
    summary = summary.assign(Calvert_Coverage = calvertgendercoverage)
    summary = summary.assign(Rimes_Coverage = rimesgendercoverage)

    summary

#The main driver function that calls the other function to get data
#this function calls the other function with the asOfDate and eff_Date parameters to calculate data by kpiId returnd by first query
def func(asOfDate, eff_date, kpiId):
    # this is first query



    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=bed-994-307,61202;'
                          'Database=Summit;'
                          'Trusted_Connection=yes;')

    kpimetrics = pd.read_sql_query(
        ''' 
    DECLARE @measureName VARCHAR(200)
    SELECT @measureName = 'Gender/Diversity'

    SELECT meas.measureId
    , meas.measureName
    , meas.measureType
    , calc.logicOperation
    , calc.includeCash
    , calc.calcOwnership
    , esgkpi.'''+kpiId+'''
    , esgkpi.operator
    , esgkpi.threshold
    , esgkpi.sequence
    from summit.analytics.measureMaster meas
    JOIN CustomAnalyticsEngineDB.dbo.measureCalc calc
    ON meas.measureId = calc.measureId
    JOIN CustomAnalyticsEngineDB.dbo.measureESGDetail esgkpi
    ON meas.measureId =  esgkpi.measureId
    WHERE measureName = @measureName
    '''
        , conn)

    df1 = pd.DataFrame(kpimetrics,)
    kpimetrics.head(n=3)

######################################
    KpiIds = kpimetrics['kpiId'] 
    for id in KpiIds:
        pullingDataSet(id, asOfDate, eff_date )
###################################

### Query and calculation for 2nd set

# Import Position detail file 
#carbem_pos_ext = pd.read_excel("P:\Documents\ESG Analytics\Carbon Emission/Position_detail_carbem_v1.xlsx")

 # Import data from SQL Srver and convert into a data frame

def calSecond(asOfDate, eff_date, kpiId):
    
    conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=bed-400-307;'
                        'Database=Summit;'
                        'Trusted_Connection=yes;')

    carbem_pos_ext = pd.read_sql_query(
    '''SELECT 

    Pos.effectiveDate, Pos.entityId, Pos.sourceCode, Pos.securityId, Pos.marketValueIncomeBase, 
    Pos.marketValueBase,pos.localCurrency, 
    secm.securityId, secm.issuerId, secm.issueName, secm.issueDescription, 
    kpi.kpiid, kpi.kpivalue, kpi.asOfDate, kpi.kpiDimensionIdentifier,
    pr.currentMarketCap, pr.priceCurrency, pr.sourceCode, 
    fx.fromCurrency, fx.toCurrency, fx.spotRate, fx.sourceCode
    
    , CASE
                WHEN fx.sourceCode <> 'USD' THEN
                        pr.currentMarketCap * fx.spotRate
                ELSE
                        pr.currentMarketCap
                END AS currentMarketCapUSD

        , Case 
                when  pos.localCurrency <> 'USD' THEN
                    Pos.marketValueBase * fx.spotRate 
                ELSE
                        Pos.marketValueBase 
                End As marketValueBaseUSD
    

    FROM Summit.ent.vwPositionDetail Pos
    Join Summit.ent.vwSecurityMaster secm 
    ON Pos.securityid = secm.securityid
    Left Join ESG.esg.kpi kpi
    ON kpi.kpiDimensionIdentifier = secm.issuerId
        and kpi.kpiid in ('''+kpiId+''')
    and kpi.asOfDate = '''+asOfDate+'''

    LEFT OUTER JOIN summit.ent.vwSecurityPrice pr
        ON pos.securityId = pr.securityId
                AND pr.effectiveDate = pos.effectiveDate
                AND pr.sourceCode = 'BB'
    LEFT OUTER JOIN summit.ent.vwFxRate fx
        ON pr.priceCurrency = fx.fromCurrency
                AND pr.effectiveDate = fx.effectiveDate
                AND fx.toCurrency = 'USD'
                AND fx.sourceCode = 'REUTERSWM'

    where Pos.sourcecode = 'SSBADJ' 
    and pos.entityId = 116509 
    and pos.effectiveDate = '''+eff_date+''' ''', conn)

    df = pd.DataFrame(carbem_pos_ext, )
    print (df)


    #carbem_pos_ext.head(n=5)

    #### extracting needed columns 
    carbem_pos = carbem_pos_ext.loc[0:,["issueName","marketValueBaseUSD","kpiid","kpivalue","currentMarketCapUSD"]]
    carbem_pos.head()

    # converting datatype for kpis  and filling NUll values with 0s

    carbem_pos['kpivalue'] = carbem_pos['kpivalue'].apply(pd.to_numeric)
    carbem_pos['kpivalue'] = carbem_pos['kpivalue'].fillna(0)
    carbem_pos['currentMarketCapUSD'] = carbem_pos['currentMarketCapUSD'].fillna(0)
    carbem_pos['marketValueBaseUSD'] = carbem_pos['marketValueBaseUSD'].fillna(0)

    carbem_pos.info()

    # Getting total for MarketValueBase and converting the marketcap amount to units
    carbem_pos['currentMarketcapUSDmm'] = carbem_pos['currentMarketCapUSD'] * 1000000
    Totalmarketval_pos = sum(carbem_pos['marketValueBaseUSD'])

    # New columns to calculate weights for positions in portfolio and percentage of ownership
    carbem_pos['Position_weights'] = carbem_pos['marketValueBaseUSD']/Totalmarketval_pos
    carbem_pos['Position_ownership'] = carbem_pos['marketValueBaseUSD']/carbem_pos['currentMarketcapUSDmm']

    #carbem_pos['Carbon_Emission_Score_pos'] = carbem_pos['Position_weights'] * carbem_pos['Position_ownership'] * carbem_pos['kpivalue'] 
    carbem_pos['Carbon_Emission_Score_pos'] = carbem_pos['Position_ownership'] * carbem_pos['kpivalue']

    carbem_pos['Position_weights'] = carbem_pos['Position_weights'].fillna(0)
    carbem_pos['Position_ownership'] = carbem_pos['Position_ownership'].fillna(0)
    carbem_pos['Carbon_Emission_Score_pos'] = carbem_pos['Carbon_Emission_Score_pos'].fillna(0)


    carbem_pos.head(5)

    # Getting total for weights MarketValueBase
    Totalweights_pos = sum(carbem_pos['Position_weights'])
    Totalownership_pos = sum(carbem_pos['Position_ownership'])
    TotalCarbonEmission_Calvert = sum(carbem_pos['Carbon_Emission_Score_pos'])

    Totalweights_pos, Totalownership_pos, TotalCarbonEmission_Calvert


    carbem_pos_write = carbem_pos.copy()
    carbem_bench_write = carbem_bench.copy()

    ## writing data to local folder 

    with pd.ExcelWriter("C:\Python Outputs\Carbem_Outputs_06.01.xlsx") as writer:
        carbem_pos_write.to_excel(writer, sheet_name='Carb_Em_fund', index=False)
        carbem_bench_write.to_excel(writer, sheet_name='Carb_Em_bench', index=False)

################################

if __name__ == "__main__":
    asOfDate= '2021-04-01'
    eff_date = '2021-03-31'
    kpiId = 1227260
    func(asOfDate, eff_date, kpiId)
    calSecond(asOfDate, eff_date, kpiId)
    
