#!/usr/bin/env python
# coding: utf-8

# In[299]:


import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus , quote
import datetime
import pandas as pd
from google.cloud import bigquery


def mongo_to_bq(self):

# In[300]:

# Escape the username and password
escaped_username = quote_plus(username)
escaped_password = quote_plus(password)

# Construct the MongoDB URI
uri = f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority"

# Create a new client and connect to the server
self.mongo_client = MongoClient(uri, server_api=ServerApi('1'))
db = self.mongo_client[db_name]
collection = db["StockFinancials"]

#tod = datetime.datetime.now()
#d = datetime.timedelta(days = 7)
#a = tod - d

x = collection.find().sort('created_at',-1).limit(10)

l = []
for data in x:
    l.append(data)


# In[293]:


df_basic_details = pd.DataFrame(columns = ['created_at','quarter','year','country','currency','exchange','finnhubIndustry','logo','name','symbol'])
df_news =pd.DataFrame(columns = ['created_at','quarter','year','headline', 'symbol', 'summary', 'url']) 
df_basic_finance = pd.DataFrame(columns=['created_at',"symbol",'quarter','year','52WeekHigh','52WeekHighDate','52WeekLow','52WeekLowDate','epsGrowth3Y','marketCapitalization','netProfitMarginAnnual','roiAnnual','totalDebttotalEquityAnnual','totalDebttotalEquityQuarterly'])
df_finance_reported = pd.DataFrame(columns=['created_at','symbol','quarter','year',"Net_income",'Gross_margin','Total_current_assets','Term_debt','Total_liabilities'])

for i in l:
    company_data= i['company_details']
    news_data = i['news']
    b_fin_data = i['basic_financials']
    fin_rep_data = i['financials_as_reported']
    created_at = i['created_at']
    symbol = company_data['ticker'] 
    market_cap = company_data['marketCapitalization']
    
    fin_d_1 = fin_rep_data['data'][0]
    year = fin_d_1['year']
    quarter = fin_d_1['quarter']
    bs = fin_d_1['report']['bs']
    cf = fin_d_1['report']['cf']
    ic = fin_d_1['report']['ic']
    
    total_current_assets= None
    term_debt= None
    total_liabilities= None
    net_income= None
    gross_margin = None
    
    #print([total_current_assets,term_debt,total_liabilities,net_income,gross_margin])
    for bs_kpi in bs:
        if bs_kpi['label']=='Total current assets' or bs_kpi['label']=='Assets Current':
            total_current_assets = bs_kpi['value']
        
        if bs_kpi['label']=='Term debt' or bs_kpi['label']=='Long Term Debt Noncurrent':
            term_debt = bs_kpi['value']
        
        if bs_kpi['label'] == 'Total liabilities' or bs_kpi['label']=='Liabilities Current':
            total_liabilities = bs_kpi['value']
        
    for cf_kpi in cf:
        if cf_kpi['label']=='Net income':
            net_income = cf_kpi['value']
    
    for ic_kpi in ic:
        if ic_kpi['label']=='Gross margin' or ic_kpi['label']=='Revenue':
            gross_margin = ic_kpi['value']
    #print([created_at,symbol,quarter,year,net_income,gross_margin,total_current_assets,term_debt,total_liabilities])
    df_finance_reported.loc[len(df_finance_reported)] = [created_at,symbol,quarter,year,
                                                         net_income,gross_margin,total_current_assets,
                                                         term_debt,total_liabilities]
    
    for new in news_data:
        headline = new['headline']
        related = new['related']
        summary = new['summary']
        url = new['url']
        df_news.loc[len(df_news)] = [created_at,quarter,year,headline, related, summary, url]
        
    metrics = b_fin_data['metric']
    df_basic_finance.loc[len(df_basic_finance)] = [created_at,symbol,quarter,year,metrics['52WeekHigh'],metrics['52WeekHighDate'],metrics['52WeekLow'],metrics['52WeekLowDate'],metrics['epsGrowth3Y'],market_cap,metrics['netProfitMarginAnnual'],metrics['roiAnnual'],metrics['totalDebt/totalEquityAnnual'],metrics['totalDebt/totalEquityQuarterly']]
    df_basic_details.loc[len(df_basic_details)] = [created_at,quarter,year,company_data['country'],company_data['currency'],company_data['exchange'],company_data['finnhubIndustry'],company_data['logo'],company_data['name'],company_data['ticker']]
df_basic_finance = df_basic_finance.drop_duplicates('symbol')    


# In[307]:



client = bigquery.Client('dataengg2')
job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.TIMESTAMP),
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("quarter", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("headline", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("summary", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("url", bigquery.enums.SqlTypeNames.STRING)   
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
table_id = "dataengg2.De2Stocks.news_data"

job = client.load_table_from_dataframe(
    df_news, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)


# In[308]:


job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.TIMESTAMP),
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("quarter", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("52WeekHigh", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("52WeekHighDate", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("52WeekLow", bigquery.enums.SqlTypeNames.FLOAT64), 
        bigquery.SchemaField("52WeekLowDate", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("epsGrowth3Y", bigquery.enums.SqlTypeNames.FLOAT64), 
        bigquery.SchemaField("marketCapitalization", bigquery.enums.SqlTypeNames.FLOAT64), 
        bigquery.SchemaField("netProfitMarginAnnual", bigquery.enums.SqlTypeNames.FLOAT64), 
        bigquery.SchemaField("roiAnnual", bigquery.enums.SqlTypeNames.FLOAT64), 
        bigquery.SchemaField("totalDebttotalEquityAnnual", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("totalDebttotalEquityQuarterly", bigquery.enums.SqlTypeNames.FLOAT64)
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
table_id = "dataengg2.De2Stocks.basic_finance_data"

job = client.load_table_from_dataframe(
    df_basic_finance, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)


# In[309]:



job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.TIMESTAMP),
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("quarter", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("Net_income", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("Gross_margin", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("Total_current_assets", bigquery.enums.SqlTypeNames.INTEGER), 
        bigquery.SchemaField("Term_debt", bigquery.enums.SqlTypeNames.INTEGER), 
        bigquery.SchemaField("Total_liabilities", bigquery.enums.SqlTypeNames.INTEGER), 
        ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
table_id = "dataengg2.De2Stocks.finance_reported"

df_temp = df_finance_reported
df_temp = df_temp.sort_values(by=["year", "quarter"])

df_temp = df_temp.sort_values(by="symbol")

df_temp = df_temp.fillna(0)


df_temp.reset_index(drop=True)

df_temp["Net_income_diff"] = df_temp.groupby(by="symbol")["Net_income"].diff()
df_temp["Gross_margin_diff"] = df_temp.groupby(by="symbol")["Gross_margin"].diff()
df_temp["Total_current_assets_diff"] = df_temp.groupby(by="symbol")["Total_current_assets"].diff()
df_temp["Term_debt_diff"] = df_temp.groupby(by="symbol")["Term_debt"].diff()
df_temp["Total_liabilities_diff"] = df_temp.groupby(by="symbol")["Total_liabilities"].diff()

df_temp = df_temp.dropna()
df_finance_reported = df_temp

job = client.load_table_from_dataframe(
    df_finance_reported, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)


# In[310]:



job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("quarter", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INTEGER),
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("country", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("currency", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("exchange", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("finnhubIndustry", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("logo", bigquery.enums.SqlTypeNames.STRING),         
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),         
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),        
        ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
table_id = "dataengg2.De2Stocks.basic_details"

job = client.load_table_from_dataframe(
    df_basic_details, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
