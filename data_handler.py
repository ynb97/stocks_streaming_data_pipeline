import requests
import json
import os
import finnhub
import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus , quote
from google.cloud import secretmanager
from datetime import datetime as dt
from google.cloud import bigquery
import pandas as pd


import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus , quote
import datetime
import pandas as pd
from google.cloud import bigquery

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, '.env')) as file:
        ENV = json.load(file)


class GCPSecretManager():
    def __init__(self, project_id=None, secret_config=None) -> None:
        if not project_id:
            project_id = ENV.get("project_id")
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()
        if not secret_config:
            self.secret_config = ENV.get("twelvedata_config").get("gcp_secret")
        else:
            self.secret_config = ENV.get(secret_config).get("gcp_secret")


    def get_secret(self, secret_id=None, version_id=None):
        if not secret_id:
            secret_id = self.secret_config.get("secret_id")
        if not version_id:
            version_id = self.secret_config.get("version_id")
        secret_uri = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        response = self.client.access_secret_version(name=secret_uri)
        return response.payload.data.decode("utf-8")


class HistoricDataFetcher:
    def __init__(self, *params, **kwparams) -> None:
        self.url = ENV.get("finnhub_config").get("api_url", "https://finnhub.io/api/v1/stock/financials-reported")
        # = "?symbol=AAPL&freq=quarterly&from=2022-06-01&to=2022-12-01&token=chu91ihr01qphnn26110chu91ihr01qphnn2611g"
        self.filters = kwparams.get("filters", "all")
        self.reported_freq = kwparams.get("reportd_freq", "annual")
        self.stock_symbol = kwparams.get("symbol", "AAPL")
        self._from = kwparams.get("_from", "2023-01-01")
        self._to = kwparams.get("_to", "2023-03-01")
        self.api_key = GCPSecretManager(secret_config="finnhub_config").get_secret()
        self.client = finnhub.Client(api_key=self.api_key)
        self.fetch_index = kwparams.get("fetch_index", 0)

        print(self._from)
        print(self._to)
    
    def get_data(self, *params, **kwparams):
        print(self.api_key)
        stock_symbol = kwparams.get("symbol", self.stock_symbol)

        # url = f"{self.url}"

        query_params = {
            "token": self.api_key,
            "freq": self.reported_freq,
            "symbol": stock_symbol
        }
        
        # print(query_params)
        financials_as_reported = json.loads(requests.get(self.url, params=query_params).text)

        reported_financial_data = financials_as_reported.get("data", [None])

        try:
            financials_as_reported = {
                "cik": financials_as_reported.get("cik", ""),
                "data": [
                    reported_financial_data[self.fetch_index]
                ],
                "symbol": financials_as_reported.get("symbol", "")
            }
        except IndexError:
            financials_as_reported = []

        historic_data = {
            "company_details": self.client.company_profile2(symbol=stock_symbol),
            "news": self.client.company_news(
                        stock_symbol, 
                        _from=kwparams.get("_from", self._from), 
                        to=kwparams.get("_to", self._to)
                    ),
            "basic_financials": self.client.company_basic_financials(stock_symbol, kwparams.get("filters", self.filters)),
            "financials_as_reported": financials_as_reported,
            "created_at": dt.now()
        }
        print(historic_data["company_details"])

        return historic_data


class StreamDataFetcher:
    def __init__(self, *params, **kwparams) -> None:
        self.api_key = GCPSecretManager().get_secret()
        self.url = ENV.get("twelvedata_config").get("api_url")
        self.interval = kwparams.get("interval", "1min")
        self.symbol = kwparams.get("symbol", "MSFT")
        self.exchange = kwparams.get("exchange", "NASDAQ")
        self.timezone = kwparams.get("timezone", "exchange")
        self.start_date = kwparams.get("start_date", "2023-06-07 10:22:00")
        self.end_date = kwparams.get("end_date", "2023-06-07 10:22:00")

    
    def get_data(self, *params, **kwparams):

        query_params = {
            "apikey": self.api_key
        }

        query_params.update(
            {
                "interval": kwparams.get("interval", self.interval),
                "symbol": kwparams.get("symbol", self.symbol),
                "exchange": kwparams.get("exchange", self.exchange),
                "timezone": kwparams.get("timezone", self.timezone),
                "start_date": kwparams.get("start_date", self.start_date),
                "end_date": kwparams.get("end_date", self.end_date) 
            }
        )

        # print(query_params)

        return requests.get(self.url, params=query_params).text


class DataBaseHandler:
    def __init__(self) -> None:
        # Define the MongoDB connection details
        self.mongo_creds = json.loads(GCPSecretManager(secret_config="mongoatlas_config").get_secret())
        username = self.mongo_creds["username"]
        password = quote_plus(self.mongo_creds["password"])
        cluster = ENV.get("mongoatlas_config").get("cluster")
        db_name = ENV.get("mongoatlas_config").get("db_name")

        # Escape the username and password
        escaped_username = quote_plus(username)
        escaped_password = quote_plus(password)

        # Construct the MongoDB URI
        uri = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/?retryWrites=true&w=majority"

        # Create a new client and connect to the server
        self.mongo_client = MongoClient(uri, server_api=ServerApi('1'))
        self.db = self.mongo_client[db_name]
    

    def store_data(self, collection_name, data):
        # Get the collection from the database
        collection = self.db[collection_name]
        # Insert the data into the collection
        collection.insert_many(data)
        print(f"Stored {len(data)} records in the {collection_name} collection.")
    

    def mongo_to_bq(self):
        collection = self.db["StockFinancials"]

        #tod = datetime.datetime.now()
        #d = datetime.timedelta(days = 7)
        #a = tod - d

        x = collection.find().sort('created_at',-1).limit(10)

        l = []
        for data in x:
            l.append(data)

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
    
    
    def send_to_bq(self,data):

        df = pd.DataFrame(columns=['symbol','datetime','open', 'high', 'low', 'close', 'volume'])

        for i in data:

            meta = json.loads(i)['meta']

            values = json.loads(i)["values"][0]

            symbol,datetime_data,open_price, high, low, close_price, volume = meta['symbol'],values['datetime'],values['open'],values['high'],values['low'], values['close'],values['volume']

            #final_data = list(symbol,interval,currency,exhange_tz, exc,mic,type_stk,datetime_data,open_price, high, low, close_price, volume)

            df.loc[len(df)] = [symbol,datetime_data,open_price, high, low, close_price, volume]

        df['datetime'] = pd.to_datetime(df['datetime'])
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)




        client = bigquery.Client('dataengg2')

        job_config = bigquery.LoadJobConfig(

    # Specify a (partial) schema. All columns are always written to the

    # table. The schema is used to assist in data type definitions.

        schema=[

                # Specify the type of columns whose type cannot be auto-detected. For

                # example the "title" column uses pandas dtype "object", so its

                # data type is ambiguous.

                bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),

                # Indexes are written if included in the schema by name.

                bigquery.SchemaField("datetime", bigquery.enums.SqlTypeNames.TIMESTAMP),

                bigquery.SchemaField("open", bigquery.enums.SqlTypeNames.FLOAT64),

                bigquery.SchemaField("high", bigquery.enums.SqlTypeNames.FLOAT64),

                bigquery.SchemaField("low", bigquery.enums.SqlTypeNames.FLOAT64),

                bigquery.SchemaField("close", bigquery.enums.SqlTypeNames.FLOAT64),

                bigquery.SchemaField("volume", bigquery.enums.SqlTypeNames.FLOAT64)      

            ],

            # Optionally, set the write disposition. BigQuery appends loaded rows

            # to an existing table by default, but with WRITE_TRUNCATE write

            # disposition it replaces the table with the loaded data.

            write_disposition="WRITE_APPEND",

        )

        table_id = "dataengg2.De2Stocks.stocks_data"




        job = client.load_table_from_dataframe(

            df, table_id, job_config=job_config

        )  # Make an API request.

        job.result()  # Wait for the job to complete.




        table = client.get_table(table_id)  # Make an API request.

        print(

            "Loaded {} rows and {} columns to {}".format(

                table.num_rows, len(table.schema), table_id

            )

        )




        return print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))



if __name__ == "__main__":
    db_handler = DataBaseHandler()
    db_handler.mongo_client.close()

    # stream_datafetcher = StreamDataFetcher()
    # print(stream_datafetcher.get_data())