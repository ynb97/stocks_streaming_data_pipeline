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
        self.filters = kwparams.get("filters", "all")
        self.reported_freq = kwparams.get("reportd_freq", "annual")
        self.stock_symbol = kwparams.get("symbol", "AAPL")
        self._from = kwparams.get("_from", "2023-01-01")
        self._to = kwparams.get("_to", "2023-03-01")
        self.api_key = GCPSecretManager(secret_config="finnhub_config").get_secret()
        self.client = finnhub.Client(api_key=self.api_key)

    
    def get_data(self, *params, **kwparams):
        print(self.api_key)
        stock_symbol = kwparams.get("symbol", self.stock_symbol)
        historic_data = {
            "company_details": self.client.company_profile2(symbol=stock_symbol),
            "news": self.client.company_news(
                        stock_symbol, 
                        _from=kwparams.get("_from", self._from), 
                        to=kwparams.get("_to", self._to)
                    ),
            "basic_financials": self.client.company_basic_financials(stock_symbol, kwparams.get("filters", self.filters)),
            "financials_as_reported": self.client.financials_reported(
                                            symbol=stock_symbol, 
                                            freq=kwparams.get("reported_freq", self.reported_freq)
                                        ),
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
        self.client = MongoClient(uri, server_api=ServerApi('1'))
        self.db = self.client[db_name]
    

    def store_data(self, collection_name, data):
        # Get the collection from the database
        collection = self.db[collection_name]
        # Insert the data into the collection
        collection.insert_many(data)
        print(f"Stored {len(data)} records in the {collection_name} collection.")
    

    #TODO: Add the logic to push data to big query
    def send_to_bq(self,data):
        df = pd.DataFrame(columns=['symbol','datetime','open', 'high', 'low', 'close', 'volume'])
        for i in data:
            meta = i['meta']
            values = i["values"][0]
            symbol,datetime_data,open_price, high, low, close_price, volume = meta['symbol'],values['datetime'],values['open'],values['high'],values['low'], values['close'],values['volume']
            #final_data = list(symbol,interval,currency,exhange_tz, exc,mic,type_stk,datetime_data,open_price, high, low, close_price, volume)
            df.loc[len(df)] = [symbol,datetime_data,open_price, high, low, close_price, volume]

        client = bigquery.Client('de2-gcp')
        job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
        schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(" order_customer_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField(" order_status", bigquery.enums.SqlTypeNames.STRING)
        
        ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE",
        )
        table_id = "de2-gcp.partdata01.part_data_table"

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

    # db_handler.store_data("Financials", [{"dummy": "dummy value"}])
    db_handler.client.close()
    # historic_datafetcher = HistoricDataFetcher()
    # historic_datafetcher.get_data()

    # stream_datafetcher = StreamDataFetcher()
    # print(stream_datafetcher.get_data())