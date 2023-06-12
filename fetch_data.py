import requests
import json
import os
import finnhub
from google.cloud import secretmanager

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
        self.api_key = GCPSecretManager(secret_config="finnhub_config").get_secret()
        self.client = finnhub.Client(api_key=self.api_key)

    
    def get_data(self, *params, **kwparams):
        stock_symbol = kwparams.get("symbol", "AAPL")
        print(self.api_key)

        historic_data = {
            "company_details": self.client.company_profile2(symbol=stock_symbol),
            "news": self.client.company_news(
                        stock_symbol, 
                        _from=kwparams.get("from", "2023-01-01"), 
                        to=kwparams.get("to", "2023-03-01")
                    ),
            "basic_financials": self.client.company_basic_financials(stock_symbol, kwparams.get("filters", "all")),
            "financials_as_reported": self.client.financials_reported(symbol=stock_symbol, freq=kwparams.get("reported_freq", "annual"))
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


if __name__ == "__main__":
    historic_datafetcher = HistoricDataFetcher()
    historic_datafetcher.get_data()

    stream_datafetcher = StreamDataFetcher()
    print(stream_datafetcher.get_data())