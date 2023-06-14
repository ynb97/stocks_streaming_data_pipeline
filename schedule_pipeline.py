import pytz
import json
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime as dt, time as t, timedelta as td
from data_handler import StreamDataFetcher, HistoricDataFetcher, DataBaseHandler
from kafka_client import KafkaClient

class DataPipeline:
    def __init__(self) -> None:
        self.daily_scheduler = BlockingScheduler()
        self.weekly_scheduler = BlockingScheduler()
        self.stream_scheduler = BackgroundScheduler()
        self.db_update_scheduler = BackgroundScheduler()
        self.db_scheduler = BlockingScheduler()
        self.EST = pytz.timezone("US/Eastern")
        self.UTC = pytz.utc
        self.current_date = dt.today()
        self.market_start_time = dt.combine(self.current_date, t(9, 30, 00, 0, self.EST))
        self.kafka_client = KafkaClient()

        self.stock_symbols = [
            "AAPL",
            "MSFT",
            "GOOG",
            "AMZN",
            "NVDA"
        ]


    def stock_data_streamer(self):
        print("streaming data")

        stock_data = []

        time_head_str = self.time_head.strftime("%Y-%m-%d %H:%M:%S")
        data_fetcher = StreamDataFetcher(
            interval="1min", 
            exchange="NASDAQ", 
            timezone="exchange", 
            start_date=time_head_str,
            end_date=time_head_str
        )

        for stock in self.stock_symbols:
            stock_data.append(data_fetcher.get_data(symbol=stock))
        
        self.kafka_client.send_data(json.dumps(stock_data))

        if self.time_head.minute > 50:
            self.stream_scheduler.pause()
            print("streamer paused")

        self.time_head = self.time_head + td(minutes=1)
                

    def daily_stocks_update(self):
        print("daily production...")
        self.time_head = self.market_start_time
        print(self.time_head)
        if self.stream_scheduler.state == 2:
            self.stream_scheduler.resume()
        else:
            self.stream_scheduler.add_job(self.stock_data_streamer, "interval", minutes=1, id='streamer')
            self.stream_scheduler.start()
        print("streamer added and running...")
    

    def historic_stocks_update(self):
        historic_datafetcher0 = HistoricDataFetcher(
                                _from=(self.current_date.today() - td(days=7)).date().strftime("%Y-%m-%d"), 
                                _to=self.current_date.today().date().strftime("%Y-%m-%d"),
                                fetch_index=0
                            )
        
        historic_datafetcher1 = HistoricDataFetcher(
                                _from=(self.current_date.today() - td(days=14)).date().strftime("%Y-%m-%d"), 
                                _to=(self.current_date.today() - td(days=7)).date().strftime("%Y-%m-%d"),
                                fetch_index=1
                            )

        stock_data = []

        for stock in self.stock_symbols:
            # time.sleep(2)
            stock_data.append(
                historic_datafetcher0.get_data(
                    symbol=stock
                )
            )
            stock_data.append(
                historic_datafetcher1.get_data(
                    symbol=stock
                )
            )
        self.db_handler.store_data("StockFinancials", stock_data)
        # print(stock_data)

        
    def stock_daily(self):
        print("daily added and running...")
        self.daily_scheduler.add_job(self.daily_stocks_update, 'cron', day_of_week="mon-fri", minute=52, id="daily")
        self.daily_scheduler.start()
        

    def stock_weekly_financials(self):
        self.db_handler = DataBaseHandler()
        self.weekly_scheduler.add_job(self.historic_stocks_update, "cron", minute=33)
        self.weekly_scheduler.start()
    

    def kafka_consumer_to_db(self):
        data = self.kafka_client.retrieve_data()
        self.db_handler_consumer.send_to_bq(data)

        print(data)

    
    def stocks_db_update(self):
        self.db_handler_consumer = DataBaseHandler()
        print("Stocks db updater started")
        if self.db_update_scheduler.state == 2:
            self.db_update_scheduler.resume()
        else:
            self.db_update_scheduler.add_job(self.kafka_consumer_to_db, "interval", seconds=10, id='consumer-streamer')
            self.db_update_scheduler.start()
    

    def daily_db_schedule(self):
        try:
            self.db_scheduler.add_job(self.stocks_db_update, "cron", minute=4, id="db-schedule")
            self.db_scheduler.start()
        except KeyboardInterrupt:
            pass
        finally:
            self.kafka_client.consumer.close()


if __name__ == "__main__":
    data_pipeline = DataPipeline()
    try:
        data_pipeline.daily_db_schedule()
    except KeyboardInterrupt:
        pass
    finally:
        data_pipeline.kafka_client.consumer.close()
