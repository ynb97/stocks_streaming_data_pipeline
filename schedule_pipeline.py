import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime as dt, time as t, timedelta as td
from fetch_data import StreamDataFetcher, HistoricDataFetcher


class DataPipeline:
    def __init__(self) -> None:
        self.daily_scheduler = BlockingScheduler()
        self.weekly_scheduler = BlockingScheduler()
        self.stream_scheduler = BackgroundScheduler()
        self.EST = pytz.timezone("US/Eastern")
        self.UTC = pytz.utc
        self.current_date = dt.today()
        self.market_start_time = dt.combine(self.current_date, t(9, 30, 00, 0, self.EST))

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
        
        #TODO: replace the print statement with the kafka producer method
        print(stock_data)

        if self.time_head.hour > 16:
            self.stream_scheduler.pause()
            print("streamer paused")

        self.time_head = self.time_head + td(minutes=1)
                

    def daily_production(self):
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
        #TODO: Add the logic for specifying the from and to date, filters and reported frequency
        historic_datafetcher = HistoricDataFetcher()

        stock_data = []

        for stock in self.stock_symbols:
            stock_data.append(
                historic_datafetcher.get_data(
                    symbol=stock
                )
            )
        #TODO: Add the logic for sending the financial data to the mongodb atlas
        print(stock_data)

        
    def stock_daily(self):
        print("daily added and running...")
        self.daily_scheduler.add_job(self.daily_production, 'cron', day_of_week="mon-fri", minute="*/5", id="daily")
        self.daily_scheduler.start()
        print("daily ended")
        

    def stock_weekly_financials(self):
        self.weekly_scheduler.add_job(self.historic_stocks_data, "cron", day_of_week="sat", hour=18)
        self.weekly_scheduler.start()


if __name__ == "__main__":
    data_pipeline = DataPipeline()
    data_pipeline.stock_daily()
