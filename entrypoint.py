from schedule_pipeline import DataPipeline

import argparse

parser = argparse.ArgumentParser(description="Script for starting the data pipelines, daily or weekly. By default the daily pipeline is started.",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-t", "--type", type=str, action="store", help="Daily or weekly", default="daily", choices=["daily", "weekly"])

args = parser.parse_args()
config = vars(args)
print(config)
data_pipeline = DataPipeline()

pipelines = {
    "daily": data_pipeline.stock_daily,
    "weekly": data_pipeline.stock_weekly_financials
}

pipelines[config["type"]]()