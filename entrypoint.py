from schedule_pipeline import DataPipeline

import argparse

parser = argparse.ArgumentParser(description="Script for starting the data pipelines. By default the stocks_daily pipeline is started.",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument(
    "-t", 
    "--type", 
    type=str, 
    action="store", 
    help="Stocks daily or weekly or db update", 
    default="sd", 
    choices=["sd", "sw", "db"]
)

args = parser.parse_args()
config = vars(args)
print(config)
data_pipeline = DataPipeline()

pipelines = {
    "sd": data_pipeline.stock_daily,
    "sw": data_pipeline.stock_weekly_financials,
    "db": data_pipeline.daily_db_schedule
}

pipelines[config["type"]]()