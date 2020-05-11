# -*- coding: utf-8 -*-
# Author: Steven Field
from flask import Flask
from flask import jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import quotation
import cudf
import pandas as pd
import numpy as np
import datetime

app = Flask(__name__)

app.fetcher = quotation.Sina()
app.snapshot_json = {}
app.snapshot_cudf = None
app.snapshot_df = None
app.cache_timedelta = datetime.timedelta(minutes=5)


def initialize_fetcher():
    app.fetcher = quotation.Sina()
    quotation.update_stock_codes()


def run_fetcher():
    app.snapshot_json = {**app.snapshot_json, **app.fetcher.market_snapshot()}
    app.snapshot_df = pd.DataFrame.from_dict(app.snapshot_json, orient="index").astype(
        {"current": np.float32, "high": np.float32, "low": np.float32})


def settle_snapshot():
    current_time = datetime.datetime.now()
    snapshot_json_min_time = current_time - app.cache_timedelta


@app.route('/server_check', methods=['GET', 'POST'])
def server_check():
    return jsonify({'message': 'Server is running now!!!'})


@app.route('/get_stock_data', methods=['GET', 'POST'])
def get_stock_data():
    return str(app.snapshot_json)


# check expired features data and remove them every second
sched = BackgroundScheduler()

sched.add_job(initialize_fetcher, 'cron',
              hour='9', minute='15', second='0')
sched.add_job(run_fetcher, 'cron',
              hour='9', minute='15-59', max_instances=10, second='*/3')
sched.add_job(run_fetcher, 'cron',
              hour='10,13-14', max_instances=10, second='*/3')
sched.add_job(run_fetcher, 'cron',
              hour='11', minute='0-30', max_instances=10, second='*/3')
sched.add_job(run_fetcher, 'cron',
              hour='15', minute='0', max_instances=10, second='*/3')
sched.start()

# shut down the scheduler when exiting the app
atexit.register(lambda: sched.shutdown())

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
