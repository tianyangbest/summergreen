# -*- coding: utf-8 -*-
# Author: Steven Field

from flask import Flask
from flask import jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import quotation
from stock_merger import StockMerger
import datetime
import threading
import time

app = Flask(__name__)
app.sched = BackgroundScheduler()
quotation.update_stock_codes()
app.fetcher = quotation.Sina()
app.trade_date = datetime.datetime.now().date()
app.sm = StockMerger(app.trade_date)


def initialize():
    """
    initialize the fetcher and merger in beginning of every day.
    """
    # update stock code before initialize quotation fetcher
    quotation.update_stock_codes()
    # initialize quotation fetcher
    app.fetcher = quotation.Sina()
    # initialize stock merger for gpu calculation
    app.trade_date = datetime.datetime.now().date()
    app.sm = StockMerger(app.trade_date)
    # pause scheduler for 6 hours if current is not trading date
    if not app.sm.estimate_trading(app.fetcher.market_snapshot()):
        app.sched.pause()
        time.sleep(60 * 60 * 6)
        app.sched.resume()


def run_fetcher():
    app.sm.cache_stock_dict(app.fetcher.market_snapshot())


def run_tmp_merger():
    app.sm.stock_dict2tmp_dict()


def run_merger():
    app.sm.tmp2persistent_delayed()


def save_merger():
    app.sm.tmp2persistent()
    app.sm.save_persistent2parquet(f"""/mnt/stock_data/tmp_cudf/{app.trade_date}.parquet""")


@app.route('/server_check', methods=['GET', 'POST'])
def server_check():
    return jsonify({'message': 'Server is running now!!!'})


@app.route('/get_stock_data', methods=['GET', 'POST'])
def get_stock_data():
    return jsonify({'message': f'{app.sched.state}'})


# # test scheduler
# app.sched.add_job(initialize, 'cron',
#                   hour='2', minute='16', max_instances=1, second='30')
#
# app.sched.add_job(run_fetcher, 'cron',
#                   max_instances=1, second='*')

# initialize scheduler
app.sched.add_job(initialize, 'cron',
                  hour='9', minute='14', max_instances=1, second='30')

# fetcher scheduler
app.sched.add_job(run_fetcher, 'cron',
                  hour='9', minute='15-59', max_instances=10, second='*')
app.sched.add_job(run_fetcher, 'cron',
                  hour='10,13-14', max_instances=10, second='*')
app.sched.add_job(run_fetcher, 'cron',
                  hour='11', minute='0-31', max_instances=10, second='*')
app.sched.add_job(run_fetcher, 'cron',
                  hour='15', minute='0', max_instances=10, second='*')

# merger scheduler
app.sched.add_job(run_merger, 'cron',
                  hour='9', minute='15-59', max_instances=1, second='*/30')
app.sched.add_job(run_merger, 'cron',
                  hour='10,13-14', max_instances=1, second='*/30')
app.sched.add_job(run_merger, 'cron',
                  hour='11', minute='0-30', max_instances=1, second='*/30')
app.sched.add_job(run_merger, 'cron',
                  hour='15', minute='0', max_instances=1, second='*/30')

# save scheduler
app.sched.add_job(save_merger, 'cron',
                  hour='15', minute='1', max_instances=1, second='30')

app.sched.start()

# shut down the scheduler when exiting the app
atexit.register(lambda: app.sched.shutdown())

if __name__ == '__main__':
    threading.Thread(target=run_tmp_merger, daemon=True).start()
    app.run(debug=False, host='0.0.0.0', port=5001)
