# -*- coding: utf-8 -*-
# Author: Steven Field

from flask import Flask
from flask import jsonify
from stock_merger import StockMerger
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import atexit

app = Flask(__name__)
app.sm = StockMerger("/mnt/stock_data/tmp_cudf/")
app.sm.initialize_one_day_job(datetime.datetime.now().date())
app.sm.start_plan()

app.bs = BackgroundScheduler()
app.bs.add_job(app.sm.initialize_one_day_job, 'cron', hour='9', minute='14', max_instances=1, second='30',
               args=[datetime.datetime.now().date()])


@app.route('/server_check', methods=['GET', 'POST'])
def server_check():
    return jsonify({'message': 'Server is running now!!!'})


@app.route('/get_stock_info', methods=['GET', 'POST'])
def get_stock_info():
    res = jsonify(
        {'tmp_df shape': f'{app.sm.tmp_df.shape}:{app.sm.tmp_df.index.map(lambda x: x[1]).max()}',
         'persistent_cdf shape': f'{app.sm.persistent_cdf.shape}',
         'stock_queue shape': f'{app.sm.stock_queue.qsize()}',
         'scheduler state': f'{app.sm.stock_scheduler.state}'
         }
    )
    # app.sm.initialize_one_day_job(datetime.datetime.now().date())
    # app.sm.save_persistent2parquet()
    return res


@app.route('/pause_plan', methods=['GET', 'POST'])
def pause_plan():
    app.sm.pause_plan()
    res = jsonify(
        {'tmp_df shape': f'{app.sm.tmp_df.shape}:{app.sm.tmp_df.index.map(lambda x: x[1]).max()}',
         'persistent_cdf shape': f'{app.sm.persistent_cdf.shape}',
         'stock_queue shape': f'{app.sm.stock_queue.qsize()}',
         'scheduler state': f'{app.sm.stock_scheduler.state}'
         }
    )
    # app.sm.initialize_one_day_job(datetime.datetime.now().date())
    # app.sm.save_persistent2parquet()
    return res


@app.route('/initialize_one_day_job', methods=['GET', 'POST'])
def initialize_one_day_job():
    app.sm.initialize_one_day_job(datetime.datetime.now().date())
    res = jsonify(
        {'tmp_df shape': f'{app.sm.tmp_df.shape}:{app.sm.tmp_df.index.map(lambda x: x[1]).max()}',
         'persistent_cdf shape': f'{app.sm.persistent_cdf.shape}',
         'stock_queue shape': f'{app.sm.stock_queue.qsize()}',
         'scheduler state': f'{app.sm.stock_scheduler.state}'
         }
    )
    # app.sm.initialize_one_day_job(datetime.datetime.now().date())
    # app.sm.save_persistent2parquet()
    return res


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5001)

    # shut down the scheduler when exiting the app
    atexit.register(lambda: app.bs.shutdown())
