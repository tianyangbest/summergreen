# -*- coding: utf-8 -*-
# Author: Steven Field

from flask import Flask
from flask import jsonify
from stock_merger import StockMerger
from apscheduler.schedulers.background import BackgroundScheduler
import datetime

app = Flask(__name__)
app.sm = StockMerger("/mnt/stock_data/tmp_cudf/")
app.sm.initialize_one_day_job(datetime.datetime.now().date())
app.sm.start_plan()


def initialize():
    app.sm.initialize_one_day_job(datetime.datetime.now().date())


app.bs = BackgroundScheduler()
app.bs.add_job(initialize, 'cron', hour='9', minute='14', max_instances=1, second='30')


@app.route('/server_check', methods=['GET', 'POST'])
def server_check():
    return jsonify({'message': 'Server is running now!!!'})


@app.route('/get_stock_data', methods=['GET', 'POST'])
def get_stock_data():
    res = jsonify({'tmp_df shape': f'{app.sm.tmp_df.shape}:{app.sm.tmp_df.index.map(lambda x: x[1]).max()}',
                   'persistent_cdf shape': f'{app.sm.persistent_cdf.shape}',
                   'stock_queue shape': f'{app.sm.stock_queue.qsize()}'})
    # app.sm.stop_merger_plan()
    # initialize()
    # app.sm.save_persistent2parquet()
    return res


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5001)
