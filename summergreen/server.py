# -*- coding: utf-8 -*-
# Author: Steven Field

from flask import Flask
from flask import jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import atexit
import summerquotation
import redis

app = Flask(__name__)
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
sq = summerquotation.use("sina")


def snap2redis():
    try:
        snap_pipe = r.pipeline()
        snap = sq.market_snapshot()
        for k, v in snap.items():
            snap_pipe.hset("stock_tick_current_day", k, v)
        snap_pipe.execute()
    except Exception as e:
        print(e)


app.bs = BackgroundScheduler()

app.bs.add_job(summerquotation.update_stock_codes, 'cron', hour='9', minute='14', max_instances=1, second='30')
app.bs.add_job(snap2redis, 'cron',
               hour='9', minute='15-59', max_instances=10, second='*')
app.bs.add_job(snap2redis, 'cron',
               hour='10,13-14', max_instances=10, second='*')
app.bs.add_job(snap2redis, 'cron',
               hour='11', minute='0-31', max_instances=10, second='*')
app.bs.add_job(snap2redis, 'cron',
               hour='15', minute='0', max_instances=10, second='*')


@app.route('/server_check', methods=['GET', 'POST'])
def server_check():
    return jsonify({'message': 'Server is running now!!!'})


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5001)

    # shut down the scheduler when exiting the app
    atexit.register(lambda: app.bs.shutdown())
