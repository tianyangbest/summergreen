# -*- coding: utf-8 -*-
# Author: Steven Field

from flask import Flask
from flask import jsonify
from summergreen import FetcherScheduler

app = Flask(__name__)
fs = FetcherScheduler()


@app.route('/server_check', methods=['GET', 'POST'])
def server_check():
    return jsonify({'message': 'Server is running now!!!'})


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5001)
