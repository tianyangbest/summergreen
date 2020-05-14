# -*- coding: utf-8 -*-
# Author: Steven Field

import re
import time
from . import basequotation
import datetime


class Sina(basequotation.BaseQuotation):
    """新浪免费行情获取"""

    max_num = 800
    grep_detail = re.compile(
        r"(\d+)=[^\s]([^\s,]+?)%s%s"
        % (r",([\.\d]+)" * 29, r",([-\.\d:]+)" * 2)
    )
    grep_detail_with_prefix = re.compile(
        r"(\w{2}\d+)=[^\s]([^\s,]+?)%s%s"
        % (r",([\.\d]+)" * 29, r",([-\.\d:]+)" * 2)
    )
    del_null_data_stock = re.compile(
        r"(\w{2}\d+)=\"\";"
    )

    @property
    def stock_api(self) -> str:
        return f"http://hq.sinajs.cn/rn={int(time.time() * 1000)}&list="

    def format_response_data(self, rep_data, prefix=False):
        stocks_detail = "".join(rep_data)
        stocks_detail = self.del_null_data_stock.sub('', stocks_detail)
        grep_str = self.grep_detail_with_prefix if prefix else self.grep_detail
        result = grep_str.finditer(stocks_detail)
        stock_dict = dict()
        for stock_match_object in result:
            stock = stock_match_object.groups()
            stock_dict[(int(stock[0]), datetime.datetime.strptime(stock[31]+' '+stock[32], '%Y-%m-%d %H:%M:%S'))] = dict(
                current=float(stock[4]),
                high=float(stock[5]),
                low=float(stock[6]),
                volume=int(stock[9]),
                money=float(stock[10]),
                b1_v=int(stock[11]),
                b1_p=float(stock[12]),
                b2_v=int(stock[13]),
                b2_p=float(stock[14]),
                b3_v=int(stock[15]),
                b3_p=float(stock[16]),
                b4_v=int(stock[17]),
                b4_p=float(stock[18]),
                b5_v=int(stock[19]),
                b5_p=float(stock[20]),
                a1_v=int(stock[21]),
                a1_p=float(stock[22]),
                a2_v=int(stock[23]),
                a2_p=float(stock[24]),
                a3_v=int(stock[25]),
                a3_p=float(stock[26]),
                a4_v=int(stock[27]),
                a4_p=float(stock[28]),
                a5_v=int(stock[29]),
                a5_p=float(stock[30]),
            )
        return stock_dict
