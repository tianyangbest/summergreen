# coding:utf8
import re
import time
from . import basequotation


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
        stocks_detail = stocks_detail.replace(' ', '')
        grep_str = self.grep_detail_with_prefix if prefix else self.grep_detail
        result = grep_str.finditer(stocks_detail)
        stock_dict = dict()
        for stock_match_object in result:
            stock = stock_match_object.groups()
            stock_dict[(f"""{stock[0]}""", f"""{stock[31]} {stock[32]}""")] = """,""".join([str(i) for i in [
                stock[4],
                stock[5],
                stock[6],
                stock[9],
                stock[10],
                stock[11],
                stock[12],
                stock[13],
                stock[14],
                stock[15],
                stock[16],
                stock[17],
                stock[18],
                stock[19],
                stock[20],
                stock[21],
                stock[22],
                stock[23],
                stock[24],
                stock[25],
                stock[26],
                stock[27],
                stock[28],
                stock[29],
                stock[30]]])
                                                                                 
        return stock_dict
