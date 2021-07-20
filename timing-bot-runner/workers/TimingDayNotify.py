import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
# 절대 경로로 패키지 추가 가능
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # 버전차이 무시

import pandas as pd
import timing_db_info
from sqlalchemy import create_engine
from datetime import datetime
from datetime import timedelta
import requests


class TimingDayNotify:

    def __init__(self, market_loc_cd):
        self.engine = create_engine(
            f'mysql+pymysql://{timing_db_info.user}:{timing_db_info.passwd}@{timing_db_info.db_url}/{timing_db_info.db_name}')

        #  MARKET_LOC_CD는 crontab 분할을 위해 사용
        self.market_loc_cd = market_loc_cd

    def notify_user(self):
        nowDate = datetime.now().date().strftime("%Y-%m-%d")
        nowDate_7 = (datetime.now().date() - timedelta(days=7)).strftime('%Y-%m-%d')

        sql = f"""
            SELECT a.analysis_date, a.market_cd, a.stock_cd, c.stock_nm, sum(a.buy) as buy_count
                     FROM NOTIFY a join MARKET b
                                    ON a.market_cd = b.market_cd
                                   join STOCK_LIST c
                                    ON a.market_cd = c.market_cd
                                     and a.stock_cd = c.stock_cd
                   WHERE a.analysis_date = '{nowDate}'
                  AND a.stock_date BETWEEN '{nowDate_7}' AND '{nowDate}'
                  AND b.market_loc_cd = '{self.market_loc_cd}'
                GROUP BY a.analysis_date, a.market_cd, a.stock_cd, c.stock_nm
                having sum(a.buy)>=6
                order by sum(a.buy) desc
        """




        notify_buy_summary = pd.read_sql(sql, con=self.engine)
        print("매수 요약: ",notify_buy_summary)

        sql = f"""
                    SELECT a.analysis_date, a.market_cd, a.stock_cd, c.stock_nm, sum(a.sell) as sell_count
                             FROM NOTIFY a join MARKET b
                                            ON a.market_cd = b.market_cd
                                           join STOCK_LIST c
                                            ON a.market_cd = c.market_cd
                                             and a.stock_cd = c.stock_cd
                           WHERE a.analysis_date = '{nowDate}'
                          AND a.stock_date BETWEEN '{nowDate_7}' AND '{nowDate}'
                          AND b.market_loc_cd = '{self.market_loc_cd}'
                        GROUP BY a.analysis_date, a.market_cd, a.stock_cd, c.stock_nm
                        having sum(a.sell)>=4
                        order by sum(a.sell) desc
                """

        notify_sell_summary = pd.read_sql(sql, con=self.engine)
        print("매도 요약: ",notify_sell_summary)

        message = notify_sell_summary.to_string()
        print(type (message))

        data = {'Content-Type': 'application/x-www-form-urlencoded',
                'token': timing_db_info.slack_token,
                'channel': timing_db_info.channel_id,
                'text': message
                }

        URL = "https://slack.com/api/chat.postMessage"
        res = requests.post(URL, data=data)
        print(res)


if __name__ == '__main__':
    a = TimingDayNotify('KR')
    a.notify_user()

