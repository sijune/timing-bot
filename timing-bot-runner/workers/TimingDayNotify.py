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

from slack import WebClient
from slack.errors import SlackApiError
import json


class TimingDayNotify:

    def __init__(self, market_loc_cd):
        self.engine = create_engine(
            f'mysql+pymysql://{timing_db_info.user}:{timing_db_info.passwd}@{timing_db_info.db_url}/{timing_db_info.db_name}')

        #  MARKET_LOC_CD는 crontab 분할을 위해 사용
        self.market_loc_cd = market_loc_cd

    def notify_user(self):
        # nowDate = datetime.now().date().strftime("%Y-%m-%d")
        nowDate = '2021-07-19'
        nowDate_7 = (datetime.now().date() - timedelta(days=7)).strftime('%Y-%m-%d')

        sql = f"""
            SELECT a.analysis_date, a.market_cd, a.stock_cd, c.stock_nm
                , case when sum(a.buy) >= 8 then '강매수'
                               else '매수' end as buy_opinion
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
                    SELECT a.analysis_date, a.market_cd, a.stock_cd, c.stock_nm
                        , case when sum(a.sell) >= 6 then '강매도'
                               else '매도' end as sell_opinion
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
       #  print("매도 요약: ",notify_sell_summary)

        message = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "추천 종목 - " + notify_sell_summary['analysis_date'].values[0]
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "text": "[매수 추천 종목]",
                        "type": "mrkdwn"
                    },
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": "*종목명*"
                        },
                        {
                            "type": "mrkdwn",
                            "text": "*매수의견*"
                        },
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "text": "[매도 추천 종목]",
                        "type": "mrkdwn"
                    },
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": "*종목명*"
                        },
                        {
                            "type": "mrkdwn",
                            "text": "*매도의견*"
                        },
                    ]
                }
            ]

        if len(notify_buy_summary) == 0:
            message[1]["fields"].append({
                "type": "plain_text",
                "text": "없음"
            })
            message[1]["fields"].append({
                "type": "plain_text",
                "text": "없음"
            })
        else:
            for row in notify_buy_summary.itertuples():
                message[1]["fields"].append({
                                "type": "plain_text",
                                "text": f"[{row[2]}] {row[4]}({str(row[3])})"
                            })
                message[1]["fields"].append({
                                "type": "plain_text",
                                "text": str(row[5])
                            })

        if len(notify_sell_summary) == 0:
            message[2]["fields"].append({
                "type": "plain_text",
                "text": "없음"
            })
            message[2]["fields"].append({
                "type": "plain_text",
                "text": "없음"
            })
        else:
            for row in notify_sell_summary.itertuples():

                message[2]["fields"].append({
                                "type": "plain_text",
                                "text": f"[{row[2]}] {row[4]}({str(row[3])})"
                            })
                message[2]["fields"].append({
                                "type": "plain_text",
                                "text": str(row[5])
                            })

        print(message)

        client = WebClient(token=timing_db_info.slack_token)

        try:
            response = client.chat_postMessage(
                channel=timing_db_info.channel_name,
                blocks=json.dumps(message))
            print(response)
            assert response["ok"] == True
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")


if __name__ == '__main__':
    a = TimingDayNotify('KR')
    a.notify_user()

