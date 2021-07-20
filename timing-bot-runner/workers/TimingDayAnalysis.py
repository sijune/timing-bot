import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
# 절대 경로로 패키지 추가 가능
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)  # 버전차이 무시

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import timing_db_info
from sqlalchemy import create_engine
import sqlalchemy
from Analysis import Analysis
from AnalysisDay10 import AnalysisDay10
from AnalysisDay20 import AnalysisDay20
from AnalysisDay30 import AnalysisDay30
from Analysis import Analysis


class TimingDayAnalysis:

    def __init__(self, market_loc_cd):
        self.engine = create_engine(
            f'mysql+pymysql://{timing_db_info.user}:{timing_db_info.passwd}@{timing_db_info.db_url}/{timing_db_info.db_name}')

        with self.engine.connect() as connection:
            sql = """
                                    CREATE TABLE IF NOT EXISTS TRADE_CAL10 (
                                        analysis_date VARCHAR(100), 
                                        market_cd VARCHAR(100),
                                        stock_cd VARCHAR(100),
                                        stock_date VARCHAR(100),
                                        close DECIMAL(24,6),
                                        ema130 DECIMAL(24,6),
                                        ema60 DECIMAL(24,6),
                                        macd DECIMAL(24,6),
                                        macd_signal DECIMAL(24,6),
                                        macd_hist DECIMAL(24,6),
                                        fast_k DECIMAL(24,6),
                                        slow_d DECIMAL(24,6),
                                        last_update DATE,
                                        PRIMARY KEY (analysis_date, market_cd, stock_cd, stock_date)
                                    )
                                """

            connection.execute(sql)

            sql = """
                                            CREATE TABLE IF NOT EXISTS TRADE_CAL20 (
                                                analysis_date VARCHAR(100), 
                                                market_cd VARCHAR(100),
                                                stock_cd VARCHAR(100),
                                                stock_date VARCHAR(100),
                                                close DECIMAL(24,6),
                                                ma20 DECIMAL(24,6),
                                                upper DECIMAL(24,6),
                                                lower DECIMAL(24,6),
                                                pb DECIMAL(24,6),
                                                mfi10 DECIMAL(24,6),
                                                last_update DATE,
                                                PRIMARY KEY (analysis_date, market_cd, stock_cd, stock_date)
                                            )
                                        """

            connection.execute(sql)

            sql = """
                                                    CREATE TABLE IF NOT EXISTS TRADE_CAL30 (
                                                        analysis_date VARCHAR(100), 
                                                        market_cd VARCHAR(100),
                                                        stock_cd VARCHAR(100),
                                                        stock_date VARCHAR(100),
                                                        close DECIMAL(24,6),
                                                        ma20 DECIMAL(24,6),
                                                        upper DECIMAL(24,6),
                                                        lower DECIMAL(24,6),
                                                        pb DECIMAL(24,6),
                                                        iip21 DECIMAL(24,6),
                                                        last_update DATE,
                                                        PRIMARY KEY (analysis_date, market_cd, stock_cd, stock_date)
                                                    )
                                                """

            connection.execute(sql)

            sql = """
                            CREATE TABLE IF NOT EXISTS NOTIFY(
                                analysis_date VARCHAR(100), 
                                market_cd VARCHAR(100),
                                stock_cd VARCHAR(100),
                                stock_date VARCHAR(100),
                                trade_cls_cd VARCHAR(4),
                                sell INT(3),
                                buy INT(3),
                                last_update DATE,
                                PRIMARY KEY (analysis_date, market_cd, stock_cd, stock_date, trade_cls_cd)
                            )
                        """

            connection.execute(sql)

        self.stock_list_volume = pd.DataFrame()
        self.stock_list_user = pd.DataFrame()

        #  MARKET_LOC_CD는 crontab 분할을 위해 사용
        self.market_loc_cd = market_loc_cd

    def get_stock_list_volume(self):
        """최근일자 기준 시장별 거래량 상위 100개 주식 추출"""
        try:
            nowDate = datetime.now().date().strftime("%Y-%m-%d")
            with self.engine.connect() as connection:
                sql = f"""
                                        SELECT market_cd
                                          FROM MARKET
                                         WHERE market_loc_cd = '{self.market_loc_cd}'
                                        """
                market_list = connection.execute(sql).fetchall()

            analysis_target_list = []
            for market in market_list:
                # 최근날짜 구하기
                sql = f"""
                        SELECT max(stock_date) as recent_date
                         FROM PRICE_DAY
                        WHERE market_cd = '{market[0]}'            
                        """
                recent_date = self.engine.execute(sql).fetchone()

                sql = f"""
                          SELECT *
                         FROM PRICE_DAY
                        WHERE market_cd = '{market[0]}'
                          AND stock_date = '{recent_date[0]}'           
                        """
                temp_stock = pd.read_sql(sql, con=self.engine)
                if temp_stock is None:
                    continue
                stocks = temp_stock.sort_values(by='volume', ascending=False).head(100)
                analysis_target_list.append(stocks)

            temp_analysis_stocks = pd.concat(analysis_target_list)
            if temp_analysis_stocks is None:
                return

            analysis_stocks = temp_analysis_stocks[['market_cd', 'stock_cd', 'stock_date']]
            analysis_stocks = analysis_stocks.reset_index()
            self.stock_list_volume = analysis_stocks

            print("-----FINISH TO GET STOCK PRICE-----")

        except Exception as e:
            print('Exception occured : ', str(e))
            return None

    def analysis_day_price(self, df_prices, trade_way, analysis_period, trade_cls_cd, market_cd, stock_cd):
        # 매매 기법 대상 분석
        trade_way = eval(trade_way)
        tw = trade_way(df_prices, analysis_period, trade_cls_cd, market_cd, stock_cd)
        rs = tw.get_analysis_result()
        return rs

    def notify_day_price(self, target_list):
        # 1. 분석기간 가장 큰 기간 구하기 --> ST_PRICE_DAY 조회 조건에 사용
        period_list = [a.get_timing_period() for a in Analysis]
        max_period = max(period_list)
        from_date = (datetime.now().date() - relativedelta(months=max_period)).strftime('%Y-%m-%d')

        # 2. 분석날짜에 저장된 데이터가 있다면 삭제
        with self.engine.connect() as connection:
            sql = f"""SELECT market_cd
                               FROM MARKET
                              WHERE market_loc_cd = '{self.market_loc_cd}'
                                                 """
            del_list = connection.execute(sql).fetchall()
            nowDate = datetime.today().strftime('%Y-%m-%d')
            for dl in del_list:
                sql = f"DELETE FROM NOTIFY WHERE market_cd = '{dl[0]}' and analysis_date >= '{nowDate}'"
                connection.execute(sql)

                for a in Analysis:
                    if a.get_use_yn() == 'Y':
                        table_name = a.get_table_name()
                        sql = f"DELETE FROM {table_name} WHERE market_cd = '{dl[0]}' and analysis_date >= '{nowDate}'"
                        connection.execute(sql)



        # 3. 한 종목씩 DB업데이트 호출
        lst_analysis_result = []
        for idx, stock in target_list.iterrows():
            sql = f"""
                                 SELECT *
                                   FROM PRICE_DAY
                                  WHERE market_cd = '{stock.market_cd}'
                                    AND stock_cd = '{stock.stock_cd}'
                                    AND stock_date >= '{from_date}'
                             """
            df_prices = pd.read_sql(sql, con=self.engine)
            for a in Analysis:
                if a.get_use_yn() == 'Y':
                    batch_name = a.get_batch_name()
                    analysis_period = a.get_timing_period()
                    trade_cls_cd = a.get_trade_cls_cd()
                    rs_analysis = self.analysis_day_price(df_prices, batch_name, analysis_period, trade_cls_cd,
                                                          stock.market_cd, stock.stock_cd)
                    if rs_analysis is None:
                        continue

                    lst_analysis_result.append(rs_analysis)
                    print('[{}] #{:06d} {} ({}) : [{}] {} rows >  DATAFRAME FOR NOTIFY [OK]'.format(
                        datetime.now().strftime('%Y-%m-%d %H:%M'), idx + 1, stock['stock_cd'], stock['market_cd'],
                        a.name, len(rs_analysis)))

        ds_analysis = pd.concat(lst_analysis_result)

        # 4. df 저장
        with self.engine.connect() as connection:
            ds_analysis.to_sql(name='NOTIFY', con=connection, if_exists='append', index=False,
                               dtype={'analysis_date': sqlalchemy.types.VARCHAR(100),
                                      'market_cd': sqlalchemy.types.VARCHAR(100),
                                      'stock_cd': sqlalchemy.types.VARCHAR(100),
                                      'stock_date': sqlalchemy.types.VARCHAR(100),
                                      'trade_cls_cd': sqlalchemy.types.VARCHAR(4),
                                      'sell': sqlalchemy.types.INTEGER,
                                      'buy': sqlalchemy.types.INTEGER,
                                      'last_update': sqlalchemy.DateTime()})
            print('-------------FINISH UPDATE NOTIFY-------------- ')

    def analysis_stocks_volume(self):
        self.get_stock_list_volume()  # 1. 시장별 100개 대상 먼저 뽑고
        target_list = self.stock_list_volume
        self.notify_day_price(target_list)  # 2. 분석해서 테이블에 저장

        # 업데이트 시간 구하기
        nowTime = datetime.now()
        print("Finish Analysis ({}) ...".format(nowTime.strftime('%Y-%m-%d %H:%M')))


if __name__ == '__main__':
    a = TimingDayAnalysis('KR')
    a.analysis_stocks_volume()
