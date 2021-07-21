import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)  # 버전차이 무시

import pymysql
import pandas as pd
from datetime import datetime
from datetime import timedelta
import json
import timing_info
import FinanceDataReader as fdr
from sqlalchemy import create_engine
import sqlalchemy


class TimingDBUpdater:
    def __init__(self, market_loc_cd):
        """생성자: DB연결 및 사용할 테이블 생성
            --------------------------------------
            MARKET : 한국, 미국 주식시장 심볼
            STOCK_LIST : 상장 주식 리스트
            PRICE_DAY : 일자별 주가시세 데이터
            --------------------------------------
        """
        self.conn = pymysql.connect(host=timing_info.db_url, user=timing_info.user, passwd=timing_info.passwd,
                                    db=timing_info.db_name)
        self.engine = create_engine(
            f'mysql+pymysql://{timing_info.user}:{timing_info.passwd}@{timing_info.db_url}/{timing_info.db_name}')  # ORM

        self.market_loc_cd = market_loc_cd

        with self.engine.connect() as connection:
            sql = """
                            CREATE TABLE IF NOT EXISTS MARKET (
                                market_cd VARCHAR(100),
                                market_nm VARCHAR(200),
                                market_cls_cd VARCHAR(100),
                                market_loc_cd VARCHAR(100),
                                last_update DATE,
                                PRIMARY KEY (market_cd)
                            )
                        """
            # MARKET_LOC_CD와 MARKET_CLS_CD는 추후 추가될 가능성을 위해 추가
            connection.execute(sql)

            sql = """
                            CREATE TABLE IF NOT EXISTS STOCK_LIST (
                                market_cd VARCHAR(100),
                                stock_cd VARCHAR(100),  
                                stock_nm VARCHAR(200),
                                last_update DATE,
                                PRIMARY KEY (market_cd, stock_cd)
                            )
                        """
            connection.execute(sql)

            sql = """
                            CREATE TABLE IF NOT EXISTS PRICE_DAY (
                                market_cd VARCHAR(100),
                                stock_cd VARCHAR(100),
                                stock_date VARCHAR(100),
                                open DECIMAL(24,6),
                                high DECIMAL(24,6),
                                low DECIMAL(24,6),
                                close DECIMAL(24,6),
                                diff DECIMAL(24,6),
                                volume DECIMAL(24,6),
                                last_update DATE,
                                PRIMARY KEY (market_cd, stock_cd, stock_date)
                            )
                        """

            connection.execute(sql)

        self.markets = pd.DataFrame()
        self.stocks = pd.DataFrame()
        self.indices = pd.DataFrame()

    def read_stock_code(self):
        """finance data reader로부터 주가시장 및 상장기업 DF로 반환"""

        # 1. 주가시장 심볼 가져오기
        sql = f"SELECT market_cd, market_cls_cd FROM MARKET WHERE market_loc_cd = '{self.market_loc_cd}'"
        market_list = self.engine.execute(sql).fetchall()

        # 1-1. 만약에 테이블의 정보를 못가져온다면, 하드코딩으로 작업해 주가시장 리스트를 가져온다.
        if len(market_list) == 0:
            with self.engine.connect() as connection:
                connection.execute("""insert into MARKET values('KONEX', 'KONEX', 'KRX', 'KR', '2021-04-13')""")
                connection.execute("""insert into MARKET values('KOSDAQ', 'KOSDAQ', 'KRX', 'KR', '2021-04-13')""")
                connection.execute("""insert into MARKET values('KOSPI', 'KOSPI', 'KRX', 'KR', '2021-04-13')""")
            market_list = [('KONEX','KRX'), ('KOSDAQ','KRX'), ('KOSPI','KRX')]

        # 2. 주가시장에 대한 주식 리스트를 가져와서 한번에 concat작업으로 DF를 만든다. -->더 빠름
        df_list = []
        for market in market_list:
            print(f"{market[0]} is start...")
            df_stock_code = fdr.StockListing(market[0])[['Symbol', 'Name']]
            df_stock_code['Market'] = market[0]
            df_stock_code['MarketClsCd'] = market[1]
            df_stock_code['last_update'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            df_list.append(df_stock_code)
            print(f"{market[0]} is done...")
        df_stock = pd.concat(df_list)  # list 속 DataFrame들을 일괄 병합함.
        df_stock = df_stock.rename(columns={'Symbol': 'stock_cd', 'Name': 'stock_nm', 'Market': 'market_cd', 'MarketClsCd':'market_cls_cd'})
        df_stock = df_stock.reset_index()
        return df_stock

    def update_stock_list(self):
        """주가시장 및 종목코드를 ST_LIST 업데이트 , DataFrame에 저장"""

        # 1. 저장된 주가 리스트를 가져온다,.
        sql = f"""SELECT a.market_cd, a.stock_cd, b.market_cls_cd
                          FROM STOCK_LIST a join MARKET b
                            ON a.market_cd = b.market_cd
                         WHERE b.market_loc_cd = '{self.market_loc_cd}'
                             """
        # self.stocks = pd.DataFrame(self.engine.execute(sql).fetchall())
        self.stocks = pd.read_sql(sql, con=self.engine)
        print(self.stocks)

        # 2. 마지막 업데이트 날짜와 오늘 날짜 비교
        sql = f"""SELECT max(a.last_update) 
                   FROM STOCK_LIST a JOIN MARKET b
                    ON a.market_cd = b.market_cd
                 WHERE b.market_loc_cd = '{self.market_loc_cd}'"""
        rs = self.engine.execute(sql).fetchone()
        today = datetime.today().strftime('%Y-%m-%d')

        # 3. 오늘날짜가 최신이라면 전부 새롭게 업데이트한다.
        if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
            with self.engine.connect() as connection:
                sql = f"""SELECT market_cd
                           FROM MARKET 
                          WHERE market_loc_cd = '{self.market_loc_cd}'
                                             """
                del_list = connection.execute(sql).fetchall()


                for dl in del_list:
                    sql = f"DELETE FROM STOCK_LIST WHERE market_cd = '{dl[0]}'"
                    connection.execute(sql)

            tmp_stocks = self.read_stock_code()
            self.stocks = tmp_stocks  # stocks 인스턴수 변수에 DF 저장

            rs_stocks = tmp_stocks[['market_cd', 'stock_cd', 'stock_nm', 'last_update']]
            with self.engine.connect() as connection:
                rs_stocks.to_sql(name='STOCK_LIST', con=connection, if_exists='append', index=False,
                                  dtype={'market_cd': sqlalchemy.types.VARCHAR(100),
                                         'stock_cd': sqlalchemy.types.VARCHAR(100),
                                         'stock_nm': sqlalchemy.types.VARCHAR(200),
                                         'last_update': sqlalchemy.DateTime()})
                print('-------------FINISH UPDATE STOCK_LIST-------------- ')

    def save_update_price(self, num, market_cd, stock_cd, market_cls_cd, from_date):
        try:
            # 1. 주가를 from_date로부터 가져와서 DF에 저장
            df_prices = fdr.DataReader(stock_cd, from_date, exchange=market_cls_cd).reset_index()
            df_prices['stock_date'] = df_prices['Date'].dt.strftime('%Y-%m-%d')
            del df_prices['Date']
            df_prices['last_update'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            df_prices['market_cd'] = market_cd
            df_prices['stock_cd'] = stock_cd
            return df_prices

        except Exception as e:
            print('Exception occured : ', str(e))
            return None

    def update_daily_price(self, from_date):
        """DF를 DB에 업데이트"""
        df_temp = []

        # 1. 하나의 데이터 프레임으로 생성
        for idx, row in self.stocks.iterrows():
            if (idx+1) % 200 == 0:
                sql = f"""SELECT "AAA"
                            FROM DUAL
                                    """
                self.engine.execute(sql)
                print("DB 연결접속 정상 ")

            rs_prices = self.save_update_price(idx, row['market_cd'], row['stock_cd'], row['market_cls_cd'], from_date)
            if rs_prices is None:
                continue
            df_temp.append(rs_prices)
            print('[{}] #{:06d} {} ({}) : {} rows > UPDATE DATAFRAME FOR PRICE_DAY [OK]'.format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), idx + 1, row['stock_cd'], row['market_cd'], len(rs_prices)))


        df_returns = pd.concat(df_temp)
        print(df_returns)
        # 2. 필요없는 데이터 일괄 삭제 및 칼럼 변경
        df_returns = df_returns.dropna()  # n/a 제거
        df_returns = df_returns.rename(columns={'Open': 'open',
                                              'High': 'high',
                                              'Low': 'low',
                                              'Close': 'close',
                                              'Change': 'diff',
                                              'Volume': 'volume'})
        df_returns = df_returns[['market_cd', 'stock_cd', 'stock_date', 'open', 'high', 'low', 'close', 'diff', 'volume', 'last_update']]

        with self.engine.connect() as connection:
            sql = f"""SELECT market_cd
                       FROM MARKET
                      WHERE market_loc_cd = '{self.market_loc_cd}'
                                         """
            del_list = connection.execute(sql).fetchall()

            for dl in del_list:
                print("###"+dl[0])
                connection.execute(
                    f"DELETE FROM PRICE_DAY WHERE market_cd = '{dl[0]}' and stock_date >= '{from_date}'")

            print(df_returns)
        # 3. DB에 일괄 업데이트
            df_returns.to_sql(name='PRICE_DAY', con=connection, if_exists='append', index=False,
                             dtype={'market_cd': sqlalchemy.types.VARCHAR(100),
                                    'stock_cd': sqlalchemy.types.VARCHAR(100),
                                    'stock_date': sqlalchemy.types.VARCHAR(100),
                                    'open': sqlalchemy.types.DECIMAL(24, 6),
                                    'high': sqlalchemy.types.DECIMAL(24, 6),
                                    'low': sqlalchemy.types.DECIMAL(24, 6),
                                    'close': sqlalchemy.types.DECIMAL(24, 6),
                                    'diff': sqlalchemy.types.DECIMAL(24, 6),
                                    'volume': sqlalchemy.types.DECIMAL(24, 6),
                                    'last_update': sqlalchemy.DateTime()}
                             )

        print('-------------FINISH UPDATE PRICE_DAY-------------- ')

    def execute_daily(self):
        """실행 즉시 or 매일 오후 5시에 테이블 업데이트"""
        self.update_stock_list()

        try:
            with open(f'config{self.market_loc_cd}.json', 'r') as in_file:
                config = json.load(in_file)
                days_flag = config['days_flag']
                from_date = (datetime.now().date() - timedelta(days=20)).strftime('%Y-%m-%d')
        except FileNotFoundError:
            with open(f'config{self.market_loc_cd}.json', 'w') as out_file:
                config = {'days_flag': 20}
                json.dump(config, out_file)
                from_date = '2016-01-01'
        self.update_daily_price(from_date)
        print('FINISH UPDATE DAILY PRICE')

        # 업데이트 시간 구하기
        next_day = datetime.now() + timedelta(days=1)
        print("Waiting for next update ({}) ...".format(next_day.strftime('%Y-%m-%d %H:%M')))

if __name__ == '__main__':
    a = TimingDBUpdater('KR')
    a.execute_daily()

