import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)  # 버전차이 무시

from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import sqlalchemy
import timing_db_info


class AnalysisDay20:
    def __init__(self, df_input, analysis_period, trade_cls_cd, market_cd, stock_cd):
        self.engine = create_engine(
            f'mysql+pymysql://{timing_db_info.user}:{timing_db_info.passwd}@{timing_db_info.db_url}/{timing_db_info.db_name}')

        # 1. 변수 초기화
        cal_from_date = (datetime.now().date() - relativedelta(months=analysis_period)).strftime('%Y-%m-%d')
        self.df_prices = df_input[df_input['stock_date'] > cal_from_date]
        self.trade_cls_cd = trade_cls_cd
        self.market_cd = market_cd
        self.stock_cd = stock_cd

    def get_analysis_result(self):

        df_prices = self.df_prices

        # 1.볼린저 밴드, 추세추종 매매 기법
        df_prices['ma20'] = df_prices['close'].rolling(window=20).mean()
        df_prices['stddev'] = df_prices['close'].rolling(window=20).std()
        df_prices['upper'] = df_prices['ma20'] + (df_prices['stddev'] * 2)
        df_prices['lower'] = df_prices['ma20'] - (df_prices['stddev'] * 2)

        # 2. %b
        df_prices['pb'] = (df_prices['close'] - df_prices['lower']) / (df_prices['upper'] - df_prices['lower'])
        df_prices['tp'] = (df_prices['high'] + df_prices['low'] + df_prices['close']) / 3 # typical price
        df_prices['pmf'] = 0
        df_prices['nmf'] = 0

        for i in range(len(df_prices.close) - 1):
            if df_prices.tp.values[i] < df_prices.tp.values[i + 1]:
                df_prices.pmf.values[i + 1] = df_prices.tp.values[i + 1] * df_prices.volume.values[i + 1]
                df_prices.nmf.values[i + 1] = 0
            else:
                df_prices.nmf.values[i + 1] = df_prices.tp.values[i + 1] * df_prices.volume.values[i + 1]
                df_prices.pmf.values[i + 1] = 0

        # 3. 현금흐름지표(확증지표)
        df_prices['mfr'] = df_prices.pmf.rolling(window=10).sum() / df_prices.nmf.rolling(window=10).sum()
        df_prices['mfi10'] = 100 - 100 / (1 + df_prices['mfr'])

        df_prices = df_prices.copy()[19:]


        # 4. DB저장
        df_prices = df_prices[['market_cd', 'stock_cd', 'stock_date', 'close', 'ma20', 'upper', 'lower', 'pb', 'mfi10']]
        df_prices['analysis_date'] = datetime.today().strftime('%Y-%m-%d')
        df_prices['last_update'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        with self.engine.connect() as connection:
            df_prices.to_sql(name='TRADE_CAL20', con=connection, if_exists='append', index=False,
                             dtype={'analysis_date': sqlalchemy.types.VARCHAR(100),
                                    'market_cd': sqlalchemy.types.VARCHAR(100),
                                    'stock_cd': sqlalchemy.types.VARCHAR(100),
                                    'stock_date': sqlalchemy.types.VARCHAR(100),
                                    'close': sqlalchemy.types.DECIMAL(24, 6),
                                    'ma20': sqlalchemy.types.DECIMAL(24, 6),
                                    'upper': sqlalchemy.types.DECIMAL(24, 6),
                                    'lower': sqlalchemy.types.DECIMAL(24, 6),
                                    'pb': sqlalchemy.types.DECIMAL(24, 6),
                                    'mfi10': sqlalchemy.types.DECIMAL(24, 6),
                                    'last_update': sqlalchemy.DateTime()})
            print('-------------FINISH UPDATE TRADE_CAL20-------------- ')


        # 5. 매수, 매도 타이밍 저장
        df_prices['sell'] = 0
        df_prices['buy'] = 0
        for i in range(len(df_prices.close)):
            if df_prices.pb.values[i] > 0.8 and df_prices.mfi10.values[i] > 80:
                df_prices.sell.values[i] = 0
                df_prices.buy.values[i] = 1
            elif df_prices.pb.values[i] < 0.2 and df_prices.mfi10.values[i] < 20:
                df_prices.sell.values[i] = 1
                df_prices.buy.values[i] = 0
            else:
                continue

        df_result = df_prices[(df_prices['sell'] > 0) | (df_prices['buy'] > 0)]
        df_result = df_result[['market_cd', 'stock_cd', 'stock_date', 'sell', 'buy', 'analysis_date', 'last_update']]
        df_result['trade_cls_cd'] = self.trade_cls_cd

        return df_result