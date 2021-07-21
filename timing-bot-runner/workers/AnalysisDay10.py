import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)  # 버전차이 무시

from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import sqlalchemy
import timing_info


class AnalysisDay10:
    def __init__(self, df_input, analysis_period, trade_cls_cd, market_cd, stock_cd):
        self.engine = create_engine(
            f'mysql+pymysql://{timing_info.user}:{timing_info.passwd}@{timing_info.db_url}/{timing_info.db_name}')

        # 1. 변수 초기화
        cal_from_date = (datetime.now().date() - relativedelta(months=analysis_period)).strftime('%Y-%m-%d')
        self.df_prices = df_input[df_input['stock_date'] > cal_from_date]
        self.trade_cls_cd = trade_cls_cd
        self.market_cd = market_cd
        self.stock_cd = stock_cd

    def get_analysis_result(self):

        df_prices = self.df_prices

        # 1. 시장 조류: 장기차트 분석
        ema60 = df_prices.close.ewm(span=60).mean()  # 12주 지수이평선
        ema130 = df_prices.close.ewm(span=130).mean()  # 26주 지수이평선

        macd = ema60 - ema130  # macd
        macd_signal = macd.ewm(span=45).mean()
        macd_hist = macd - macd_signal  # macd  히스토그램

        df_prices = df_prices.assign(ema130=ema130, ema60=ema60, macd=macd, macd_signal=macd_signal, macd_hist=macd_hist).dropna()

        # 2. 시장 파도: 반전 체크, 현재 상황 파악
        ndays_high = df_prices.high.rolling(window=14, min_periods=1).max()
        ndays_low = df_prices.low.rolling(window=14, min_periods=1).min()
        fast_k = (df_prices.close - ndays_low) / (ndays_high - ndays_low) * 100
        slow_d = fast_k.rolling(window=3).mean()
        df_prices = df_prices.assign(fast_k=fast_k, slow_d=slow_d).dropna() # assign 메서드는 새로운 오브젝트를 리턴한다.


        df_prices = df_prices[['market_cd', 'stock_cd', 'stock_date', 'close', 'ema130', 'ema60', 'macd', 'macd_signal', 'macd_hist', 'fast_k', 'slow_d']]
        df_prices['analysis_date'] = datetime.today().strftime('%Y-%m-%d')
        df_prices['last_update'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        with self.engine.connect() as connection:
            df_prices.to_sql(name='TRADE_CAL10', con=connection, if_exists='append', index=False,
                             dtype={'analysis_date': sqlalchemy.types.VARCHAR(100),
                                    'market_cd': sqlalchemy.types.VARCHAR(100),
                                    'stock_cd': sqlalchemy.types.VARCHAR(100),
                                    'stock_date': sqlalchemy.types.VARCHAR(100),
                                    'close': sqlalchemy.types.VARCHAR(100),
                                    'ema130': sqlalchemy.types.DECIMAL(24, 6),
                                    'ema60': sqlalchemy.types.DECIMAL(24, 6),
                                    'macd': sqlalchemy.types.DECIMAL(24, 6),
                                    'macd_signal': sqlalchemy.types.DECIMAL(24, 6),
                                    'macd_hist': sqlalchemy.types.DECIMAL(24, 6),
                                    'fast_k': sqlalchemy.types.DECIMAL(24, 6),
                                    'slow_d': sqlalchemy.types.DECIMAL(24, 6),
                                    'last_update': sqlalchemy.DateTime()})
            print('-------------FINISH UPDATE TRADE_CAL10-------------- ')

        # 4. 진입기술: 추세가 상승하지만 과매도가 날 경우 매수, 추세가 하락하지만 과매수가 날 경우 매도
        df_prices['sell'] = 0
        df_prices['buy'] = 0
        for i in range(1, len(df_prices.close)):
            # 좀 더 확실한 신호를 위해 %K가 아닌 %D를 사용, %D 가 낮다는 것은 그만큼 과매도가 되고 있다는 뜻 -> 매수의 기회
            if df_prices.ema130.values[i - 1] < df_prices.ema130.values[i] \
                    and df_prices.macd_hist.values[i - 1] < df_prices.macd_hist.values[i] \
                    and df_prices.slow_d.values[i - 1] >= 20 and df_prices.slow_d.values[i] < 20:
                # 추세가 상승인데 스토캐스틱은 하락인 경우
                df_prices.sell.values[i] = 0
                df_prices.buy.values[i] = 1
            elif df_prices.ema130.values[i - 1] > df_prices.ema130.values[i] \
                    and df_prices.macd_hist.values[i - 1] > df_prices.macd_hist.values[i] \
                    and df_prices.slow_d.values[i - 1] <= 80 and df_prices.slow_d.values[i] > 80:
                # 추세가 하락인데 스토캐스틱은 상승인 경우
                df_prices.sell.values[i] = 1
                df_prices.buy.values[i] = 0


        df_result = df_prices[(df_prices['sell'] > 0) | (df_prices['buy'] > 0)]
        df_result = df_result[['market_cd', 'stock_cd', 'stock_date', 'sell', 'buy', 'analysis_date', 'last_update']]
        df_result['trade_cls_cd'] = self.trade_cls_cd

        return df_result