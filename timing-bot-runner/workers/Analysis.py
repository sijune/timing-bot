from enum import Enum

class Analysis(Enum):
    TRIPLE_SCREEN   = ('10', 24, 70, 'AnalysisDay10', 'TRADE_CAL10','Y')
    TRADE_FOLLOWING = ('20', 24, 50, 'AnalysisDay20', 'TRADE_CAL20','Y')
    REVERSALS = ('30', 24, 50, 'AnalysisDay30', 'TRADE_CAL30', 'Y')

    def __init__(self, trade_cls_cd, timing_period, safe_degree, batch_name, table_name, use_yn):
        self.trade_cls_cd = trade_cls_cd
        self.timing_period = timing_period
        self.safe_degree = safe_degree
        self.batch_name = batch_name
        self.table_name = table_name
        self.use_yn = use_yn

    def get_trade_cls_cd(self):
        return self.trade_cls_cd

    def get_batch_name(self):
        return self.batch_name

    def get_timing_period(self):
        return self.timing_period

    def get_use_yn(self):
        return self.use_yn

    def get_table_name(self):
        return self.table_name





