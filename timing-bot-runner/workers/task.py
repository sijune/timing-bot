from pyrunner import Worker
import TimingDBUpdater
import TimingDayAnalysis
import TimingDayNotify

class DBUpdateKR(Worker):
  def run(self):
    tuk = TimingDBUpdater.TimingDBUpdater('KR')
    tuk.execute_daily()

class DBAnalysisKR(Worker):
  def run(self):
    tda = TimingDayAnalysis.TimingDayAnalysis('KR')
    tda.analysis_stocks_volume()

class DBNotifyKR(Worker):
  def run(self):
    tdn = TimingDayNotify.TimingDayNotify('KR')
    tdn.notify_user()

