import schedule
import time
import os
import datetime
import pandas as pd
from tqdm import tqdm
from lib import technical_analysis
from IPython import display

def update(qtd_days=1, write_path="data/yfinance/", hist_path='hist_market_trading_yfinance.csv', plot_risk_and_return=None):
    r = technical_analysis.daily_analysis_yfinance(write_path=write_path, hist_path=hist_path, qtd_days=qtd_days, plot_risk_and_return=plot_risk_and_return)
    display.display(r[1])
    print(f"Shape dos dados: {r[1].shape}\n\nPróxima execução: {datetime.datetime.now() + datetime.timedelta(minutes=schedule_every)}\n")

if not os.path.exists('data/'):
    os.mkdir('data/')
    os.mkdir('data/yfinance/')
if not os.path.exists('reports/'):
    os.mkdir('reports/')
    os.mkdir('reports/yfinance/')

update(qtd_days=None, plot_risk_and_return=True)
schedule.every(1).minutes.do(update)
while True:
    schedule.run_pending()
    time.sleep(1)
