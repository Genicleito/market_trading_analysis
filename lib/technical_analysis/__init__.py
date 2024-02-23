import sklearn
import matplotlib.pyplot as plt
import requests, json, datetime, math
import pandas as pd
import numpy as np
from tqdm import tqdm
from IPython import display
import logging
import time
import os
import math
import urllib3
# http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/cotacoes/cotacoes/

_INTERVAL_DAYS = 1
today = datetime.datetime.today()
limit_time = 1730
ncores = os.cpu_count()
http = urllib3.PoolManager()
_YAHOO_API_URL = 'https://query1.finance.yahoo.com/v7/finance/download/'
_TIMESERIE_DF_COLUMNS = ['date', 'ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
_PERIODS_EMA = [8, 20, 72, 200]
_NOW = lambda: datetime.datetime.now()
_DEBUG = True

_PERIOD1 = 946695600 # 2000-01-01
_PERIOD2 = int(datetime.datetime(year=today.year, month=today.month, day=today.day, hour=today.hour, minute=today.minute, second=today.second, microsecond=today.microsecond).timestamp())

_hist_path = 'hist_market_trading_yfinance.csv.zip'

def create_pool():
    from multiprocessing.dummy import Pool as ThreadPool
    return ThreadPool(max(ncores//2), 1)

def updated_period2_to_now():
    now = datetime.datetime.today()
    return int(datetime.datetime(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute, second=now.second, microsecond=now.microsecond).timestamp())

_CODES = [
    'CEAB3', 'OIBR3', 'EMBR3', 'VALE3', 'GOLL4', 'COGN3', 'IRBR3', 'ABEV3', 'AZUL4', 'VULC3', 'SUZB3', 'ALSO3', 'QUAL3', 'CXSE3',
    'BMGB4', 'ECOR3', 'TOTS3', 'ITUB4', 'LREN3', 'GGBR4', 'USIM5', 'MRFG3', 'RENT3', 'MOVI3', 'VIVA3', 'ARZZ3', 'ETER3', 'PCAR3',
    'BRKM5', 'PFRM3', 'SOMA3', 'ABCB4', 'AMAR3', 'ANIM3', 'BPAN4', 'BRPR3', 'PETR4', 'SAPR3', 'MEAL3', 'TEND3', 'CIEL3', 'MILS3',
    'CCRO3', 'BEEF3', 'MGLU3', 'BBAS3', 'WEGE3', 'CYRE3', 'JHSF3', 'KLBN11', 'SHOW3', 'MRVE3', 'CSAN3', 'NTCO3', 'MDNE3',
    'SAPR11', 'JBSS3', 'BRFS3', 'CSNA3', 'ELET3', 'CMIG4', 'PDGR3', 'LPSB3', 'PRNR3', 'EZTC3', 'ENAT3', 'DMVF3', 'GUAR3',
    'SBSP3', 'RANI3', 'LWSA3', 'SAPR4', 'CAML3', 'GRND3', 'AGRO3', 'CRFB3', 'LAVV3', 'PGMN3', 'SMTO3', 'MYPK3', 'POMO4', 'STBP3', 'PETZ3',
    'ITSA4', 'PTBL3', 'ENJU3', 'AERI3', 'GMAT3', 'CRFB3', 'RAPT4', 'CXSE3', 'BHIA3', 'ITSA4', 'VBBR3'
]

another_codes = [
    'CIEL3', 'ENAT3', 'MMXM11', 'SHOW3', 'GUAR3', 'BPAN4', 'MYPK3', 'ARZZ3', 'VULC3', 'ESTR4', 'LWSA3', 'EZTC3', 'BBAS3', 'VIVA3', 'BMGB4',
    'BRFS3', 'TOTS3', 'ALSO3', 'POMO4', 'MRVE3', 'MDIA3', 'PDGR3', 'BRCO11', 'SAPR3', 'SEER3', 'PETZ3', 'LAVV3', 'CAML3', 'ABCB4', 'LLIS3',
    'IRBR3', 'BRML3', 'SLBG34', 'STBP3', 'SMTO3', 'ITSA4', 'RENT3', 'CEAB3', 'VALE3', 'ELET3', 'MGLU3', 'GOLL4', 'MEAL3', 'UGPA3', 'ANIM3',
    'WIZS3', 'SBSP3', 'CSNA3', 'DMVF3', 'OIBR3', 'BRKM5', 'CRFB3', 'PTBL3', 'EMBR3', 'JHSF3', 'CYRE3', 'QUAL3', 'SUZB3', 'AGRO3', 'SAPR4',
    'ECOR3', 'GRND3', 'ETER3', 'SOMA3', 'USIM5', 'BKBR3', 'ITUB4', 'BRDT3', 'COGN3', 'AZUL4', 'VIFI11', 'DMMO3', 'TRNT11', 'CMIG4', 'CSAN3',
    'VIIA3', 'PETR4', 'PLPL3', 'RAPT4', 'SANB3', 'NTCO3', 'LREN3', 'JBSS3', 'AMAR3', 'LAME4', 'GGBR4', 'MILS3', 'PRNR3', 'RANI3', 'PFRM3',
    'MDNE3', 'UNIP3', 'KLBN11', 'BRPR3', 'BBVJ11', 'MOVI3', 'SAPR11', 'ABEV3', 'BEEF3', 'GFSA3', 'WEGE3', 'MRFG3', 'PGMN3', 'LPSB3', 'BCFF11',
    'TEND3', 'IGBR3', 'BIDI4', 'INEP3', 'ENJU3', 'AERI3', 'GMAT3'
]

def get_main_codes():
    return _CODES

def get_all_tickers():
    return _CODES + list(set(another_codes) - set(_CODES))

def is_bullish_candle(candle):
    return candle['close'] > candle['open']

### Candle types ###
def candle_type(df): 
    # begin local functions
    def is_bearish_harami(candle, candle_ant, candle_ant_ant): # ok
        if (not is_bullish_candle(candle) and candle['open'] < candle_ant['close'] and candle['close'] > candle_ant['open'] and is_bullish_candle(candle_ant)): # and candle_ant_ant['close'] > candle_ant_ant['open']):
            return {'candle_type': 'bearish_harami', 'ind_trend': 'DOWN'}
        return None
        
    def is_bullish_harami(candle, candle_ant, candle_ant_ant): # ok
        if (is_bullish_candle(candle) and candle['close'] < candle_ant['open'] and candle['open'] > candle_ant['close'] and not is_bullish_candle(candle_ant)): # and candle_ant_ant['close'] < candle_ant_ant['open']):
            return {'candle_type': 'bullish_harami', 'ind_trend': 'UP'}
        return None

    def is_engulfing_bullish(candle, candle_ant, candle_ant_ant):
        # if (candle['close'] >= candle_ant['open'] and candle['open'] <= candle_ant['close']):
        if (candle['close'] > candle_ant['open'] and candle['open'] < candle_ant['close'] and is_bullish_candle(candle) and not is_bullish_candle(candle_ant)): # and candle_ant_ant['close'] < candle_ant_ant['open']):
            return {'candle_type': 'engulfing_bullish', 'ind_trend': 'UP'}
        return None
        
    def is_engulfing_bearish(candle, candle_ant, candle_ant_ant):
        if (candle['open'] > candle_ant['close'] and candle['close'] < candle_ant['open'] and not is_bullish_candle(candle) and is_bullish_candle(candle_ant)): # and candle_ant_ant['close'] > candle_ant_ant['open']):
            return {'candle_type': 'engulfing_bearish', 'ind_trend': 'DOWN'}
        return None
        
    def is_piercing(candle, candle_ant, candle_ant_ant=None):
        if (not is_bullish_candle(candle_ant) and is_bullish_candle(candle) and candle['high'] <= candle_ant['open'] and candle['low'] >= candle_ant['close']):
            return {'candle_type': 'piercing', 'ind_trend': 'UP'}
        return None
        
    def is_evening_star(candle, candle_ant, candle_ant_ant):
        first_cond = (candle_ant['low'] <= candle_ant_ant['low'] and candle_ant['high'] < candle_ant_ant['close'] and max(candle_ant['open'], candle_ant['close']) > candle_ant_ant['open'] and candle['close'] < candle['open'] and candle_ant_ant['close'] > candle_ant_ant['open'])
        second_cond = (candle_ant['low'] <= candle['low'] and candle_ant['high'] < candle['open'])
        if (first_cond and second_cond):
            return {'candle_type': 'evening_star', 'ind_trend': 'DOWN'}
        return None

    def is_morning_star(candle, candle_ant, candle_ant_ant):
        # candle, candle_ant, candle_next # era
        first_cond = (candle_ant['low'] <= candle_ant_ant['low'] and candle_ant['high'] > candle_ant_ant['close'] and max(candle_ant['open'], candle_ant['close']) > candle_ant_ant['close'] and candle['close'] > candle['open'] and candle_ant_ant['close'] < candle_ant_ant['open'])
        second_cond = (candle_ant['low'] <= candle['low'] and candle_ant['high'] < candle['close'])
        if (first_cond and second_cond):
            return {'candle_type': 'morning_star', 'ind_trend': 'UP'}
        return None

    def is_high_soldiers(candle, candle_ant, candle_ant_ant):
        first_cond = (candle['low'] >= candle_ant['open'] and candle['close'] >= candle_ant['high'])
        second_cond = (candle_ant['low'] >= candle_ant_ant['open'] and candle_ant['close'] >= candle_ant_ant['high'])
        third_condition = is_bullish_candle(candle_ant_ant) and is_bullish_candle(candle_ant) and is_bullish_candle(candle)
        if (first_cond and second_cond and third_condition):
            return {'candle_type': 'high_soldiers', 'ind_trend': 'UP'}
        return None

    def is_down_soldiers(candle, candle_ant, candle_ant_ant):
        first_cond = (candle['high'] <= candle_ant['open'] and candle['close'] <= candle_ant['low'])
        second_cond = (candle_ant['high'] <= candle_ant_ant['open'] and candle_ant['close'] <= candle_ant_ant['low'])
        third_condition = not is_bullish_candle(candle_ant_ant) and not is_bullish_candle(candle_ant) and not is_bullish_candle(candle)
        if (first_cond and second_cond and third_condition):
            return {'candle_type': 'down_soldiers', 'ind_trend': 'DOWN'}
        return None

    def is_hammer(candle, candle_ant, candle_ant_ant=None):
        def is_hammer_bullish(candle, candle_ant):
            first_cond = (candle['high'] - min(candle['open'], candle['close'])) < (min(candle['open'], candle['close']) - candle['low']) # formato de martelo
            second_cond = (candle['low'] <= candle_ant['low']) and (candle['high'] < candle_ant['open']) # and (candle['high'] > candle_ant['low'])
            third_cond = (candle_ant['close'] < candle_ant['open']) # candle anterior de baixa
            fourth_cond = (candle['high'] - max(candle['open'], candle['close'])) <= (abs(candle['open'] - candle['close']))
            if (first_cond and second_cond and third_cond and fourth_cond):
                return True
            return False

        def is_inverted_hammer_bullish(candle, candle_ant):
            first_cond = (max(candle['open'], candle['close']) - candle['low']) < (candle['high'] - max(candle['open'], candle['close'])) # formato de martelo
            second_cond = (candle['low'] < candle_ant['low']) and (candle['high'] < candle_ant['open'])
            third_cond = (candle_ant['open'] > candle_ant['close']) # candle enterior de baixa
            fourth_cond = (min(candle['open'], candle['close']) - candle['low']) <= (abs(candle['open'] - candle['close'])) # open - close = candle body
            if (first_cond and second_cond and third_cond and fourth_cond):
                return True
            return False
        
        def is_hammer_bearish(candle, candle_ant):
            first_cond = (candle['high'] - min(candle['open'], candle['close'])) < (min(candle['open'], candle['close']) - candle['low']) # formato de martelo
            second_cond = (candle['low'] < candle_ant['low']) and (candle['high'] < candle_ant['open'])
            third_cond = (candle_ant['open'] < candle_ant['close']) # candle enterior de alta
            fourth_cond = (candle['high'] - max(candle['open'], candle['close'])) <= (abs(candle['open'] - candle['close']))
            if (first_cond and second_cond and third_cond and fourth_cond):
                return True
            return False
        
        def is_shooting_star(candle, candle_ant): # bearish
            first_cond = (max(candle['open'], candle['close']) - candle['low']) < (candle['high'] - max(candle['open'], candle['close'])) # formato de martelo
            second_cond = (candle['low'] < candle_ant['low']) and (candle['high'] < candle_ant['open'])
            third_cond = (candle_ant['open'] < candle_ant['close']) # candle enterior de alta
            fourth_cond = (min(candle['open'], candle['close']) - candle['low']) <= (abs(candle['open'] - candle['close'])) # open - close = candle body
            if (first_cond and second_cond and third_cond and fourth_cond):
                return True
            return False
        
        if is_hammer_bullish(candle, candle_ant):
            return {'candle_type': 'hammer_bullish', 'ind_trend': 'UP'}
        elif is_inverted_hammer_bullish(candle, candle_ant):
            return {'candle_type': 'inverted_hammer_bullish', 'ind_trend': 'UP'}
        elif is_hammer_bearish(candle, candle_ant): # enforcado
            return {'candle_type': 'hammer_bearish', 'ind_trend': 'DOWN'}
        elif is_shooting_star(candle, candle_ant): # estrela cadente
            return {'candle_type': 'shooting_star', 'ind_trend': 'DOWN'}
        return None

    # Alterar para fazer isso para todo candle
    tmp = df.sort_values(by=['date'])
    tickers = tmp['ticker'].unique()
    d = [] # pd.DataFrame()
    list_func = [is_bearish_harami, is_bullish_harami, is_engulfing_bullish, is_engulfing_bearish, is_piercing, is_evening_star, is_morning_star, is_high_soldiers, is_down_soldiers, is_hammer]
    for ticker in tickers:
        candle = tmp[tmp['ticker'] == ticker].tail(5)
        dt = candle['date'].iloc[-1]
        candle = candle[['open', 'close', 'high', 'low']]
        if candle.shape[0] < 3: # não há candles suficientes
            continue
        x = candle.iloc[-1] # today
        y = candle.iloc[-2] # yesterday
        z = candle.iloc[-3] # before yesterday
        for f in list_func:
            r = f(x, y, z)
            if r:
                d.append(pd.DataFrame({'date': [dt], 'ticker': [ticker], 'candle_type': [f"{r['ind_trend']}: {r['candle_type']}"]}))
                # only the first
                break
    return df.merge(pd.concat(d, ignore_index=True), on=['date', 'ticker'], how='left') if d.shape[0] > 0 else df.assign(candle_type=None)

### end candle types ###

### Obter os candles que tiveram os melhores ganhos históricos
def get_buy_candles(df, qtd_days=8, gain_ratio=2, min_pct_gain=None): # rever se o min_pct_gain faz sentido
    tmp = df.sort_values(by=['date']).copy()
    ids = {'buy': [], 'sell': []}
    for i in range(tmp.shape[0] - 1):
        if len(ids['sell']) > 0 and i <= ids['sell'][-1]:
            continue
        b = tmp.iloc[i]
        s = (None, -1)
        for j in range(i + 1, i + qtd_days):
            if j == tmp.shape[0]:
                break
            target = b['close'] + gain_ratio * (b['close'] - b['low'])
            gain = tmp.iloc[j]['close'] - b['close']
            if ((tmp.iloc[j]['close'] >= target) and (tmp.iloc[j]['close'] > s[1])):
                if not min_pct_gain:
                    s = (j, tmp.iloc[j]['close'])
                elif gain >= min_pct_gain/100 * b['close']:
                    s = (j, tmp.iloc[j]['close'])
        if s[0]:
            ids['buy'].append(i)
            ids['sell'].append(s[0])
    trades_buy = tmp.iloc[ids['buy']][['date', 'close', 'low']].reset_index().rename(columns={'date': 'buy_date', 'index': 'original_index', 'low': 'loss'}).reset_index()
    trades_sell = tmp.iloc[ids['sell']][['date', 'close']].reset_index(drop=True).rename(columns={'date': 'sell_date', 'close': 'gain'}).reset_index()
    return trades_buy.merge(trades_sell, on=['index'], how='inner').drop('index', axis=1).rename(columns={'original_index': 'index'})
    # # Exemplo
    # t_s = datetime.datetime.now()
    # buy_candles = get_trades_recom(df)
    # buy_dates = buy_candles['buy_date']
    # df_buy = df[df['date'].isin(buy_dates)]
    # df_sell = df[~df['date'].isin(buy_dates)]
    # df = df_buy.assign(buy=1).append(df_sell.assign(buy=0))
    # print(datetime.datetime.now() - t_s)

def prepare_dataframe_to_ml(df, split_df=True, col_predict_name='y'):
    dataset = pd.DataFrame()
    dataset['candle_crossing_ema8'] = candle_crossing_ema(df, period=8)
    dataset['candle_crossing_ema20'] = candle_crossing_ema(df, period=20)
    dataset['candle_crossing_ema72'] = candle_crossing_ema(df, period=72)
    dataset['crossing_ema_short_term'] = crossing_ema_short_term(df)
    dataset['crossing_ema_long_term'] = crossing_ema_long_term(df)
    dataset['today_candle'] = np.where(df['close'] > df['open'], 1, 0)
    dataset['trend_today'] = np.where(df['close'] > df['close'].shift(1), 1, 0)
    dataset['ind_volume'] = np.where(df['ind_volume'] >= 0, 1, 0)
    dataset['macd_signal'] = np.where(df['macd'] >= df['macd_signal'], 1, 0)
    dataset['trend_tomorrow'] = np.where(df['close'] < df['close'].shift(-1), 1, 0)
    dataset['ema8_position'] = np.where(df['close'] >= df['close_ema8'], 1, 0)
    dataset['ema20_position'] = np.where(df['close'] >= df['close_ema20'], 1, 0)
    dataset['ema72_position'] = np.where(df['close'] >= df['close_ema72'], 1, 0)
    dataset[col_predict_name] = df[col_predict_name].values
    dataset = dataset.drop_duplicates()
    test_ids = np.random.randint(1, dataset.shape[0], int(30/100*dataset.shape[0]))
    test_ids = dataset.iloc[test_ids].index
    if split_df:
        train = dataset[~dataset.index.isin(test_ids)]
        test = dataset[dataset.index.isin(test_ids)].drop(col_predict_name, axis=1)
        X = train.drop(col_predict_name, axis=1)
        Y = train[col_predict_name]
    else:
        X = dataset[~dataset.index.isin(test_ids)].drop(col_predict_name, axis=1)
        Y = dataset[~dataset.index.isin(test_ids)][[col_predict_name]]
        test = None
    return {'X': X, 'Y': Y, 'test': test}

def series_to_supervised(df, n_prev=1, n_later=0, cols_fut=None, dropna=True, partition_by=None, sort_by=None, debug=True): # , shift_columns=None
    # cols_fut examle: {'<column_name>': <qtd_future_shift>, '<column_name>': <qtd_future_shift>}
    if debug: from tqdm import tqdm
    if n_later != 0: cols_fut = None
    columns = df.columns
    df_result = pd.DataFrame()
    dfs_flt = [df] if not partition_by else [df.query(f'{partition_by} == "{x}"') for x in df[partition_by].unique()]
    for tmp in (dfs_flt if not debug else tqdm(dfs_flt)):
        if sort_by: tmp.sort_values(by=sort_by)
        dfs = [tmp.rename(columns={col: f'{col}_t-{j}' for col in columns}).shift(j) for j in range(1, n_prev + 1)]
        dfs += [tmp]
        dfs += [tmp.rename(columns={col: f'{col}_t+{j+1}' for col in columns}).shift(-(j+1)) for j in range(n_later)]
        if cols_fut:
            i_df = -2 if n_later > 0 else -1
            dfs[i_df] = dfs[i_df].assign(**{
                f'{x}_t+{j+1}': dfs[i_df][x].shift(-(j+1)) for x in cols_fut.keys() for j in range(cols_fut[x])
            })
        # df_result = df_result.append(
        #     pd.concat(dfs, axis=1).dropna() if dropna else pd.concat(dfs, axis=1),
        #     ignore_index=True
        # )
        df_result.append(pd.concat(dfs, axis=1).dropna() if dropna else pd.concat(dfs, axis=1))

    # return df_result
    return pd.concat(df_result, ignore_index=True)

def split_random_train_test(df, tain_frac=0.7, drop_test_columns=None, debug=True):
    train = df.sample(frac=0.7)
    test = df.drop(train.index) if not drop_test_columns else df.drop(train.index).drop(drop_test_columns, axis=1)
    if debug: print(f'Train shape: {train.shape} | Test shape: {test.shape}')
    return train, test

def drop_columns(df, pattern):
    import re
    return df[[x for x in df.columns if not re.search(pattern, x)]]

### Perceptron
class PerceptronModel:
    def __init__(self, weights, bias, learning_rate, threshold):
        self.weights = weights
        self.bias = bias
        self.threshold = threshold
        self.learning_rate = learning_rate

    def predict(self, test):
        u = self.calc_u(test)
        return self.activation_function(u)

    def calc_u(self, x, m_factor=1): # b = bias, w = weight, x = value
        return round((self.weights * x).sum() + m_factor * self.bias, 4)

    def update_weights(self, x, expected, predicted):
        self.weights = self.weights + self.learning_rate * x * (expected - predicted)

    def update_bias(self, expected, predicted):
        self.bias = round(self.bias + self.learning_rate * (expected - predicted), 4)

    def activation_function(self, u):
        return 1 if u >= self.threshold else -1

    def get_error(self, expected, predicted, k, error):
        return error + (abs(expected - predicted) / k)

    def fit(self, df, iterations=100, debug=True):
        import pandas as pd
        import numpy as np
        from tqdm import tqdm
        _fit = False
        train = df.iloc[:, :-1]
        label = df.iloc[:, -1]
        for it in (tqdm(range(iterations)) if debug else range(iterations)):
            # if debug: print(f"{it+1}/{iterations}")
            e = 0
            for i in range(train.shape[0]):
                x = train.iloc[i].to_numpy()
                expected = label.iloc[i]
                u = self.calc_u(x)
                predicted = self.activation_function(u)
                if predicted != expected:
                    _fit = True
                    self.update_weights(x, expected, predicted)
                    self.update_bias(expected, predicted)
                    e = self.get_error(expected, predicted, i + 1, e)
            if not _fit: break
            _fit = False

class WKNN:
    def __init__(self, train_data, k=5, pred_column='y', task='r'):
        import pandas as pd
        import math
        from tqdm import tqdm
        self._pd = pd
        self._math = math
        self._tqdm = tqdm
        self.train_data = train_data if isinstance(train_data, pd.DataFrame) else pd.DataFrame(train_data)
        self.k = k
        self.pred_column = pred_column
        self.task = task
        
    def agg_weights(self, w_candidates):
        w_candidates['weights'] = np.where(
            w_candidates['weights'] == float('inf'),
            w_candidates['weights'][w_candidates['weights'] != float('inf')].max() * 10,
            w_candidates['weights']
        )
        scores = w_candidates.groupby('labels').sum().reset_index()
        # id of the max score
        max_score_indice = scores['weights'].idxmax()
        # label of the max score
        predict_label = scores.loc[max_score_indice]['labels']
        return predict_label

    # Calculate the Euclidian Distance
    def euclidian_distance(self, x, y):
        return self._math.sqrt((x - y).pow(2).sum())

    def __get_best_candidates(self, x_serie):
        # Calculate the distance of test and all elements of data train
        dists = self.train_data.drop(self.pred_column, axis=1).apply(
            self.euclidian_distance,
            args=(x_serie,),
            axis=1
        )
        # relation between distances and weights
        proximities = 1/dists
        # Sort k distances
        k_nearest_indices = dists.sort_values().iloc[:self.k].index
        candidates = self.train_data[self.pred_column].loc[k_nearest_indices]
        weights = proximities.loc[k_nearest_indices]
        w_candidates = pd.DataFrame({'labels': candidates, 'weights': weights})
        return candidates if self.task == 'r' else w_candidates
    
    def fit(self, test_data, debug=True, all_columns_df=False):
        # tsk = 'r' if self.train_data[self.pred_column].dtypes == 'float64' else self.task
        if isinstance(test_data, self._pd.Series): test_data = test_data.to_frame().T
        result = {'y_real': [], 'y_predict': []}
        for i in (test_data.index if not debug else self._tqdm(test_data.index)):
            x_test = test_data.loc[i]
            # Store the real (test) value
            result['y_real'] += [x_test[self.pred_column]]
            # Get best candidates without predict column in test data
            candidates = self.__get_best_candidates(x_test.drop(self.pred_column))
            # Make the prediction
            result['y_predict'].append(candidates.mean() if self.task == 'r' else self.agg_weights(candidates))
        if all_columns_df:
            self.results = self._pd.DataFrame({**{x: test_data[x] for x in test_data.columns}, **result})
        else: # Only two new columns
            self.results = self._pd.DataFrame({**{'original_index': test_data.index}, **result})
        return self.results
    
    def predict(self, new_data, debug=True):
        if isinstance(new_data, self._pd.Series): new_data = new_data.to_frame().T
        y_predict = []
        for i in (new_data.index if not debug else self._tqdm(new_data.index)):
            row = new_data.loc[i]
            # Get best candidates to new data
            candidates = self.__get_best_candidates(row)
            # Make the prediction
            y_predict.append(candidates.mean() if self.task == 'r' else self.agg_weights(candidates))
        return new_data.assign(y_predict=y_predict)

def buy_candles(df):
    return np.where((df['close'] < df['close'].shift(-5)) & (df['close'].shift(-5) >= (df['close'] + 2 * (df['close'] - df['open']))), 1, 0)

def check_model(dataset, model, test, predict_label='y'):
    qtd_errors = 0
    for i in test.index:
        y = model.predict(test.loc[i])
        if y != dataset.loc[i, predict_label]:
            qtd_errors += 1
    acc = 100 - (qtd_errors / test.shape[0] * 100)
    print(f'Acurácia: {acc}%')
    return acc

def perceptron(df, ticker=None, n_try=200):
    df_ml = prepare_dataframe_to_ml(df, split_df=False)
    bias = 0.5 # random.random()
    learning_rate = 0.4 # random.random()
    threshold = 0

    # df_unique = df.drop_duplicates().reset_index(drop=True)
    weights = np.random.random(df.shape[1] - 1)
    train = df_ml['X'] # X
    label = df_ml['Y'] # Y
    test = df_ml['test']

    # Model creation
    model = PerceptronModel(weights, bias, learning_rate, threshold=0) # sign function (degrau)
    # Fit model
    model.fit(train, debug=True)
    # Predict
    # print(f"{' '.join(map(str, model.weights.round(4)))} {model.bias}")
    result = test.apply(model.predict, axis=1).tolist()
    return model, result
### Fim Perceptron

def m2m(x, m=15):
    if m > 60: m = 60
    x = str(x)
    p2 = int(x[2:])
    p2 = str(m * (p2 // m))
    if len(p2) == 1: p2 = '0' + p2
    return x[:2] + p2

def get_polyfit_trend(serie, index, deg=1):
    """
    If the result is a +ve value --> increasing trend
    If the result is a -ve value --> decreasing trend
    If the result is a zero value --> No trend
    """
    import numpy as np
    return np.polyfit(index, serie, deg=deg, rcond=None, full=False, w=None, cov=False)

def get_stock_prices(tickers=None, old_hist=pd.DataFrame(), days=0, start_date=None, source='b3'):
    # FIXME
    df = pd.DataFrame()
    b3_path = 'https://arquivos.b3.com.br/apinegocios/tickercsv/'

    t_s = datetime.datetime.now()
    es_time = None
    codes_ = list(tickers) if tickers else _CODES[:]
    for code in codes_:
        if not start_date: start_date = (datetime.datetime.today()-datetime.timedelta(days=days))
        tmp = old_hist[old_hist['ticker'] == code]['date'] if not old_hist.empty else pd.DataFrame()
        for j in range(days + 1):
            dt = (start_date + datetime.timedelta(days=j)).date()
            if dt.weekday() in range(5, 7): continue
            if not tmp.empty and today.date() != dt and tmp[tmp.dt.date == dt].shape[0] > 0:
                continue
            try:
                df = df.append(pd.read_csv(f'{b3_path}{code}/{dt}', compression='zip', sep=';'), ignore_index=True)
                print(df.shape)
            except:
                None
    if not df.empty: df['GrssTradAmt'] = df['GrssTradAmt'].astype(str).str.replace(',', '.').astype(float)
    return df

def extract_minute_b3hist(df, var_to_extract='NtryTm'):
    return df.assign(minute=df['NtryTm'].apply(lambda x: str(x)[:4]).astype(int))

def aggregate_b3_hist(df, by='minute', limit_time=None):
    # df_trades = extract_time_b3hist(df, time=by)
    df_trades = df.copy()
    df_trades['minute'] = df_trades['minute'].astype(int)
    if limit_time: df_trades = df_trades[df_trades['minute'] < limit_time]
    df_trades['TradQty'] = df_trades['TradQty'].astype(int)
    df_trades['GrssTradAmt'] = df_trades['GrssTradAmt'].astype(str).str.replace(',', '.').astype(float)
    df_trades['financial_volume'] = df_trades['GrssTradAmt'] * df_trades['TradQty']
    
    df_trades = df_trades.drop(['GrssTradAmt', 'TradgSsnId', 'RptDt', 'NtryTm'], axis=1)
    l = ['TradDt', 'TckrSymb', by] if by == 'minute' else ['TradDt', 'TckrSymb']
    df_agg = df_trades.groupby(by=l).sum().reset_index()
    df_agg = df_agg.rename(columns={'TckrSymb': 'ticker', 'UpdActn': 'updactn', 'TradQty': 'volume', 'TradId': 'id', 'TradDt': 'date'})
    if by == 'minute':
        df_agg['date'] = (df_agg['date'].astype(str) + df_agg['minute'].astype(str)).apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d%H%M'))
    else:
        df_agg['date'] = pd.to_datetime(df_agg['date'], infer_datetime_format=True)
    return df_agg.drop('minute', axis=1)

def vwap(price, volume, decimal=2):
    return (np.cumsum(price * volume).cumsum() / volume.cumsum()).round(decimal)

def macd(fast_ma, slow_ma, decimal=2):
    return (fast_ma - slow_ma).round(decimal)

def ewm_std(x, ewm):
    return (x - ewm.shift(1))

def get_close(df, var_date='TradDt', limit_time=1730):
    df_ = df.copy()
    r = pd.DataFrame()
    for code in df['TckrSymb'].unique():
        for date in df_[var_date].sort_values().unique():
            r_ = df_.query(f"({var_date} == '{date}') & (TckrSymb == '{code}') & (minute < {limit_time})").sort_values(by=['TradId'], ascending=False).drop_duplicates(subset=['minute'])
            r = r.append(r_, ignore_index=True)
    r['date'] = (r['TradDt'].astype(str) + r['minute'].astype(str)).apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d%H%M'))
    # calcular min, max, open
    r = r.rename(columns={'TckrSymb': 'ticker', 'NtryTm': 'time', 'TradId': 'trade_id', 'GrssTradAmt': 'close'})
    return r.sort_values(by=['date', 'minute'])[['date', 'ticker', 'close', 'time', 'trade_id', 'minute']].reset_index(drop=True)

def get_candle_variables(df, by='minute', var_date='TradDt', limit_time=1730):
    # FIXME
    df_ = df.rename(columns={'TckrSymb': 'ticker', 'NtryTm': 'time', 'TradId': 'trade_id'}).copy()
    r = pd.DataFrame()
    if by != 'minute': by = 'TradDt'
    for code in df_['ticker'].unique():
        for date in df_[var_date].sort_values().unique():
            # r_ = df_.query(f"({var_date} == '{date}') & (ticker == '{code}') & (minute < {limit_time})")
            r_ = df_[(df_[var_date] == date) & (df_['ticker'] == code) & (df_['minute'].astype(int) < limit_time)]
            r0 = r_.sort_values(by=['trade_id'], ascending=False).drop_duplicates(subset=[by]).rename(columns={'GrssTradAmt': 'close'})
            r0 = r0.merge(r_.sort_values(by=['trade_id']).drop_duplicates(subset=[by])[[by, 'GrssTradAmt']].rename(columns={'GrssTradAmt': 'open'}), on=by, how='inner')
            r0 = r0.merge(r_.sort_values(by=['GrssTradAmt'], ascending=False).drop_duplicates(subset=[by])[[by, 'GrssTradAmt']].rename(columns={'GrssTradAmt': 'max'}), on=by, how='inner')
            r0 = r0.merge(r_.sort_values(by=['GrssTradAmt']).drop_duplicates(subset=[by])[[by, 'GrssTradAmt']].rename(columns={'GrssTradAmt': 'min'}), on=by, how='inner')
            r = r.append(r0, ignore_index=True)
    if by == 'minute':
        r['date'] = (r['TradDt'].astype(str) + r['minute'].astype(str)).apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d%H%M'))
        list_var = ['date', 'ticker', 'open', 'close', 'min', 'max', 'time', 'trade_id', 'minute']
        return r.sort_values(by=['date', 'minute'])[list_var].reset_index(drop=True)
    else:
        r['date'] = pd.to_datetime(r['TradDt'], infer_datetime_format=True)
        list_var = ['date', 'ticker', 'open', 'close', 'min', 'max', 'time', 'trade_id']
        return r.sort_values(by=['date'])[list_var].reset_index(drop=True)

def ema(serie, period):
    return serie.ewm(span=period, min_periods=period, adjust=False).mean().round(2)

def create_ema(df, periods=None):
    df_ = pd.DataFrame()
    tmp = df.copy()
    if not periods: periods = _PERIODS_EMA
    if not isinstance(periods, list): periods = [periods]
    for code in tmp['ticker'].unique():
        tmp = tmp[tmp['ticker'] == code].sort_values(by=['date'])
        for p in periods:
            # call ema function
            tmp[f'close_ema{p}'] = tmp['close'].ewm(span=p, min_periods=p, adjust=False).mean().round(2)
            if p == 20:
                # call ema function
                tmp[f'volume_ema{p}'] = tmp['volume'].ewm(span=p, min_periods=p, adjust=False).mean().round(0).astype(int, errors='ignore')
        df_ = df_.append(tmp, ignore_index=True)
    return df_

def get_signals(df):
    """
    Obtém as informações com base em análsie técnica dos indicadores.
    """
    tmp = df.copy()
    tmp['buy'] = np.where(
        ((tmp['close_ema8'] > tmp['close_ema20']) & (tmp['close_ema8'].shift(1) == tmp['close_ema20'].shift(1))) |\
            ((tmp['close_ema8'] > tmp['close_ema200']) & (tmp['close_ema8'].shift(1) == tmp['close_ema200'].shift(1))) |\
            ((tmp['close_ema20'] > tmp['close_ema200']) & (tmp['close_ema20'].shift(1) == tmp['close_ema200'].shift(1))) |\
                ((tmp['close'] > tmp['close_ema8']) & (tmp['close'].shift(1) <= tmp['close_ema8'].shift(1))) |\
                    ((tmp['close'] > tmp['close_ema20']) & (tmp['close'].shift(1) <= tmp['close_ema20'].shift(1))) |\
                        ((tmp['close'] > tmp['close_ema200']) & (tmp['close'].shift(1) <= tmp['close_ema200'].shift(1))) |\
                            ((tmp['close'] > tmp['close_ema72']) & (tmp['close'].shift(1) <= tmp['close_ema72'].shift(1))),
                                # ((tmp['macd'] > tmp['macd_signal']) & (tmp['macd'].shift(1) <= tmp['macd_signal'].shift(1))),
        1, 0
    )
    tmp['sell'] = np.where(
        ((tmp['close_ema20'] > tmp['close_ema8']) & (tmp['close_ema20'].shift(1) == tmp['close_ema8'].shift(1))) |\
            ((tmp['close_ema200'] > tmp['close_ema8']) & (tmp['close_ema200'].shift(1) == tmp['close_ema8'].shift(1))) |\
            ((tmp['close_ema200'] > tmp['close_ema20']) & (tmp['close_ema200'].shift(1) == tmp['close_ema20'].shift(1))) |\
                ((tmp['close'] < tmp['close_ema8']) & (tmp['close'].shift(1) >= tmp['close_ema8'].shift(1))) |\
                    ((tmp['close'] < tmp['close_ema20']) & (tmp['close'].shift(1) >= tmp['close_ema20'].shift(1))) |\
                        ((tmp['close'] < tmp['close_ema200']) & (tmp['close'].shift(1) >= tmp['close_ema200'].shift(1))) |\
                            ((tmp['close'] < tmp['close_ema72']) & (tmp['close'].shift(1) >= tmp['close_ema72'].shift(1))),
                                # ((tmp['macd'] < tmp['macd_signal']) & (tmp['macd'].shift(1) >= tmp['macd_signal'].shift(1))),
        1, 0
    )
    return tmp

def crossing_ema_short_term(df):
    return np.where((df['close_ema8'] <= df['close_ema20']) & (df['close_ema8'].shift(-1) > df['close_ema20'].shift(-1)), 1, 0)
    
def crossing_ema_long_term(df):
    return np.where((df['close_ema8'] <= df['close_ema72']) & (df['close_ema8'].shift(-1) > df['close_ema72'].shift(-1)), 1, 0)

def candle_crossing_ema(df, period):
    return np.where(((df['close'] > df[f'close_ema{period}']) & (df['close'].shift(1) <= df[f'close_ema{period}'].shift(1))), 1, 0)

def get_trades_recom(df, qtd_days=8, gain_ratio=2, min_pct_gain=5): # min_pct_gain=5
    tmp = df.sort_values(by=['date']).copy()
    ids = {'buy': [], 'sell': []}
    for i in range(tmp.shape[0] - 1):
        if len(ids['sell']) > 0 and i <= ids['sell'][-1]:
            continue
        b = tmp.iloc[i]
        s = (None, -1)
        for j in range(i + 1, i + qtd_days):
            if j == tmp.shape[0]:
                break
            target = b['close'] + gain_ratio * (b['close'] - b['low'])
            gain = tmp.iloc[j]['close'] - b['close']
            if ((tmp.iloc[j]['close'] >= target) and (tmp.iloc[j]['close'] > s[1])):
                if not min_pct_gain:
                    s = (j, tmp.iloc[j]['close'])
                elif gain >= min_pct_gain/100 * b['close']:
                    s = (j, tmp.iloc[j]['close'])
        if s[0]:
            ids['buy'].append(i)
            ids['sell'].append(s[0])
    trades_buy = tmp.iloc[ids['buy']][['date', 'close', 'low']].reset_index().rename(columns={'date': 'buy_date', 'index': 'original_index', 'low': 'loss'}).reset_index()
    trades_sell = tmp.iloc[ids['sell']][['date', 'close']].reset_index(drop=True).rename(columns={'date': 'sell_date', 'close': 'gain'}).reset_index()
    return trades_buy.merge(trades_sell, on=['index'], how='inner').drop('index', axis=1).rename(columns={'original_index': 'index'})

def normalize_recommendations(df):
    tmp = df.copy()
    tmp['buy'] = np.where(tmp['volume'] < tmp['volume_ema20'], 0, df['buy'])
    tmp['sell'] = np.where(tmp['volume'] < tmp['volume_ema20'], 0, df['sell'])
    return tmp

def flag_volume(df):
    return df.assign(ind_volume=np.where(
        df['volume'].notna() & (df['volume'] < df['volume_ema20']), 
        -1, np.where(df['volume'].notna() & (df['volume'] > df['volume_ema20']), 1, 0))
        )

def candle_trend(df):
    return df.assign(candle_trend=np.where(
        df['close'] < df['open'], 
        -1, 
        np.where(df['close'] == df['open'], 0, 1)
        )
    )

def plot_risk_return(retscomp, path_fig=None, risk_return_period=None):
    plt_ = plt
    plt_.style.use('ggplot')
    f, ax = plt_.subplots(figsize=(22, 12))
    plt_.scatter(retscomp.mean(), retscomp.std())
    if risk_return_period: ax.set_title(f'Risk x Return | {risk_return_period} days')
    plt_.xlabel('Expected returns')
    plt_.ylabel('Risk')
    for label, x, y in zip(retscomp.columns, retscomp.mean(), retscomp.std()): 
        plt_.annotate( 
            label, 
            xy = (x, y), 
            xytext = (20, -20), 
            textcoords = 'offset points', ha = 'right', va = 'bottom', 
            bbox = dict(boxstyle = 'round,pad=0.5', fc = 'yellow', alpha = 0.5), 
            arrowprops = dict(arrowstyle = '->')#, connectionstyle = 'arc3,rad=0')
        )
    if path_fig: plt_.savefig(path_fig, format='svg', dpi=1000)

def get_return_rate_and_risk(df, plot_risk_and_return=True, risk_return_period=None):
    if risk_return_period: 
        df = df[pd.to_datetime(df['date']).dt.date >= (today - datetime.timedelta(risk_return_period)).date()]
    dfcomp = df.pivot(index='date', columns='ticker', values='close').dropna()
    retscomp = dfcomp.pct_change()
    corr = retscomp.corr()
    if plot_risk_and_return: plot_risk_return(retscomp, "risk_return.svg", risk_return_period)
    return retscomp, corr

def create_yahoo_download_query(code, period1=946695600, period2=None, interval_days=1, events='history'):
    now = datetime.datetime.today()
    if not period2: period2 = int(datetime.datetime(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute, second=now.second, microsecond=now.microsecond).timestamp())
    # if _DEBUG: print(f'Period1: {datetime.datetime.fromtimestamp(period1)} | Period2: {datetime.datetime.fromtimestamp(period2)}')
    return f'{_YAHOO_API_URL}{code}.SA?period1={period1}&period2={period2}&interval={str(interval_days)}d&events={events}'

def get_yahoo_finance(code, interval_days=1, period1=946695600, period2=None, events='history', ttl=3):
    df = pd.DataFrame()
    if not isinstance(code, list): code = [code]
    success = []
    errors = []
    for ticker in code:
        url = create_yahoo_download_query(code=ticker, period1=period1, period2=period2, interval_days=interval_days, events=events)
        try:
            tmp = pd.read_csv(url).assign(ticker=ticker)
            tmp = tmp.rename(columns={x: x.lower().replace(' ', '_') for x in tmp.columns})[_TIMESERIE_DF_COLUMNS]
            tmp = tmp[tmp != 'null'].dropna()
            for col in _TIMESERIE_DF_COLUMNS:
                tmp[col] = tmp[col].astype(float, errors='ignore')
            df = df.append(tmp, ignore_index=True)
            success.append(ticker)
        except Exception as exp:
            errors.append(ticker)
            print(f'{_NOW()} [{ticker}] Error: {exp}')
    # print(f'Downloads - Success: {count_success} | Error {count_error}')
    return df

def daily_analysis_yfinance(ticker=None, write_path=None, hist_path=_hist_path, get_recom=True, tickers=set(_CODES + another_codes), norm_rec=False, interval_days=1, period1=946695600, period2=None, qtd_days=None, plot_risk_and_return=False, risk_return_period=365):
    codes = [x for x in tickers if not x.endswith('F')] if not ticker else [ticker]
    now = datetime.datetime.today()
    df = pd.DataFrame()
    hist = pd.DataFrame()
    print(f"Baixando dados para os últimos {qtd_days if qtd_days else '<todo o período>'} dias...")
    if hist_path and os.path.exists(hist_path): 
        hist = pd.read_csv(hist_path)[_TIMESERIE_DF_COLUMNS]
    if qtd_days: period1 = int((datetime.datetime(year=now.year, month=now.month, day=now.day, hour=1) - datetime.timedelta(days=qtd_days)).timestamp())

    for ticker_code in tqdm(codes):
        tmp = get_yahoo_finance(code=ticker_code, interval_days=interval_days, period1=period1, period2=period2)
        if tmp.shape[0] > 1:
            tmp = tmp.rename(columns={'code': 'ticker'})
            if qtd_days and os.path.exists(hist_path): tmp = tmp.append(hist[list(tmp.columns)][hist['ticker'] == ticker_code], ignore_index=True).drop_duplicates(subset=['date', 'ticker'])
            tmp = create_ema(tmp.drop_duplicates(['date', 'ticker']))
            tmp = flag_volume(tmp)
            tmp['macd'] = macd(fast_ma=tmp['close_ema8'], slow_ma=tmp['close_ema20']).round(2)
            tmp['macd_signal'] = ema(serie=tmp['macd'], period=8)
            tmp = get_signals(tmp)
            if write_path and not qtd_days:
                tmp.to_csv(f"{write_path + ticker_code}.csv.zip", index=False, compression='zip')
            df = df.append(tmp, ignore_index=True)
    # # get and plot risk and return of main tickers codes
    # get_return_rate_and_risk(df[df['ticker'].isin(_CODES)], plot_risk_and_return=plot_risk_and_return, risk_return_period=risk_return_period)
    display.display(df)
    # get recommendations
    df_recom = pd.DataFrame()
    if get_recom and not df.empty:
        # df = candle_type(df) # get candle types
        df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
        df_recom = df[(df['buy'] == 1) | (df['sell'] == 1) | (df['close'] > df['high'].shift(1))].sort_values(by=['date'], ascending=False).drop_duplicates(subset=['ticker'])
        query = "((buy == 1) | (sell == 1)) & ((volume > 0) & (close < 100))"
        if norm_rec: df_recom = normalize_recommendations(df_recom).query(query)
        df_recom['volume_ema20'] = df_recom['volume_ema20'].round(2)
        df_recom = df_recom.drop(['high', 'low', 'adj_close', 'volume'], axis=1).sort_values(by=['ticker'])
        df_recom = df_recom[df_recom['ticker'].isin(_CODES)].sort_values(by=['date'], ascending=False).append(df_recom[~df_recom['ticker'].isin(_CODES)], ignore_index=True)
        # df_recom.to_html(f'reports/yfinance/recommendations_yfinance.html', index=False)
        # df_recom.to_csv('data/recommendations_yfinance.csv.zip', index=False, compression='zip')

        df = df.assign(
            **{
                'candle_crossing_ema20': candle_crossing_ema(df, period=20),
                'crossing_8ema_x_20ema': np.where((df['close_ema8'] >= df['close_ema20']) & (df['close_ema8'].shift(1) < df['close_ema20'].shift(1)), 1, 0),
                'crossing_8ema_x_72ema': np.where((df['close_ema8'] >= df['close_ema72']) & (df['close_ema8'].shift(1) < df['close_ema72'].shift(1)), 1, 0),
                'crossing_20ema_x_72ema': np.where((df['close_ema20'] >= df['close_ema72']) & (df['close_ema20'].shift(1) < df['close_ema72'].shift(1)), 1, 0),
                'trend_tomorrow': np.where((df['close'].shift(-1).notna()) & (df['close'] < df['close'].shift(-1)), 1, 0)
            }
        )
        
        tmp = df_recom[df_recom['date'].dt.date == min(today.date(), df['date'].max())]
        display.display(tmp)
        if tmp.shape[0] == 0:
            tmp = candle_trend(df[df['date'].dt.date == min(today.date(), df['date'].max())])[['date', 'buy', 'ticker', 'close', 'candle_trend']] #, 'candle_type']]
            print("Filtro por data atual:", tmp.shape)
            tmp = tmp[tmp['ticker'].isin(_CODES)]
            print("Filtro por códigos principais:", tmp.shape)
        tmp = tmp[tmp['ticker'].isin(_CODES)].sort_values(by=['close']).reset_index(drop=True)
        # tmp.to_html(f'reports/recommendations_yfinance_today.html')#, index=False)
        # print(f'Recommendations saved in: recommendations_yfinance.html')
        df_recom = tmp[tmp['buy'] == 1][['date', 'ticker', 'close', 'buy']]
    if not df.empty and (hist.empty or hist[pd.to_datetime(hist['date']).dt.date == (today - datetime.timedelta(1)).date()].shape[0] == 0):
        if not hist.empty: print(hist['date'].max())
        tmp_hist = df[df['date'] != today.date()].sort_values(['ticker', 'date'])
        print(f'A data {(today - datetime.timedelta(1)).date()} não está na base\nEscrevendo em {hist_path} sem a data {today.date()}: {tmp_hist.shape}')
        tmp_hist.to_csv(hist_path, index=False)
    return df, df_recom

def get_and_agg_stock_prices(write_path=None, by=['d'], tickers=set(_CODES + another_codes), days=0, start_date=None, source='b3'):
    dfs = {'m': pd.DataFrame(), 'h': pd.DataFrame(), 'd': pd.DataFrame()}
    # tmp = []
    # l_ = pool.map(get_stock_prices, tickers)

    # Alterar isso para baixar todo o dataframe da b3
    tmp = get_stock_prices(tickers=tickers, days=days, start_date=start_date, source=source)
    # tmp = pool.map(get_stock_prices, [x)
    if not tmp.empty:
        df = extract_minute_b3hist(tmp)
        del(tmp)
        for b in by:
            if b == 'd':
                df_agg = aggregate_b3_hist(df, by='date', limit_time=limit_time)
                tmp = get_candle_variables(df, by='date')
                tmp = tmp.merge(df_agg, on=['date', 'ticker'], how='inner')
                t = 60 * 24
            else:
                if b == 'h':
                    t = 60
                elif type(b) == int:
                    t = b
                else:
                    t = 1
                # t = 1 if b == 'm' else 60
                # tmp = df.copy()
                tmp = df.assign(minute=df['minute'].apply(m2m, args=(t,)))
                # tmp['minute'] = tmp['minute'].apply(m2m, args=(t,))
                df_agg = aggregate_b3_hist(tmp, limit_time=limit_time)
                tmp = get_candle_variables(tmp, by='minute')
                tmp = tmp.merge(df_agg, on=['date', 'ticker'], how='inner')
            if not write_path: 
                dfs[b] = dfs[b].append(tmp, ignore_index=True)
            else:
                tmp.to_csv(f"{write_path + x}_agg{t}m.csv.zip", index=False, compression='zip')
                del(tmp)
                # dfs = {'m': pd.DataFrame(), 'h': pd.DataFrame(), 'd': pd.DataFrame()}
    if write_path: print("Arquivos salvos em:", write_path)
    return dfs
