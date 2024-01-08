import pandas as pd
import numpy as np
import datetime
import pytz
from tqdm import tqdm
from IPython import display
import requests
import json
import time
import os
from lib.technical_analysis import (
    _CODES, get_yahoo_finance, _INTERVAL_DAYS, _PERIOD1, _PERIOD2, create_ema, flag_volume,
    macd, ema, get_signals
)



################### Criação de Funções

def calc_erro(df, columns, n_folds=10):
    erros = []
    tmp = df[df['y'].notna()]
    for k in range(n_folds):
        train = tmp.sample(frac=0.7)[columns + ['y']]
        test = tmp.drop(train.index)[columns + ['y']]

        if k == n_folds - 1: print(f"\tcalc_erro() | K = {k + 1}/{n_folds} | Train: {train.shape[0]} | Test: {test.shape[0]}")
        wknn = WKNN(train)

        r = wknn.predict(test, debug=False)[['y', 'y_predict']]
        r = r.assign(erro=r['y_predict'] - r['y'])
        erro_medio = np.where(r['erro'].isna(), 0, r['erro']).mean()

        erros.append(erro_medio)
    print('')
    return np.array(erros).mean()


################### Criação de indicadores
dfs = []
for ticker in tqdm(set(_CODES)):
    tmp = get_yahoo_finance(tickers=ticker, interval_days=_INTERVAL_DAYS, period1=_PERIOD1, period2=_PERIOD2)
    if tmp.shape[0] > 1:
        tmp = tmp.rename(columns={'code': 'ticker'})
        # if _QTD_DAYS and os.path.exists(hist_path): tmp = tmp.append(hist[list(tmp.columns)][hist['ticker'] == ticker], ignore_index=True).drop_duplicates(subset=['date', 'ticker'])
        tmp = create_ema(tmp.drop_duplicates(['date', 'ticker']))
        tmp = flag_volume(tmp)
        tmp['macd'] = macd(fast_ma=tmp['close_ema8'], slow_ma=tmp['close_ema20']).round(2)
        tmp['macd_signal'] = ema(serie=tmp['macd'], period=8)
        tmp = get_signals(tmp)
        # if write_path and not qtd_days:
        #     tmp.to_csv(f"{write_path + ticker}.csv.zip", index=False, compression='zip')
        dfs.append(tmp)

# Une os DataFrames
df = pd.concat(dfs, ignore_index=True)




# Definição de constantes
n_prev = 60 # 60
rolling_window_max_close = 15
if datetime.datetime.now(tz=pytz.timezone('America/Sao_Paulo')).hour < 16:
    # Caso seja menos de 16 horas são considerados os dados do dia anterior
    reference_date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') # '2024-01-03'
else:
    # Caso seja mais de 16 horas são considerados os dados do dia atual
    reference_date = datetime.datetime.today().strftime('%Y-%m-%d')

columns_ref = [
    'open', 'high', 'low', 'close', 'volume', 'close_ema8', 'close_ema20', 'close_ema72', 'close_ema200',
    'volume_ema20', 'macd',
]

columns = [f"{x}_t-{i}" for i in range(n_prev, 0, -1) for x in columns_ref] + columns_ref

results = dict.fromkeys(df['ticker'].unique()) # {f"{x}": [] for x in df['ticker'].unique()}
erros = []
menor_erro = {'ticker': None, 'erro': 999999999}

time_start = _NOW()
for t, ticker in enumerate(df['ticker'].unique()):
    ts = _NOW()
    # ticker_test = 'GOLL4'
    tmp = df.query(f"ticker == '{ticker}'")
    # buy_candles = get_trades_recom(tmp).assign(ticker=ticker)
    # buy_dates = buy_candles['buy_date']
    # df_buy = tmp[tmp['date'].isin(buy_dates)]
    # df_sell = tmp[~tmp['date'].isin(buy_dates)]
    # df_trades_recom = pd.concat([df_buy.assign(buy=1), df_sell.assign(buy=0)])

    # Transforma uma serie temporal em dados supervisionados de acordo com regra de negócio definida
    df_supervised = series_to_supervised(
        tmp,
        n_prev=n_prev
    ).sort_values(
        by=['date']
    )

    # TODO ajustar período que deve ser considerado para treinamento
    # Mantém apenas os utimos registros ordenados por data
    df_supervised = df_supervised.sort_values('date').tail(n_prev)

    # Obtém o valor máximo nos próximos X rolling_window_max_close
    # df_supervised[f'max_close_{rolling_window_max_close}_days'] = df_supervised.sort_values(
    statistics = {'avg': [], 'max': [], 'prop_gain': []}
    for i in range(df_supervised.shape[0]):
        # Calculando o máximo
        statistics['max'].append(df_supervised['close'].iloc[i + 1:i + rolling_window_max_close + 1].max())
        # Calculando a média
        statistics['avg'].append(df_supervised['close'].iloc[i + 1:i + rolling_window_max_close + 1].mean())
        # Calculando o percentual de ganho no valor máximo atingido em X dias
        statistics['prop_gain'].append((df_supervised['close'].iloc[i + 1:i + rolling_window_max_close + 1].max() - df_supervised['close'].iloc[i]) / df_supervised['close'].iloc[i] * 100)

    # Constroi as colunas com média ou máximo em um período determinado
    df_supervised = df_supervised.assign(
        **{
            f'max_close_{rolling_window_max_close}_days': statistics['max'],
            f'avg_close_{rolling_window_max_close}_days': statistics['avg'],
            f'gain_close_{rolling_window_max_close}_days': statistics['prop_gain'],
        }
    )

    df_supervised = df_supervised.rename(columns={f'max_close_{rolling_window_max_close}_days': 'y'})
    # df_supervised = df_supervised.rename(columns={f'gain_close_{rolling_window_max_close}_days': 'y'})
    # df_supervised = df_supervised.rename(columns={f'avg_close_{rolling_window_max_close}_days': 'y'})

    train_data = df_supervised[df_supervised['y'].notna()].query(f"date < '{reference_date}'")[columns + ['y']]
    input_data = df_supervised.query(f"date == '{reference_date}'")[columns + ['y']]
    # print(train_data.shape, input_data.shape)
    wknn = WKNN(train_data) # f'max_close_{rolling_window_max_close}_days'

    # Realiza a predição
    r = wknn.predict(input_data)[columns_ref + ['y', 'y_predict']]
    # Faz join com o dataset de referencia para obter a data e ticker
    r = df_supervised[['date', 'ticker']].reset_index().merge(
        r.assign(erro=r['y_predict'] - r['y']).reset_index(),
        on='index',
        how='inner'
    ).drop('index', axis=1)

    # Salva os resultados
    results[ticker] = r

    print(f"{t + 1}/{len(df['ticker'].unique())} [{ticker}] | Reference date: {reference_date} | Elapsed time: {datetime.datetime.now() - ts} | Remaining time: {(_NOW() - time_start) / (t + 1) * (len(df['ticker'].unique()) - (t + 1))}")

    # Calcula o erro médio em K-folds
    erro_medio = calc_erro(df_supervised, columns)

    if abs(erro_medio) < abs(menor_erro['erro']):
        menor_erro['erro'] = erro_medio
        menor_erro['ticker'] = ticker

    print(f"Erro médio [{ticker}]: {erro_medio}")

    # Lista com os erros médios
    erros.append((ticker, erro_medio, ))

    print('')
print(f"Menor erro: {menor_erro}")





###########
df_results = pd.concat([results[x] for x in results.keys()])
df_results = df_results.assign(
    **{
        'diff': df_results['y_predict'] - df_results['close'],
        'prop_gain': (df_results['y_predict'] - df_results['close']) / df_results['close']
    }
)

# df_results.query(f"ticker == \"{menor_erro['ticker']}\"")

######### Erro médio
for x in erros:
    df_results.loc[df_results['ticker'] == x[0], 'erro_medio'] = abs(x[1])



######### Salva os dados
# Escreve os dados em um CSV
df_results.sort_values(by=['date', 'prop_gain'], ascending=[False, False]).to_csv('/tmp/predicoes_acoes.csv', index=False)
