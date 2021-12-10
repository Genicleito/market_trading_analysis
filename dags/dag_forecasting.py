import os, sys, inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, currentdir)
sys.path.insert(0, parentdir)
home_path = parentdir
sys.path.insert(0, f'{home_path}/lib')

# Airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.dates import days_ago

# ML
from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics

# Visualization
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# Other modules
from functools import reduce
import pickle, os, re
import logging
import datetime

# Data transformation
import pandas as pd
import numpy as np

# Personal modules
import technical_analysis

root_path = f'{home_path}/trading_forecasting'
data_path = f'{root_path}/data'
assets_path = f'{data_path}/assets/yfinance'
ml_path = f'{root_path}/ml'
models_path = f'{ml_path}/models'
random_forest_regression_path = f'{models_path}/random_forest_regression'
predicted_path = f'{data_path}/predicted'
history_path = f'{data_path}/hist_market_trading.csv.zip'
series_to_supervised_path = f'{data_path}/series_market_to_supervisioned.csv.zip'
prediction_history_path = f'{predicted_path}/history_prediction.csv.zip'
# Constantes
yahoo_api_url = 'https://query1.finance.yahoo.com/v7/finance/download/'
now = lambda: datetime.datetime.now()
SELECT_SERIES_TO_SUPERVISIONED_COLUMNS = [
    'date', 'ticker', 'open', 'close', 'low', 'high', 'volume', 'close_ema8', 'close_ema20', 'volume_ema20', 'close_ema72',
    'close_ema200', 'ind_volume', 'macd', 'macd_signal', 'buy'
]
PREDICT_COLUMN = 'close_t+1'
# main_assets = ['ABEV3', 'IRBR3', 'MGLU3', 'VIIA3', 'GOLL4', 'VALE3', 'PETR4']
assets = technical_analysis.get_all_tickers()

# Cria caminhos
os.system(f'mkdir -p {data_path} {ml_path} {models_path} {assets_path} {predicted_path} {random_forest_regression_path}')

args = {
    'owner': 'geni', # or 'admin'
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': 1
}
dag = DAG(
    dag_id='get_tickers_yfinance',
    default_args=args,
    description='DAG para obter e transformar dados de ativos do Yahoo Finance', 
    schedule_interval='0 6-23 * * 1-5',
    start_date=datetime.datetime.now() # days_ago(1)
)

def update_assets(ticker, write_path):
    ts = now()
    if not isinstance(ticker, list): ticker = [ticker]
    errors = []
    for i, t in enumerate(ticker):
        try:
            url = technical_analysis.create_yahoo_download_query(t)
            df = pd.read_csv(url).assign(ticker=t)
            df = (df[df != 'null']
                .dropna()
                .drop('adj_close', axis=1, errors='ignore')
                .rename(columns={x: x.lower().replace(' ', '_').strip() for x in df.columns})
                # .rename(columns={'code': 'ticker'})
                .astype(float, errors='ignore')
                .drop_duplicates(['date', 'ticker'])
            )[technical_analysis.default_yahoo_df_columns]
            df['pct_change'] = ((df['close'] - df['close'].shift(1)) / df['close'].shift(1)).fillna(value=0)
            df = technical_analysis.create_ema(df)
            df = technical_analysis.flag_volume(df)
            df['macd'] = technical_analysis.macd(fast_ma=df['close_ema8'], slow_ma=df['close_ema20']).round(2)
            df['macd_signal'] = technical_analysis.ema(serie=df['macd'], period=8)
            df = technical_analysis.get_signals(df)
            # Write file
            if df.shape[0] > 1: df.to_csv(f'{write_path}/{t}.csv.zip', index=False)
            # Debug
            print(f'{i+1}/{len(ticker)}: {t} -> {df.shape} | {now() - ts}')
        except Exception as e:
            logging.error(f'{i+1}/{len(ticker)} {t} | ERROR: {e}')
            errors.append(t)
    if len(ticker) - len(errors) == 0: raise ValueError(' Nenhum ativo foi atualizado!')
    print(f'{len(ticker) - len(errors)} assets atualizados com sucesso!')

def write_market_history(write_path):
    df = pd.concat([pd.read_csv(f'{assets_path}/{x}') for x in os.listdir(assets_path)], ignore_index=True)
    df.sort_values(['date', 'ticker']).to_csv(write_path, index=False)
    logging.info(f"\n{df.groupby('ticker').mean().sort_values('pct_change', ascending=False).head(5).T.to_markdown()}")
    logging.info(f"\n{df.describe().T.to_markdown()}")

def series_to_supervised(read_path, write_path, columns=None, n_prev=3, cols_fut={'close': 1}, partition_by='ticker', dropna=False):
    if not columns: columns = SELECT_SERIES_TO_SUPERVISIONED_COLUMNS
    df = pd.read_csv(read_path)
    df = technical_analysis.series_to_supervised(
        df[columns].sort_values(['date', 'ticker']),
        partition_by=partition_by,
        n_prev=n_prev,
        cols_fut=cols_fut,
        dropna=dropna
    ).reset_index(drop=True)
    df = technical_analysis.drop_columns(df, pattern='date_t-|ticker_t-')
    df.to_csv(write_path, index=False)
    logging.info(df.describe().T.to_markdown())

def train_model(read_path, model_path, write_path, predict_column, n_estimators=1000):
    # Paths to save the models in disk
    if not os.path.exists(root_path): os.mkdir(root_path)
    df_supervised = pd.read_csv(read_path).dropna()
    tickers = sorted(df_supervised['ticker'].unique())
    models = dict.fromkeys(tickers)
    dfs_ml = [df_supervised.query(f'ticker == "{x}"') for x in tickers]
    for i, ticker in enumerate(sorted(tickers)):
        print(f"{i+1}/{len(models.keys())}: {ticker}")
        if os.path.exists(f"{write_path}/assets/{ticker}.csv.zip"): continue
        tmp = technical_analysis.drop_columns(dfs_ml[i], pattern='date(_|$)|ticker(_|$)')
        train, test = technical_analysis.split_random_train_test(tmp, debug=False)
        X = train.drop(predict_column, axis=1)
        y = train[predict_column]
        # Cria o modelo
        regr = RandomForestRegressor(n_estimators=n_estimators, max_depth=None, random_state=None, n_jobs=3) #, verbose=1)
        # Treina o modelo
        regr.fit(X, y)
        # Predição do split de teste (para validação do modelo)
        r = regr.predict(test.drop(predict_column, axis=1))
        # Salva o modelo para esse ticker
        models[ticker] = regr
        model_name = f'{model_path}/{ticker}_rfr.sav'
        # Salva o modelo
        pickle.dump(models[ticker], open(model_name, 'wb'))
        # Recria o DataFrame com a coluna de predição
        result = (test[['close', predict_column]]
            .assign(**{
                'pred': r, # Coluna de predição
                'date': dfs_ml[i]['date'].loc[test.index],
                'ticker': dfs_ml[i]['ticker'].loc[test.index],
            })
        )
        mse = metrics.mean_squared_error(
            y_true=result[predict_column],
            y_pred=result['pred']
        )
        result['mse'] = mse
        # Write partial result in temp path (slower but less memory overhead when data volume is large)
        result.to_csv(f"{write_path}/assets/{ticker}.csv.zip", index=False)

def predict(history_path, models_path, write_path, columns=None, n_prev=3, cols_fut={'close': 1}, partition_by='ticker'):
    # write_path = f'{predicted_path}/history_prediction.csv.zip'
    if not columns: columns = SELECT_SERIES_TO_SUPERVISIONED_COLUMNS
    df = technical_analysis.series_to_supervised(
        pd.read_csv(history_path)[columns].sort_values(['date', 'ticker']),
        partition_by=partition_by,
        n_prev=n_prev,
        cols_fut=cols_fut,
        dropna=False
    )
    
    df = (
        technical_analysis.drop_columns(df, pattern='date_t-|ticker_t-')
        .sort_values(['date', 'ticker'], ascending=False)
        .drop_duplicates('ticker')
        .dropna(subset=['ticker'])
    )
    d = dict.fromkeys(df['ticker'].unique())
    forecasting = []
    for ticker in d.keys():
        filename = f'{models_path}/{ticker}_rfr.sav'
        try:
            m = pickle.load(open(filename, 'rb'))
        except Exception as e:
            logging.info(f'[{ticker}] Error: {e}')
            continue
        tmp = (df
            .query(f'ticker == "{ticker}"')
            .drop(['ticker', 'close_t+1', 'date', 'buy'], axis=1, errors='ignore')
        )
        r = m.predict(tmp)
        forecasting.append(
            tmp.assign(**{
                'pred_close_t+1': r,
                'date': df.loc[tmp.index]['date'],
                'ticker': df.loc[tmp.index]['ticker'],
            })
        )
    forecasting = pd.concat(forecasting, ignore_index=True)
    if os.path.exists(write_path):
        (pd.read_csv(write_path)
            .append(forecasting, ignore_index=True)
            .assign(updated_at=now())
        ).drop_duplicates(['ticker', 'date']).to_csv(write_path, index=False)
    else:
        forecasting.assign(updated_at=now()).to_csv(write_path, index=False)

def create_dummy_task(dag, task_id='dummy_task'):
    return DummyOperator(dag=dag, task_id=task_id)

start_task = create_dummy_task(dag, task_id='start_task')
end_task = create_dummy_task(dag, task_id='end_task')

update_assets_task = PythonOperator(
    dag=dag,
    task_id=f'update_assets',
    python_callable=update_assets,
    op_kwargs={'ticker': assets, 'write_path': f'{assets_path}/'}
)

market_history_task = PythonOperator(
    dag=dag,
    task_id=f'build_market_history',
    python_callable=write_market_history,
    op_kwargs={'write_path': history_path}
)

series_to_supervised_task = PythonOperator(
    dag=dag,
    task_id=f'series_to_supervised',
    python_callable=series_to_supervised,
    op_kwargs={'read_path': history_path, 'write_path': series_to_supervised_path}
)

if now().hour >= 19:
    train_model_task = PythonOperator(
        dag=dag,
        task_id=f'train_model',
        python_callable=train_model,
        op_kwargs={'read_path': series_to_supervised_path, 'model_path': random_forest_regression_path, 'write_path': predicted_path, 'predict_column': PREDICT_COLUMN}
    )
    predict_task = PythonOperator(
        dag=dag,
        task_id=f'predict',
        python_callable=predict,
        op_kwargs={'history_path': history_path, 'models_path': random_forest_regression_path, 'write_path': prediction_history_path}
    )
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda dt, p: 'train_model' if (os.path.exists(p) and dt.weekday() in [5, 6]) else 'predict',
        op_kwargs={'dt': now(), 'p': prediction_history_path}
    )
    series_to_supervised_task >> branching
    branching >> Label('Treinamento do modelo') >> train_model_task
    branching >> Label('Predição') >> predict_task
    [train_model_task, predict_task] >> end_task
else:
    series_to_supervised_task >> end_task

start_task >> update_assets_task >> market_history_task >> series_to_supervised_task
