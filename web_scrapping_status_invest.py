import os
import time
import json
import pandas as pd
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service 
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# # https://selenium-python.readthedocs.io/installation.html#drivers
# # Drivers: https://googlechromelabs.github.io/chrome-for-testing/
# # https://storage.googleapis.com/chrome-for-testing-public/122.0.6261.128/win64/chromedriver-win64.zip

# # Instalando o driver
# servico = Service(ChromeDriverManager().install())

caminho_chromedriver = 'chromedriver.exe'
# Crie uma instância do Service
servico = webdriver.chrome.service.Service(caminho_chromedriver)

#Abrindo o navegador
navegador = webdriver.Chrome(service=servico)

_FUNDOS = [
    "MXRF11", "HGLG11", "KNRI11", "KNCR11", "XPML11"
]

_ACOES = [
    'BBSE3', 'ITUB4', 'CSNA3', 'GGBR4', 'CMIG4', 'BBDC3',
    'ITSA4', 'USIM5', 'CXSE3', 'KLBN4', 'TAEE4', 'SAPR4',
    'PETR4', 'CMIG3', 'BBAS3'
]

_URL = f"https://statusinvest.com.br"
_PATH_FUNDOS = f"/fundos-imobiliarios"
_PATH_ACOES = f"/acoes"

_XPATHS = {
    x: {
    'segmento': '//*[@id="fund-section"]/div/div/div[4]/div/div[1]/div/div/div/a/strong'
        if x in _FUNDOS else '//*[@id="company-section"]/div[1]/div/div[3]/div/div[3]/div/div/div/a/strong',
    'tipo_anbima': '//*[@id="fund-section"]/div/div/div[2]/div/div[5]/div/div/div/strong'
        if x in _FUNDOS else None,
    'valor_atual': '//*[@id="main-2"]/div[2]/div[1]/div[1]/div/div[1]/strong'
        if x in _FUNDOS else '//*[@id="main-2"]/div[2]/div/div[1]/div/div[1]/div/div[1]/strong',
    'dividend_yield': '//*[@id="main-2"]/div[2]/div[1]/div[4]/div/div[1]/strong'
        if x in _FUNDOS else '//*[@id="main-2"]/div[2]/div/div[1]/div/div[4]/div/div[1]/strong',
    'pvp_acima_de_1_valorizado': '//*[@id="main-2"]/div[2]/div[5]/div/div[2]/div/div[1]/strong'
        if x in _FUNDOS else '//*[@id="indicators-section"]/div[2]/div/div[1]/div/div[4]/div/div/strong',
    'pl_anos_retorno_investimento': '//*[@id="indicators-section"]/div[2]/div/div[1]/div/div[2]/div/div/strong'
        if x in _ACOES else None,
    'divida_liquida': '//*[@id="indicators-section"]/div[2]/div/div[2]/div/div[1]/div/div/strong'
        if x in _ACOES else None,
    'patrimonio_liquido': '//*[@id="company-section"]/div[1]/div/div[2]/div[1]/div/div/strong'
        if x in _ACOES else None,
    'valor_de_mercado': '//*[@id="company-section"]/div[1]/div/div[2]/div[7]/div/div/strong'
        if x in _ACOES else None,
    'roe': '//*[@id="indicators-section"]/div[2]/div/div[4]/div/div[1]/div/div/strong'
        if x in _ACOES else None,
    'numero_de_acoes': '//*[@id="company-section"]/div[1]/div/div[2]/div[9]/div/div/strong'
        if x in _ACOES else None,
    'amount_dividendos_12meses': '//*[@id="main-2"]/div[2]/div/div[1]/div/div[4]/div/div[2]/div/span[2]'
        if x in _ACOES else '//*[@id="main-2"]/div[2]/div[1]/div[4]/div/div[2]/div/span[2]'
    } for x in _FUNDOS + _ACOES
}

indicadores = {
    "ticker": [],
    "segmento": [],
    "tipo_anbima": [],
    "valor_atual": [],
    # "variacao_valor_atual": [],
    "dividend_yield": [], # Percentual pago de dividendos comparado ao valor atual da ação
    "pvp_acima_de_1_valorizado": [], # P/VP (Preço sobre Valor Patrimonial) indica que as ações podem estar "caras" ou "baratas"
    "pl_anos_retorno_investimento": [], # PL indica o tempo (em anos) para reaver o valor investido (ideal: menor ou igual a 10)
    "divida_liquida": [],
    'patrimonio_liquido': [],
    'preco_teto': [], # Preço máximo para obter pelo menos 6% de dividendos a.a
    'pvp_status': [], # Verifica o status do PVP (`<= 1`: barata; `> 1` e `<= 1.5`: levemente_alta; `> 1.5`: alta)
    'valor_de_mercado': [],
    'numero_de_acoes': [],
    'roe': [], # (Retorno sobre o Patrimonio Líquido) Indica quanto a empresa obtém de retorno com os investimentos dos recursos dela (ideal: acima de 10%)
}

for ticker in tqdm(_ACOES + _FUNDOS):
    # Criando url
    url = f"{_URL}{_PATH_FUNDOS if ticker in _FUNDOS else _PATH_ACOES}/{ticker.lower()}"

    try:
        # Abrindo o navegador
        navegador.get(url)

        time.sleep(1)

        # Coletando dados
        indicadores["ticker"].append(ticker)
        if _XPATHS[ticker]['segmento']:
            indicadores["segmento"].append(
                navegador.find_element(By.XPATH, _XPATHS[ticker]['segmento']).text
            )
        else:
            indicadores["segmento"].append(None)

        if _XPATHS[ticker]['tipo_anbima']:
            indicadores["tipo_anbima"].append(
                navegador.find_element(By.XPATH, _XPATHS[ticker]['tipo_anbima']).text
            )
        else:
            indicadores["tipo_anbima"].append(None)

        if _XPATHS[ticker]['valor_atual']:
            indicadores["valor_atual"].append(
                navegador.find_element(By.XPATH, _XPATHS[ticker]['valor_atual']).text.replace(',', '.')
            )
        else:
            indicadores["valor_atual"].append(None)

        if _XPATHS[ticker]['dividend_yield']:
            indicadores["dividend_yield"].append(
                round(
                    float(navegador.find_element(By.XPATH, _XPATHS[ticker]['dividend_yield']).text.replace(',', '.').replace('%', ''))
                    / 100, # Transforma em proporção
                    2
                )
            )
        else:
            indicadores["dividend_yield"].append(None)

        if _XPATHS[ticker]['pvp_acima_de_1_valorizado']:
            indicadores["pvp_acima_de_1_valorizado"].append(
                navegador.find_element(By.XPATH, _XPATHS[ticker]['pvp_acima_de_1_valorizado']).text.replace(',', '.')
            )
        else:
            indicadores["pvp_acima_de_1_valorizado"].append(None)

        if _XPATHS[ticker]['pl_anos_retorno_investimento']:
            indicadores['pl_anos_retorno_investimento'].append(
                navegador.find_element(By.XPATH, _XPATHS[ticker]['pl_anos_retorno_investimento']).text.replace(',', '.')
            )
        else:
            indicadores['pl_anos_retorno_investimento'].append(None)

        if _XPATHS[ticker]['divida_liquida']:
            indicadores["divida_liquida"].append(
                navegador.find_element(By.XPATH, _XPATHS[ticker]['divida_liquida']).text.replace('.', '').replace(',', '.')
            )
        else:
            indicadores["divida_liquida"].append(None)

        if _XPATHS[ticker]['patrimonio_liquido']:
            indicadores["patrimonio_liquido"].append(
                navegador.find_element(By.XPATH, _XPATHS[ticker]['patrimonio_liquido']).text.replace('.', '').replace(',', '.')
            )
        else:
            indicadores["patrimonio_liquido"].append(None)

        if _XPATHS[ticker]['valor_de_mercado']:
            indicadores["valor_de_mercado"].append(
                navegador.find_element(By.XPATH, _XPATHS[ticker]['valor_de_mercado']).text.replace('.', '').replace(',', '.')
            )
        else:
            indicadores["valor_de_mercado"].append(None)

        if _XPATHS[ticker]['numero_de_acoes']:
            indicadores["numero_de_acoes"].append(
                navegador.find_element(By.XPATH, _XPATHS[ticker]['numero_de_acoes']).text.replace('.', '').replace(',', '.')
            )
        else:
            indicadores["numero_de_acoes"].append(None)

        if _XPATHS[ticker]['roe']:
            indicadores["roe"].append(
                round(
                    float(navegador.find_element(By.XPATH, _XPATHS[ticker]['roe']).text.replace(',', '.').replace('%', ''))
                    / 100, # Transforma em proporção
                    2
                )
            )
        else:
            indicadores["roe"].append(None)

        if _XPATHS[ticker]['amount_dividendos_12meses']:
            indicadores['preco_teto'].append(
                round(
                    float(
                    navegador.find_element(By.XPATH, _XPATHS[ticker]['amount_dividendos_12meses']).text.replace(',', '.').replace('R$ ', '')
                    ) / 0.06,
                    2
                )
            )
        else:
            indicadores['preco_teto'].append(None)

        if _XPATHS[ticker]['pvp_acima_de_1_valorizado']:
            status = 'CARO'
            if float(indicadores["pvp_acima_de_1_valorizado"][-1]) <= 1:
                status = 'BARATO'
            elif float(indicadores["pvp_acima_de_1_valorizado"][-1]) <= 1.5:
                status = 'LEVEMENTE CARO'
            indicadores['pvp_status'].append(status)
        else:
            indicadores['pvp_status'].append(None)

    except Exception as exc:
        print(exc)
        navegador.close()
        navegador.quit()

# Fecha o navegador
navegador.quit()

df = pd.DataFrame(indicadores)
df.head(15)

# Write
df.to_csv('indicadores-status-invest.csv', index=False)
