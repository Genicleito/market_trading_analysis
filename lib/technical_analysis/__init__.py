from multiprocessing.dummy import Pool as ThreadPool
import sklearn
import matplotlib.pyplot as plt
import requests, json, datetime, math
import pandas as pd
import numpy as np
from tqdm import tqdm
from IPython import display
import schedule
import time
import os
import math
# http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/cotacoes/cotacoes/

today = datetime.datetime.today()
limit_time = 1730
pool = ThreadPool(4)
yahoo_api_path = 'https://query1.finance.yahoo.com/v7/finance/download/'
default_yahoo_df_columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume']

_PERIOD1 = 946695600 # 2000-01-01
_PERIOD2 = int(datetime.datetime(year=today.year, month=today.month, day=today.day, hour=today.hour, minute=today.minute, second=today.second, microsecond=today.microsecond).timestamp())

def updated_period2_to_now():
    now = datetime.datetime.today()
    return int(datetime.datetime(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute, second=now.second, microsecond=now.microsecond).timestamp())

main_codes = [
    'CEAB3', 'OIBR3', 'EMBR3', 'VALE3', 'GOLL4', 'COGN3', 'IRBR3', 'HGTX3', 'ABEV3', 'BBDC4', 'VULC3', 'SUZB3', 
    'ALSO3', 'AZUL4', 'QUAL3', 'CNTO3', 'SEER3', 'DMMO3', 'BMGB4', 'ECOR3', 'TOTS3', 'LINX3', 'LLIS3', 'ITUB4', 
    'LREN3', 'GGBR4', 'USIM5', 'MRFG3', 'RENT3', 'MOVI3', 'VIVA3', 'ARZZ3', 'ETER3', 'BRKM5', 'BKBR3', 'PFRM3', 
    'SOMA3', 'ABCB4', 'AMAR3', 'ANIM3', 'BPAN4', 'BRPR3', 'PETR4', 'SAPR3', 'MEAL3', 'TEND3', 'CIEL3', 'MILS3', 
    'CCRO3', 'BEEF3', 'MGLU3', 'BIDI4', 'BBAS3', 'WEGE3', 'CYRE3', 'JHSF3', 'KLBN11', 'SHOW3', 'MRVE3', 'CSAN3', 
    'NTCO3', 'LAME4', 'MDNE3', 'SAPR11', 'JBSS3', 'BRFS3', 'BRML3', 'CSNA3', 'ELET3', 'CMIG4', 'PDGR3', 'LPSB3', 
    'RLOG3', 'PRNR3', 'EZTC3', 'BRDT3', 'ENAT3', 'DMVF3', 'GUAR3', 'SBSP3', 'RANI3', 'LWSA3', 'SAPR4', 'CAML3', 
    'GRND3', 'AGRO3', 'CRFB3', 'LAVV3', 'PGMN3', 'SMTO3', 'MYPK3', 'POMO4', 'STBP3', 'PETZ3', 'ITSA4', 'PTBL3',
    'ENJU3', 'AERI3', 'GMAT3', 'SMLS3', 'CRFB3', 'VVAR3', 'RAPT4'
]
main_codes = [x + "F" for x in main_codes] + main_codes

another_codes = [
    'PINE4F', 'GGBR3', 'TPRY34', 'RBVA11', 'AZEV3F', 'DI1F28', 'BEES3', 'SQIA3F', 'CIEL3', 'PETR3F', 'LIGT3F', 
    'TEKA4F', 'ALUP11', 'LREN3F', 'PETRV95', 'PNVL3F', 'RNGO11', 'ENAT3', 'AALR3', 'IBOVX93', 'DI1N21', 'GOLL11', 
    'ELET6', 'DISB34', 'TAEE11F', 'SAPR4F', 'BEEF3F', 'DI1J22', 'BBRC11', 'SLCE3F', 'DMMO11F', 'TASA17F', 'LJQQ3F', 
    'PETRJ20', 'VGIR11', 'MCCI11', 'GSFI11', 'MMXM11', 'COCA34', 'ENGI11', 'ELET3F', 'EVEN3', 'TXRX3F', 'A1PA34', 
    'LOGG3F', 'SUZB3F', 'PARD3F', 'VALEOV20', 'JBSSK21', 'SANB11', 'AMBP3F', 'LVBI11', 'SHOW3', 'DI1U21', 'AFCR11', 
    'DI1F21', 'GUAR3', 'BPAN4', 'WHRL3F', 'CCPR3F', 'SIMH3', 'MYPK3', 'ARZZ3', 'GOAU4F', 'SULA11F', 'HBSA3F', 
    'FIIB11', 'SANB3F', 'CTSA4', 'VULC3', 'RFOF11', 'TORD11', 'DI1V21', 'TASA4F', 'VALEX33', 'DI1J23', 'MMXM3', 
    'PRIO3F', 'STBP3F', 'HAPV3F', 'KEPL3F', 'RANI4', 'ESTR4', 'BIDI11', 'LWSA3', 'CESP6F', 'PETRW43', 'EZTC3', 
    'PETRV80', 'BBAS3', 'VIVA3', 'BMGB4', 'FRCF23', 'FRP0', 'CVCB3', 'BRFS3', 'OUJP11', 'LUPA3', 'WINV20', 'AMBP3', 
    'GGBR4F', 'TOTS3', 'HGTX3F', 'JBSSL25', 'ENBR3', 'CTNM4', 'PVBI11', 'MOVI3F', 'MRFG3F', 'RADL3', 'IRBRA35', 
    'PETZ3F', 'TASA3F', 'LGCP11', 'FLMA11', 'RBRL11', 'WIZS3F', 'VIVR3F', 'ALSO3', 'TIET3F', 'POMO4', 'SULA3', 
    'MRVE3', 'IBOVK97', 'MDIA3', 'KLBN4F', 'DOHL4', 'DI1F22', 'CURY3F', 'BOBR4', 'MATB11', 'DI1K21', 'LINX3F', 
    'AMAR3F', 'NSLU11', 'PDGR3', 'BRCO11', 'DMVF3F', 'CPLE6', 'PETRK27', 'SAPR3', 'SEER3', 'BRIV4', 'HLOG11', 
    'ALPA3F', 'EURO11', 'ALSO3F', 'RAIL3F', 'TAEE3F', 'KLBN3', 'CGRA3F', 'CIELOV20', 'PARD3', 'TGMA3F', 'BIOM3F', 
    'FLRY3', 'TIET4F', 'FHER3', 'BBPO11', 'DTEX3', 'PETRM21', 'RCSL4F', 'GBRX20', 'EZTC3F', 'OIBR4F', 'VULC3F', 
    'ISPZ20', 'MTSA4F', 'CEOC11', 'VIVT4', 'EQTL3F', 'SNSY5F', 'DI1Q21', 'CVCB11', 'PETZ3', 'GEOO34', 'TEPP11', 
    'BCSA34', 'SCAR3F', 'RSID3F', 'CESP6', 'EQPA3F', 'SHPH11', 'POSI3F', 'DIVO11', 'TEKA3', 'JPMC34', 'PFRM3F', 
    'FSRF11', 'EUPX20', 'BEES3F', 'DI1F25', 'IFRA11', 'TCNO3', 'JPSA3', 'FNAM11', 'DIRR3F', 'XPCM11', 'CMIG4F', 
    'BIDI3', 'VGIP11', 'LUXM4F', 'CNTO3', 'WHRL4F', 'HABT11', 'JAPX20', 'BCIA11', 'LAVV3', 'CVCB3F', 'EMBR3F', 
    'DTEX3F', 'CAML3', 'RECR11', 'ITUBB21', 'DI1F26', 'VINO11', 'ABCB4', 'LLIS3', 'MFII11', 'IDVL4F', 'SANB11F', 
    'ITUB3F', 'CURY3', 'BRAP4', 'ODPV3', 'GOAU4', 'TECN3', 'TASA13', 'IRBR3', 'BRPR3F', 'BRML3', 'SLBG34', 'GTWR11', 
    'ENGI3', 'GOGL34', 'TCSA3F', 'STBP3', 'PRNR3F', 'SMTO3', 'MWET4', 'LAME3', 'YDUQ3', 'CPFE3', 'ARZZ3F', 'SGPS3', 
    'BRKM5F', 'RPMG3F', 'BABA34', 'QAGR11', 'ARRI11', 'CSRN3F', 'PLRI11', 'WHRL4', 'PFIZ34', 'BRAP3', 'ITSA4', 
    'EALT4', 'PETRJ80', 'EQTL3', 'LIGT3', 'TASA15F', 'TUPY3', 'SJCX20', 'THRA11', 'RENT3', 'HBSA3', 'HOSI11', 
    'CEAB3', 'AALR3F', 'TCNO3F', 'TIET11F', 'SPTW11', 'SLED3F', 'CTSA3F', 'TXRX4F', 'MEXX20', 'KFOF11', 'SULA4F', 
    'BRML3F', 'BPAC11F', 'VALE3', 'OIBR3F', 'FIVN11', 'DI1F31', 'EGIE3', 'BPAC5F', 'TIET11', 'ATSA11', 'LUPA3F', 
    'PETRV17', 'BRFS3F', 'ALPA4', 'CGRA4F', 'HGCR11', 'SARE11', 'ELET3', 'JBDU3F', 'POMO3F', 'AURA32', 'CRDE3F', 
    'GOGL35', 'GRLV11', 'BOAS3F', 'MGLU3', 'IBOVV93', 'TGAR11', 'DASA3F', 'MILS3F', 'XPSF11', 'SADI11', 'GOLL4', 
    'TRIS3', 'AZEV3', 'UCAS3', 'NVDC34', 'BPAC11', 'HSML11', 'BRSR3F', 'PCAR3', 'TESA3F', 'BTOW3', 'XPPR11', 
    'TASA15', 'CCMX20', 'MEAL3', 'SULA4', 'TAEE3', 'WSPZ20', 'LCAM3', 'BEEF11F', 'RLOG3', 'ONEF11', 'UGPA3', 
    'WEGE3F', 'ANIM3', 'LAME3F', 'HYPE3', 'CPLE3F', 'SMTO3F', 'ABEV3F', 'WSPH21', 'HGLG11', 'VIVT3F', 'WIZS3', 
    'B3SAOX20', 'RLOG3F', 'JBDU4', 'MNPR3', 'ROMI3F', 'JRDM11', 'HGBS11', 'VALE3F', 'BIDI4F', 'EGIE3F', 'IBOVX5', 
    'IDVL3F', 'TAEE4F', 'SBSP3', 'MNDL3F', 'LINX3', 'LJQQ3', 'SAPR11F', 'DI1J21', 'EVEN3F', 'IVVB11', 'VLID3F', 
    'KNIP11', 'ITUB3', 'CRPG5', 'CRFB3F', 'VCJR11', 'HSAF11', 'GPIV33', 'HBOR3F', 'MNDL3', 'RNEW4F', 'PETRV15', 
    'MSFT34', 'PETRW46', 'CSNA3', 'RCSL3', 'RBDS11', 'IGTA3', 'SULA11', 'KEPL3', 'DMVF3', 'HBOR3', 'RENT3F', 
    'ALUP11F', 'BOVAV77', 'ENEV3F', 'ICFZ20', 'HPQB34', 'PCAR3F', 'HCTR11', 'QUAL3F', 'IBOVL95', 'HAGA4', 'CVBI11', 
    'ITSA4F', 'HGPO11', 'FIND11', 'BOVAV75', 'BBRK3', 'CMIGW94', 'CRPG5F', 'PETRJ22', 'OIBR3', 'PNVL4', 'KLBN3F', 
    'T10Z20', 'BEEF11', 'BRKM5', 'TXRX4', 'EUPZ20', 'TAEE4', 'BRGE6F', 'ODPV3F', 'TGMA3', 'FESA4F', 'CEDO4F', 
    'L1MN34', 'VIVT4F', 'VIVR1', 'CARE11', 'PETRP2', 'CRFB3', 'VILG11', 'TIET3', 'TASA4', 'TELB4F', 'A1MD34', 
    'HAPV3', 'EXXO34', 'PTBL3', 'PLPL3F', 'ITSAJ99', 'EMBR3', 'SOND5F', 'TRIS3F', 'BGIZ20', 'XPML11', 'MULT3', 
    'SMLS3F', 'IDVL4', 'JHSF3', 'CYRE3', 'CMIGV98', 'PSSA3F', 'UNIP5F', 'IBOVK98', 'BTLG12', 'DI1N22', 'BRSR6F', 
    'LPSB3F', 'SAPR3F', 'LOGN3', 'AGRO3F', 'PATC11', 'QUAL3', 'FRIO3F', 'SUZB3', 'EALT4F', 'CEBR3F', 'AGRO3', 
    'JSLG11', 'RCFA11', 'SAPR4', 'AMZO34', 'BOBR4F', 'POSI3', 'FHER3F', 'IBOVV92', 'PRIO3', 'TUPY3F', 'PETRV19', 
    'BTLG11', 'DOLX20', 'TECN3F', 'LLIS3F', 'DI1M21', 'AZUL4F', 'SPXI11', 'TRXF11', 'PIBB11', 'ECOR3', 'HAGA4F', 
    'TEXA34', 'WHRL3', 'BKBR3F', 'POMO4F', 'RECT11', 'EAIN34', 'UNIP3F', 'GRND3', 'CMIGV11', 'SLED4F', 'VOTS11', 
    'LCAM3F', 'ETER3', 'BTOW3F', 'FEXC11', 'PETRK26', 'ABCB10F', 'WLMM3F', 'BOVV11', 'DMMO11', 'OUFF11', 'ENEV3', 
    'GOLL4F', 'KNCR11', 'SOMA3', 'LAME4F', 'GOAU3F', 'DOLZ20', 'B3SA3', 'ALUP4', 'GEPA4F', 'VLOL11', 'SANB4F', 
    'LWSA3F', 'USIM5', 'SLED4', 'DI1F23', 'BKBR3', 'UNIP6F', 'ITUB4', 'BEES4', 'SCPF11', 'BIDI3F', 'ABCB4F', 
    'RCSL4', 'WPLZ11', 'BRDT3', 'EDGA11', 'DI1N24', 'SIMH3F', 'DI1V22', 'COGN3', 'BOVAL95', 'AUSX20', 'PPLA11', 
    'MULT3F', 'ORCL34', 'DMAC11', 'CTXT11', 'RNDP11', 'ETER3F', 'INEP4', 'XTED11', 'BGIX20', 'CVCB11F', 'ADHM3F', 
    'FVPQ11', 'CSAN3F', 'ENGI3F', 'GPCP3F', 'TAEE11', 'TCNO4', 'SLED3', 'AZUL4', 'FAED11', 'CAMB3', 'MDNE3F', 
    'DI1N23', 'ITUB4F', 'IRDM11', 'VISC11', 'SBSP3F', 'SHUL4F', 'BPFF11', 'DI1X20', 'VALEW7', 'AFLT3F', 'ENGI4', 
    'GRND3F', 'BSEV3', 'CCPR3', 'BPAN4F', 'BBSE3F', 'BOVAV63', 'CBOP11', 'PETRV16', 'NEOE3', 'BEES4F', 'KDIF11', 
    'JHSF3F', 'IBOVW92', 'TOTS3F', 'RBIV11', 'VIFI11', 'JSRE11', 'DMMO3', 'TRNT11', 'BOVAV69', 'BBAS3F', 'CMIG4', 
    'CSAN3', 'VIVA3F', 'ELET6F', 'HYPE3F', 'FRAS3F', 'MDIA3F', 'SMAL11', 'DI1F27', 'USIM3', 'GFSA3F', 'RBFF11', 
    'FRCF31', 'OIBR4', 'FESA4', 'PETRV47', 'ATOM3F', 'BPAC3', 'VVAR3', 'HGRE11', 'SHUL4', 'RDPD11', 'RBRF11', 
    'WDOX20', 'PETR4', 'BNBR3F', 'RBED11', 'LOGG3', 'BSLI3F', 'PLPL3', 'VVPR11', 'BCRI11', 'CTKA4F', 'RAPT4', 
    'GNDI3F', 'CMIG3F', 'VVAR3F', 'DI1H21', 'PMAM3', 'PETR4F', 'MELK3', 'CNES11', 'TIET4', 'CTKA4', 'GOAU3', 
    'LEVE3F', 'ALUP3F', 'CIEL3F', 'SMLS3', 'ROMI3', 'ALUP3', 'SANB3', 'DI1F24', 'NTCO3', 'ALPA4F', 'WLMM3', 
    'IFIE11', 'CAML3F', 'URPR11', 'FBOK34', 'LREN3', 'AAPL34', 'ITSA3', 'MTRE3F', 'ITSA3F', 'JBSS3', 'RDSA34', 
    'CPLE6F', 'SHOW3F', 'CYREV26', 'ENGI4F', 'PINE4', 'XPCI11', 'FNOR11', 'AMAR3', 'LAME4', 'CTSA3', 'HRDF11', 
    'ECOO11', 'RAPT4F', 'GOVE11', 'VALEV34', 'CPFF11', 'TASA3', 'WINZ20', 'PNVL3', 'PETRJ19', 'MTRE3', 'PTBL3F', 
    'RNEW11F', 'MMXM3F', 'SNSY5', 'ALZR11', 'TEND3F', 'FLRY3F', 'TIMP3F', 'MXRF11', 'PETR3', 'B3SA3F', 'TSLA34', 
    'JBSS3F', 'IRBR3F', 'IGTA3F', 'ALUP4F', 'VIVR3', 'CARD3', 'TELB3', 'MEAL3F', 'VIVR1F', 'PORD11', 'SOMA3F', 
    'CSMG3F', 'USIM5F', 'ATOM3', 'COGN3F', 'GGBR4', 'MILS3', 'CEBR6', 'PRNR3', 'PSSAOX20', 'ENBR3F', 'ANIM3F', 
    'VALEOX20', 'PETRW47', 'KLBN4', 'PNVL4F', 'PDTC3F', 'RBRR11', 'RSID3', 'CARD3F', 'PSSA3', 'RANI4F', 'SLCE3', 
    'MELK3F', 'LUGG11', 'XPIN11', 'RANI3', 'EMAE4F', 'ECOR3F', 'CSMG3', 'BOVB11', 'EUCA4', 'CCMF21', 'NTCO3F', 
    'FRAS3', 'EUCA4F', 'SWIX20', 'JBDU3', 'JBDU4F', 'BRCR11', 'MALL11', 'PFRM3', 'MDNE3', 'UNIP3', 'PMAM3F', 
    'FCFL11', 'PETRK23', 'ULEV34', 'RAPT3', 'OULG11', 'CCMK21', 'FSRF11F', 'KLBN11', 'VTLT11', 'DAPQ22', 
    'RIGG34', 'ALPK3', 'RADL3F', 'JDCO34', 'APER3F', 'APER3', 'UGPA3F', 'TRPL4', 'A1AP34', 'MFAI11', 
    'HGTX3', 'CGAS3F', 'TCSA3', 'XPLG11', 'VLID3', 'BRPR3', 'XPIE11', 'MGLU3F', 'BBRK3F', 'CEAB3F', 
    'INDV20', 'BBVJ11', 'BPML11', 'LOGN3F', 'CHVX34', 'TESA3', 'BRSR6', 'MOVI3', 'FIGS11', 'GUAR3F', 
    'CCMU21', 'MYPK3F', 'SAPR11', 'BBSE3', 'GILD34', 'KNRI11', 'YDUQ3F', 'ABEV3', 'BEEF3', 'MRVE3F', 
    'DMMO3F', 'IMAB11', 'RBCO11', 'RBRY11', 'IBOVJ96', 'RBVO11', 'EURX20', 'HTMX11', 'PETRA22', 'OMGE3F', 
    'GPCP3', 'BRAP4F', 'RNEW3F', 'UCAS3F', 'TRPL3', 'VIVT3', 'BOVAX85', 'BERK34', 'ENAT3F', 'CPFE3F', 
    'RCSL3F', 'ISUS11', 'GFSA3', 'TRPL4F', 'BMGB4F', 'USIM3F', 'CPTS11', 'IBOVW89', 'MMXM11F', 'BARI11', 
    'LEVE3', 'CANX20', 'WEGE3', 'MGFF11', 'IBFF11', 'ALPK3F', 'BIOM3', 'DIRR3', 'SEER3F', 'DI1F29', 'BBASJ35', 
    'EMAE4', 'TPIS3F', 'HFOF11', 'GSHP3F', 'POMO3', 'TEKA4', 'BSEV3F', 'PGMN3F', 'CVSH34', 'GNDI3', 'RVBI11', 
    'PATL11', 'RAPT3F', 'IBOVV97', 'MRFG3', 'PGMN3', 'SQIA3', 'CMIG3', 'CNTO3F', 'ENGI11F', 'RBRP11', 'UNIP6', 
    'TRPL3F', 'CRPG6', 'CYRE3F', 'LPSB3', 'NCHB11', 'KLBN11F', 'BCFF11', 'TEND3', 'CPLE3', 'RBRD11', 'RANI3F', 
    'SANB4', 'ABCP11', 'HGFF11', 'GOLL11F', 'NEOE3F', 'BPAC5', 'VRTA11', 'BAZA3F', 'CSNA3F', 'TCNO4F', 'PETRV18', 
    'BOVA11', 'PYPL34', 'BTCR11', 'DI1G21', 'IGBR3', 'COCE5F', 'ASML34', 'BIDI4', 'BNFS11', 'BOAS3', 'BGIV20', 
    'GFSAJ74', 'SMAC11', 'RCRB11', 'HGRU11', 'RDNI3', 'CEPE5F', 'IBOVW95', 'OMGE3', 'BIDI11F', 'SDIL11', 'TIMP3', 
    'INEP3', 'TPIS3', 'CRIV3F', 'BRDT3F', 'IBOVW93', 'RAIL3', 'MELI34', 'AZEV4', 'PDTC3', 'ICFH21', 'JPSA3F', 'GGBR3F',
    'ENJU3F', 'ENJU3', 'AERI3', 'AERI3F', 'GMAT3', 'GMAT3F', 'HBRE3', 'HBRE3F'
]

def get_main_codes():
    return main_codes

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

    # Apenas para o último candle
    tmp = df.sort_values(by=['date'])
    tickers = tmp['ticker'].unique()
    d = pd.DataFrame()
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
                d = d.append(pd.DataFrame({'date': [dt], 'ticker': [ticker], 'candle_type': [f"{r['ind_trend']}: {r['candle_type']}"]}), ignore_index=True)
                # only the first
                break
    return df.merge(d, on=['date', 'ticker'], how='left') if d.shape[0] > 0 else df.assign(candle_type=None)
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
                if not min_pct_gain or (gain >= min_pct_gain/100 * b['close']):
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
    if split_df:
        test_ids = np.random.randint(1, dataset.shape[0], int(30/100*dataset.shape[0]))
        test_ids = dataset.iloc[test_ids].index
        train = dataset[~dataset.index.isin(test_ids)]
        test = dataset[dataset.index.isin(test_ids)].drop(col_predict_name, axis=1)
        X = train.drop(col_predict_name, axis=1)
        Y = train[col_predict_name]
    else:
        X = dataset[~dataset.index.isin(test_ids)].drop(col_predict_name, axis=1)
        Y = dataset[~dataset.index.isin(test_ids)][[col_predict_name]]
        test = None
    return {'X': X, 'Y': Y, 'test': test}

### Perceptron
class PerceptronModel:
    def __init__(self, weights=None, bias=None):
        self.weights = weights
        self.bias = bias
    def predict(self, tst):
        u = get_u(self.weights, tst, self.bias)
        return activation_function(u) 

    def get_u(w, x, bias, m_factor=1): # b = bias, w = weight, x = value
        return (w * x).sum() + m_factor * bias

    def update_weights(weight, expected, predicted, x, a=0.4): # a = N
        return weight + a * x * (expected - predicted)

    def update_bias(bias, expected, predicted, a=0.4):
        return bias + a * (expected - predicted)

    def activation_function(u, threshold=0):
        return 1 if u >= threshold else -1

    def get_error(expected, predicted, k, e=0):
        return e + (abs(expected - predicted) / k)

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

def run_perceptron(df, ticker=None, n_try=200):
    dfs_ml = prepare_dataframe_to_ml(df, split_df=False)
    bias = 0.5
    a = 0.4
    e = 0
    fit = False

    weights = np.random.random(df.shape[1] - 1)
    train = dfs_ml['X'] # X
    label = dfs_ml['Y'] # Y
    test = dfs_ml['test']

    errors = np.array([])
    best_model = {'e': float('inf'), 'model': None}

    for _ in tqdm(range(n_try)):
        # e = 0
        for i in range(train.shape[0] - 1):
            x = train.iloc[i].to_numpy()
            expected = label.iloc[i][0]
            u = get_u(weights, x, bias)
            predicted = activation_function(u)
            if predicted != expected:
                fit = True
                weights = update_weights(weights, expected, predicted, x, a=a)
                bias = update_bias(bias, expected, predicted, a=a)
                e = get_error(expected, predicted, i + 1, e=e)
        if best_model['e'] > e:
            best_model['e'] = e
            best_model['model'] = PerceptronModel(weights, bias)
        errors = np.append(errors, e)
        if not fit: break
        fit = False
    m = PerceptronModel(weights, bias)
    # print(f'Model: w = {weights.round(2).tolist()} | bias = {bias}')
    print(f'Best model: w = {best_model["model"].weights.round(2).tolist()} | bias = {best_model["model"].bias}')
    return m
### Fim Perceptron

# def run_ml_model(model_name='perceptron'):
#     if model_name == 'perceptron':

# wknn
class wknn:
    def agg_weights(labels, weights):
        tmp = pd.DataFrame({'labels': labels, 'weights': weights})
        scores = tmp.groupBy('labels').sum().reset_index()
        # id of the max score
        max_score_indice = scores['weights'].idxmax()
        # label of the max score
        predict_label = scores.loc[max_score_indice]['labels']
        return predict_label

    # Calculate the Euclidian Distance
    def euclidian_distance(x, y):
        return math.sqrt((x - y).pow(2).sum())

    def wknn(data, rotulo, k, test, task='r'):
        # Calculate the distance of test and all elements of data train
        dists = data.apply(euclidian_distance, args=(test,), axis=1)
        # relation between distances and weights
        proximities = 1/dists
        
        k_nearest_indices = [x[0] for x in sorted(zip(dists.index, dists), key=lambda x: x[1])[:k]]
        candidates = rotulo.loc[k_nearest_indices]
        weights = proximities.loc[k_nearest_indices]
        
        if task == 'c':
            return agg_weights(candidates, weights)
        return candidates.mean()

def get_polyfit_trend(serie, index, deg=1):
    """
    If the result is a +ve value --> increasing trend
    If the result is a -ve value --> decreasing trend
    If the result is a zero value --> No trend
    """
    import numpy as np
    return np.polyfit(index, serie, deg=deg, rcond=None, full=False, w=None, cov=False)

def vwap(price, volume, decimal=2):
    return (np.cumsum(price * volume).cumsum() / volume.cumsum()).round(decimal)

def macd(fast_ma, slow_ma, decimal=2):
    return (fast_ma - slow_ma).round(decimal)

def ewm_std(x, ewm):
    return (x - ewm.shift(1))

def ema(serie, period):
    return serie.ewm(span=period, min_periods=period, adjust=False).mean().round(2)

def create_ema(df, periods=[8, 20, 72, 200]):
    df_ = pd.DataFrame()
    tmp = df.copy()
    for code in tmp['ticker'].unique():
        tmp = tmp[tmp['ticker'] == code].sort_values(by=['date'])
        for p in periods:
            # call ema function
            tmp[f'close_ema{p}'] = ema(serie=tmp['close'], period=p)
            if p == 20:
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

def normalize_recommendations(df):
    tmp = df.copy()
    tmp['buy'] = np.where(tmp['volume'] < tmp['volume_ema20'], 0, df['buy'])
    tmp['sell'] = np.where(tmp['volume'] < tmp['volume_ema20'], 0, df['sell'])
    return tmp

def flag_volume(df):
    return df.assign(ind_volume=np.where(
        df['volume'].notna() & (df['volume'] < df['volume_ema20']), 
        -1, 
        np.where(df['volume'].notna() & (df['volume'] > df['volume_ema20']), 1, 0))
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
            arrowprops = dict(arrowstyle = '->')
        )
    if path_fig: plt_.savefig(path_fig, format='svg', dpi=1000)

def get_return_rate_and_risk(df, plot_risk_and_return=False, risk_return_period=None):
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
    return f'{yahoo_api_path}{code}.SA?period1={period1}&period2={period2}&interval={str(interval_days)}d&events={events}'

def get_yahoo_finance(code, interval_days=1, period1=946695600, period2=None, events='history'):
    url = create_yahoo_download_query(code=code, period1=period1, interval_days=interval_days, events=events)
    try:
        df = pd.read_csv(url).assign(ticker=code)
        return df.rename(columns={x: x.lower().replace(' ', '_') for x in df.columns})[default_yahoo_df_columns]
    except:
        return pd.DataFrame()

def daily_analysis_yfinance(write_path=None, hist_path='hist_market_trading_yfinance.csv', get_recom=True, tickers=set(main_codes + another_codes), norm_rec=False, interval_days=1, period1=946695600, period2=None, qtd_days=None, plot_risk_and_return=False, risk_return_period=365):
    codes = [x for x in tickers if not x.endswith('F')]
    df = pd.DataFrame()
    hist = pd.DataFrame()
    print(f"Baixando dados para os últimos {qtd_days if qtd_days else '<todo o período>'} dias...")
    if hist_path and os.path.exists(hist_path): 
        hist = pd.read_csv(hist_path)[default_yahoo_df_columns]
    for ticker in tqdm(codes):
        if qtd_days: period1 = int((datetime.datetime.today() - datetime.timedelta(days=qtd_days)).timestamp())
        tmp = get_yahoo_finance(code=ticker, interval_days=interval_days, period1=period1, period2=period2)
        if not tmp.empty or tmp.shape[0] > 0:
            tmp = tmp.rename(columns={'code': 'ticker'})
            if qtd_days and os.path.exists(hist_path): tmp = tmp.append(hist[list(tmp.columns)][hist['ticker'] == ticker], ignore_index=True).drop_duplicates(subset=['date', 'ticker'])
            tmp = create_ema(tmp.drop_duplicates(['date', 'ticker']))
            tmp = flag_volume(tmp)
            tmp['macd'] = macd(fast_ma=tmp['close_ema8'], slow_ma=tmp['close_ema20']).round(2)
            tmp['macd_signal'] = ema(serie=tmp['macd'], period=8)
            tmp = get_signals(tmp)
            if write_path and not qtd_days:
                tmp.to_csv(f"{write_path + ticker}.csv", index=False)
            df = df.append(tmp, ignore_index=True)
    
    df_recom = pd.DataFrame()
    if get_recom and not df.empty:
        df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
        df_recom = df[(df['buy'] == 1) | (df['sell'] == 1) | (df['close'] > df['high'].shift(1))].sort_values(by=['date'], ascending=False).drop_duplicates(subset=['ticker'])
        query = "((buy == 1) | (sell == 1)) & ((volume > 0) & (close < 100))"
        if norm_rec: df_recom = normalize_recommendations(df_recom).query(query)
        df_recom['volume_ema20'] = df_recom['volume_ema20'].round(2)
        df_recom = df_recom.drop(['high', 'low', 'adj_close', 'volume'], axis=1).sort_values(by=['ticker'])
        df_recom = df_recom[df_recom['ticker'].isin(main_codes)].sort_values(by=['date'], ascending=False).append(df_recom[~df_recom['ticker'].isin(main_codes)], ignore_index=True)
        df_recom.to_html(f'reports/yfinance/radar_yfinance.html', index=False)
        df_recom.to_csv('data/radar_yfinance.csv', index=False)
        tmp = df_recom[df_recom['date'].dt.date == min(today.date(), df['date'].max())]
        if tmp.shape[0] == 0:
            tmp = candle_trend(df[df['date'].dt.date == min(today.date(), df['date'].max())])[['date', 'ticker', 'candle_trend']]
            print("Filtro por data atual:", tmp.shape)
            tmp = tmp[tmp['ticker'].isin(main_codes)]
            print("Filtro por códigos principais:", tmp.shape)
        tmp = tmp[tmp['ticker'].isin(main_codes)].sort_values(by=['close']).reset_index(drop=True)
        tmp.to_html(f'reports/radar_yfinance_today.html')#, index=False)
        print(f'Radar saved in: radar_yfinance.html')
        # to return
        df_recom = tmp[tmp['buy'] == 1][['date', 'ticker', 'close']]
    if hist.empty or hist[pd.to_datetime(hist['date']).dt.date == (today - datetime.timedelta(1)).date()].shape[0] == 0:
        if not hist.empty: print(hist['date'].max())
        tmp_hist = df[df['date'] != today.date()].sort_values(['ticker', 'date'])
        print(f'A data {(today - datetime.timedelta(1)).date()} não está na base\nEscrevendo em {hist_path} sem a data {today.date()}: {tmp_hist.shape}')
        tmp_hist.to_csv(hist_path, index=False)
    return df, df_recom
