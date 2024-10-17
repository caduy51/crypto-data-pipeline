import json, os, requests
import pandas as pd
import datetime

def _extract_symbol_list():
    # Take symbols from saved file
    symbol_list = []
    with open("/opt/airflow/data/get_symbols/symbols.json", "r") as f:
        symbols = json.load(f)
        for symbol in symbols["symbols"]:
            symbol_list.append(symbol["symbol"])
    return symbol_list
    
def get_symbols(): # done
    # create folder if not exists
    price_dir = "/opt/airflow/data/get_symbols"
    if not os.path.exists(price_dir):
        os.mkdir(price_dir)

    # api pull
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()

    with open(os.path.join(price_dir, "symbols.json"), "w") as f:
        json.dump(data, f, indent=2)

def get_bars(interval='1d'):
    symbol_list = _extract_symbol_list()
    print(f"There are {len(symbol_list)} charts")
    # create folder if not exists
    price_folder = "/opt/airflow/data/get_bars"
    if not os.path.exists(price_folder):
        os.mkdir(price_folder)

    for index, symbol in enumerate(symbol_list[:100]):
        # loops through list
        root_url = 'https://api.binance.com/api/v3/klines'
        url = root_url + '?symbol=' + symbol + '&interval=' + interval
        # Send api requests
        data = requests.get(url).json()
        df = pd.DataFrame(data) 
        df.columns = ['open_time',
                'open', 'high', 'low', 'close', 'vol',
                'close_time', 'quote_asset_vol', 'num_trades',
                'taker_base_vol', 'taker_quote_vol', 'ignore']
        df.insert(loc=1, column='symbol_name', value=symbol)
        df["date"] = [datetime.datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d') for x in df.close_time]
        # reorder columns
        df = df[["date", 'open_time', 'symbol_name',
                'open', 'high', 'low', 'close', 'vol',
                'close_time', 'quote_asset_vol', 'num_trades',
                'taker_base_vol', 'taker_quote_vol', 'ignore']]
        # write file
        df.to_csv(f"/opt/airflow/data/get_bars/{symbol}.csv", mode='w', index_label=False, index=False)
        print(f"Chart {index + 1} {symbol} - saved !")


