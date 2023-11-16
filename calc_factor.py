import qlib
from qlib.data import D
from qlib.constant import REG_CN
import pandas as pd
import argh
import os
import glob

def get_stock_code_list():
   df = pd.read_csv('/home/greetlist/workspace/data_storage/tushare/raw/total_stock.csv', usecols=['ts_code'])
   return list(df['ts_code'].values)

def change_vol_name():
    raw_df_path = '/home/greetlist/workspace/data_storage/tushare/raw/day/*'
    for f in glob.glob(raw_df_path):
        df = pd.read_csv(f)
        df = df.rename(columns={'vol':'volumn'})
        df.to_csv(f, index=False)

def calc(factor_list_str):
    convert_save_dir = '/home/greetlist/workspace/data_storage/convert_features'
    qlib.init(provider_uri="/home/greetlist/workspace/data_storage/qlib/", region=REG_CN)
    code_list = get_stock_code_list()
    #code_list = ["002142.SZ", "600000.SH"]
    fields = ["$high", "$open", "$low", "$close", "$vol"]
    factor_list = factor_list_str.split(',')
    rename_dict = dict()
    for factor in factor_list:
        if 'ma' == factor:
            fields += [
                "Mean($close, 13)",
                "Mean($close, 34)",
                "Mean($close, 55)",
            ]
            rename_dict["Mean($close, 13)"] = "MA13"
            rename_dict["Mean($close, 34)"] = "MA34"
            rename_dict["Mean($close, 55)"] = "MA55"
        elif 'bias' == factor:
            fields += [
                "($close - Mean($close, 13)) / Mean($close, 13) * 100",
                "($close - Mean($close, 34)) / Mean($close, 34) * 100",
                "($close - Mean($close, 55)) / Mean($close, 55) * 100",
            ]
            rename_dict["($close - Mean($close, 13)) / Mean($close, 13) * 100"] = "BIAS13"
            rename_dict["($close - Mean($close, 34)) / Mean($close, 34) * 100"] = "BIAS34"
            rename_dict["($close - Mean($close, 55)) / Mean($close, 55) * 100"] = "BIAS55"

    for code in code_list:
        data = D.features([code], fields, start_time="2001-01-01", freq="day")
        data = data.rename(columns=rename_dict).reset_index()
        data.to_csv(os.path.join(convert_save_dir, code + '.csv'), index=False)

if __name__ == '__main__':
    argh.dispatch_commands([calc, change_vol_name])
