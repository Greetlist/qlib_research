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
        df = df.rename(columns={'vol':'volume'})
        df.to_csv(f, index=False)

def calc(factor_list_str):
    convert_save_dir = '/home/greetlist/workspace/data_storage/convert_features'
    qlib.init(provider_uri="/home/greetlist/workspace/data_storage/qlib/", region=REG_CN)
    code_list = get_stock_code_list()
    #code_list = ["002142.SZ", "600000.SH"]
    fields = ["$high", "$open", "$low", "$close", "$volume"]
    factor_list = factor_list_str.split(',')
    rename_dict = dict()
    n_days_list = [13, 21, 34, 55]
    for factor in factor_list:
        if 'ma' == factor:
            factor_calc_template = "Mean($close, {day})"
            rename_col_template = "MA{day}"
        elif 'bias' == factor:
            factor_calc_template = "($close - Mean($close, {day})) / Mean($close, {day}) * 100"
            rename_col_template = "BIAS{day}"
        elif 'rsv' == factor:
            factor_calc_template = "($close-Min($low, {day}))/(Max($high, {day})-Min($low, {day})+1e-12)"
            rename_col_template = "RSV{day}"

        for day in n_days_list:
            factor_calc = factor_calc_template.format(**locals())
            fields.append(factor_calc)
            rename_dict[factor_calc] = rename_col_template.format(**locals())

    for code in code_list:
        data = D.features([code], fields, start_time="2001-01-01", freq="day")
        data = data.rename(columns=rename_dict).reset_index()
        data.to_csv(os.path.join(convert_save_dir, code + '.csv'), index=False)

if __name__ == '__main__':
    argh.dispatch_commands([calc, change_vol_name])
