#!/bin/bash
rsync -avP --bwlimit=8.5m greetlist@192.168.199.123:/home/greetlist/workspace/data_storage/raw/day/SSE/6*.csv /home/greetlist/workspace/data_storage/tushare/raw/day/
rsync -avP --bwlimit=8.5m greetlist@192.168.199.123:/home/greetlist/workspace/data_storage/raw/day/SZSE/0*.csv /home/greetlist/workspace/data_storage/tushare/raw/day/
rsync -avP --bwlimit=8.5m greetlist@192.168.199.123:/home/greetlist/workspace/data_storage/raw/total_stock.csv /home/greetlist/workspace/data_storage/tushare/raw/
/opt/miniconda/envs/stock/bin/python calc_factor.py change-vol-name
/opt/miniconda/envs/stock/bin/python script/convert_raw_data.py dump_all --symbol_field_name ts_code --date_field_name trade_date --include_fields high,open,low,close,volumn --csv_path /home/greetlist/workspace/data_storage/tushare/raw/day --qlib_dir /home/greetlist/workspace/data_storage/qlib/
/opt/miniconda/envs/stock/bin/python calc_factor.py calc ma,bias
