import qlib
from qlib.data import D
from qlib.constant import REG_CN

qlib.init(provider_uri="/home/greetlist/workspace/data_storage/qlib/SSE", region=REG_CN)

code_list = ["600759.sh"]
fields = ["$high", "$open", "$low", "$close", "$vol", "Mean($close, 13)", "Mean($close, 34)", "Mean($close, 55)"]
fields = ["$close", "$vol", "($close - Mean($close, 13)) / Mean($close, 13) * 100", "Mean($close, 34)", "Mean($close, 55)"]

data = D.features(code_list, fields, start_time="2018-01-01", freq="day")
print(data)
