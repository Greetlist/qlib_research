import qlib
from qlib.data import D
from qlib.constant import REG_CN
from qlib.workflow import R
from qlib.utils import init_instance_by_config, flatten_dict
from qlib.workflow.record_temp import SignalRecord, PortAnaRecord

from bias_data_handler import BIASDataHandler

qlib.init(provider_uri="/home/greetlist/workspace/data_storage/qlib/SSE", region=REG_CN)

code_list = ["600759.sh"]
fields = ["$high", "$open", "$low", "$close", "$vol", "Mean($close, 13)", "Mean($close, 34)", "Mean($close, 55)"]
fields = ["$close", "$vol", "($close - Mean($close, 13)) / Mean($close, 13) * 100", "Mean($close, 34)", "Mean($close, 55)"]

data = D.features(code_list, fields, start_time="2017-01-01", freq="day")
print(data)

data_handler_config = {
    "start_time": "2007-01-01",
    "end_time": "2023-11-01",
    "fit_start_time": "2007-01-01",
    "fit_end_time": "2022-01-01",
    #"instruments": ["601607.SH", "600759.SH"],
    "instruments": ["601607.SH", "600239.SH", "600251.SH"],
}

h = BIASDataHandler(**data_handler_config)

print(h.get_cols())
print(h.fetch(col_set="label"))
print(h.fetch(col_set="feature"))

task = {
    "model": {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "kwargs": {
            "loss": "mse",
            "colsample_bytree": 0.8879,
            "learning_rate": 0.0421,
            "subsample": 0.8789,
            "lambda_l1": 205.6999,
            "lambda_l2": 580.9768,
            "max_depth": 8,
            "num_leaves": 210,
            "num_threads": 10,
        },
    },
    "dataset": {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": {
                "class": "BIASDataHandler",
                "module_path": "bias_data_handler",
                "kwargs": data_handler_config,
            },
            "segments": {
                "train": ("2007-01-01", "2021-12-31"),
                "valid": ("2022-01-01", "2022-12-31"),
                "test": ("2023-05-01", "2023-11-01"),
            },
        },
    },
}

model = init_instance_by_config(task["model"])
dataset = init_instance_by_config(task["dataset"])

with R.start(experiment_name="workflow"):
    # train
    R.log_params(**flatten_dict(task))
    model.fit(dataset)

    res = model.predict(dataset).reset_index()
    res.to_csv('test.csv', index=False)
    
    # prediction
    #recorder = R.get_recorder()
    #sr = SignalRecord(model, dataset, recorder)
    #sr.generate()

