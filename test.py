import qlib
from qlib.data import D
from qlib.constant import REG_CN
from qlib.workflow import R
from qlib.utils import init_instance_by_config, flatten_dict
from qlib.workflow.record_temp import SignalRecord, PortAnaRecord
from qlib.contrib.evaluate import backtest_daily
from qlib.contrib.evaluate import risk_analysis
from qlib.contrib.strategy import TopkDropoutStrategy

from bias_data_handler import BIASDataHandler

import pandas as pd
import sys

qlib.init(provider_uri="/home/greetlist/workspace/data_storage/qlib/", region=REG_CN)

code_list = ["002142.sz"]
fields = ["$high", "$open", "$low", "$close", "$vol", "Mean($close, 13)", "Mean($close, 34)", "Mean($close, 55)"]
fields = ["$close", "$vol", "($close - Mean($close, 13)) / Mean($close, 13) * 100", "Mean($close, 34)", "Mean($close, 55)"]

data = D.features(code_list, fields, start_time="2017-01-01", freq="day")
print(data)
sys.exit(0)

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
                "test": ("2023-01-01", "2023-05-31"),
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
    recorder = R.get_recorder()
    sr = SignalRecord(model, dataset, recorder)
    sr.generate()

pred_score = pd.read_pickle("score.pkl")["score"]

FREQ = "day"
STRATEGY_CONFIG = {
    "topk": 50,
    "n_drop": 5,
    # pred_score, pd.Series
    "signal": pred_score,
}

EXECUTOR_CONFIG = {
    "time_per_step": "day",
    "generate_portfolio_metrics": True,
}

backtest_config = {
    "start_time": "2023-06-01",
    "end_time": "2023-11-01",
    "account": 100000000,
    "benchmark": "601607.SH",
    "exchange_kwargs": {
        "freq": FREQ,
        "limit_threshold": 0.095,
        "deal_price": "close",
        "open_cost": 0.0005,
        "close_cost": 0.0015,
        "min_cost": 5,
    },
}

# strategy object
strategy_obj = TopkDropoutStrategy(**STRATEGY_CONFIG)
# executor object
executor_obj = executor.SimulatorExecutor(**EXECUTOR_CONFIG)
# backtest
portfolio_metric_dict, indicator_dict = backtest(executor=executor_obj, strategy=strategy_obj, **backtest_config)
analysis_freq = "{0}{1}".format(*Freq.parse(FREQ))
# backtest info
report_normal, positions_normal = portfolio_metric_dict.get(analysis_freq)

# analysis
analysis = dict()
analysis["excess_return_without_cost"] = risk_analysis(
    report_normal["return"] - report_normal["bench"], freq=analysis_freq
)
analysis["excess_return_with_cost"] = risk_analysis(
    report_normal["return"] - report_normal["bench"] - report_normal["cost"], freq=analysis_freq
)

analysis_df = pd.concat(analysis)  # type: pd.DataFrame
# log metrics
analysis_dict = flatten_dict(analysis_df["risk"].unstack().T.to_dict())
# print out results
print(f"The following are analysis results of benchmark return({analysis_freq}).")
print(risk_analysis(report_normal["bench"], freq=analysis_freq))
print(f"The following are analysis results of the excess return without cost({analysis_freq}).")
print(analysis["excess_return_without_cost"])
print(f"The following are analysis results of the excess return with cost({analysis_freq}).")
print(analysis["excess_return_with_cost"])
