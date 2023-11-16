import qlib
import argh
from qlib.data import D
from qlib.constant import REG_CN
from qlib.contrib.data.handler import Alpha158
from qlib.utils import init_instance_by_config, flatten_dict
from qlib.workflow.record_temp import SignalRecord
from qlib.contrib.evaluate import risk_analysis

from qlib.contrib.evaluate import backtest_daily

def gen_config(code_list):
    data_handler_config = {
        "start_time": "2016-01-01",
        "end_time": "2023-11-01",
        "fit_start_time": "2016-01-01",
        "fit_end_time": "2022-01-01",
        "instruments": code_list,
    }
    task_config = {
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
                    "class": "Alpha158",
                    "module_path": "qlib.contrib.data.handler",
                    "kwargs": data_handler_config,
                },
                "segments": {
                    "train": ("2016-01-01", "2021-12-31"),
                    "valid": ("2022-01-01", "2022-12-31"),
                    "test": ("2023-01-01", "2023-04-30"),
                },
            },
        },
    }
    return data_handler_config, task_config

def train(data_handler_config, task_config):
    model = init_instance_by_config(task_config["model"])
    dataset = init_instance_by_config(task_config["dataset"])

    with R.start(experiment_name="workflow"):
        # train
        R.log_params(**flatten_dict(task_config))
        model.fit(dataset)
        # prediction
        recorder = R.get_recorder()
        sr = SignalRecord(model, dataset, recorder)
        sr.generate()

def simulation():
    FREQ = "day"
    STRATEGY_CONFIG = {
        "topk": 10,
        "n_drop": 5,
        # pred_score, pd.Series
        "signal": pred_score,
    }
    
    EXECUTOR_CONFIG = {
        "time_per_step": "day",
        "generate_portfolio_metrics": True,
    }
    
    backtest_config = {
        "start_time": "2023-05-01",
        "end_time": "2023-11-01",
        "account": 100000000,
        "exchange_kwargs": {
            "freq": FREQ,
            "limit_threshold": 0.095,
            "deal_price": "close",
            "open_cost": 0.005,
            "close_cost": 0.005,
            "min_cost": 5,
        },
    }
    strategy_obj = TopkDropoutStrategy(**STRATEGY_CONFIG)
    executor_obj = executor.SimulatorExecutor(**EXECUTOR_CONFIG)
    portfolio_metric_dict, indicator_dict = backtest(executor=executor_obj, strategy=strategy_obj, **backtest_config)
    analysis_freq = "{0}{1}".format(*Freq.parse(FREQ))
    report_normal, positions_normal = portfolio_metric_dict.get(analysis_freq)

    analysis = dict()
    analysis["excess_return_without_cost"] = risk_analysis(
        report_normal["return"] - report_normal["bench"], freq=analysis_freq
    )
    analysis["excess_return_with_cost"] = risk_analysis(
        report_normal["return"] - report_normal["bench"] - report_normal["cost"], freq=analysis_freq
    )

    analysis_df = pd.concat(analysis)  # type: pd.DataFrame
    analysis_dict = flatten_dict(analysis_df["risk"].unstack().T.to_dict())
    # print out results
    print(f"The following are analysis results of benchmark return({analysis_freq}).")
    print(risk_analysis(report_normal["bench"], freq=analysis_freq))
    print(f"The following are analysis results of the excess return without cost({analysis_freq}).")
    print(analysis["excess_return_without_cost"])
    print(f"The following are analysis results of the excess return with cost({analysis_freq}).")
    print(analysis["excess_return_with_cost"])

def get_total_stock_code():
    stock_csv = '/home/greetlist/workspace/data_storage/tushare/raw/total_stock.csv'
    df = pd.read_csv(stock_csv, usecols=['ts_code'])
    df = df[df['ts_code'].str.startswith('00') | df['ts_code'].str.startwith('60')]
    return list(df['ts_code'].values)

def run(stock_code='all'):
    qlib.init(provider_uri="/home/greetlist/workspace/data_storage/qlib/", region=REG_CN)
    code_list = stock_code.split(',') if stock_code != 'all' else get_total_stock_code()
    data_handler_config, task_config = gen_config(code_list)
    train(data_handler_config, task_config)
    simulation()

if __name__ == '__main__':
    argh.dispatch_commands([run])
