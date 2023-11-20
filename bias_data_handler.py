from qlib.data.dataset.handler import DataHandlerLP

_DEFAULT_LEARN_PROCESSORS = [
    {"class": "DropnaLabel"},
    {"class": "CSZScoreNorm", "kwargs": {"fields_group": "label"}},
]
_DEFAULT_INFER_PROCESSORS = [
    {"class": "ProcessInf", "kwargs": {}},
    {"class": "ZScoreNorm", "kwargs": {}},
    {"class": "Fillna", "kwargs": {}},
]

class BIASDataHandler(DataHandlerLP):
    def __init__(
        self,
        instruments="002142.sz",
        start_time=None,
        end_time=None,
        freq="day",
        infer_processors=_DEFAULT_INFER_PROCESSORS,
        learn_processors=_DEFAULT_LEARN_PROCESSORS,
        fit_start_time=None,
        fit_end_time=None,
        filter_pipe=None,
        inst_processors=None,
        **kwargs
    ):
        data_loader = {
            "class": "QlibDataLoader",
            "kwargs": {
                "config": {
                    "feature": self.get_feature_config(),
                    "label": kwargs.pop("label", self.get_label_config()),
                },
                "filter_pipe": filter_pipe,
                "freq": freq,
                "inst_processors": inst_processors,
            },
        }

        super().__init__(
            instruments=instruments,
            start_time=start_time,
            end_time=end_time,
            data_loader=data_loader,
            **kwargs
        )

    def get_label_config(self):
        return ["Ref($close, -2)/Ref($close, -1) - 1"], ["LABEL0"]

    @staticmethod
    def get_feature_config():
        period_list = [13, 34, 55]
        #fields = ["Mean($close, {})".format(p) for p in period_list]
        #names = ["MA{}".format(p) for p in period_list]
        fields = []
        names = []
        for period in period_list:
            fields.append("($close - Mean($close, {})) / Mean($close, {}) * 100".format(period, period))
            names.append("BIAS{}".format(period))
        fields.append("$vol")
        names.append("Volume")
        return fields, names
