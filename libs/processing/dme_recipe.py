import json
from abc import ABC, abstractmethod

from libs.config.config_vars import CONFIG


class DmeRecipe(ABC):
    def __init__(self, output):
        with open(f'{CONFIG["input_variables"]}/query_variables.json', "r") as f:
            data = json.load(f)
            self.ap_data_sector = data["ap_data_sector"]
            self.analysis_year = data["analysis_year"]
            self.analysis_run_group = data["analysis_run_group"]
            self.pipeline_runid = data["target_pipeline_runid"]
            self.breakout_level = data["breakout_level"]
            self.source_ids = data["source_ids"]
            self.output = output
            self.output_source = CONFIG["output_source"]
            self.mapping = {
                "DKU_DST_ap_data_sector": self.ap_data_sector,
                "DKU_DST_analysis_year": self.analysis_year,
                "DKU_DST_analysis_run_group": self.analysis_run_group,
                "ap_data_sector": self.ap_data_sector,
                "analysis_year": self.analysis_year,
                "analysis_run_group": self.analysis_run_group,
                "source_ids": self.source_ids,
                "breakout_level": self.breakout_level,
            }

    @abstractmethod
    def process(self, template):
        pass
