from string import Template

from libs.snowflake.snowflake_connection import SnowflakeConnection
from libs.event_bridge.event import error_event
from libs.processing.dme_recipe import DmeRecipe


class DmeSqlRecipe(DmeRecipe):

    def process(self, template, save_output=True):
        try:
            with SnowflakeConnection() as sc:
                t = Template(template)
                sql = t.substitute(**self.mapping)

                if save_output:
                    data_path = f"{self.output_source}{self.output}.csv"
                    sc.save_to_csv(sql, data_path)

                else:
                    df = sc.get_data(sql)
                    return df
        except Exception as e:
            error_event(
                self.ap_data_sector, self.analysis_year, self.pipeline_runid, str(e)
            )
            raise e
