from string import Template

from pyspark.sql import SparkSession

from libs.config.config_vars import CONFIG
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.processing.dme_recipe import DmeRecipe


class DmeSparkSqlRecipe(DmeRecipe):

    def __init__(self, output, input_views):
        super().__init__(output)
        self.input_views = input_views
        self.spark = SparkSession.builder.appName('PySparkApp').getOrCreate()

        self.spark.sparkContext._jsc.hadoopConfiguration().set('mapred.output.committer.class',
                                                               'org.apache.hadoop.mapred.FileOutputCommitter')
        self.spark.sparkContext._jsc.hadoopConfiguration().setBoolean('fs.s3a.sse.enabled', True)
        self.spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.server-side-encryption-algorithm', 'SSE-KMS')
        self.spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.sse.kms.keyId', CONFIG['output_kms_key'])
        self.logger = CloudWatchLogger.get_logger()

    def register_view(self, s3_path, view_name):
        self.logger.info(f'Register spark view: {s3_path} {view_name}')
        if s3_path.endswith('parquet'):
            df = self.spark.read.parquet(s3_path, header=True, inferSchema=True)
        else:
            df = self.spark.read.csv(s3_path, header=True, inferSchema=True)
        df.createOrReplaceTempView(view_name)

    def process(self, template, csv_name=None):
        try:
            t = Template(template)
            sql = t.substitute(**self.mapping)
            self.logger.info(sql)
            for i in self.input_views:
                self.register_view(self.input_views[i], i)

            result = self.spark.sql(sql)
            self.logger.info(f'Write parquet file {self.output}')
            result.write.mode('overwrite').parquet(self.output)

            self.spark.stop()
        except Exception as e:
            self.logger.error(e)
            error_event(self.ap_data_sector, self.analysis_year, self.pipeline_runid, str(e))
            raise e
