# import packages
import os
import json
import boto3
import argparse

from libs.event_bridge.event import error_event, create_event
from libs.performance_lib import data_ingestion_library
from libs.denodo.denodo_connection import DenodoConnection
from libs.snowflake.snowflake_connection import SnowflakeConnection
from libs.performance_lib.load_ingested_data import load_ingested_data
from libs.config.config_vars import CONFIG, S3_DATA_PREFIX

bucket = CONFIG['bucket']  # Replace with your s3 bucket name
s3 = boto3.client('s3')


def data_ingestion(ap_data_sector,
                   analysis_type,
                   input_years,
                   args,
                   write_output=1,
                   get_parents=0):
    # setup out dir if writing to s3
    if write_output == 1:
        out_dir = args.output_load_data_ingestion_folder
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
    else:
        out_dir = None

    # files are saved within ingestion functions
    if ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
        data_ingestion_library.run_corn_brazil_safrinha_ingestion(ap_data_sector,
                                                                  analysis_type,
                                                                  input_years=input_years,
                                                                  write_outputs=write_output,
                                                                  out_dir=out_dir,
                                                                  get_parents=get_parents
                                                                  )
    elif ap_data_sector == 'CORN_BRAZIL_SUMMER':
        data_ingestion_library.run_corn_brazil_summer_ingestion(ap_data_sector,
                                                                analysis_type,
                                                                input_years=input_years,
                                                                write_outputs=write_output,
                                                                out_dir=out_dir,
                                                                get_parents=get_parents
                                                                )
    elif ap_data_sector == 'CORN_LAS_SUMMER':
        data_ingestion_library.run_corn_las_ingestion(ap_data_sector,
                                                      analysis_type,
                                                      input_years=input_years,
                                                      write_outputs=write_output,
                                                      out_dir=out_dir,
                                                      get_parents=get_parents
                                                      )

    elif ap_data_sector == 'CORN_NA_SUMMER':
        data_ingestion_library.run_corn_na_ingestion(ap_data_sector,
                                                      analysis_type,
                                                      input_years=input_years,
                                                      write_outputs=write_output,
                                                      out_dir=out_dir,
                                                     get_parents=get_parents
                                                      )

    elif ap_data_sector == 'SOY_BRAZIL_SUMMER':
        data_ingestion_library.run_soy_brazil_ingestion(ap_data_sector,
                                                        analysis_type,
                                                        input_years=input_years,
                                                        write_outputs=write_output,
                                                        out_dir=out_dir,
                                                        get_parents=get_parents
                                                        )

    elif ap_data_sector == 'SOY_LAS_SUMMER':
        data_ingestion_library.run_soy_las_ingestion(ap_data_sector,
                                                     analysis_type,
                                                     input_years=input_years,
                                                     write_outputs=write_output,
                                                     out_dir=out_dir,
                                                     get_parents=get_parents
                                                     )

    elif ap_data_sector == 'SOY_NA_SUMMER':
        data_ingestion_library.run_soy_na_ingestion(ap_data_sector,
                                                     analysis_type,
                                                     input_years=input_years,
                                                     write_outputs=write_output,
                                                     out_dir=out_dir,
                                                    get_parents=get_parents
                                                     )
    elif ap_data_sector == 'CORNGRAIN_EAME_SUMMER':
        data_ingestion_library.run_corngrain_eame_ingestion(ap_data_sector,
                                                    analysis_type,
                                                    input_years=input_years,
                                                    write_outputs=write_output,
                                                    out_dir=out_dir,
                                                    get_parents=get_parents
                                                    )
    elif ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
        data_ingestion_library.run_cornsilage_eame_ingestion(ap_data_sector,
                                                            analysis_type,
                                                            input_years=input_years,
                                                            write_outputs=write_output,
                                                            out_dir=out_dir,
                                                             get_parents=get_parents
                                                            )
    elif ap_data_sector == "SUNFLOWER_EAME_SUMMER":
        data_ingestion_library.run_sunflower_eame_ingestion(ap_data_sector,
                                                             analysis_type,
                                                             input_years=input_years,
                                                             write_outputs=write_output,
                                                             out_dir=out_dir,
                                                            get_parents=get_parents
                                                             )
    elif ap_data_sector == 'CORN_CHINA_SPRING':
        data_ingestion_library.run_corn_china_spring_ingestion(ap_data_sector,
                                                                  analysis_type,
                                                                  input_years=input_years,
                                                                  write_outputs=write_output,
                                                                  out_dir=out_dir,
                                                               get_parents=get_parents
                                                                  )
    elif ap_data_sector == 'CORN_CHINA_SUMMER':
        data_ingestion_library.run_corn_china_summer_ingestion(ap_data_sector,
                                                                analysis_type,
                                                                input_years=input_years,
                                                                write_outputs=write_output,
                                                                out_dir=out_dir,
                                                               get_parents=get_parents
                                                                )
    elif 'CORN_PHILIPPINES_' in ap_data_sector:
        data_ingestion_library.run_corn_philippines_all_ingestion(ap_data_sector,
                                                                analysis_type,
                                                                input_years=input_years,
                                                                write_outputs=write_output,
                                                                out_dir=out_dir,
                                                               get_parents=get_parents
                                                                )
    elif 'CORN_THAILAND_' in ap_data_sector:
        data_ingestion_library.run_corn_thailand_all_ingestion(ap_data_sector,
                                                                analysis_type,
                                                                input_years=input_years,
                                                                write_outputs=write_output,
                                                                out_dir=out_dir,
                                                               get_parents=get_parents
                                                                )
    elif 'CORN_INDONESIA_' in ap_data_sector:
        data_ingestion_library.run_corn_indonesia_all_ingestion(ap_data_sector,
                                                                analysis_type,
                                                                input_years=input_years,
                                                                write_outputs=write_output,
                                                                out_dir=out_dir,
                                                               get_parents=get_parents
                                                                )
    elif 'CORN_PAKISTAN_' in ap_data_sector:
        data_ingestion_library.run_corn_pakistan_all_ingestion(ap_data_sector,
                                                                analysis_type,
                                                                input_years=input_years,
                                                                write_outputs=write_output,
                                                                out_dir=out_dir,
                                                               get_parents=get_parents
                                                                )
    elif 'CORN_BANGLADESH_' in ap_data_sector:
        data_ingestion_library.run_corn_bangladesh_ingestion(ap_data_sector,
                                                                analysis_type,
                                                                input_years=input_years,
                                                                write_outputs=write_output,
                                                                out_dir=out_dir,
                                                               get_parents=get_parents
                                                                )
    elif 'CORN_INDIA_' in ap_data_sector:
        data_ingestion_library.run_corn_india_ingestion(ap_data_sector,
                                                                analysis_type,
                                                                input_years=input_years,
                                                                write_outputs=write_output,
                                                                out_dir=out_dir,
                                                               get_parents=get_parents
                                                                )
    elif 'CORN_VIETNAM_' in ap_data_sector:
        data_ingestion_library.run_corn_vietnam_ingestion(ap_data_sector,
                                                        analysis_type,
                                                        input_years=input_years,
                                                        write_outputs=write_output,
                                                        out_dir=out_dir,
                                                        get_parents=get_parents
                                                        )

    else:
        print(ap_data_sector, 'CORN_INDIA_' in ap_data_sector)


def chk_data_for_ingestion(ap_data_sector,
                           input_years,
                           is_inferring,
                           write_output=1):
    if is_inferring == 0:
        print('Proceed to training ingestion.')
        return True
    else:
        for analysis_year in input_years:
            lcl_folder_path = '/opt/ml/processing/input/last_update/'
            if not os.path.exists(lcl_folder_path):
                os.makedirs(lcl_folder_path)
            lcl_fpath = os.path.join(lcl_folder_path, 'last_update.json')
            try:
                with open(lcl_fpath, 'r') as f:
                    data = json.load(f)

                    row_count = int(data.get('row_count'))
                    print('row_count = ', row_count)

                    with SnowflakeConnection() as dc:
                        new_data = dc.get_data(
                            """
                            SELECT count(*) as "row_count"
                            FROM rv_trial_pheno_analytic_dataset rvt
                            WHERE ap_data_sector = {0}
                                AND trait_measure_code in ('YGSMN','YSDMN')
                                AND EXTRACT(YEAR FROM pr_last_chg_date) = {1}
                                AND rvt.year = {1}
                            """.format("'" + ap_data_sector + "'", analysis_year)
                        )

                        analysis_rows = new_data["row_count"].iloc[0]

                    if analysis_rows == 0:#analysis_rows <= row_count or row_count == 0:
                        print('No new data found for:', analysis_year)
                        return False

                    else:
                        print('New data found for: ', analysis_year)
                        print('New row_count = ', analysis_rows)

                        out_dir_json = '/opt/ml/processing/data/data_check/last_update/'
                        if not os.path.exists(out_dir_json):
                            os.makedirs(out_dir_json)
                        lcl_fpath_json = os.path.join(out_dir_json, 'last_update.json')

                        new_data_dict = {"row_count": str(new_data["row_count"].iloc[0])}

                        if write_output == 1:
                            with open(lcl_fpath_json, 'w') as a:
                                json.dump(new_data_dict, a)

                        print('Json updated. Proceed to inference ingestion.')
                        return True, row_count

            except Exception as e:
                print('Error:', e)


def find_prefix(prefixes, target):
    for prefix in prefixes:
        if target in prefix['Prefix']:
            return True
        if 'CommonPrefixes' in prefix:
            result = find_prefix(prefix['CommonPrefixes'], target)
            if result:
                return True
    return False


def folder_check_adv_mdl(ap_data_sector,
                 analysis_year,
                 folder_name='adv_mdls',
                 write_output=1):
    # check if folder exists in s3
    s3_loc = f'{S3_DATA_PREFIX}/{ap_data_sector}/train/{analysis_year}/' # ends with a /
    s3_folder = f'{s3_loc}{folder_name}/'

    # check if any obj were found
    response = s3.list_objects(Bucket=bucket, Prefix=s3_loc, Delimiter='/')
    if not find_prefix(response.get('CommonPrefixes', []), s3_folder):
        return False
    else:
        return True


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_data_ingestion_folder', type=str,
                        help='s3 input data ingestion folder', required=True)
    parser.add_argument('--output_load_data_ingestion_folder', type=str,
                        help='local output folder', required=True)
    args = parser.parse_args()

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        input_years = data['input_years']
        analysis_type = data['analysis_type']
        pipeline_runid = data['target_pipeline_runid']
        is_inferring = int(data['is_inferring'])

    try:
        chk_data_for_ingestion_flag = chk_data_for_ingestion(
            ap_data_sector=ap_data_sector,
            input_years=input_years,
            is_inferring=is_inferring
        )

        if not chk_data_for_ingestion_flag:
            # send email saying no new data, did not run.
            # do nothing
            x=1
            """
            create_event(
                event_bus=CONFIG.get("event_bus"),
                ap_data_sector=ap_data_sector,
                analysis_year=input_years,
                target_pipeline_runid=pipeline_runid,
                analysis_run_group='performance',
                stages=None,
                status="ERROR",
                message="no new data found for {}, not running ingestion and inference".format(ap_data_sector)
            )
            """
        else:
            get_parents = 0
            if ap_data_sector == 'CORN_NA_SUMMER' or \
                    ap_data_sector == "CORNGRAIN_EAME_SUMMER" or \
                    ap_data_sector == "CORNSILAGE_EAME_SUMMER" or \
                    ap_data_sector == "SUNFLOWER_EAME_SUMMER" or \
                    ap_data_sector == "CORN_CHINA_SPRING" or \
                    ap_data_sector == "CORN_CHINA_SUMMER" or \
                    ap_data_sector == "CORN_BRAZIL_SUMMER" or \
                    ap_data_sector == "CORN_BRAZIL_SAFRINHA":
                get_parents = 1

            data_ingestion(
                ap_data_sector=ap_data_sector,
                analysis_type=analysis_type,
                input_years=input_years,
                args=args,
                get_parents=get_parents
            )

            marker_traits = load_ingested_data(
                ap_data_sector=ap_data_sector,
                input_years=input_years,
                get_parents=get_parents,
                pipeline_runid=pipeline_runid,
                is_infer=is_inferring,
                args=args
            )

            # kick-off inference pipeline if requested
            if is_inferring==1:
                # check for adv_mdls folder
                folder_flag = folder_check_adv_mdl(ap_data_sector=ap_data_sector, analysis_year=input_years[-1])
                if folder_flag: # kick-off inference pipeline
                    sagemaker_client = boto3.client('sagemaker', region_name='us-east-1')
                    pipeline_name = CONFIG['performance_pipeline_arn']
                    forward_model_year = str(input_years[-1]) # input years is a list, should be a string for infer pipe

                    execution_name = f'{ap_data_sector}-{forward_model_year}-{analysis_type}'.replace('_', '-')
                    pipeline_parameters = {
                        'ap_data_sector': ap_data_sector,
                        'forward_model_year': forward_model_year,
                        'material_type': 'entry',
                        'target_pipeline_runid': pipeline_runid,
                        'force_refresh': 'False'
                    }
                    response = sagemaker_client.start_pipeline_execution(
                        PipelineExecutionDescription=f'Sagemaker Performance pipeline',
                        PipelineExecutionDisplayName=execution_name,
                        PipelineName=pipeline_name,
                        PipelineParameters=[{'Name': k, 'Value': v} for k, v in pipeline_parameters.items()])
                else:
                    print("no model found, can't run inference")

    except Exception as e:
        error_event(ap_data_sector, input_years, pipeline_runid, str(e))
        raise e


if __name__ == '__main__':
    main()
