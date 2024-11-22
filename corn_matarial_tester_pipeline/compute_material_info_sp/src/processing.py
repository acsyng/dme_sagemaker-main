"""
This recipe gets geno sample list via a sql query

"""

import json
import os

import pandas as pd

from libs.cmt_lib import cmt_sql_recipes



def compute_material_info_sp(crop_guid='B79D32C9-7850-41D0-BE44-894EC95AB285',
                             write_outputs=1):
    # crop guid is corn by default
    # call sql query
    df = cmt_sql_recipes.get_material_info_sp(crop_guid=crop_guid) # default is corn

    check_df = cmt_sql_recipes.get_check_info()
    df = df.merge(check_df, left_on='material_guid', right_on = 'planted_material_guid', how = 'left')
    check_df = []

    tops_check_df = cmt_sql_recipes.get_check_info_tops()
    df = df.merge(tops_check_df, on = 'be_bid', how = 'left')
    df['cpi'] = df['cpi'].combine_first(df['cpi_tops'])
    tops_check_df = []

    ga_df = cmt_sql_recipes.get_genetic_affiliation()
    df = df.merge(ga_df, on='material_guid', how = 'left')
    ga_df = []

    line_df = cmt_sql_recipes.get_line_info()
    df = df.merge(line_df, on='line_guid', how = 'left')
    df['region_lid'] = df['region_lid_line'].combine_first(df['region_lid'])
    line_df = []
    df = df.drop(columns = ['planted_material_guid', 'region_lid_line', 'cpi_tops'])
    # write output
    if write_outputs == 1:
        out_dir = '/opt/ml/processing/data/'
        out_fname = 'material_info_sp.parquet'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        df.to_parquet(os.path.join(out_dir, out_fname), index=False)


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)

        # currently removed, this could allow us to run cmt for different crops, but it's not needed so not implemented
        #crop_guid = data['crop_guid']

        compute_material_info_sp()


if __name__ == '__main__':
    main()
